#Stover - November 13, 2025
#Differential sampling with gain of 4

#!/usr/bin/env python3
import os, json, time, asyncio, signal, traceback, base64, random
from datetime import datetime, timezone
import socket
import struct

import numpy as np
import ADS1256
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

SAMPLE_RATE_HZ = int(os.getenv("SAMPLE_RATE_HZ", "1000"))
FRAME_SAMPLES  = int(os.getenv("FRAME_SAMPLES", "1000"))   # 1s of data at 1 kSPS
DIFF_CHANNEL   = int(os.getenv("DIFF_CHANNEL", "0"))
VREF           = float(os.getenv("VREF", "5.0"))
OUTPUT_NAME    = "telemetry"  # hard-coded to match IoT Edge route
DEVICE_ID      = os.getenv("IOTEDGE_DEVICEID", "UNKNOWN_DEVICE")
TWIN_FLAG_KEY  = "analog_sampling_enabled"
SINE_FREQ_HZ   = 22.0

#GPS_LAT_PLACEHOLDER = 36.13706
#GPS_LONG_PLACEHOLDER = -94.14972
#BATTERY_PLACEHOLDER = 7.2

# Web ui variables
UDP_TARGET_HOST = os.getenv("WEBUI_HOST", "webui")  # Docker hostname of web module
UDP_TARGET_PORT = int(os.getenv("WEBUI_PORT", "9000"))

UDP_HEADER_FMT = "<2sBBIQQfI"  # magic, version, codec, frame_index, start_us, end_us, sr, n
UDP_HEADER_SIZE = struct.calcsize(UDP_HEADER_FMT)

udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_target = (UDP_TARGET_HOST, UDP_TARGET_PORT)
udp_frame_counter = 0

def utc_us_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


async def publisher(client: IoTHubModuleClient, queue: asyncio.Queue):
    """
    Take frames from the queue and send them as JSON with Base64-encoded int32 samples.

    Added Nov 17
    A synthetic 22 Hz sine wave (1 kSPS) is included as Base64-encoded float32 values
    in the "dataB64" field to simulate a second sensor stream.

    Body JSON structure:

    {
        "deviceId": "...",
        "startUs":  ...,
        "endUs":    ...,
        "sampleRateHz": 1000,
        "frameSamples": 1000,
        "format":  "int32-le-base64-v1",
        "vref":    5.0,
        "samplesB64": "<base64 of int32 little-endian samples>",
        "format":  "int32-le-base64-v1",
        "dataB64": "<base64 of float32 little-endian sine samples>",
        "dataFormat": "float32-le-base64-v1"
    }
    """

    sine_phase_sample = 0  # running sample counter for sine generation

    while True:
        buf, start_us, end_us = await queue.get()

        try:
            # buf is np.int32; make sure it's little-endian and pack to bytes
            samples_bytes = buf.astype("<i4", copy=False).tobytes()
            samples_b64 = base64.b64encode(samples_bytes).decode("ascii")

            # Generate simulated sine wave at SINE_FREQ_HZ
            # ~~~~ for testing only ~~~~~
            sample_indices = sine_phase_sample + np.arange(len(buf))
            sine_wave = np.sin(2 * np.pi * SINE_FREQ_HZ * sample_indices / SAMPLE_RATE_HZ)
            sine_bytes = sine_wave.astype("<f4", copy=False).tobytes()
            sine_b64 = base64.b64encode(sine_bytes).decode("ascii")
            sine_phase_sample += len(buf)
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

            payload = {
                "deviceId": DEVICE_ID,
                "startUs": int(start_us),
                "endUs": int(end_us),
                "sampleRateHz": SAMPLE_RATE_HZ,
                "frameSamples": int(len(buf)),
                "vref": VREF,
                "samplesB64": samples_b64,
                "format": "int32-le-base64-v1",
                "dataB64": sine_b64,
                "dataFormat": "float32-le-base64-v1",
            }

            body = json.dumps(payload)
        except Exception:
            traceback.print_exc()
            queue.task_done()
            continue

        msg = Message(body)
        msg.content_type = "application/json"
        msg.content_encoding = "utf-8"

        # Optional: properties for routing/filtering
       # msg.custom_properties["GPS_Latitude"] = str(GPS_LAT_PLACEHOLDER)
       # msg.custom_properties["GPS_Longitude"] = str(GPS_LONG_PLACEHOLDER)
       # msg.custom_properties["Battery_Voltage"] = f"{random.uniform(BATTERY_PLACEHOLDER - 0.1, BATTERY_PLACEHOLDER + 0.1):.2f}"

        try:
            await client.send_message_to_output(msg, OUTPUT_NAME)
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(0.1)
        finally:
            queue.task_done()


async def sampler_polling(adc: ADS1256.ADS1256, queue: asyncio.Queue, running_flag: asyncio.Event):
    """
    Capture raw ADS1256 counts into int32 buffers.
    Timing is derived from sample rate + frame index.
    """
    Ts_us = int(1_000_000 / SAMPLE_RATE_HZ)  # microseconds per sample

    # Ping-pong buffers: raw ADC counts as int32
    buf_a = np.zeros(FRAME_SAMPLES, dtype=np.int32)
    buf_b = np.zeros(FRAME_SAMPLES, dtype=np.int32)
    current_buf = buf_a
    alt_buf = buf_b

    # Establish base start time for frame 0
    base_start_us = utc_us_now()
    frame_index = 0

    while True:
        await running_flag.wait()

        # Compute timestamps purely from sample rate + frame index
        start_us = base_start_us + frame_index * FRAME_SAMPLES * Ts_us
        end_us   = start_us + (FRAME_SAMPLES - 1) * Ts_us

        # Capture FRAME_SAMPLES samples as fast as the ADC delivers them
        for i in range(FRAME_SAMPLES):
            try:
                raw = adc.ADS1256_GetChannalValue(DIFF_CHANNEL)
            except Exception:
                traceback.print_exc()
                raw = 0

            current_buf[i] = int(raw)

            # No time.sleep(): rely on ADS1256 DRATE timing

        # Hand a copy of the buffer to the publisher so it cannot be mutated
        # by the next capture loop while it is being Base64-encoded.
        try:
            frame_copy = current_buf.copy()
            await queue.put((frame_copy, start_us, end_us))
        except Exception:
            traceback.print_exc()

        # Send UDP tap for local web visualization (non-blocking best-effort)
        send_udp_frame(current_buf, start_us, end_us)
        
        current_buf, alt_buf = alt_buf, current_buf
        frame_index += 1


async def twin_control(client: IoTHubModuleClient, running_flag: asyncio.Event):
    # Initial desired state
    try:
        twin = await client.get_twin()
    except Exception:
        traceback.print_exc()
        running_flag.set()
        return

    desired = twin.get("desired", twin.get("properties", {}).get("desired", {}))
    enabled = bool(desired.get(TWIN_FLAG_KEY, True))

    if enabled:
        running_flag.set()
    else:
        running_flag.clear()

    try:
        await client.patch_twin_reported_properties({TWIN_FLAG_KEY: enabled})
    except Exception:
        traceback.print_exc()

    # Listen for desired property changes
    while True:
        try:
            patch = await client.receive_twin_desired_properties_patch()
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(1)
            continue

        if TWIN_FLAG_KEY in patch:
            enabled = bool(patch[TWIN_FLAG_KEY])
            if enabled:
                running_flag.set()
            else:
                running_flag.clear()

            try:
                await client.patch_twin_reported_properties({TWIN_FLAG_KEY: enabled})
            except Exception:
                traceback.print_exc()

def send_udp_frame(buf: np.ndarray, start_us: int, end_us: int):
    """
    Fire-and-forget UDP frame for local visualization.

    Frame format:
      header: <2sBBIQQfI
      payload: n * int32 little-endian samples
    """
    global udp_frame_counter

    frame_index = udp_frame_counter
    udp_frame_counter += 1

    sample_rate_hz = float(SAMPLE_RATE_HZ)
    frame_samples = int(len(buf))

    header = struct.pack(
        UDP_HEADER_FMT,
        b"PG",             # magic "Pigboy Geophone"
        1,                 # version
        0,                 # codec 0 = int32 LE
        frame_index,
        int(start_us),
        int(end_us),
        sample_rate_hz,
        frame_samples,
    )

    payload = buf.astype("<i4", copy=False).tobytes()
    packet = header + payload

    try:
        udp_sock.sendto(packet, udp_target)
    except OSError:
        # Visualization-only path; ignore failures
        pass


async def main():
    # ADC init
    try:
        adc = ADS1256.ADS1256()
    except Exception:
        traceback.print_exc()
        return

    try:
        rc = adc.ADS1256_init()
        print(f"ADS1256_init returned rc={rc}")
    except Exception:
        traceback.print_exc()
        return

    if rc != 0:
        raise RuntimeError("ADS1256 init failed")

    # sets reading to differential input mode between selected DIFF_CHANNEL
    ADS1256.ScanMode = 1

    # Configure data rate / gain if supported
    try:
        adc.ADS1256_ConfigADC(
            ADS1256.ADS1256_GAIN_E['ADS1256_GAIN_4'],
            ADS1256.ADS1256_DRATE_E['ADS1256_1000SPS']
        )
    except Exception:
        traceback.print_exc()

    # IoT Hub module client
    try:
        client = IoTHubModuleClient.create_from_edge_environment()
    except Exception:
        traceback.print_exc()
        return

    try:
        await client.connect()
    except Exception:
        traceback.print_exc()
        return

    running_flag = asyncio.Event()
    running_flag.set()  # start enabled

    q = asyncio.Queue(maxsize=4)
    stop_evt = asyncio.Event()

    def _handle_signal(signum, frame):
        stop_evt.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handle_signal)
        except Exception:
            traceback.print_exc()

    try:
        await asyncio.gather(
            sampler_polling(adc, q, running_flag),
            publisher(client, q),
            twin_control(client, running_flag),
            stop_evt.wait(),
        )
    except Exception:
        traceback.print_exc()
    finally:
        try:
            await client.shutdown()
        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
