#!/usr/bin/env python3
import os, json, time, asyncio, signal, traceback
from datetime import datetime, timezone
import numpy as np
import ADS1256
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

SAMPLE_RATE_HZ = int(os.getenv("SAMPLE_RATE_HZ", "1000"))
FRAME_SAMPLES  = int(os.getenv("FRAME_SAMPLES", "1000"))   # 1s of data at 1 kSPS
CHANNEL        = int(os.getenv("CHANNEL", "0"))
VREF           = float(os.getenv("VREF", "5.0"))
OUTPUT_NAME    = "telemetry"  # hard-coded to match IoT Edge route
DEVICE_ID      = os.getenv("IOTEDGE_DEVICEID", "UNKNOWN_DEVICE")
TWIN_FLAG_KEY  = "analog_sampling_enabled"


def utc_us_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


async def publisher(client: IoTHubModuleClient, queue: asyncio.Queue):
    while True:
        buf, start_us, end_us = await queue.get()

        payload = {
            "deviceId": DEVICE_ID,
            "startUs": int(start_us),
            "endUs": int(end_us),
            "samples": buf.tolist(),
        }

        try:
            body = json.dumps(payload)
        except Exception as e:
            # Serialization failure: drop frame, continue
            traceback.print_exc()
            queue.task_done()
            continue

        msg = Message(body)
        msg.content_type = "application/json"
        msg.content_encoding = "utf-8"

        try:
            await client.send_message_to_output(msg, OUTPUT_NAME)
        except Exception:
            # Network / IoT Edge issue – log and continue
            traceback.print_exc()
            await asyncio.sleep(0.1)
        finally:
            queue.task_done()


async def sampler_polling(adc: ADS1256.ADS1256, queue: asyncio.Queue, running_flag: asyncio.Event):
    scale = VREF / 0x7FFFFF
    Ts_us = int(1_000_000 / SAMPLE_RATE_HZ)  # microseconds per sample

    # Ping-pong buffers
    buf_a = np.zeros(FRAME_SAMPLES, dtype=np.float32)
    buf_b = np.zeros(FRAME_SAMPLES, dtype=np.float32)
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
                raw = adc.ADS1256_GetChannalValue(CHANNEL)
            except Exception:
                traceback.print_exc()
                raw = 0

            current_buf[i] = raw * scale

            # No time.sleep() here; assume the ADS1256 DRATE setting
            # controls the actual sampling period.

        # Hand buffer to publisher and swap
        try:
            await queue.put((current_buf, start_us, end_us))
        except Exception:
            traceback.print_exc()

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

    # Configure data rate / gain if supported
    try:
        adc.ADS1256_ConfigADC(
            ADS1256.ADS1256_GAIN_E['ADS1256_GAIN_1'],
            ADS1256.ADS1256_DRATE_E['ADS1256_1000SPS']
        )
    except Exception:
        # If not supported, ignore
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
