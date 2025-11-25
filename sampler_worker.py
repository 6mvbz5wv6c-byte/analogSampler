import os
import socket
import struct
import traceback
import numpy as np
from datetime import datetime, timezone
import ADS1256

SAMPLE_RATE_HZ = int(os.getenv("SAMPLE_RATE_HZ", "1000"))
FRAME_SAMPLES  = int(os.getenv("FRAME_SAMPLES", "1000"))
DIFF_CHANNEL   = int(os.getenv("DIFF_CHANNEL", "0"))

# Web UI UDP config
UDP_TARGET_HOST = os.getenv("WEBUI_HOST", "webui")
UDP_TARGET_PORT = int(os.getenv("WEBUI_PORT", "9000"))

UDP_HEADER_FMT = "<2sBBIQQfI"  # magic, version, codec, frame_index, start_us, end_us, sr, n
UDP_HEADER_SIZE = struct.calcsize(UDP_HEADER_FMT)

udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_target = (UDP_TARGET_HOST, UDP_TARGET_PORT)
udp_frame_counter = 0


def utc_us_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def send_udp_frame(buf: np.ndarray, start_us: int, end_us: int):
    """
    Fire-and-forget UDP frame for local visualization.
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


def sampler_process_main(frame_queue):
    """
    Runs in its own process.
    Continuously samples ADS1256 in RDATAC continuous mode and pushes frames to frame_queue.

    Each item: (start_us: int, end_us: int, samples_bytes: bytes)
    """
    try:
        adc = ADS1256.ADS1256()
        rc = adc.ADS1256_init()
        print(f"[sampler_proc] ADS1256_init rc={rc}")
        if rc != 0:
            raise RuntimeError("ADS1256 init failed")

        # Differential mode
        adc.ADS1256_SetMode(1)

        # Configure and enter continuous conversion on the chosen diff channel
        adc.ADS1256_StartContinuousDiff(
            diff_channel=DIFF_CHANNEL,
            gain=ADS1256.ADS1256_GAIN_E["ADS1256_GAIN_4"],
            drate=ADS1256.ADS1256_DRATE_E["ADS1256_1000SPS"],
        )

    except Exception:
        traceback.print_exc()
        return

    # Nominal 1 kHz → 1 ms between samples
    Ts_us = int(1_000_000 / SAMPLE_RATE_HZ)
    buf = np.zeros(FRAME_SAMPLES, dtype=np.int32)

    from multiprocessing import queues
    Full = queues.Full

    # Establish a fixed, ideal time grid for the frames
    base_start_us = utc_us_now()
    frame_index = 0

    while True:
        # Idealized timestamps from the fixed time grid
        start_us = base_start_us + frame_index * FRAME_SAMPLES * Ts_us

        # Read exactly FRAME_SAMPLES conversions; each call waits on DRDY
        for i in range(FRAME_SAMPLES):
            buf[i] = adc.ADS1256_ReadContinuousSample()

        end_us = start_us + (FRAME_SAMPLES - 1) * Ts_us

        # UDP tap
        send_udp_frame(buf, start_us, end_us)

        # Convert to bytes for IoT Edge publisher
        samples_bytes = buf.astype("<i4", copy=False).tobytes()

        # Non-blocking enqueue with drop-oldest policy
        try:
            frame_queue.put_nowait((start_us, end_us, samples_bytes))
        except Full:
            try:
                _ = frame_queue.get_nowait()  # drop oldest
            except Exception:
                pass
            try:
                frame_queue.put_nowait((start_us, end_us, samples_bytes))
            except Full:
                # Still full: drop this frame
                pass

        frame_index += 1
        if frame_index % 10 == 0:
            try:
                qs = frame_queue.qsize()
            except NotImplementedError:
                qs = -1
            print(f"[sampler_proc] frame {frame_index}, qsize={qs}")
