#!/usr/bin/env python3
import os, json, time, asyncio, signal, traceback, base64, random
from multiprocessing import Process, Queue

from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

from sampler_worker import sampler_process_main  # worker process

OUTPUT_NAME            = "telemetry"
DEVICE_ID              = os.getenv("IOTEDGE_DEVICEID", "UNKNOWN_DEVICE")
GPS_LAT_PLACEHOLDER    = 36.13706
GPS_LONG_PLACEHOLDER   = -94.14972
BATTERY_PLACEHOLDER    = 7.2
TWIN_FLAG_KEY          = "analog_sampling_enabled"  # desired property key


async def twin_control(client: IoTHubModuleClient, streaming_enabled: asyncio.Event):
    """
    Control streaming based on module twin.

    Desired property: { "analog_sampling_enabled": true/false }

    When false: publisher will drop frames (no cloud messages).
    When true: publisher will send frames as normal.
    """
    try:
        twin = await client.get_twin()
        desired = twin.get("desired", twin.get("properties", {}).get("desired", {}))
    except Exception:
        traceback.print_exc()
        streaming_enabled.set()
        return

    enabled = bool(desired.get(TWIN_FLAG_KEY, True))
    if enabled:
        streaming_enabled.set()
    else:
        streaming_enabled.clear()

    try:
        await client.patch_twin_reported_properties({TWIN_FLAG_KEY: enabled})
    except Exception:
        traceback.print_exc()

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
                streaming_enabled.set()
                print("[twin] streaming_enabled = True")
            else:
                streaming_enabled.clear()
                print("[twin] streaming_enabled = False")

            try:
                await client.patch_twin_reported_properties({TWIN_FLAG_KEY: enabled})
            except Exception:
                traceback.print_exc()


async def publisher_from_mpqueue(
    client: IoTHubModuleClient,
    mp_queue: Queue,
    streaming_enabled: asyncio.Event,
):
    """
    Async publisher that consumes frames from multiprocessing.Queue and sends to IoT Edge.

    Twin flag 'streaming_enabled' controls whether we actually send or just drop frames.
    """
    while True:
        # Block in a thread so the asyncio event loop isn't blocked on mp_queue.get()
        start_us, end_us, samples_bytes = await asyncio.to_thread(mp_queue.get)

        if not streaming_enabled.is_set():
            # Drop frame when streaming disabled
            continue

        samples_b64 = base64.b64encode(samples_bytes).decode("ascii")

        payload = {
            "deviceId": DEVICE_ID,
            "startUs": int(start_us),
            "endUs": int(end_us),
            "samplesB64": samples_b64,
        }

        body = json.dumps(payload)

        msg = Message(body)
        msg.content_type = "application/json"
        msg.content_encoding = "utf-8"

        msg.custom_properties["Enc"] = "int32-b64-v1"
        msg.custom_properties["GPS_Latitude"] = str(GPS_LAT_PLACEHOLDER)
        msg.custom_properties["GPS_Longitude"] = str(GPS_LONG_PLACEHOLDER)
        msg.custom_properties["Battery_Voltage"] = (
            f"{random.uniform(BATTERY_PLACEHOLDER - 0.1, BATTERY_PLACEHOLDER + 0.1):.2f}"
        )

        t0 = time.time()
        try:
            await client.send_message_to_output(msg, OUTPUT_NAME)
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(0.1)
        else:
            dt = time.time() - t0
            # print(f"[publisher] sent frame starting {start_us}, send took {dt:.3f}s")


async def main():
    # Inter-process queue and sampler process
    mp_queue = Queue(maxsize=16)

    sampler_proc = Process(target=sampler_process_main, args=(mp_queue,), daemon=True)
    sampler_proc.start()
    print("[main] sampler process started")

    # IoT Hub module client
    try:
        client = IoTHubModuleClient.create_from_edge_environment()
        await client.connect()
    except Exception:
        traceback.print_exc()
        return

    # Twin-controlled streaming flag
    streaming_enabled = asyncio.Event()
    streaming_enabled.set()

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
            publisher_from_mpqueue(client, mp_queue, streaming_enabled),
            twin_control(client, streaming_enabled),
            stop_evt.wait(),
        )
    finally:
        try:
            await client.shutdown()
        except Exception:
            traceback.print_exc()
        sampler_proc.terminate()
        sampler_proc.join(timeout=2.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
