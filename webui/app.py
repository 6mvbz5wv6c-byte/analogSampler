#!/usr/bin/env python3
import asyncio
import contextlib
import os
import socket
import struct
from collections import deque

import numpy as np
from aiohttp import web

# UDP config
UDP_LISTEN_PORT = int(os.getenv("UDP_LISTEN_PORT", "9000"))
UDP_HEADER_FMT = "<2sBBIQQfI"  # magic, version, codec, frame_index, start_us, end_us, sr, n
UDP_HEADER_SIZE = struct.calcsize(UDP_HEADER_FMT)

# Ring buffer for oscilloscope view
class RingBuffer:
    """Ring buffer constrained by duration instead of sample count."""

    def __init__(self, max_duration_s: float):
        self.max_duration_s = float(max_duration_s)
        self.t_abs = deque()
        self.geo = deque()

    def _prune_old(self):
        if not self.t_abs:
            return

        newest = self.t_abs[-1]
        cutoff = newest - self.max_duration_s
        while self.t_abs and self.t_abs[0] < cutoff:
            self.t_abs.popleft()
            self.geo.popleft()

    def add_frame(self, t_abs, geo):
        self.t_abs.extend(t_abs)
        self.geo.extend(geo)
        self._prune_old()

    def snapshot(self):
        if not self.t_abs:
            return np.array([]), np.array([])
        t = np.array(self.t_abs, dtype=np.float64)
        t0 = t[0]
        t_rel = t - t0
        geo = np.array(self.geo, dtype=np.float32)
        return t_rel, geo


buf = RingBuffer(max_duration_s=45.0)


async def udp_receiver():
    """
    Receives UDP packets from sampler module and fills ring buffer.
    """
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_LISTEN_PORT))
    sock.setblocking(True)

    while True:
        # recvfrom is blocking; run it in executor so event loop stays responsive
        data, addr = await loop.run_in_executor(None, sock.recvfrom, 65535)
        if len(data) < UDP_HEADER_SIZE:
            continue

        header = data[:UDP_HEADER_SIZE]
        payload = data[UDP_HEADER_SIZE:]

        magic, version, codec, frame_index, start_us, end_us, sr, n = struct.unpack(
            UDP_HEADER_FMT, header
        )

        if magic != b"PG":
            continue
        if codec != 0:
            continue
        if len(payload) != n * 4:
            continue

        geo = np.frombuffer(payload, dtype="<i4").astype(np.float32)
        t_abs = np.linspace(start_us, end_us, n, dtype=np.float64) / 1_000_000.0
        buf.add_frame(t_abs, geo)


async def ws_handler(request):
    """
    WebSocket endpoint that periodically pushes latest oscilloscope window.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        while True:
            await asyncio.sleep(0.1)  # 10 Hz update
            t, geo = buf.snapshot()
            if t.size == 0:
                continue

            # Downsample for UI (max 1000 points)
            step = max(1, len(t) // 1000)
            t_ds = t[::step]
            g_ds = geo[::step]

            await ws.send_json({
                "t": t_ds.tolist(),
                "geo": g_ds.tolist()
            })
    except asyncio.CancelledError:
        pass
    finally:
        await ws.close()

    return ws


async def index_handler(request):
    # Serve the index.html
    return web.FileResponse(path="static/index.html")


async def init_app():
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", ws_handler)
    # If you later add more assets (JS/CSS), you can do:
    # app.router.add_static("/static", path="static")
    return app


async def main():
    app = await init_app()
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()

    # Start UDP receiver
    udp_task = asyncio.create_task(udp_receiver())

    # Keep running until killed
    try:
        await asyncio.Future()
    finally:
        udp_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await udp_task


if __name__ == "__main__":
    import contextlib
    asyncio.run(main())
