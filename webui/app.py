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
    def __init__(self, max_samples=10_000):
        self.max_samples = max_samples
        self.t_abs = deque(maxlen=max_samples)
        self.geo = deque(maxlen=max_samples)
        self.seq_start = 0
        self.seq_next = 0

    def add_frame(self, t_abs, geo):
        len_before = len(self.t_abs)
        len_new = len(t_abs)
        dropped = max(0, len_before + len_new - self.max_samples)

        self.t_abs.extend(t_abs)
        self.geo.extend(geo)

        self.seq_next += len_new
        self.seq_start += dropped

    def snapshot(self):
        if not self.t_abs:
            return np.array([]), np.array([])
        t = np.array(self.t_abs, dtype=np.float64)
        t0 = t[0]
        t_rel = t - t0
        geo = np.array(self.geo, dtype=np.float32)
        return t_rel, geo

    def get_since(self, seq):
        if not self.t_abs:
            return np.array([]), np.array([]), self.seq_start

        offset = max(0, seq - self.seq_start)
        if offset >= len(self.t_abs):
            return np.array([]), np.array([]), self.seq_start + len(self.t_abs)

        t = np.array(list(self.t_abs)[offset:], dtype=np.float64)
        geo = np.array(list(self.geo)[offset:], dtype=np.float32)
        next_seq = self.seq_start + len(self.t_abs)
        return t, geo, next_seq


buf = RingBuffer(max_samples=20_000)


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

    last_seq = buf.seq_start

    try:
        while True:
            await asyncio.sleep(0.1)  # 10 Hz update
            t, geo, next_seq = buf.get_since(last_seq)
            if t.size == 0:
                continue

            last_seq = next_seq

            # Downsample only the new slice (max ~1000 points per burst)
            step = max(1, len(t) // 1000)
            t_ds = t[::step]
            g_ds = geo[::step]

            await ws.send_json({
                "t": t_ds.tolist(),
                "geo": g_ds.tolist(),
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
    app.router.add_static("/static", path="static")
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
    asyncio.run(main())
