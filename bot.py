#!/usr/bin/env python3
"""bot.py CODE [--relay URL] [--name N] [--seconds S]: a test player that joins a co-op game and
sends a scripted controller (runs in a circle, attacks, jumps) through the real session protocol.
It has no game of its own: it reports the host's zones so it is shown in the same level, and its
'[bot]' name tells the games to drive it by its controller only."""
import argparse
import asyncio
import json
import math
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wsock
from coop_agent import DEFAULT_RELAY, STATE

CROSS, SQUARE, TRIANGLE = 0x4000, 0x8000, 0x1000


async def run(a):
    url = a.relay if ':' not in a.code or a.code.startswith('ws') else f'ws://{a.code}/'
    code = '*' if ':' in a.code and not a.code.startswith('ws') else a.code.upper()
    ws = await wsock.connect(url, timeout=75)
    await ws.send(json.dumps({'t': 'join', 'code': code, 'name': f'[bot] {a.name}'}))
    welcome = json.loads(await ws.recv())
    if welcome.get('t') != 'welcome':
        print('bot:', welcome); return 1
    slot = welcome['slot']
    print(f'bot {a.name}: player {slot + 1} in {welcome["code"]}', flush=True)
    zones = [0] * 6
    start = time.time()

    async def listen():
        nonlocal zones
        while True:
            try:
                m = await ws.recv()
            except wsock.Closed:
                return
            if isinstance(m, bytes) and len(m) == 1 + STATE.size and m[0] == 0:
                zones = list(STATE.unpack(m[1:])[-6:])
    task = asyncio.ensure_future(listen())
    frame = 0
    while time.time() - start < a.seconds:
        t = time.time() - start
        ang = t * 0.9 + slot * 2.1
        lx, ly = (128, 128) if a.idle else (int(128 + 127 * math.cos(ang)), int(128 + 127 * math.sin(ang)))
        buttons = 0
        phase = t % 6 if not (a.calm or a.idle) else -1
        if 2.0 < phase < 2.15 or 2.5 < phase < 2.65:
            buttons |= SQUARE
        if 4.0 < phase < 4.2:
            buttons |= CROSS
        if 5.0 < phase < 5.4:
            buttons |= TRIANGLE
        frame += 1
        pkt = STATE.pack(b'G3CO', 1, 1, 1, slot % 6, frame, buttons, lx, ly, 128, 128, 0, 0,
                         0, 0, 0, 0, 0, 1, 100.0, *zones)
        try:
            await ws.send(pkt)
        except wsock.Closed:
            print(f'bot {a.name}: the session ended', flush=True)
            break
        await asyncio.sleep(1 / 30)
    task.cancel()
    await ws.close()
    return 0


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('code')
    p.add_argument('--relay', default=DEFAULT_RELAY)
    p.add_argument('--name', default='Bot')
    p.add_argument('--seconds', type=float, default=60)
    p.add_argument('--idle', action='store_true', help='stand still')
    p.add_argument('--calm', action='store_true', help='walk only, no attacks or jumps')
    sys.exit(asyncio.run(run(p.parse_args())))
