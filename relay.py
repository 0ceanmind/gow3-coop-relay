#!/usr/bin/env python3
"""God of War III co-op relay: rooms of up to 4 players found by a short join code.

Protocol (one WebSocket per player):
  -> {"t":"host","name":...}            <- {"t":"welcome","code":"K7QX2M","slot":0}
  -> {"t":"join","code":...,"name":...} <- {"t":"welcome","code":...,"slot":n} | {"t":"error","msg":...}
  <- {"t":"roster","players":[{"slot":0,"name":...},...]}   whenever the room changes
  <- {"t":"closed"}                                          the host left
  binary messages are forwarded to every other player, prefixed with the sender's slot byte.
Plain HTTP GET answers a health page (Render)."""
import asyncio
import json
import os
import secrets
import time

from wsock import Closed, accept, read_http_head

MAX_PLAYERS = 4
ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
rooms = {}


class Room:
    def __init__(self, code):
        self.code, self.players, self.created = code, {}, time.time()

    def roster(self):
        return json.dumps({'t': 'roster', 'players': [{'slot': s, 'name': p['name']} for s, p in sorted(self.players.items())]})

    async def broadcast(self, message, skip=None):
        for slot, p in list(self.players.items()):
            if slot != skip:
                try:
                    await p['ws'].send(message)
                except Closed:
                    pass


def new_code():
    while True:
        code = ''.join(secrets.choice(ALPHABET) for _ in range(6))
        if code not in rooms:
            return code


async def session(ws):
    room, slot = None, None
    try:
        hello = json.loads(await asyncio.wait_for(ws.recv(), 30))
        name = str(hello.get('name', 'Spartan'))[:24]
        if hello.get('t') == 'host':
            code = new_code()
            room = rooms[code] = Room(code)
            slot = 0
        elif hello.get('t') == 'join':
            room = rooms.get(str(hello.get('code', '')).strip().upper())
            if not room:
                await ws.send(json.dumps({'t': 'error', 'msg': 'No game with that code.'})); return
            free = [s for s in range(MAX_PLAYERS) if s not in room.players]
            if not free:
                room = None
                await ws.send(json.dumps({'t': 'error', 'msg': 'That game is full (4 players).'})); return
            slot = free[0]
        else:
            return
        room.players[slot] = {'ws': ws, 'name': name}
        await ws.send(json.dumps({'t': 'welcome', 'code': room.code, 'slot': slot}))
        await room.broadcast(room.roster())
        print(f'room {room.code}: {name} is player {slot + 1} ({len(room.players)} in room)', flush=True)
        while True:
            msg = await ws.recv()
            if isinstance(msg, bytes):
                await room.broadcast(bytes([slot]) + msg, skip=slot)
            else:
                data = json.loads(msg)
                if data.get('t') == 'chat':
                    await room.broadcast(json.dumps({'t': 'chat', 'slot': slot, 'text': str(data.get('text', ''))[:200]}))
    except (Closed, asyncio.TimeoutError, ValueError, ConnectionError):
        pass
    finally:
        if room and slot is not None and room.players.get(slot, {}).get('ws') is ws:
            del room.players[slot]
            if slot == 0:
                await room.broadcast(json.dumps({'t': 'closed'}))
                for p in list(room.players.values()):
                    await p['ws'].close()
                rooms.pop(room.code, None)
                print(f'room {room.code}: host left, closed', flush=True)
            else:
                await room.broadcast(room.roster())
                print(f'room {room.code}: player {slot + 1} left', flush=True)
        await ws.close()


async def handle(reader, writer):
    try:
        request, headers = await asyncio.wait_for(read_http_head(reader), 30)
    except Exception:
        writer.close(); return
    if headers.get('upgrade', '').lower() == 'websocket':
        await session(await accept(reader, writer, request, headers))
        return
    body = json.dumps({'service': 'gow3-coop-relay', 'rooms': len(rooms),
                       'players': sum(len(r.players) for r in rooms.values())}).encode()
    writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n' % len(body) + body)
    await writer.drain()
    writer.close()


async def main(port=None):
    port = int(port or os.environ.get('PORT', 10000))
    server = await asyncio.start_server(handle, '0.0.0.0', port)
    print(f'gow3 co-op relay on port {port}', flush=True)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
