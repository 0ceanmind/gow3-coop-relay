#!/usr/bin/env python3
"""God of War III co-op agent: links one game (UDP on localhost) to a co-op session.

  coop_agent.py --game-port P host [--relay URL | --direct PORT] [--name NAME]
  coop_agent.py --game-port P join CODE [--relay URL]            (code from the host)
  coop_agent.py --game-port P join HOST:PORT                     (direct connection)

The session runs through the relay (join codes; works through any router) or directly, with
the host's agent serving the same protocol on a TCP port. --status FILE keeps a JSON status for
the launcher (state, code, players, message). Python standard library only."""
import argparse
import asyncio
import json
import os
import socket
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wsock

DEFAULT_RELAY = 'wss://gow3-coop-relay.onrender.com/'
MAX_PLAYERS = 4
STATE = struct.Struct('<4sBBBBI I6B 3f3ff 6I')        # game -> agent (74 bytes)
REMOTE = struct.Struct('<BBBB I6B 3f3ff 6I 16s')       # one other player, agent -> game
HEADER = struct.Struct('<4sBBBB')
KIND_STATE, KIND_REMOTES, KIND_NOTICE = 1, 2, 3


class Agent:
    def __init__(self, args):
        self.args = args
        self.slot = None
        self.code = None
        self.names = {}
        self.states = {}        # slot -> (time, raw state bytes)
        self.game_addr = None
        self.ws = None
        self.status = {'state': 'starting', 'code': None, 'players': [], 'message': ''}
        self.loop = None
        self.stats = {'from_game': 0, 'to_game': 0, 'from_net': 0, 'to_net': 0}

    # ---- status for the launcher ----
    def set_status(self, **kw):
        self.status.update(kw)
        if self.args.status:
            # The launcher reads this file at any time: a replace that meets an open reader on
            # Windows is skipped (the next update writes it again).
            tmp = self.args.status + '.tmp'
            try:
                with open(tmp, 'w') as f:
                    json.dump(self.status, f)
                os.replace(tmp, self.args.status)
            except OSError:
                pass
        if kw.get('message'):
            print('Co-op:', kw['message'], flush=True)

    def local_index(self, slot):
        return slot + 1 if slot < self.slot else slot

    # ---- game link ----
    def notice(self, text):
        if self.game_addr:
            self.udp.sendto(HEADER.pack(b'G3CO', 1, KIND_NOTICE, 0, 0) + text.encode()[:200] + b'\0', self.game_addr)

    def send_remotes(self):
        if not self.game_addr or self.slot is None:
            return
        now = time.time()
        empty = REMOTE.pack(0, 0, 0, 0, 0, 128, 128, 128, 128, 0, 0, *[0.0] * 7, *[0] * 6, b'')
        entries = [empty] * (MAX_PLAYERS - 1)
        for slot, (t, raw) in self.states.items():
            if slot == self.slot or slot not in self.names or now - t > 3:
                continue
            f = STATE.unpack(raw)
            # magic, version, kind, in_game, costume, frame, buttons, lx..r2 (6), pos(3), fwd(3), hp, zones(6)
            in_game, costume, flags = f[3], f[4], 1 if self.names[slot].startswith('[bot]') else 0
            pad, rest = f[6:13], f[13:]
            entries[self.local_index(slot) - 1] = REMOTE.pack(1, in_game, costume, flags, *pad, *rest,
                                                              self.names[slot].encode()[:15])
        self.udp.sendto(HEADER.pack(b'G3CO', 1, KIND_REMOTES, MAX_PLAYERS - 1, self.slot) + b''.join(entries), self.game_addr)
        self.stats['to_game'] += 1

    def on_game_packet(self, data, addr):
        if len(data) != STATE.size or data[:4] != b'G3CO':
            return
        self.game_addr = addr
        self.stats['from_game'] += 1
        if self.ws and self.slot is not None:
            asyncio.ensure_future(self.ws.send(data))
            self.stats['to_net'] += 1
        self.states[self.slot] = (time.time(), data)

    # ---- session ----
    def on_roster(self, players):
        before = dict(self.names)
        self.names = {p['slot']: p['name'] for p in players}
        for slot, name in self.names.items():
            if slot not in before and slot != self.slot:
                self.notice(f'{name} joined as player {slot + 1}')
        for slot, name in before.items():
            if slot not in self.names:
                self.states.pop(slot, None)
                self.notice(f'{name} left the game')
        self.set_status(players=[{'slot': s, 'name': n, 'you': s == self.slot} for s, n in sorted(self.names.items())])

    async def session(self, url, hello):
        self.set_status(state='connecting', message='Connecting...')
        for attempt in range(6):
            try:
                self.ws = await wsock.connect(url, timeout=75)
                break
            except (OSError, ConnectionError, asyncio.TimeoutError) as e:
                self.set_status(message=f'Waiting for the server ({e.__class__.__name__}), retrying...')
                await asyncio.sleep(3)
        else:
            self.set_status(state='error', message='Could not reach the co-op server.')
            return
        await self.ws.send(json.dumps(hello))
        while True:
            try:
                msg = await self.ws.recv()
            except wsock.Closed:
                self.set_status(state='error', message='Disconnected from the session.')
                self.notice('Co-op: disconnected')
                return
            if isinstance(msg, bytes):
                self.stats['from_net'] += 1
                if len(msg) == 1 + STATE.size:
                    self.states[msg[0]] = (time.time(), msg[1:])
                continue
            data = json.loads(msg)
            t = data.get('t')
            if t == 'welcome':
                self.slot, self.code = data['slot'], data['code']
                role = 'hosting' if self.slot == 0 else 'joined'
                shown = self.code if not self.args.direct_code else self.args.direct_code
                self.set_status(state=role, code=shown,
                                message=f'Hosting. Join code: {shown}' if self.slot == 0 else f'Joined game {shown} as player {self.slot + 1}')
                self.notice(f'Co-op: {"hosting, code " + shown if self.slot == 0 else "joined as player " + str(self.slot + 1)}')
            elif t == 'roster':
                self.on_roster(data['players'])
            elif t == 'error':
                self.set_status(state='error', message=data.get('msg', 'Error'))
                return
            elif t == 'closed':
                self.set_status(state='error', message='The host ended the game.')
                self.notice('Co-op: the host ended the game')
                self.names = {self.slot: self.names.get(self.slot, '')}
                return

    async def keepalive(self):
        while True:
            await asyncio.sleep(20)
            if self.ws and not self.ws.closed:
                try:
                    await self.ws.send(json.dumps({'t': 'ping'}))
                except wsock.Closed:
                    pass

    async def pump(self):
        last = 0
        while True:
            try:
                self.send_remotes()
                if time.time() - last > 2:
                    last = time.time()
                    self.set_status(stats=dict(self.stats))
            except Exception:
                import traceback
                traceback.print_exc()
            await asyncio.sleep(1 / 60)

    async def run(self):
        self.loop = asyncio.get_running_loop()
        agent = self

        class Game(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                agent.on_game_packet(data, addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        no_udp_reset(sock)
        sock.bind(('127.0.0.1', self.args.game_port))
        transport, _ = await self.loop.create_datagram_endpoint(Game, sock=sock)
        self.udp = transport
        a = self.args
        name = a.name or os.environ.get('USERNAME', 'Spartan')
        if a.mode == 'host':
            if a.direct:
                import relay
                asyncio.ensure_future(relay.main(a.direct))
                await asyncio.sleep(0.3)
                url = f'ws://127.0.0.1:{a.direct}/'
                a.direct_code = f'{local_ip()}:{a.direct}'
            else:
                url = a.relay
            hello = {'t': 'host', 'name': name}
        else:
            target = a.target.strip()
            if ':' in target:
                # Direct: the host's agent serves one game; '*' joins it.
                url, hello = f'ws://{target}/', {'t': 'join', 'code': '*', 'name': name}
                a.direct_code = target
            else:
                url, hello = a.relay, {'t': 'join', 'code': target.upper(), 'name': name}
        asyncio.ensure_future(self.keepalive())
        asyncio.ensure_future(self.pump())
        await self.session(url, hello)
        await asyncio.sleep(1)


def no_udp_reset(sock):
    """Windows reports a datagram sent to a closed port (the game restarted) as a reset of this
    socket, after which asyncio stops receiving: SIO_UDP_CONNRESET off."""
    if sys.platform != 'win32':
        return
    import ctypes
    from ctypes import wintypes
    SIO_UDP_CONNRESET = 0x9800000C
    off = wintypes.BOOL(False)
    returned = wintypes.DWORD()
    ctypes.windll.ws2_32.WSAIoctl(ctypes.c_size_t(sock.fileno()), SIO_UDP_CONNRESET, ctypes.byref(off),
                                  ctypes.sizeof(off), None, 0, ctypes.byref(returned), None, None)


def local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return '127.0.0.1'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--game-port', type=int, required=True)
    p.add_argument('--relay', default=os.environ.get('GOW3_COOP_RELAY', DEFAULT_RELAY))
    p.add_argument('--direct', type=int, default=0, help='host: serve the session directly on this TCP port')
    p.add_argument('--name', default='')
    p.add_argument('--status', default='')
    p.add_argument('mode', choices=['host', 'join'])
    p.add_argument('target', nargs='?', default='')
    a = p.parse_args()
    a.direct_code = ''
    asyncio.run(Agent(a).run())


if __name__ == '__main__':
    main()
