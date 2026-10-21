"""Minimal RFC 6455 WebSocket over asyncio streams (no dependencies): server and client
handshakes, text/binary messages, ping/pong and close. Shared by the relay and the co-op agent."""
import asyncio
import base64
import hashlib
import os
import ssl
import struct
from urllib.parse import urlparse

GUID = b'258EAFA5-E914-47DA-95CA-C5AB0DC11B65'
MAX_MESSAGE = 1 << 20


class Closed(Exception):
    pass


class WebSocket:
    def __init__(self, reader, writer, mask):
        self.r, self.w, self.mask = reader, writer, mask
        self.lock = asyncio.Lock()
        self.closed = False

    async def _send_frame(self, opcode, data):
        head = bytearray([0x80 | opcode])
        n = len(data)
        bit = 0x80 if self.mask else 0
        if n < 126:
            head.append(bit | n)
        elif n < 65536:
            head.append(bit | 126); head += struct.pack('>H', n)
        else:
            head.append(bit | 127); head += struct.pack('>Q', n)
        if self.mask:
            key = os.urandom(4)
            head += key
            data = bytes(b ^ key[i & 3] for i, b in enumerate(data))
        async with self.lock:
            if self.closed:
                raise Closed()
            self.w.write(bytes(head) + data)
            await self.w.drain()

    async def send(self, message):
        if isinstance(message, str):
            await self._send_frame(1, message.encode())
        else:
            await self._send_frame(2, bytes(message))

    async def recv(self):
        """The next text (str) or binary (bytes) message; raises Closed."""
        parts, opcode = [], None
        while True:
            try:
                b0, b1 = await self.r.readexactly(2)
                n = b1 & 0x7f
                if n == 126:
                    n = struct.unpack('>H', await self.r.readexactly(2))[0]
                elif n == 127:
                    n = struct.unpack('>Q', await self.r.readexactly(8))[0]
                if n > MAX_MESSAGE:
                    raise Closed()
                key = await self.r.readexactly(4) if b1 & 0x80 else None
                data = await self.r.readexactly(n)
            except (asyncio.IncompleteReadError, ConnectionError, OSError):
                self.closed = True
                raise Closed()
            if key:
                data = bytes(b ^ key[i & 3] for i, b in enumerate(data))
            op = b0 & 0x0f
            if op == 8:
                await self.close()
                raise Closed()
            if op == 9:
                await self._send_frame(10, data)
                continue
            if op == 10:
                continue
            if op != 0:
                opcode = op
            parts.append(data)
            if b0 & 0x80:
                payload = b''.join(parts)
                return payload.decode(errors='replace') if opcode == 1 else payload

    async def close(self):
        if self.closed:
            return
        try:
            await self._send_frame(8, b'')
        except Exception:
            pass
        self.closed = True
        try:
            self.w.close()
        except Exception:
            pass


async def read_http_head(reader):
    head = await reader.readuntil(b'\r\n\r\n')
    lines = head.decode('latin1').split('\r\n')
    headers = {}
    for line in lines[1:]:
        if ':' in line:
            k, v = line.split(':', 1)
            headers[k.strip().lower()] = v.strip()
    return lines[0], headers


async def accept(reader, writer, request_line, headers):
    """Server side: completes the handshake for an upgrade request already read."""
    key = headers.get('sec-websocket-key', '').encode()
    accept_key = base64.b64encode(hashlib.sha1(key + GUID).digest()).decode()
    writer.write(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                  f'Sec-WebSocket-Accept: {accept_key}\r\n\r\n').encode())
    await writer.drain()
    return WebSocket(reader, writer, mask=False)


async def connect(url, timeout=60):
    """Client side: ws:// or wss:// URL."""
    u = urlparse(url)
    secure = u.scheme == 'wss'
    port = u.port or (443 if secure else 80)
    ctx = ssl.create_default_context() if secure else None
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(u.hostname, port, ssl=ctx, server_hostname=u.hostname if secure else None), timeout)
    key = base64.b64encode(os.urandom(16)).decode()
    path = u.path or '/'
    writer.write((f'GET {path} HTTP/1.1\r\nHost: {u.hostname}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                  f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: gow3-coop\r\n\r\n').encode())
    await writer.drain()
    status, headers = await asyncio.wait_for(read_http_head(reader), timeout)
    if ' 101 ' not in status + ' ':
        writer.close()
        raise ConnectionError(f'handshake refused: {status}')
    return WebSocket(reader, writer, mask=True)
