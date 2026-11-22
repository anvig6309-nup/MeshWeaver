"""Week 1 / Mesh Setup: asyncio UDP node that can ping other nodes.

Messages are small JSON datagrams:
    {"type": "PING", "id": "<hex>", "node_id": "<hex>", "ts": <float>}
    {"type": "PONG", "id": "<same hex>", "node_id": "<hex>", "ts": <float>}
"""

import asyncio
import json
import time
import uuid


class _MeshProtocol(asyncio.DatagramProtocol):
    """Thin adapter: forwards UDP events to the owning MeshNode."""

    def __init__(self, node):
        self.node = node
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.node._on_datagram(data, addr)

    def error_received(self, exc):
        # e.g. ICMP "port unreachable" - a ping to a dead peer just times out.
        pass


class MeshNode:
    def __init__(self, host="127.0.0.1", port=0, node_id=None):
        self.host = host
        self.port = port
        self.node_id = node_id or uuid.uuid4().hex[:8]
        self.peers = {}  # node_id -> (host, port) learned from PING/PONG
        self._transport = None
        self._pending = {}  # ping id -> Future

    # ---------- lifecycle ----------
    async def start(self):
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _MeshProtocol(self), local_addr=(self.host, self.port)
        )
        self.port = self._transport.get_extra_info("sockname")[1]
        return self

    async def stop(self):
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *exc):
        await self.stop()

    # ---------- sending ----------
    def _send(self, msg, addr):
        self._transport.sendto(json.dumps(msg).encode("utf-8"), addr)

    async def ping(self, host, port, timeout=2.0):
        """Ping a peer. Returns round-trip time in seconds, or None on timeout."""
        ping_id = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self._pending[ping_id] = fut
        start = time.perf_counter()
        self._send(
            {"type": "PING", "id": ping_id, "node_id": self.node_id, "ts": time.time()},
            (host, port),
        )
        try:
            await asyncio.wait_for(fut, timeout)
            return time.perf_counter() - start
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending.pop(ping_id, None)

    # ---------- receiving ----------
    def _on_datagram(self, data, addr):
        try:
            msg = json.loads(data.decode("utf-8"))
            mtype = msg["type"]
        except (ValueError, KeyError, UnicodeDecodeError):
            return  # ignore garbage packets

        peer_id = msg.get("node_id")
        if peer_id and peer_id != self.node_id:
            self.peers[peer_id] = addr

        if mtype == "PING":
            self._send(
                {"type": "PONG", "id": msg.get("id"), "node_id": self.node_id,
                 "ts": time.time()},
                addr,
            )
        elif mtype == "PONG":
            fut = self._pending.get(msg.get("id"))
            if fut is not None and not fut.done():
                fut.set_result(msg)