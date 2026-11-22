"""Week 1 / Mesh Setup: asyncio UDP node that can ping other nodes.

Messages are small JSON datagrams:
    {"type": "PING", "id": "<hex>", "node_id": "<hex>", "ts": <float>}
    {"type": "PONG", "id": "<same hex>", "node_id": "<hex>", "ts": <float>}
"""

import asyncio
import json
import time
import uuid

from .dht import PeerTable, generate_node_id
from .gossip import GossipEngine


class _MeshProtocol(asyncio.DatagramProtocol):
    """Thin adapter: forwards UDP events to the owning MeshNode."""

    def __init__(self, node):
        self.node = node
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.node._on_transport_lost(exc)

    def datagram_received(self, data, addr):
        self.node._on_datagram(data, addr)

    def error_received(self, exc):
        # e.g. ICMP "port unreachable" - a ping to a dead peer just times out.
        pass


class MeshNode:
    def __init__(
        self,
        host="127.0.0.1",
        port=0,
        node_id=None,
        bootstrap_nodes=None,
        gossip_interval=5.0,
        stale_after=30.0,
        refresh_interval=30.0,
        max_peers=256,
        peer_timeout=1.0,
    ):
        self.host = host
        self.port = port
        self.node_id = node_id or generate_node_id()
        self.peers = {}  # node_id -> (host, port) learned from PING/PONG
        self.peer_table = PeerTable(self.node_id, max_peers=max_peers)
        self.bootstrap_nodes = list(bootstrap_nodes or [])
        self.refresh_interval = refresh_interval
        self.peer_timeout = peer_timeout
        self._transport = None
        self._transport_closed = None
        self._pending = {}  # ping id -> Future
        self.gossip = GossipEngine(
            self.node_id,
            interval=gossip_interval,
            stale_after=stale_after,
        )
        self.gossip.set_sender(self._send)
        self.gossip.set_peer_provider(self._gossip_peers)
        self._background_tasks = set()

    # ---------- lifecycle ----------
    async def start(self):
        if self._transport is not None:
            return self
        loop = asyncio.get_running_loop()
        self._transport_closed = loop.create_future()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _MeshProtocol(self), local_addr=(self.host, self.port)
        )
        self.port = self._transport.get_extra_info("sockname")[1]
        await self._bootstrap()
        await self.gossip.start()
        if self.refresh_interval > 0:
            self._background_tasks.add(
                asyncio.create_task(self._refresh_loop())
            )
        return self

    async def stop(self):
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
        await self.gossip.stop()
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        if self._transport is not None:
            self._transport.close()
            if self._transport_closed is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._transport_closed),
                        timeout=1.0,
                    )
                except asyncio.TimeoutError:
                    pass
            self._transport = None
            self._transport_closed = None

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *exc):
        await self.stop()

    # ---------- sending ----------
    def _send(self, msg, addr):
        if self._transport is not None:
            try:
                raw = json.dumps(msg).encode("utf-8")
                if len(raw) <= 64 * 1024:
                    self._transport.sendto(raw, addr)
            except (TypeError, ValueError):
                return

    def _on_transport_lost(self, exc):
        if self._transport_closed is not None and not self._transport_closed.done():
            self._transport_closed.set_result(None)

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

    async def bootstrap(self, host, port, timeout=2.0):
        """Join through a bootstrap address and request peer information."""
        await self.ping(host, port, timeout=timeout)
        self._send(
            {
                "type": "FIND_PEERS",
                "node_id": self.node_id,
                "target": self.node_id,
                "request_id": uuid.uuid4().hex,
            },
            (host, port),
        )

    async def _bootstrap(self):
        for bootstrap in self.bootstrap_nodes:
            try:
                host, port = bootstrap
                await self.bootstrap(host, int(port), timeout=1.0)
            except (TypeError, ValueError, OSError):
                continue

    def _gossip_peers(self):
        return list(self.peer_table.items())

    async def _refresh_loop(self):
        try:
            while True:
                await asyncio.sleep(self.refresh_interval)
                await self._refresh_peers()
        except asyncio.CancelledError:
            return

    async def _refresh_peers(self):
        probes = [
            self._probe_peer(peer_id, host, port)
            for peer_id, host, port in self.peer_table.rank(self.node_id, limit=20)
        ]
        if probes:
            await asyncio.gather(*probes)

    async def _probe_peer(self, peer_id, host, port):
        try:
            response = await self.ping(host, port, timeout=self.peer_timeout)
        except (OSError, ValueError):
            response = None
        if response is None:
            if self.peer_table.mark_failure(peer_id):
                self.peers.pop(peer_id, None)
            return
        self._send(
            {
                "type": "FIND_PEERS",
                "node_id": self.node_id,
                "target": self.node_id,
                "request_id": uuid.uuid4().hex,
            },
            (host, port),
        )

    def _announce_peer(self, peer_id, addr):
        host, port = addr
        for known_id, known_host, known_port in self.peer_table.items():
            if known_id in (peer_id, self.node_id):
                continue
            self._send(
                {
                    "type": "PEERS_FOUND",
                    "node_id": self.node_id,
                    "request_id": uuid.uuid4().hex,
                    "peers": [
                        {"node_id": peer_id, "host": host, "port": port}
                    ],
                },
                (known_host, known_port),
            )

    # ---------- receiving ----------
    def _on_datagram(self, data, addr):
        try:
            if len(data) > 64 * 1024:
                return
            msg = json.loads(data.decode("utf-8"))
            mtype = msg["type"]
        except (TypeError, ValueError, KeyError, UnicodeDecodeError):
            return  # ignore garbage packets

        peer_id = msg.get("node_id")
        valid_peer_id = False
        if isinstance(peer_id, str) and peer_id:
            try:
                bytes.fromhex(peer_id)
                valid_peer_id = peer_id != self.node_id
            except ValueError:
                valid_peer_id = False
        if valid_peer_id:
            known_peer = peer_id in self.peer_table.peers
            inserted = self.peer_table.insert(peer_id, addr[0], addr[1])
            if inserted:
                self.peers[peer_id] = addr
                self.peer_table.mark_success(peer_id)
                if not known_peer:
                    self.gossip.share_with_peer(peer_id, addr[0], addr[1])

        if mtype == "PING":
            self._send(
                {"type": "PONG", "id": msg.get("id"), "node_id": self.node_id,
                 "ts": time.time()},
                addr,
            )
            if peer_id and peer_id != self.node_id:
                self._announce_peer(peer_id, addr)
        elif mtype == "PONG":
            fut = self._pending.get(msg.get("id"))
            if fut is not None and not fut.done():
                fut.set_result(msg)
        elif mtype == "FIND_PEERS":
            target = msg.get("target")
            request_id = msg.get("request_id")
            if not isinstance(target, str) or not isinstance(request_id, str):
                return
            try:
                peers = self.peer_table.rank(target, limit=50)
            except ValueError:
                return
            entries = [
                {"node_id": peer_id, "host": host, "port": port}
                for peer_id, host, port in peers
                if peer_id != msg.get("node_id")
            ]
            entries.append(
                {"node_id": self.node_id, "host": self.host, "port": self.port}
            )
            entries = entries[-50:]
            self._send(
                {
                    "type": "PEERS_FOUND",
                    "node_id": self.node_id,
                    "request_id": request_id,
                    "peers": entries,
                },
                addr,
            )
        elif mtype == "PEERS_FOUND":
            peers = msg.get("peers")
            if not isinstance(peers, list):
                return
            for entry in peers[:50]:
                if not isinstance(entry, dict):
                    continue
                peer_id = entry.get("node_id")
                host = entry.get("host")
                port = entry.get("port")
                if (
                    isinstance(peer_id, str)
                    and peer_id != self.node_id
                    and isinstance(host, str)
                    and isinstance(port, int)
                ):
                    known_peer = peer_id in self.peer_table.peers
                    if self.peer_table.insert(peer_id, host, port):
                        self.peers[peer_id] = (host, port)
                        if not known_peer:
                            self.gossip.share_with_peer(peer_id, host, port)
        elif mtype == "GOSSIP_RESOURCE":
            if self.gossip.update(msg):
                self.gossip.forward(msg)