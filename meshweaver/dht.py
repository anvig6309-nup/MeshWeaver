"""Lightweight Kademlia-inspired peer discovery."""

from __future__ import annotations

import hashlib
import secrets
from typing import Iterable


def generate_node_id() -> str:
    """Generate a 160-bit, cryptographically random node identifier."""
    random_bytes = secrets.token_bytes(32)
    return hashlib.sha256(random_bytes).hexdigest()


def xor_distance(first: str, second: str) -> int:
    """Return the XOR distance between two hexadecimal node identifiers."""
    try:
        a = bytes.fromhex(first)
        b = bytes.fromhex(second)
    except (TypeError, ValueError):
        raise ValueError("node identifiers must be hexadecimal")
    if len(a) != len(b):
        raise ValueError("node identifiers must have equal length")
    return int.from_bytes(bytes(x ^ y for x, y in zip(a, b)), "big")


def bucket_index(local_id: str, peer_id: str) -> int:
    distance = xor_distance(local_id, peer_id)
    if distance == 0:
        return 0
    return distance.bit_length() - 1


class PeerTable:
    """A bounded peer table with simple XOR-ranked Kademlia buckets."""

    def __init__(self, local_node_id: str, max_peers: int = 256, bucket_size: int = 20):
        self.local_node_id = local_node_id
        self.max_peers = max_peers
        self.bucket_size = bucket_size
        self.peers: dict[str, tuple[str, int]] = {}
        self.buckets: dict[int, list[str]] = {}
        self._failures: dict[str, int] = {}

    def insert(self, peer_id: str, host: str, port: int) -> bool:
        if not isinstance(peer_id, str) or not peer_id:
            return False
        try:
            bytes.fromhex(peer_id)
        except ValueError:
            return False
        if peer_id == self.local_node_id:
            return False
        if not isinstance(host, str) or not (1 <= port <= 65535):
            return False
        if peer_id in self.peers:
            if self.peers[peer_id] != (host, port):
                self.peers[peer_id] = (host, port)
            self._failures.pop(peer_id, None)
            return True
        if len(self.peers) >= self.max_peers:
            return False

        try:
            index = bucket_index(self.local_node_id, peer_id)
        except ValueError:
            return False
        bucket = self.buckets.setdefault(index, [])
        if len(bucket) >= self.bucket_size:
            return False

        bucket.append(peer_id)
        self.peers[peer_id] = (host, port)
        self._failures.pop(peer_id, None)
        return True

    def remove(self, peer_id: str) -> bool:
        if peer_id not in self.peers:
            return False
        del self.peers[peer_id]
        for bucket in self.buckets.values():
            if peer_id in bucket:
                bucket.remove(peer_id)
        self._failures.pop(peer_id, None)
        return True

    def mark_failure(self, peer_id: str) -> None:
        self._failures[peer_id] = self._failures.get(peer_id, 0) + 1
        if self._failures[peer_id] >= 3:
            self.remove(peer_id)

    def rank(self, target_id: str, limit: int = 50) -> list[tuple[str, str, int]]:
        if limit <= 0:
            return []
        ranked = sorted(
            self.peers,
            key=lambda peer_id: xor_distance(target_id, peer_id),
        )
        return [
            (peer_id, self.peers[peer_id][0], self.peers[peer_id][1])
            for peer_id in ranked[:limit]
        ]

    def items(self) -> Iterable[tuple[str, str, int]]:
        for peer_id, (host, port) in list(self.peers.items()):
            yield peer_id, host, port
