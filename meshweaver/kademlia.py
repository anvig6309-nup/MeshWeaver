"""Week 2 / DHT, part 1: Kademlia building blocks (no networking yet).

Kademlia gives every node a 160-bit ID. The "distance" between two nodes is the
XOR of their IDs, read as an integer. Each node keeps a routing table made of
k-buckets: bucket i holds up to K contacts whose distance from us is in
[2**i, 2**(i+1)). Close nodes are known well, far nodes only roughly, so a
lookup can reach any node in about log2(N) steps.

This module only holds the data structures. UDP RPCs (PING / FIND_NODE) come
in the next commits.
"""

import hashlib
import os
import time

ID_BITS = 160  # SHA-1 sized IDs, as in the Kademlia paper
K = 20  # max contacts per bucket (the paper's replication parameter)


# ---------- node IDs and distance ----------
def generate_node_id(seed=None):
    """Return a 160-bit integer ID. Random by default; seed (bytes/str) makes it
    deterministic, which is handy for tests and for stable node identities."""
    if seed is None:
        raw = os.urandom(32)
    elif isinstance(seed, str):
        raw = seed.encode("utf-8")
    else:
        raw = bytes(seed)
    return int.from_bytes(hashlib.sha1(raw).digest(), "big")


def distance(a, b):
    """XOR distance between two IDs."""
    return a ^ b


def bucket_index(own_id, other_id):
    """Index (0..159) of the bucket that other_id belongs to, from own_id's view.

    It is the position of the highest differing bit, i.e. floor(log2(distance)).
    A node has no bucket for itself, so identical IDs raise ValueError.
    """
    d = own_id ^ other_id
    if d == 0:
        raise ValueError("a node has no bucket for its own ID")
    return d.bit_length() - 1


# ---------- contacts ----------
class Contact:
    """A known peer: its 160-bit ID plus the UDP address to reach it."""

    __slots__ = ("node_id", "host", "port", "last_seen")

    def __init__(self, node_id, host, port, last_seen=None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.last_seen = time.time() if last_seen is None else last_seen

    @property
    def address(self):
        return (self.host, self.port)

    def __eq__(self, other):
        return isinstance(other, Contact) and self.node_id == other.node_id

    def __hash__(self):
        return hash(self.node_id)

    def __repr__(self):
        return f"Contact(id={self.node_id:040x}, addr={self.host}:{self.port})"


# ---------- k-bucket ----------
class KBucket:
    """Up to k contacts, ordered from least recently seen (front) to most
    recently seen (back). Kademlia prefers long-lived nodes, because a node that
    has been up for a long time is likely to stay up."""

    def __init__(self, k=K):
        self.k = k
        self._contacts = []

    def __len__(self):
        return len(self._contacts)

    def __iter__(self):
        return iter(list(self._contacts))

    def __contains__(self, node_id):
        return any(c.node_id == node_id for c in self._contacts)

    @property
    def contacts(self):
        return list(self._contacts)

    def add(self, contact):
        """Record that we just heard from contact.

        - already known  -> refresh it and move it to the most-recent end
        - bucket has room -> append it
        - bucket is full  -> do NOT add; return the least-recently-seen contact
          so the caller can ping it. If it is dead, call remove() and add()
          again; if it answers, call add() on it to refresh it (the newcomer is
          dropped, favouring the old, proven node).

        Returns None when the contact was stored/refreshed, otherwise the
        least-recently-seen contact of the full bucket.
        """
        for i, existing in enumerate(self._contacts):
            if existing.node_id == contact.node_id:
                del self._contacts[i]
                contact.last_seen = time.time()
                self._contacts.append(contact)
                return None
        if len(self._contacts) < self.k:
            self._contacts.append(contact)
            return None
        return self._contacts[0]

    def remove(self, node_id):
        """Drop a contact (e.g. it stopped answering). Returns True if removed."""
        for i, existing in enumerate(self._contacts):
            if existing.node_id == node_id:
                del self._contacts[i]
                return True
        return False


# ---------- routing table ----------
class RoutingTable:
    """All the k-buckets of one node, keyed by bucket index (created lazily)."""

    def __init__(self, own_id, k=K):
        self.own_id = own_id
        self.k = k
        self._buckets = {}  # bucket index -> KBucket

    def __len__(self):
        return sum(len(b) for b in self._buckets.values())

    def add_contact(self, contact):
        """Add or refresh a peer. Our own ID is ignored.

        Returns None on success, or the least-recently-seen contact of the full
        bucket that should be pinged (see KBucket.add).
        """
        if contact.node_id == self.own_id:
            return None
        idx = bucket_index(self.own_id, contact.node_id)
        bucket = self._buckets.setdefault(idx, KBucket(self.k))
        return bucket.add(contact)

    def remove_contact(self, node_id):
        if node_id == self.own_id:
            return False
        idx = bucket_index(self.own_id, node_id)
        bucket = self._buckets.get(idx)
        if bucket is None:
            return False
        removed = bucket.remove(node_id)
        if removed and len(bucket) == 0:
            del self._buckets[idx]
        return removed

    def get_contact(self, node_id):
        if node_id == self.own_id:
            return None
        bucket = self._buckets.get(bucket_index(self.own_id, node_id))
        if bucket is None:
            return None
        for c in bucket:
            if c.node_id == node_id:
                return c
        return None

    def all_contacts(self):
        return [c for b in self._buckets.values() for c in b]

    def find_closest(self, target_id, count=None, exclude=None):
        """The `count` known contacts closest (by XOR) to target_id, nearest first.

        exclude is an optional node ID to leave out (typically the asker, so a
        node never gets itself back in a FIND_NODE reply).
        """
        count = self.k if count is None else count
        candidates = [c for c in self.all_contacts() if c.node_id != exclude]
        candidates.sort(key=lambda c: c.node_id ^ target_id)
        return candidates[:count]
