"""Select a peer using its latest gossip resource report."""

import time

from .gossip import GossipEngine


def select_best_node(peers: GossipEngine) -> str | None:
    """Return the ID of the freshest peer with the lowest CPU, then RAM, load."""
    now = time.time()
    max_age = 3 * peers.interval
    candidates = [
        (record.cpu_percent, record.ram_percent, node_id)
        for node_id, record in peers.records.items()
        if now - record.timestamp <= max_age
    ]
    if not candidates:
        return None
    return min(candidates)[2]
