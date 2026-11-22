"""MeshWeaver - zero-central-server P2P async task broker."""

from .mesh_node import MeshNode
from .task_node import TaskNode, RemoteTaskError, IntegrityError
from .dht import (
    PeerTable,
    generate_node_id as generate_dht_node_id,
    xor_distance,
)
from .gossip import GossipEngine, ResourceRecord
from .kademlia import Contact, KBucket, RoutingTable, generate_node_id, distance

__all__ = [
    "MeshNode",
    "TaskNode",
    "RemoteTaskError",
    "IntegrityError",
    "PeerTable",
    "generate_dht_node_id",
    "generate_node_id",
    "xor_distance",
    "GossipEngine",
    "ResourceRecord",
    "Contact",
    "KBucket",
    "RoutingTable",
    "distance",
]
__version__ = "0.1.0"
