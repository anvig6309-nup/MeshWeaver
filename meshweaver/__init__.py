"""MeshWeaver - zero-central-server P2P async task broker (Week 1)."""

from .mesh_node import MeshNode
from .task_node import TaskNode, RemoteTaskError, IntegrityError
from .dht import PeerTable, generate_node_id, xor_distance
from .gossip import GossipEngine, ResourceRecord

__all__ = [
    "MeshNode",
    "TaskNode",
    "RemoteTaskError",
    "IntegrityError",
    "PeerTable",
    "generate_node_id",
    "xor_distance",
    "GossipEngine",
    "ResourceRecord",
]
__version__ = "0.1.0"
