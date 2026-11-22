# MeshWeaver - Async P2P Task Broker

A decentralized task broker in Python with no Redis,
RabbitMQ, or central broker. Nodes form a peer-to-peer
mesh with `asyncio`, exchange resource information, and
ship Python functions to each other with `cloudpickle`.

**Domain:** Distributed Systems & Edge Computing

## Week 1 status

- **Distributed Networking:** asyncio UDP node; two
  instances ping each other. Done.
- **Task Execution & Reliability:** cloudpickle
  serialization, SHA-256 integrity check, and remote
  execution. Done.

## Week 2 — Completed

- **Peer Discovery:** 256-bit node IDs, XOR buckets,
  bootstrap, peer exchange, and reachability refresh.
  Done.
- **Gossip:** 5-second CPU/RAM gossip, duplicate control,
  and expiration. Done.

Week 2 extends the Week 1 UDP/TCP node design without
replacing its ping or remote task execution APIs.
Automated coverage includes DHT, three-node discovery,
resource gossip, peer failure removal, lifecycle cleanup,
and remote task execution (27 tests).

The DHT and gossip protocols are educational
implementations, not production-grade or full Kademlia
implementations. Future work can add stronger identity
and message authentication, task reassignment, and more
comprehensive failure handling.

## Project layout

```
MeshWeaver/
  meshweaver/
    __init__.py
    mesh_node.py
    dht.py
    gossip.py
    task_node.py
    run_tests.py
  demo_node.py
  demo_client.py
  demo_week2.py
  requirements.txt
```

- `mesh_node.py`: UDP mesh, peer discovery, refresh,
  and gossip integration
- `dht.py`: XOR distance and bounded peer routing table
- `gossip.py`: CPU/RAM sampling and resource gossip
- `task_node.py`: cloudpickle serialization and TCP
  task transport
- `run_tests.py`: Week 1 and Week 2 tests
- `demo_node.py`: start a node
- `demo_client.py`: ping a node and run a function on it
- `demo_week2.py`: three-node discovery and gossip demo

## Install

```
pip install -r requirements.txt
```

## Run the tests

```
python -m meshweaver.run_tests
```

## Run the demo (two terminals)

```
# terminal 1
python demo_node.py 9001

# terminal 2
python demo_client.py 9001
```

## Run the Week 2 demo

```powershell
python demo_week2.py
```

The demo starts three nodes, bootstraps C through B,
prints propagated peers, and prints CPU/RAM resource
records.

## How it works

1. **Ping (UDP):** `MeshNode.ping(host, port)` sends a
   JSON `PING`; the peer replies `PONG`. Returns
   round-trip time, or `None` on timeout. Nodes remember
   peers they hear from.
2. **Task (TCP, same port number):**
   `TaskNode.submit(host, port, func, *args)`
   - `cloudpickle.dumps((func, args, kwargs))` -> payload
   - SHA-256 of payload sent in the header
   - Receiver verifies the hash, unpickles, and runs the
     function. Async functions are awaited; blocking
     ones run in an executor. The result or remote error
     is returned.

Tasks use TCP because pickled functions can exceed a UDP
datagram.

## Week 2 peer discovery

Every `MeshNode` receives a 256-bit hexadecimal node ID
generated from a SHA-256 digest of cryptographically
random bytes. `PeerTable` maintains bounded
Kademlia-inspired buckets and ranks peers using XOR
distance.

`FIND_PEERS` and `PEERS_FOUND` JSON datagrams exchange
peer addresses. A node can join with
`bootstrap_nodes=[("127.0.0.1", port)]`; afterwards, it
learns addresses through peer exchanges rather than a
global peer list. New peers are announced to existing
peers.

Malformed, oversized, duplicate, invalid-address, and
self-peer entries are ignored or bounded. The background
refresh task pings known peers concurrently and removes
peers after three consecutive failed probes.

This is an educational, lightweight implementation and
is not a full Kademlia-compatible network protocol.

## Week 2 resource gossip

`GossipEngine` collects real CPU and RAM percentages using
`psutil` in an executor, so resource sampling does not
block the asyncio loop. It publishes immediately at
startup and then every five seconds using
`GOSSIP_RESOURCE` messages containing node ID, CPU, RAM,
timestamp, sequence, and message ID.

New resource records are forwarded to peers. Message IDs
and per-node sequence numbers suppress duplicates and
stale updates. Nodes maintain a resource table for
discovered nodes. Records older than 30 seconds are
removed or omitted from inspection by default. Gossip
forwarding is limited to the known peer set and does not
block ping or task execution.

Inspect resources with:

```python
node.gossip.get_all_resources()
node.gossip.get_resource(peer_node_id)
```

## Security note

Unpickling runs code. Only connect nodes you trust.
SHA-256 detects corruption; it does not authenticate
senders. Authentication may be added in a later week.
