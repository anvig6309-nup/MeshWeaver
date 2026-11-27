# MeshWeaver - Async P2P Task Broker

MeshWeaver is a decentralized task broker written in Python. It
requires no Redis, no RabbitMQ, and no central broker. Instead,
nodes form a peer-to-peer mesh using `asyncio`, exchange resource
information with each other, and ship Python functions to one
another for remote execution using `cloudpickle`.

**Domain:** Distributed Systems & Edge Computing

## Project overview

In many distributed setups, a central broker is a single point of
failure and a bottleneck. MeshWeaver explores an alternative in
which every node is a peer. Each node discovers other nodes,
learns about their resource availability, and can submit work to
them directly. The project is intended as a learning-oriented
implementation of these ideas, built step by step across weekly
milestones.

## Objectives

- Build a broker-free network in which nodes find and talk to
  one another without a central coordinator.
- Let nodes share lightweight resource information (CPU and RAM
  usage) across the mesh.
- Allow a node to send a Python function and its arguments to a
  peer, run it there, and receive the result.
- Verify payload integrity and keep the code readable and well
  tested.

## Features

- **Ping over UDP:** Check whether a peer is reachable and measure
  round-trip time.
- **Remote task execution over TCP:** Submit functions with
  arguments to a peer and receive the return value or a remote
  error.
- **Peer discovery:** Join the mesh through a bootstrap node and
  learn about further peers through peer exchange.
- **Resource gossip:** Periodically publish CPU and RAM readings
  that propagate through the mesh.
- **Failure handling:** Remove peers that stop responding and
  discard stale resource records.

## Week 1 status

- **Distributed Networking:** asyncio UDP node; two instances ping
  each other. Done.
- **Task Execution & Reliability:** cloudpickle serialization,
  SHA-256 integrity check, and remote execution. Done.

## Week 2 — Completed

- **Peer Discovery:** 256-bit node IDs, XOR buckets, bootstrap,
  peer exchange, and reachability refresh. Done.
- **Gossip:** 5-second CPU/RAM gossip, duplicate control, and
  expiration. Done.

Week 2 extends the Week 1 UDP/TCP node design without replacing
its ping or remote task execution APIs. Automated coverage
includes DHT, three-node discovery, resource gossip, peer failure
removal, lifecycle cleanup, and remote task execution (27 tests).

The DHT and gossip protocols are educational implementations, not
production-grade or full Kademlia implementations. Future work can
add stronger identity and message authentication, task
reassignment, and more comprehensive failure handling.

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

- `mesh_node.py`: UDP mesh, peer discovery, refresh, and gossip
  integration
- `dht.py`: XOR distance and bounded peer routing table
- `gossip.py`: CPU/RAM sampling and resource gossip
- `task_node.py`: cloudpickle serialization and TCP task transport
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

The demo starts three nodes, bootstraps C through B, prints
propagated peers, and prints CPU/RAM resource records.

## How it works

1. **Ping (UDP):** `MeshNode.ping(host, port)` sends a JSON `PING`;
   the peer replies `PONG`. It returns the round-trip time, or
   `None` on timeout. Nodes remember peers they hear from.
2. **Task (TCP, same port number):** `TaskNode.submit(host, port,
   func, *args)`
   - `cloudpickle.dumps((func, args, kwargs))` produces the payload.
   - The SHA-256 digest of the payload is sent in the header.
   - The receiver verifies the hash, unpickles the payload, and
     runs the function. Async functions are awaited; blocking
     functions run in an executor. The result or remote error is
     returned to the caller.

Tasks use TCP because pickled functions can exceed the size of a
UDP datagram.

## Technical documentation

### Message types

Control and discovery messages are exchanged as JSON over UDP:

- `PING` / `PONG`: liveness checks and round-trip timing.
- `FIND_PEERS` / `PEERS_FOUND`: peer address exchange.
- `GOSSIP_RESOURCE`: resource records containing node ID, CPU
  percentage, RAM percentage, timestamp, sequence number, and
  message ID.

Task payloads are sent over TCP and are not JSON.

### Week 2 peer discovery

Every `MeshNode` receives a 256-bit hexadecimal node ID generated
from a SHA-256 digest of cryptographically random bytes.
`PeerTable` maintains bounded Kademlia-inspired buckets and ranks
peers by XOR distance.

`FIND_PEERS` and `PEERS_FOUND` datagrams exchange peer addresses.
A node can join the mesh with `bootstrap_nodes=[("127.0.0.1",
port)]`. Afterwards, it learns addresses through peer exchanges
rather than from a global peer list. New peers are announced to
existing peers.

Malformed, oversized, duplicate, invalid-address, and self-peer
entries are ignored or bounded. A background refresh task pings
known peers concurrently and removes a peer after three consecutive
failed probes.

This is a lightweight educational implementation and is not a
full Kademlia-compatible network protocol.

### Week 2 resource gossip

`GossipEngine` collects real CPU and RAM percentages using `psutil`
in an executor, so resource sampling does not block the asyncio
loop. It publishes immediately at startup and then every five
seconds.

New resource records are forwarded to peers. Message IDs and
per-node sequence numbers suppress duplicates and stale updates.
Each node maintains a resource table for discovered nodes. Records
older than 30 seconds are removed or omitted from inspection by
default. Gossip forwarding is limited to the known peer set and
does not block ping or task execution.

Inspect resources with:

```python
node.gossip.get_all_resources()
node.gossip.get_resource(peer_node_id)
```

## Limitations

- The DHT and gossip implementations are educational and not
  production-grade.
- Peer and message authentication are not yet implemented.
- Failed tasks are not reassigned to other nodes.

## Security note

Unpickling runs code. Only connect nodes you trust. SHA-256
detects corruption; it does not authenticate senders.
Authentication may be added in a later week.
