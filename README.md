# MeshWeaver - Zero-Dependency P2P Async Task Broker

A decentralized task queue in pure Python. No Redis, no RabbitMQ, no central broker.
Nodes form a peer-to-peer mesh with `asyncio`, and ship Python functions to each other
with `cloudpickle`.

**Domain:** Distributed Systems & Edge Computing

## Week 1 status

| Track | Deliverable | Status |
|-------|-------------|--------|
| Distributed Networking | asyncio UDP node; two instances ping each other | Done |
| Task Execution & Reliability | cloudpickle serialization, SHA-256 integrity check, remote execution | Done |

Kademlia DHT, gossip protocol and failure detection/reassignment come in later weeks.

## Project layout

```
MeshWeaver/
  meshweaver/
    __init__.py
    mesh_node.py     # UDP PING/PONG mesh node
    task_node.py     # cloudpickle task serialization + TCP task transport
    run_tests.py     # 13 unit/integration tests
  demo_node.py       # start a node
  demo_client.py     # ping a node + run a function on it
  requirements.txt
```

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

## How it works

1. **Ping (UDP):** `MeshNode.ping(host, port)` sends a JSON `PING`; the peer replies `PONG`.
   Returns round-trip time, or `None` on timeout. Nodes remember peers they hear from.
2. **Task (TCP, same port number):** `TaskNode.submit(host, port, func, *args)`
   - `cloudpickle.dumps((func, args, kwargs))` -> payload
   - SHA-256 of payload sent in the header
   - receiver verifies the hash, unpickles, runs the function (async functions are awaited,
     blocking ones run in an executor), and returns the result (or the remote error).

Tasks use TCP because pickled functions can exceed a UDP datagram.

## Security note

Unpickling runs code. Only connect nodes you trust. SHA-256 detects corruption, it does not
authenticate senders (authentication is a possible later-week addition).
