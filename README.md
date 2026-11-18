
# MeshWeaver - Zero-Dependency P2P Async Task Broker

A decentralized task queue in pure Python. No Redis, no RabbitMQ, no central broker.
Nodes form a peer-to-peer mesh using asyncio, and ship Python functions to each other
using cloudpickle.

**Domain:** Distributed Systems & Edge Computing

## Problem

Distributed task queues like Celery need heavy central infrastructure (Redis, RabbitMQ,
worker nodes). For edge computing, such as hundreds of Raspberry Pis in the field, a central
broker is a single point of failure and a configuration burden.

## Idea

MeshWeaver is a 100% pure Python, decentralized task queue built on asyncio and a Kademlia
DHT. Nodes form a peer-to-peer mesh and assign work to the peer with the lowest CPU load.
If a node dies, the network detects the lost heartbeat and reassigns the task.

## Week 1 plan

- Mesh setup: asyncio UDP server/client so two Python instances can ping each other
- Task serialization: use cloudpickle to send a function over the socket and run it remotely
