"""Week 1 tests. Run from the project root:  python -m meshweaver.run_tests"""

import asyncio
import socket
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from meshweaver import (
    MeshNode,
    TaskNode,
    RemoteTaskError,
    IntegrityError,
    PeerTable,
    generate_dht_node_id,
    generate_node_id,
    xor_distance,
)
from meshweaver.gossip import ResourceRecord
from meshweaver.router import select_best_node
from meshweaver.task_node import serialize_task, deserialize_task, sha256_hex


def run(coro):
    return asyncio.run(coro)


class TestMesh(unittest.TestCase):
    def test_two_nodes_ping_each_other(self):
        async def main():
            async with MeshNode() as a, MeshNode() as b:
                rtt_ab = await a.ping("127.0.0.1", b.port)
                rtt_ba = await b.ping("127.0.0.1", a.port)
                self.assertIsNotNone(rtt_ab)
                self.assertIsNotNone(rtt_ba)
                self.assertIn(b.node_id, a.peers)
                self.assertIn(a.node_id, b.peers)
        run(main())

    def test_ping_dead_peer_times_out(self):
        async def main():
            async with MeshNode() as a:
                self.assertIsNone(await a.ping("127.0.0.1", 9, timeout=0.5))
        run(main())

    def test_garbage_datagram_ignored(self):
        async def main():
            async with MeshNode() as a, MeshNode() as b:
                a._transport.sendto(b"\xff not json", ("127.0.0.1", b.port))
                self.assertIsNotNone(await a.ping("127.0.0.1", b.port))
        run(main())

    def test_node_stop_is_idempotent(self):
        async def main():
            node = MeshNode()
            await node.start()
            transport = node._transport
            transport_closed = node._transport_closed
            await node.stop()
            await node.stop()
            self.assertIsNone(node._transport)
            self.assertTrue(transport.is_closing())
            self.assertTrue(transport_closed.done())
        run(main())


class TestSerialization(unittest.TestCase):
    def test_roundtrip_lambda_and_closure(self):
        k = 10
        payload, digest = serialize_task(lambda x, y=1: (x + y) * k, 4, y=1)
        func, args, kwargs = deserialize_task(payload, digest)
        self.assertEqual(func(*args, **kwargs), 50)

    def test_tamper_detected(self):
        payload, digest = serialize_task(len, [1, 2, 3])
        with self.assertRaises(IntegrityError):
            deserialize_task(payload[:-1] + b"X", digest)


class TestDHT(unittest.TestCase):
    def test_node_ids_are_unique(self):
        ids = {generate_dht_node_id() for _ in range(100)}
        self.assertEqual(len(ids), 100)
        self.assertTrue(all(len(node_id) == 64 for node_id in ids))

    def test_package_generate_node_id_keeps_kademlia_api(self):
        node_id = generate_node_id()
        self.assertIsInstance(node_id, int)
        self.assertLess(node_id, 2**160)

    def test_xor_distance(self):
        first = "00" * 32
        second = "ff" * 32
        self.assertEqual(xor_distance(first, second), int("ff" * 32, 16))
        with self.assertRaises(ValueError):
            xor_distance("00", "0")

    def test_peer_table_insert_duplicate_self_and_ranking(self):
        local = "00" * 32
        table = PeerTable(local)
        peers = [
            ("11" * 32, "127.0.0.1", 9001),
            ("22" * 32, "127.0.0.1", 9002),
            ("33" * 32, "127.0.0.1", 9003),
        ]
        for peer_id, host, port in peers:
            self.assertTrue(table.insert(peer_id, host, port))
        self.assertFalse(table.insert(local, "127.0.0.1", 9000))
        self.assertTrue(table.insert(peers[0][0], "127.0.0.1", 9004))
        self.assertEqual(len(table.peers), 3)
        ranked = table.rank("10" * 32)
        self.assertEqual(ranked[0][0], "11" * 32)

    def test_peer_is_removed_after_three_failed_probes(self):
        peer_id = "11" * 32
        table = PeerTable("00" * 32)
        self.assertTrue(table.insert(peer_id, "127.0.0.1", 9001))
        self.assertFalse(table.mark_failure(peer_id))
        self.assertFalse(table.mark_failure(peer_id))
        self.assertTrue(table.mark_failure(peer_id))
        self.assertNotIn(peer_id, table.peers)

    def test_invalid_discovery_message_is_ignored(self):
        async def main():
            async with MeshNode() as node:
                node._transport.sendto(
                    b'{"type":"PEERS_FOUND","node_id":"bad","peers":"wrong"}',
                    ("127.0.0.1", node.port),
                )
                await asyncio.sleep(0.02)
                self.assertEqual(len(node.peers), 0)
        run(main())

    def test_three_nodes_propagate_peers_bootstrapping_through_b(self):
        async def main():
            async with MeshNode(gossip_interval=3600) as a:
                async with MeshNode(
                    bootstrap_nodes=[("127.0.0.1", a.port)],
                    gossip_interval=3600,
                ) as b:
                    await asyncio.sleep(0.05)
                    async with MeshNode(
                        bootstrap_nodes=[("127.0.0.1", b.port)],
                        gossip_interval=3600,
                    ) as c:
                        await asyncio.sleep(0.15)
                        self.assertIn(b.node_id, a.peers)
                        self.assertIn(a.node_id, b.peers)
                        self.assertIn(b.node_id, c.peers)
                        self.assertIn(c.node_id, b.peers)
                        self.assertIn(c.node_id, a.peers)
        run(main())


class TestGossip(unittest.TestCase):
    def test_local_resource_metrics_are_collected_and_valid(self):
        async def main():
            async with MeshNode(gossip_interval=3600) as node:
                message = await node.gossip.collect_local()
                self.assertIn(message["node_id"], node.gossip.records)
                record = node.gossip.records[message["node_id"]]
                self.assertGreaterEqual(record.cpu_percent, 0)
                self.assertLessEqual(record.cpu_percent, 100)
                self.assertGreaterEqual(record.ram_percent, 0)
                self.assertLessEqual(record.ram_percent, 100)
        run(main())

    def test_two_nodes_exchange_metrics_and_duplicate_is_ignored(self):
        async def main():
            async with MeshNode(gossip_interval=3600) as a:
                async with MeshNode(gossip_interval=3600) as b:
                    message = await a.gossip.collect_local()
                    await b.ping("127.0.0.1", a.port)
                    await asyncio.sleep(0.10)
                    self.assertIn(a.node_id, b.gossip.records)
                    self.assertFalse(b.gossip.update(message))
        run(main())

    def test_three_nodes_and_expiration(self):
        async def main():
            async with MeshNode(gossip_interval=0.05, stale_after=1.0) as a:
                async with MeshNode(
                    bootstrap_nodes=[("127.0.0.1", a.port)],
                    gossip_interval=0.05,
                    stale_after=1.0,
                ) as b:
                    async with MeshNode(
                        bootstrap_nodes=[("127.0.0.1", b.port)],
                        gossip_interval=0.05,
                        stale_after=1.0,
                    ) as c:
                        nodes = (a, b, c)
                        node_ids = {node.node_id for node in nodes}

                        async def wait_for_convergence():
                            while not all(
                                node_ids.issubset(node.gossip.get_all_resources())
                                for node in nodes
                            ):
                                await asyncio.sleep(0.02)

                        await asyncio.wait_for(wait_for_convergence(), timeout=2.0)
                        c.gossip.records[c.node_id].timestamp = 0
                        self.assertEqual(c.gossip.get_resource(c.node_id), None)
        run(main())

    def test_refresh_removes_unreachable_peer(self):
        async def main():
            a = MeshNode(
                gossip_interval=3600,
                refresh_interval=0.02,
                peer_timeout=0.03,
            )
            b = MeshNode(gossip_interval=3600, refresh_interval=0)
            await a.start()
            await b.start()
            try:
                await b.ping("127.0.0.1", a.port)
                self.assertIn(b.node_id, a.peer_table.peers)
                await b.stop()

                async def wait_for_removal():
                    while b.node_id in a.peer_table.peers:
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(wait_for_removal(), timeout=1.0)
                self.assertNotIn(b.node_id, a.peers)
            finally:
                await a.stop()
                await b.stop()

    def test_gossip_stops_with_node_and_task_execution_survives(self):
        async def main():
            async with TaskNode(gossip_interval=3600) as server:
                async with TaskNode(
                    bootstrap_nodes=[("127.0.0.1", server.port)],
                    gossip_interval=3600,
                ) as client:
                    self.assertFalse(client.gossip._task.done())
                    result = await client.submit(
                        "127.0.0.1", server.port, lambda x: x * 2, 21
                    )
                    self.assertEqual(result, 42)
            self.assertTrue(client.gossip._task is None)
        run(main())


class TestRouter(unittest.TestCase):
    def test_selects_lowest_cpu_then_ram_from_fresh_records(self):
        now = 100.0
        peers = SimpleNamespace(
            interval=5.0,
            records={
                "higher-cpu": ResourceRecord(
                    "higher-cpu", 20.0, 10.0, now, 1, "message-1"
                ),
                "higher-ram": ResourceRecord(
                    "higher-ram", 10.0, 40.0, now, 1, "message-2"
                ),
                "best": ResourceRecord(
                    "best", 10.0, 20.0, now, 1, "message-3"
                ),
                "stale": ResourceRecord(
                    "stale", 0.0, 0.0, now - 16.0, 1, "message-4"
                ),
            },
        )

        with patch("meshweaver.router.time.time", return_value=now):
            self.assertEqual(select_best_node(peers), "best")

    def test_returns_none_when_every_record_is_stale(self):
        now = 100.0
        peers = SimpleNamespace(
            interval=5.0,
            records={
                "stale": ResourceRecord(
                    "stale", 1.0, 1.0, now - 16.0, 1, "message-1"
                )
            },
        )

        with patch("meshweaver.router.time.time", return_value=now):
            self.assertIsNone(select_best_node(peers))


class TestRemoteExecution(unittest.TestCase):
    def test_fixed_port_start_failure_closes_udp_transport(self):
        async def main():
            blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            blocker.bind(("127.0.0.1", 0))
            blocker.listen()
            port = blocker.getsockname()[1]
            node = TaskNode(port=port)
            try:
                with self.assertRaises(OSError):
                    await node.start()
                self.assertIsNone(node._transport)
                self.assertIsNone(node._server)
            finally:
                await node.stop()
                blocker.close()
        run(main())

    def test_repeated_start_and_stop_are_idempotent(self):
        async def main():
            node = TaskNode(gossip_interval=3600)
            await node.start()
            server = node._server
            await node.start()
            self.assertIs(node._server, server)
            await node.stop()
            await node.stop()
            self.assertIsNone(node._server)
            self.assertIsNone(node._transport)
        run(main())

    def test_execute_lambda_remotely(self):
        async def main():
            async with TaskNode() as server, TaskNode() as client:
                r = await client.submit("127.0.0.1", server.port, lambda a, b: a * b, 6, 7)
                self.assertEqual(r, 42)
                self.assertEqual(server.tasks_executed, 1)
        run(main())

    def test_execute_function_with_closure_and_kwargs(self):
        async def main():
            factor = 3
            def scale(xs, *, offset=0):
                return [x * factor + offset for x in xs]
            async with TaskNode() as server, TaskNode() as client:
                r = await client.submit("127.0.0.1", server.port, scale, [1, 2, 3], offset=1)
                self.assertEqual(r, [4, 7, 10])
        run(main())

    def test_execute_async_function(self):
        async def main():
            async def slow_add(a, b):
                await asyncio.sleep(0.05)
                return a + b
            async with TaskNode() as server, TaskNode() as client:
                self.assertEqual(await client.submit("127.0.0.1", server.port, slow_add, 2, 3), 5)
        run(main())

    def test_remote_exception_propagates(self):
        async def main():
            async with TaskNode() as server, TaskNode() as client:
                with self.assertRaises(RemoteTaskError) as ctx:
                    await client.submit("127.0.0.1", server.port, lambda: 1 / 0)
                self.assertIn("ZeroDivisionError", str(ctx.exception))
        run(main())

    def test_tampered_payload_rejected_by_receiver(self):
        async def main():
            async with TaskNode() as server, TaskNode() as client:
                payload, digest = serialize_task(lambda: "should never run")
                bad = payload[:-1] + bytes([payload[-1] ^ 0xFF])
                with self.assertRaises(IntegrityError):
                    await client._submit_raw("127.0.0.1", server.port, bad, digest)
                self.assertEqual(server.tasks_executed, 0)
        run(main())

    def test_large_payload(self):
        async def main():
            data = list(range(200_000))
            async with TaskNode() as server, TaskNode() as client:
                self.assertEqual(await client.submit("127.0.0.1", server.port, sum, data), sum(data))
        run(main())

    def test_concurrent_tasks(self):
        async def main():
            async with TaskNode() as server, TaskNode() as client:
                rs = await asyncio.gather(*[
                    client.submit("127.0.0.1", server.port, lambda n: n * n, i) for i in range(20)
                ])
                self.assertEqual(rs, [i * i for i in range(20)])
        run(main())

    def test_mesh_and_task_share_one_port(self):
        async def main():
            async with TaskNode() as a, TaskNode() as b:
                self.assertIsNotNone(await a.ping("127.0.0.1", b.port))
                self.assertEqual(await a.submit("127.0.0.1", b.port, str.upper, "ok"), "OK")
        run(main())


if __name__ == "__main__":
    unittest.main(verbosity=2)
