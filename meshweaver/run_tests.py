"""Week 1 tests. Run from the project root:  python -m meshweaver.run_tests"""

import asyncio
import unittest

from meshweaver import MeshNode, TaskNode, RemoteTaskError, IntegrityError
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


class TestRemoteExecution(unittest.TestCase):
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
