"""Ping a node and run a function on it.   python demo_client.py 9001"""
import asyncio
import sys

from meshweaver import TaskNode


def fib(n):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


async def main():
    peer_port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    async with TaskNode() as me:
        rtt = await me.ping("127.0.0.1", peer_port)
        print("ping:", f"{rtt * 1000:.2f} ms" if rtt else "no reply")
        result = await me.submit("127.0.0.1", peer_port, fib, 90)
        print("fib(90) computed remotely =", result)


if __name__ == "__main__":
    asyncio.run(main())
