"""Start a MeshWeaver node and keep it running.   python demo_node.py 9001"""
import asyncio
import sys

from meshweaver import TaskNode


async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    async with TaskNode(port=port) as node:
        print(f"[{node.node_id}] listening on 127.0.0.1:{node.port} (UDP mesh + TCP tasks)")
        await asyncio.Event().wait()  # run until Ctrl+C


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nnode stopped")
