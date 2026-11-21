"""Demonstrate Week 2 peer discovery and resource gossip."""

import asyncio

from meshweaver import TaskNode


async def main():
    async with TaskNode(gossip_interval=5.0) as a:
        print(f"A {a.node_id[:12]} listening on port {a.port}")
        async with TaskNode(
            bootstrap_nodes=[("127.0.0.1", a.port)],
            gossip_interval=5.0,
        ) as b:
            print(f"B {b.node_id[:12]} bootstrapped through A on port {b.port}")
            async with TaskNode(
                bootstrap_nodes=[("127.0.0.1", b.port)],
                gossip_interval=5.0,
            ) as c:
                print(f"C {c.node_id[:12]} bootstrapped through B on port {c.port}")
                await asyncio.sleep(0.5)

                for name, node in (("A", a), ("B", b), ("C", c)):
                    print(
                        f"{name} peers:",
                        ", ".join(peer_id[:12] for peer_id in node.peers) or "(none)",
                    )

                print("Resource table:")
                for name, node in (("A", a), ("B", b), ("C", c)):
                    resources = node.gossip.get_all_resources()
                    print(
                        name,
                        [
                            (
                                node_id[:12],
                                round(record.cpu_percent, 1),
                                round(record.ram_percent, 1),
                            )
                            for node_id, record in resources.items()
                        ],
                    )

                await asyncio.sleep(5.0)


if __name__ == "__main__":
    asyncio.run(main())
