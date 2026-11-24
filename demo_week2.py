"""Demonstrate Week 2 peer discovery and resource gossip."""

import asyncio

from meshweaver import TaskNode


async def wait_for_resource_convergence(nodes, timeout=15.0):
    node_ids = {node.node_id for node in nodes}
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if all(node_ids.issubset(node.gossip.get_all_resources()) for node in nodes):
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(
        f"Resource gossip did not converge for all demo nodes within {timeout:.1f} seconds"
    )


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
                nodes = (a, b, c)
                await wait_for_resource_convergence(nodes)

                for name, node in zip(("A", "B", "C"), nodes):
                    print(
                        f"{name} peers:",
                        ", ".join(peer_id[:12] for peer_id in node.peers) or "(none)",
                    )

                print("Resource table:")
                for name, node in zip(("A", "B", "C"), nodes):
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


if __name__ == "__main__":
    asyncio.run(main())
