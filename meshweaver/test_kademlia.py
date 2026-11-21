"""Week 2 / Day 1 tests for the Kademlia building blocks.

Run from the project root:  python -m meshweaver.test_kademlia
"""

import random
import unittest

from meshweaver.kademlia import (
    ID_BITS, K, Contact, KBucket, RoutingTable,
    bucket_index, distance, generate_node_id,
)


def make_contact(node_id, port=9000):
    return Contact(node_id, "127.0.0.1", port)


class TestIdsAndDistance(unittest.TestCase):
    def test_id_is_160_bit(self):
        for _ in range(50):
            nid = generate_node_id()
            self.assertGreaterEqual(nid, 0)
            self.assertLess(nid, 2 ** ID_BITS)

    def test_seeded_id_is_deterministic(self):
        self.assertEqual(generate_node_id("node-a"), generate_node_id("node-a"))
        self.assertNotEqual(generate_node_id("node-a"), generate_node_id("node-b"))

    def test_random_ids_differ(self):
        self.assertNotEqual(generate_node_id(), generate_node_id())

    def test_distance_properties(self):
        a, b, c = generate_node_id(), generate_node_id(), generate_node_id()
        self.assertEqual(distance(a, a), 0)
        self.assertEqual(distance(a, b), distance(b, a))
        # XOR metric satisfies the triangle inequality
        self.assertLessEqual(distance(a, c), distance(a, b) + distance(b, c))

    def test_bucket_index_known_values(self):
        self.assertEqual(bucket_index(0, 1), 0)
        self.assertEqual(bucket_index(0, 2), 1)
        self.assertEqual(bucket_index(0, 3), 1)
        self.assertEqual(bucket_index(0, 4), 2)
        self.assertEqual(bucket_index(0, 2 ** 159), 159)
        self.assertEqual(bucket_index(0b1010, 0b1000), 1)

    def test_bucket_index_same_id_raises(self):
        with self.assertRaises(ValueError):
            bucket_index(5, 5)


class TestKBucket(unittest.TestCase):
    def test_add_until_full(self):
        b = KBucket(k=3)
        for i in range(1, 4):
            self.assertIsNone(b.add(make_contact(i)))
        self.assertEqual(len(b), 3)

    def test_full_bucket_returns_least_recently_seen(self):
        b = KBucket(k=3)
        for i in (1, 2, 3):
            b.add(make_contact(i))
        oldest = b.add(make_contact(4))
        self.assertEqual(oldest.node_id, 1)
        self.assertEqual(len(b), 3)
        self.assertNotIn(4, b)  # newcomer was not stored

    def test_re_adding_moves_to_most_recent(self):
        b = KBucket(k=3)
        for i in (1, 2, 3):
            b.add(make_contact(i))
        self.assertIsNone(b.add(make_contact(1)))  # refresh, no duplicate
        self.assertEqual(len(b), 3)
        self.assertEqual([c.node_id for c in b], [2, 3, 1])
        # now 2 is the least recently seen
        self.assertEqual(b.add(make_contact(9)).node_id, 2)

    def test_remove_then_add(self):
        b = KBucket(k=2)
        b.add(make_contact(1))
        b.add(make_contact(2))
        self.assertTrue(b.remove(1))
        self.assertFalse(b.remove(1))
        self.assertIsNone(b.add(make_contact(3)))
        self.assertEqual([c.node_id for c in b], [2, 3])

    def test_refresh_keeps_latest_address(self):
        b = KBucket(k=3)
        b.add(Contact(1, "10.0.0.1", 1111))
        b.add(Contact(1, "10.0.0.2", 2222))
        self.assertEqual(b.contacts[0].address, ("10.0.0.2", 2222))


class TestRoutingTable(unittest.TestCase):
    def test_own_id_is_ignored(self):
        rt = RoutingTable(100)
        self.assertIsNone(rt.add_contact(make_contact(100)))
        self.assertEqual(len(rt), 0)

    def test_contacts_land_in_correct_bucket(self):
        rt = RoutingTable(0, k=20)
        for nid in (1, 2, 3, 4, 7, 8):
            rt.add_contact(make_contact(nid))
        self.assertEqual(len(rt), 6)
        self.assertEqual(sorted(rt._buckets), [0, 1, 2, 3])
        self.assertEqual(len(rt._buckets[1]), 2)  # ids 2, 3
        self.assertEqual(len(rt._buckets[2]), 2)  # ids 4, 7

    def test_full_bucket_signals_ping_candidate(self):
        rt = RoutingTable(0, k=2)
        rt.add_contact(make_contact(4))
        rt.add_contact(make_contact(5))
        stale = rt.add_contact(make_contact(6))  # all in bucket 2
        self.assertEqual(stale.node_id, 4)
        self.assertEqual(len(rt), 2)

    def test_remove_contact_and_get_contact(self):
        rt = RoutingTable(0)
        rt.add_contact(make_contact(5))
        self.assertEqual(rt.get_contact(5).node_id, 5)
        self.assertTrue(rt.remove_contact(5))
        self.assertIsNone(rt.get_contact(5))
        self.assertFalse(rt.remove_contact(5))
        self.assertEqual(len(rt), 0)
        self.assertEqual(rt._buckets, {})  # empty bucket cleaned up

    def test_find_closest_matches_brute_force(self):
        rng = random.Random(1234)
        own = rng.getrandbits(ID_BITS)
        rt = RoutingTable(own, k=K)
        ids = {rng.getrandbits(ID_BITS) for _ in range(300)}
        for nid in ids:
            rt.add_contact(make_contact(nid))
        stored = {c.node_id for c in rt.all_contacts()}
        for _ in range(20):
            target = rng.getrandbits(ID_BITS)
            got = [c.node_id for c in rt.find_closest(target, count=10)]
            want = sorted(stored, key=lambda n: n ^ target)[:10]
            self.assertEqual(got, want)

    def test_find_closest_respects_count_and_exclude(self):
        rt = RoutingTable(0)
        for nid in range(1, 11):
            rt.add_contact(make_contact(nid))
        self.assertEqual(len(rt.find_closest(5, count=3)), 3)
        self.assertEqual(rt.find_closest(5, count=1)[0].node_id, 5)
        self.assertNotIn(5, [c.node_id for c in rt.find_closest(5, exclude=5)])
        self.assertEqual(len(rt.find_closest(5)), 10)  # fewer than k known

    def test_find_closest_empty_table(self):
        self.assertEqual(RoutingTable(0).find_closest(123), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
