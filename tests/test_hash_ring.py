import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from hash_ring import ConsistentHashRing


def test_get_node_empty():
    ring = ConsistentHashRing()
    assert ring.get_node('somekey') is None


def test_add_remove_idempotent_and_internal_structures():
    ring = ConsistentHashRing(virtual_nodes=4)
    ring.add_node('A')
    # adding the same node again should not create duplicates
    ring.add_node('A')
    assert 'A' in ring.nodes
    # number of distinct nodes tracked is 1
    assert len(ring.nodes) == 1
    # internal node_map should map virtual positions back to node 'A'
    assert set(ring.node_map.values()) == {'A'}

    # removing non-existent node should be a no-op
    ring.remove_node('Z')
    assert 'A' in ring.nodes

    # remove actual node
    ring.remove_node('A')
    assert 'A' not in ring.nodes
    assert ring.ring == []
    assert ring.node_map == {}


def test_get_node_and_get_n_nodes_relationship():
    ring = ConsistentHashRing(nodes=['A', 'B', 'C'], virtual_nodes=3)
    primary = ring.get_node('key1')
    # get_n_nodes should include the primary node and usually put it first
    n_list = ring.get_n_nodes('key1', 3)
    assert primary in n_list
    assert n_list[0] == primary


def test_get_n_nodes_more_than_distinct_nodes():
    ring = ConsistentHashRing(nodes=['A', 'B'], virtual_nodes=2)
    # request more replicas than distinct nodes -> should return each node once
    out = ring.get_n_nodes('mykey', 5)
    assert set(out) == {'A', 'B'}
    assert len(out) == 2


def test_get_n_nodes_single_node_and_invalid_n():
    ring = ConsistentHashRing(nodes=['Solo'], virtual_nodes=3)
    # even if asking for multiple replicas, only the single node is returned once
    assert ring.get_n_nodes('x', 3) == ['Solo']

    # invalid n (<=0) returns empty list
    assert ring.get_n_nodes('x', 0) == []
    assert ring.get_n_nodes('x', -5) == []


def test_remove_node_effect_on_replication():
    ring = ConsistentHashRing(nodes=['A','B','C'], virtual_nodes=2)
    # pick a key and ensure primary is in the replication list
    key = 'replicate-me'
    primary = ring.get_node(key)
    reps_before = ring.get_n_nodes(key, 3)
    assert primary in reps_before

    # remove the primary and ensure it's no longer returned
    ring.remove_node(primary)
    reps_after = ring.get_n_nodes(key, 3)
    assert primary not in reps_after
    # remaining nodes should still be returned (possibly fewer than requested)
    assert set(reps_after).issubset({'A','B','C'})
