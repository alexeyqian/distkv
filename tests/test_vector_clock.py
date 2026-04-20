import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest

from vector_clock import VectorClock


def test_init_empty():
    vc = VectorClock()
    assert vc.clock == {}


def test_increment_new_and_existing():
    vc = VectorClock()
    vc.increment('node1')
    assert vc.clock == {'node1': 1}
    vc.increment('node1')
    assert vc.clock['node1'] == 2
    vc.increment('node2')
    assert vc.clock['node2'] == 1


def test_update_merges_max_values():
    vc = VectorClock({'a': 1, 'b': 2})
    vc.update({'a': 3, 'c': 1})
    assert vc.clock == {'a': 3, 'b': 2, 'c': 1}

    # update with smaller values shouldn't decrease existing counts
    vc.update({'a': 1, 'b': 1, 'd': 0})
    assert vc.clock == {'a': 3, 'b': 2, 'c': 1, 'd': 0}


def test_copy_returns_snapshot():
    vc = VectorClock({'x': 5})
    snap = vc.copy()
    assert isinstance(snap, dict)
    assert snap == {'x': 5}

    # mutating the snapshot must not affect original
    snap['x'] = 100
    assert vc.clock['x'] == 5

    # mutating original after snapshot must not affect the snapshot
    vc.increment('x')
    assert snap['x'] == 100


def test_compare_equal_and_dominance_and_concurrent():
    # empty clocks considered equal (0)
    assert VectorClock.compare({}, {}) == 0

    # equal clocks
    assert VectorClock.compare({'a': 1}, {'a': 1}) == 0

    # vc1 dominates vc2
    assert VectorClock.compare({'a': 2, 'b': 1}, {'a': 1, 'b': 1}) == 1

    # vc1 is dominated by vc2
    assert VectorClock.compare({'a': 1, 'b': 1}, {'a': 2, 'b': 1}) == -1

    # concurrent (some greater, some less)
    assert VectorClock.compare({'a': 2, 'b': 1}, {'a': 1, 'b': 2}) == 0

    # missing keys treated as 0 -> concurrent when disjoint single increments
    assert VectorClock.compare({'a': 1}, {'b': 1}) == 0


def test_compare_with_floats_and_non_integers():
    # compare should work with numeric types that support ordering
    assert VectorClock.compare({'a': 1.5}, {'a': 1}) == 1


def test_update_with_none_raises():
    vc = VectorClock()
    with pytest.raises(AttributeError):
        # update expects a mapping with .items()
        vc.update(None)
