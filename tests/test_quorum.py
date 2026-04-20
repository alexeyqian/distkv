import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import pytest

import quorum


def test_merge_versions_basic():
    v_old = {'value': 'old', 'vc': {'a': 1}}
    v_new = {'value': 'new', 'vc': {'a': 2}}

    merged = quorum.merge_versions([[v_old], [v_new]])
    assert merged == [v_new]


def test_merge_versions_concurrent():
    v1 = {'value': 'v1', 'vc': {'a': 1}}
    v2 = {'value': 'v2', 'vc': {'b': 1}}

    merged = quorum.merge_versions([[v1], [v2]])
    # both versions are concurrent and should be kept
    assert set(v['value'] for v in merged) == {'v1', 'v2'}


def test_merge_versions_duplicates_and_order():
    v1 = {'value': 'v1', 'vc': {'a': 1}}
    v2 = {'value': 'v2', 'vc': {'a': 1}}
    # identical clocks considered equal -> deduplication not performed by algorithm
    merged = quorum.merge_versions([[v1], [v2]])
    # both kept because compare returns 0 for equal clocks
    assert len(merged) == 2


def test_read_repair_invokes_send(monkeypatch):
    # Prepare a merged state and responses where only node C is stale
    v = {'value': 'x', 'vc': {'a': 1}}
    merged = [v]
    responses = [
        {'versions': [v]},
        {'versions': [v]},
        {'versions': [{'value': 'old', 'vc': {'a': 0}}]},
    ]
    nodes = ['http://A', 'http://B', 'http://C']

    calls = []

    async def fake_send(node, payload, client=None):
        calls.append((node, payload))
        class R: pass
        return R()

    monkeypatch.setattr(quorum, 'send_replicate_request', fake_send)

    asyncio.run(quorum.read_repair('k', merged, responses, nodes))

    # only C should be called, once per merged version (1)
    assert calls == [
        ('http://C', {'key': 'k', 'value': 'x', 'vc': {'a': 1}})
    ]


def test_read_repair_skips_none_and_continues_on_error(monkeypatch):
    v1 = {'value': 'x', 'vc': {'a': 1}}
    v2 = {'value': 'y', 'vc': {'b': 1}}
    merged = [v1, v2]
    responses = [None, {'versions': [{'value': 'stale', 'vc': {'a': 0}}]}]
    nodes = ['http://A', 'http://B']

    calls = []

    async def flaky_send(node, payload, client=None):
        # raise for node B first call to simulate network failure
        if node == 'http://B' and payload['value'] == 'x':
            raise Exception('boom')
        calls.append((node, payload))
        return None

    monkeypatch.setattr(quorum, 'send_replicate_request', flaky_send)

    # Should not raise despite one send raising; other sends still attempt
    asyncio.run(quorum.read_repair('k', merged, responses, nodes))

    # Only calls for node B for both merged entries may be recorded for the successful one
    # First payload raised and not appended; second payload appended
    assert ('http://B', {'key': 'k', 'value': 'y', 'vc': {'b': 1}}) in calls
