import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import time

import hinted_handoff


class FakeAsyncClient:
    # shared call log to make assertions easier even if instances differ
    calls = []

    def __init__(self, fail_targets=None, record=None):
        self.fail_targets = set(fail_targets or [])

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, timeout=None):
        # extract host portion (simple)
        target = url.split('/replicate')[0]
        FakeAsyncClient.calls.append((target, json))
        if target in self.fail_targets:
            raise Exception('network failure')
        class R:
            status_code = 200
        return R()


def test_add_hint_and_eviction(monkeypatch):
    hh = hinted_handoff.HintedHandoff('http://self')
    # shrink MAX_HINTS for test
    monkeypatch.setattr(hinted_handoff, 'MAX_HINTS', 3)

    hh.add_hint('A', 'k1', 'v1', {'a': 1})
    hh.add_hint('B', 'k2', 'v2', {'a': 1})
    hh.add_hint('C', 'k3', 'v3', {'a': 1})
    hh.add_hint('D', 'k4', 'v4', {'a': 1})

    # oldest (A) should be evicted, remaining are B,C,D
    assert len(hh.hints) == 3
    assert [h['target'] for h in hh.hints] == ['B', 'C', 'D']
    # timestamps present and are floats
    for h in hh.hints:
        assert 'timestamp' in h
        assert isinstance(h['timestamp'], float)


def test_replay_clears_successful(monkeypatch):
    hh = hinted_handoff.HintedHandoff('http://self')
    hh.add_hint('http://A', 'k', 'v', {'a': 1})
    hh.add_hint('http://B', 'k', 'v', {'a': 1})

    FakeAsyncClient.calls = []
    Fake = lambda: FakeAsyncClient(fail_targets=set())
    monkeypatch.setattr(hinted_handoff.httpx, 'AsyncClient', Fake)

    asyncio.run(hh.replay())

    # all hints succeeded and cleared
    assert hh.hints == []
    # ensure two posts recorded
    assert len(FakeAsyncClient.calls) == 2
    assert FakeAsyncClient.calls[0][0] == 'http://A'
    assert FakeAsyncClient.calls[1][0] == 'http://B'


def test_replay_keeps_failed(monkeypatch):
    hh = hinted_handoff.HintedHandoff('http://self')
    hh.add_hint('http://A', 'k1', 'v1', {'a': 1})
    hh.add_hint('http://C', 'k2', 'v2', {'a': 1})

    FakeAsyncClient.calls = []
    # make C fail
    Fake = lambda: FakeAsyncClient(fail_targets={'http://C'})
    monkeypatch.setattr(hinted_handoff.httpx, 'AsyncClient', Fake)

    asyncio.run(hh.replay())

    # hint for C should remain, A removed
    assert len(hh.hints) == 1
    assert hh.hints[0]['target'] == 'http://C'
    # record contains attempted posts
    assert any(r[0] == 'http://A' for r in FakeAsyncClient.calls)
    assert any(r[0] == 'http://C' for r in FakeAsyncClient.calls)


def test_replay_no_hints_returns():
    hh = hinted_handoff.HintedHandoff('http://self')
    # should not raise
    asyncio.run(hh.replay())
