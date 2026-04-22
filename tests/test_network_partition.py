import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import asyncio

import gossip
import hinted_handoff


def test_partition_membership_detection():
    # three nodes: A, B, C
    A = 'http://A'
    B = 'http://B'
    C = 'http://C'

    nodeA = gossip.GossipNode(A, [B, C])
    nodeB = gossip.GossipNode(B, [A, C])
    nodeC = gossip.GossipNode(C, [A, B])

    now = time.time()

    # simulate partition where A is isolated: from A's view B and C were last seen long ago
    nodeA.members[B] = {'status': 'alive', 'last_seen': now - 100}
    nodeA.members[C] = {'status': 'alive', 'last_seen': now - 100}

    # from B and C's view they see each other as alive recently, but A old
    nodeB.members[C] = {'status': 'alive', 'last_seen': now}
    nodeB.members[A] = {'status': 'alive', 'last_seen': now - 100}

    nodeC.members[B] = {'status': 'alive', 'last_seen': now}
    nodeC.members[A] = {'status': 'alive', 'last_seen': now - 100}

    # run failure detection with small timeout
    nodeA.detect_failures(timeout=5)
    nodeB.detect_failures(timeout=5)
    nodeC.detect_failures(timeout=5)

    # A should mark B and C dead (from A's perspective)
    assert nodeA.members[B]['status'] == 'dead'
    assert nodeA.members[C]['status'] == 'dead'

    # B and C should mark A dead, but see each other alive
    assert nodeB.members[A]['status'] == 'dead'
    assert nodeC.members[A]['status'] == 'dead'
    assert nodeB.members[C]['status'] == 'alive'
    assert nodeC.members[B]['status'] == 'alive'


class FakeAsyncClient:
    def __init__(self, fail_targets=None):
        self.fail_targets = set(fail_targets or [])

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, timeout=None):
        target = url.split('/replicate')[0]
        if target in self.fail_targets:
            raise Exception('network failure')
        class R:
            status_code = 200
        return R()


def test_hinted_handoff_partition_and_recovery(monkeypatch):
    hh = hinted_handoff.HintedHandoff('http://self')

    # add two hints for nodes that will be unreachable during partition
    hh.add_hint('http://B', 'k1', 'v1', {'a': 1})
    hh.add_hint('http://C', 'k2', 'v2', {'a': 1})

    # during partition both B and C unreachable
    monkeypatch.setattr(hinted_handoff.httpx, 'AsyncClient', lambda: FakeAsyncClient(fail_targets={'http://B', 'http://C'}))

    # replay should keep both hints
    asyncio.run(hh.replay())
    assert len(hh.hints) == 2

    # now network recovers: no failures
    monkeypatch.setattr(hinted_handoff.httpx, 'AsyncClient', lambda: FakeAsyncClient(fail_targets=set()))

    asyncio.run(hh.replay())
    # hints should be cleared
    assert hh.hints == []
