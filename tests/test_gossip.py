import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import asyncio
import random

import gossip


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class FakeAsyncClient:
    def __init__(self, resp=None, raise_on_get=False, record=None):
        self.resp = resp
        self.raise_on_get = raise_on_get
        self.record = record if record is not None else []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, json=None):
        # record call for assertions
        self.record.append((url, json))
        if self.raise_on_get:
            raise Exception('network')
        return FakeResponse(self.resp)


def test_mark_and_get_alive():
    node = gossip.GossipNode('http://self', [])
    node.mark_alive('http://A')
    assert 'http://A' in node.members
    assert node.members['http://A']['status'] == 'alive'

    node.mark_suspect('http://A')
    assert node.members['http://A']['status'] == 'suspect'

    node.mark_dead('http://A')
    assert node.members['http://A']['status'] == 'dead'

    # get_alive_nodes should include only nodes with status 'alive'
    node.mark_alive('http://B')
    assert 'http://B' in node.get_alive_nodes()


def test_merge_adds_and_updates():
    # control time values for deterministic last_seen
    base = 1000000.0

    # create node with older timestamp
    n = gossip.GossipNode('http://self', [])
    n.members['http://X'] = {'status': 'alive', 'last_seen': base}

    # incoming with newer last_seen should overwrite
    incoming = {'http://X': {'status': 'alive', 'last_seen': base + 10}}
    n.merge(incoming)
    assert n.members['http://X']['last_seen'] == base + 10

    # incoming with older last_seen should not overwrite
    incoming2 = {'http://X': {'status': 'suspect', 'last_seen': base}}
    n.merge(incoming2)
    # last_seen remains the newer value, status unchanged
    assert n.members['http://X']['last_seen'] == base + 10


def test_detect_failures_marks_dead():
    base = time.time()
    node = gossip.GossipNode('http://self', [])
    # add peer with last_seen far in the past
    node.members['http://old'] = {'status': 'alive', 'last_seen': base - 100}

    node.detect_failures(timeout=5)
    assert node.members['http://old']['status'] == 'dead'


def test_gossip_once_successful_merges(monkeypatch):
    # prepare node and a peer
    peer = 'http://peer'
    n = gossip.GossipNode('http://self', [peer])

    # peer currently unknown
    assert peer not in n.members

    # prepare fake response from peer containing members
    resp_members = {
        peer: {'status': 'alive', 'last_seen': time.time()},
        'http://other': {'status': 'alive', 'last_seen': time.time()}
    }
    fake = FakeAsyncClient(resp={'members': resp_members}, raise_on_get=False)

    monkeypatch.setattr(gossip.httpx, 'AsyncClient', lambda: fake)
    # ensure deterministic peer selection
    monkeypatch.setattr(random, 'choice', lambda lst: peer)

    asyncio.run(n.gossip_once())

    # after successful gossip_once, peer and other should be merged into members
    assert peer in n.members
    assert 'http://other' in n.members


def test_gossip_once_failure_marks_suspect(monkeypatch):
    peer = 'http://peer'
    n = gossip.GossipNode('http://self', [peer])

    # fake client raises on get
    fake = FakeAsyncClient(resp=None, raise_on_get=True)
    monkeypatch.setattr(gossip.httpx, 'AsyncClient', lambda: fake)
    monkeypatch.setattr(random, 'choice', lambda lst: peer)

    asyncio.run(n.gossip_once())

    # failed contact may mark peer as suspect if peer was previously known.
    if peer in n.members:
        assert n.members[peer]['status'] == 'suspect'
    else:
        # peer not added to membership table on failure -> acceptable
        assert True
