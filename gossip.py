import random
import time
import asyncio
import httpx

class GossipNode:
    def __init__(self, self_url, peers):
        self.self_url = self_url
        self.peers = peers
        
        #membership table
        #node -> {status, last_seen}
        self.members = {
            self_url: {'status': 'alive', 'last_seen': time.time()}
        }
    
    def mark_alive(self, node):
        self.members[node] = {'status': 'alive', 'last_seen': time.time()}
        
    def mark_suspect(self, node):
        if node in self.members:
            self.members[node]['status'] = 'suspect'
            
    def mark_dead(self, node):
        if node in self.members:
            self.members[node]['status'] = 'dead'

    def get_alive_nodes(self):
        return [
            node for node, meta in self.members.items()
            if meta['status'] == 'alive'
        ]

    async def gossip_loop(self, interval=1):
        while True:
            await asyncio.sleep(interval)
            await self.gossip_once()
            self.detect_failures()
            
    async def gossip_once(self):
        if not self.peers:
            return
        peer = random.choice(self.peers)
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{peer}/gossip", json={
                    'from': self.self_url,
                    'members': self.members
                })
                data = resp.json()
                self.merge(data['members'])
        except:
            # failed contact -> mark suspect
            self.mark_suspect(peer)

    def merge(self, incoming):
        for node, meta in incoming.items():
            if node not in self.members:
                self.members[node] = meta
            else:
                # take fresher info
                if meta['last_seen'] > self.members[node]['last_seen']:
                    self.members[node] = meta

    def detect_failures(self, timeout=5):
        now = time.time()
        for node, meta in self.members.items():
            if node == self.self_url:
                continue
            if now - meta['last_seen'] > timeout:
                self.mark_dead(node)