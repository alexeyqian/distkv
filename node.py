from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import asyncio

import config
from store import KVStore
import eventual
from hinted_handoff import HintedHandoff
from gossip import GossipNode
from hash_ring import ConsistentHashRing

app = FastAPI()

store = KVStore()
ring = ConsistentHashRing(config.get_all_nodes(), virtual_nodes=5)
hinted_handoff = HintedHandoff(config.get_self_url())
gossip = GossipNode(config.get_self_url(), config.get_peers())

class PutRequest(BaseModel):
    key: str
    value: str
    vc: dict | None = None
    
class ReplicateRequest(BaseModel):
    key: str
    value: str
    timestamp: int

@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(hinted_handoff.replay_loop())
    asyncio.create_task(gossip.gossip_loop())

@app.get('/hints')
def get_hints():
    return {'hints': hinted_handoff.hints}

# client write - PUT -> owner node -> replicas
# data ownership defines coordination authority, this is very important distributed systems principle.
@app.post('/put')
async def put(req: PutRequest):
    # delegate to eventual consistency implementation; pass hinted_handoff and gossip for integration
    return await eventual.handle_put(req, store, ring, config, hinted_handoff=hinted_handoff, gossip=gossip)

# internal replication
@app.post('/replicate')
async def replicate(req: dict):
    return await eventual.handle_replicate(req, store)

# read GET -> owner node -> replicas
@app.get('/get/{key}')
async def get(key: str):
    return await eventual.handle_get(key, store, ring, config, gossip=gossip)

# this avoids recursive quorum calls
@app.get("/local_get/{key}")
def local_get(key: str):
    return eventual.handle_local_get(key, store)

@app.post('/gossip')
async def gossip_endpoint(req:dict):
    sender = req['from']

    #mark sender alive
    gossip.mark_alive(sender)
    #merge membership tables
    gossip.merge(req['members'])
    return {'members': gossip.members}

@app.get('/members')
def get_members():
    return gossip.members