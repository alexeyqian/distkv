from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import asyncio

import config
from store import KVStore
from vector_clock import VectorClock
from hash_ring import ConsistentHashRing
from quorum import merge_versions, read_repair
from hinted_handoff import HintedHandoff

app = FastAPI()

store = KVStore()
ring = ConsistentHashRing(config.get_all_nodes(), virtual_nodes=5)
hinted_handoff = HintedHandoff(config.get_self_url())

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

@app.get('/hints')
def get_hints():
    return {'hints': hinted_handoff.hints}

# client write - PUT -> owner node -> replicas
# data ownership defines coordination authority, this is very important distributed systems principle.
@app.post('/put')
async def put(req: PutRequest):
    """
    Handles a put request in a distributed key-value store with vector clock versioning.
    If the request has no vector clock, creates a new one (client request).
    Otherwise, uses the provided vector clock (forwarded request).
    Increments the vector clock for the current node.
    Routes the request to the correct node(s) based on consistent hashing.
    If this node is not responsible, forwards to the first target node.
    Otherwise, performs local write and replicates to other target nodes.
    Args:
        req (PutRequest): The put request containing key, value, and optional vector clock.
    Returns:
        dict: Response with status 'ok' and the vector clock used for the write.
    """
    # if this is a client request -> generate timestamp
    vc = VectorClock(req.vc or {})
    # is it a problem if we increment the clock before routing? maybe not, since the client will get the updated vc and can use it for causality tracking.
    vc.increment(config.NODE_ID)
    
    target_nodes = ring.get_n_nodes(req.key, config.REPLICATION_FACTOR)

    # forward to correct node
    # Only responsible nodes coordinate the write
    # ONLY certain nodes "own" a key for consistent hasing.
    if config.get_self_url() not in target_nodes:
        async with httpx.AsyncClient() as client:
            # improve later: Instead of always forwarding to target_nodes[0]:
            # fallback coordinator if the first one is down, or even better: forward to all target nodes and use the first successful response as coordinator.
            return await client.post(
                f"{target_nodes[0]}/put",
                json={
                    'key': req.key,
                    'value': req.value,
                    'vc': vc.copy()})

    # local write
    store.put(req.key, req.value, vc.copy())
    
    async def write_to(node):
        try:
            async with httpx.AsyncClient() as client:
                return await client.post(
                    f"{node}/replicate",
                    json={
                        'key': req.key,
                        'value': req.value,
                        'vc': vc.copy()})
        except:
            hinted_handoff.add_hint(node, req.key, req.value, vc.copy())
            return None
    #replicate to others
    tasks = [write_to(node) for node in target_nodes]
    results = await asyncio.gather(*tasks)
    success = sum(1 for r in results if r is not None)
    if success >= config.QURORUM_W:
        return {'status': 'ok', 'acks': success}
    else:
        return {'status': 'fail', 'acks': success}

# internal replication
@app.post('/replicate')
async def replicate(req: dict):
    store.put(req['key'], req['value'], req['vc'])
    return {'status': 'replicated'}

# read GET -> owner node -> replicas
@app.get('/get/{key}')
async def get(key: str):
    target_nodes = ring.get_n_nodes(key, config.REPLICATION_FACTOR)
    # enforce coordinator ownership
    if config.get_self_url() not in target_nodes:
        # forward to one of the responsible nodes
        async with httpx.AsyncClient() as client:
            for node in target_nodes:
                try:
                    return await client.get(f"{node}/get/{key}")
                except:
                    continue
            return {'error': "no availble coordinator"}

    # this node is a coordinator, read locally and from other replicas, then merge versions
    async def read_from(node):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{node}/local_get/{key}")
                return resp.json()
        except:
            return None
    tasks = [read_from(node) for node in target_nodes]
    results = await asyncio.gather(*tasks)
    responses = [r for r in results if r is not None]
    #quorum check
    if(len(responses) < config.QUORUM_R):
        return {'error': 'not enough replicas available'}
    # merge versions using vector clock
    merged = merge_versions([r['versions'] for r in responses])
    # read repair (only coordinator does this)
    await read_repair(key, merged, responses, target_nodes)
    return {'versions': merged}

# this avoids recursive quorum calls
@app.get("/local_get/{key}")
def local_get(key: str):
    return {'versions': store.get(key)}