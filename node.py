from fastapi import FastAPI
from pydantic import BaseModel
import httpx

from store import KVStore
from not_used.cluster import Cluster
from not_used.clock import LamportClock
from vector_clock import VectorClock
from hash_ring import ConsistentHashRing
import config

app = FastAPI()

store = KVStore()
cluster = Cluster(config.get_peers())
clock = LamportClock()
ring = ConsistentHashRing(config.get_all_nodes(), virtual_nodes=5)

class PutRequest(BaseModel):
    key: str
    value: str
    vc: dict | None = None
    
class ReplicateRequest(BaseModel):
    key: str
    value: str
    timestamp: int
    
# client write
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
    if req.vc is None:
        vc = VectorClock()
    
    else: # forword ??
        vc = VectorClock(req.vc)
    
    vc.increment(config.NODE_ID)
    
    target_nodes = ring.get_n_nodes(req.key, 2) #replication factor = 2
    # forward to correct node
    if config.get_self_url() not in target_nodes:
        async with httpx.AsyncClient() as client:
            return await client.post(
                f"{target_nodes[0]}/put",
                json={
                    'key': req.key,
                    'value': req.value,
                    'vc': vc.copy()})

    # local write
    store.put(req.key, req.value, vc.copy())
    #replicate to others
    async with httpx.AsyncClient() as client:
        for node in target_nodes:
            if node != config.get_self_url():
                try:
                    await client.post(f"{node}/replicate", json={
                        'key': req.key,
                        'value': req.value,
                        'vc': vc.copy()
                    })
                except:
                    pass
    
    return {'status': 'ok', 'vc': vc.copy()}

# internal replication
@app.post('/replicate')
async def replicate(req: dict):
    store.put(req['key'], req['value'], req['vc'])
    return {'status': 'replicated'}

# read
@app.get('/get/{key}')
async def get(key: str):
    target_nodes = ring.get_n_nodes(key, 2)
    if config.get_self_url() in target_nodes:
        val = store.get(key)
        if val is not None:
            return {'versions': val}
    # falback to other nodes
    async with httpx.AsyncClient() as client:
        for node in target_nodes:
            try:
                resp = await client.get(f"{node}/get/{key}")
                if resp.json().get('versions') is not None:
                    return resp.json()
            except:
                pass
    return {'versions': None}