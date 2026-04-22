import asyncio
import httpx

from vector_clock import VectorClock
from quorum import merge_versions, read_repair

async def handle_put(req, store, ring, config, hinted_handoff=None, gossip=None):
    """Eventual-mode handler for client put requests.

    Accepts optional hinted_handoff and gossip objects so the handler can
    integrate with failure detection and hint storage. This keeps the core
    algorithm encapsulated and allows swapping the whole module later.
    """
    # build vector clock (client may have supplied one)
    vc = VectorClock(req.vc or {})
    vc.increment(config.NODE_ID)

    # if gossip is available, filter target nodes to currently alive ones
    all_targets = ring.get_n_nodes(req.key, config.REPLICATION_FACTOR)
    if gossip is not None:
        alive_nodes = set(gossip.get_alive_nodes())
        target_nodes = [n for n in all_targets if n in alive_nodes]
        # if filtering removed all targets, fall back to all_targets
        if not target_nodes:
            target_nodes = all_targets
    else:
        target_nodes = all_targets

    # if this node is not a responsible coordinator, forward to first target
    if config.get_self_url() not in target_nodes:
        async with httpx.AsyncClient() as client:
            return await client.post(
                f"{target_nodes[0]}/put",
                json={'key': req.key, 'value': req.value, 'vc': vc.copy()}
            )

    # perform local write
    store.put(req.key, req.value, vc.copy())

    async def write_to(node):
        try:
            async with httpx.AsyncClient() as client:
                return await client.post(
                    f"{node}/replicate",
                    json={'key': req.key, 'value': req.value, 'vc': vc.copy()}
                )
        except Exception:
            # if hinted_handoff is available and node appears dead, add hint
            if hinted_handoff is not None:
                # prefer using gossip if available to determine liveness
                if gossip is None or node not in gossip.get_alive_nodes():
                    hinted_handoff.add_hint(node, req.key, req.value, vc.copy())
            return None

    tasks = [write_to(node) for node in target_nodes]
    results = await asyncio.gather(*tasks)
    success = sum(1 for r in results if r is not None)

    if success >= config.QUORUM_W:
        return {'status': 'ok', 'acks': success}
    else:
        return {'status': 'fail', 'acks': success}


async def handle_replicate(req, store):
    """Eventual-mode internal replication endpoint."""
    store.put(req['key'], req['value'], req['vc'])
    return {'status': 'replicated'}


async def handle_get(key, store, ring, config, gossip=None):
    """Eventual-mode handler for reads: coordinate, collect from replicas,
    merge versions and perform read-repair."""
    all_targets = ring.get_n_nodes(key, config.REPLICATION_FACTOR)

    # prefer alive nodes when gossip is available
    if gossip is not None:
        alive_nodes = set(gossip.get_alive_nodes())
        target_nodes = [n for n in all_targets if n in alive_nodes]
        if not target_nodes:
            target_nodes = all_targets
    else:
        target_nodes = all_targets

    if config.get_self_url() not in target_nodes:
        async with httpx.AsyncClient() as client:
            for node in target_nodes:
                try:
                    return await client.get(f"{node}/get/{key}")
                except Exception:
                    continue
            return {'error': 'no available coordinator'}

    async def read_from(node):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{node}/local_get/{key}")
                return resp.json()
        except Exception:
            return None

    tasks = [read_from(node) for node in target_nodes]
    results = await asyncio.gather(*tasks)
    responses = [r for r in results if r is not None]

    if len(responses) < config.QUORUM_R:
        return {'error': 'not enough replicas available'}

    merged = merge_versions([r['versions'] for r in responses])
    await read_repair(key, merged, responses, target_nodes)
    return {'versions': merged}


def handle_local_get(key, store):
    return {'versions': store.get(key)}
