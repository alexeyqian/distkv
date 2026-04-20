import httpx
from typing import Optional

from vector_clock import VectorClock

# return a list of dicts
def merge_versions(versions_list):
    """Merge multiple replica version lists into a single list using vector clocks.

    versions_list: iterable of lists, where each inner list contains version
    dicts with keys: 'value' and 'vc' (VectorClock or mapping acceptable).
    Returns a list of non-dominated versions (conflicting concurrent versions
    are kept).
    """
    merged = []
    for versions in versions_list:
        for v in versions:
            keep = True
            for existing in list(merged):
                cmp = VectorClock.compare(v['vc'], existing['vc'])
                if cmp == 1:  # v is newer than existing, remove existing
                    try:
                        merged.remove(existing)
                    except ValueError:
                        pass
                elif cmp == -1:  # v is older than existing, ignore v
                    keep = False
                    break
            if keep:
                merged.append(v)
    return merged


async def send_replicate_request(node: str, payload: dict, client: Optional[httpx.AsyncClient] = None):
    """Send a replicate request to a replica node.

    This helper is extracted so network interactions can be mocked in tests.
    If an httpx.AsyncClient instance is provided it will be reused; otherwise
    a temporary client is created for the request.
    Returns the httpx.Response on success or raises on failure.
    """
    url = f"{node}/replicate"
    if client is not None:
        return await client.post(url, json=payload)

    async with httpx.AsyncClient() as _client:
        return await _client.post(url, json=payload)


async def read_repair(key, merged, responses, nodes):
    """Perform read-repair by updating stale replicas with the merged versions.

    This function contacts each replica (nodes) using the provided responses
    list (parallel to nodes). If a replica's versions differ from the merged
    result, this function posts replicate requests to that node for each
    version in merged. Network requests are delegated to send_replicate_request
    so tests can override or mock that function.
    """
    async with httpx.AsyncClient() as client:
        for node, resp in zip(nodes, responses):
            if resp is None:
                continue
            if resp.get('versions') != merged:
                for v in merged:
                    try:
                        await send_replicate_request(
                            node,
                            {'key': key, 'value': v['value'], 'vc': v['vc']},
                            client=client,
                        )
                    except Exception:
                        # ignore failures; best-effort repair
                        continue
