import hashlib
import bisect


def hash_fn(key: str) -> int:
    """Return a stable integer hash for a string key using MD5.

    MD5 is used here for simplicity and stable ordering; in production a
    different hash (or a salted/namespace-aware variant) might be preferred.
    """
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


class ConsistentHashRing:
    """Simple consistent-hashing ring with virtual nodes.

    Implementation notes:
    - self.ring: sorted list of integer hashes representing virtual node positions
    - self.node_map: dict mapping a virtual-node hash -> original node id
    - virtual_nodes: number of virtual positions created per real node; using
      multiple virtual nodes smooths distribution across the ring.
    """

    def __init__(self, nodes=None, virtual_nodes=3):
        self.virtual_nodes = virtual_nodes
        # sorted list of hash ints (virtual node positions on the ring)
        self.ring = []
        # maps hash -> node identifier (the actual node responsible)
        self.node_map = {}
        # set of currently added nodes (idempotent add/remove operations)
        self.nodes = set()

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """Add a real node by creating its virtual node positions.

        This operation is idempotent: adding the same node again has no effect.
        """
        if node in self.nodes:
            return
        self.nodes.add(node)

        for i in range(self.virtual_nodes):
            virtual_key = f"{node}#{i}"
            h = hash_fn(virtual_key)
            # avoid overwriting/map collisions (extremely unlikely for MD5)
            if h in self.node_map:
                # skip this virtual position if collision occurred
                continue
            self.ring.append(h)
            self.node_map[h] = node

        # keep ring sorted for bisect operations
        self.ring.sort()

    def remove_node(self, node):
        """Remove a node and all its virtual positions from the ring.

        If a virtual position is missing (e.g., node wasn't present), the
        operation silently ignores it to make removal idempotent.
        """
        if node not in self.nodes:
            return
        self.nodes.remove(node)

        for i in range(self.virtual_nodes):
            virtual_key = f"{node}#{i}"
            h = hash_fn(virtual_key)
            if h in self.node_map:
                # remove hash from ring list (safe-guard against ValueError)
                try:
                    self.ring.remove(h)
                except ValueError:
                    pass
                del self.node_map[h]

        # keep ring sorted after removals
        self.ring.sort()

    def get_node(self, key):
        """Return the node responsible for the given key.

        Uses binary search (bisect) to find the first virtual node hash greater
        than the key's hash; if none found, wraps around to the first ring
        position.
        Returns None when the ring is empty.
        """
        if not self.ring:
            return None

        h = hash_fn(key)
        idx = bisect.bisect(self.ring, h)
        if idx == len(self.ring):
            idx = 0  # wrap around to the first position
        return self.node_map[self.ring[idx]]

    def get_n_nodes(self, key, n):
        """Return up to n distinct nodes (in ring order) for replication.

        The method walks the ring starting from the key's position and collects
        distinct nodes until it has n of them or has traversed all virtual
        positions (whichever comes first). This prevents infinite loops when
        there are fewer distinct nodes than requested replicas.
        """
        if not self.ring or n <= 0:
            return []

        h = hash_fn(key)
        start_idx = bisect.bisect(self.ring, h)

        result = []
        steps = 0
        ring_len = len(self.ring)

        # Walk the ring, skipping duplicate nodes, until we collected n nodes
        # or we've looked at each virtual position once.
        while len(result) < n and steps < ring_len:
            node = self.node_map[self.ring[(start_idx + steps) % ring_len]]
            if node not in result:
                result.append(node)
            steps += 1

        return result
