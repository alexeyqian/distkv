NODE_ID = "node1"
PORT = 8001

SELF_URL = "http://localhost:8001"
PEERS = [
    "http://localhost:8002",
    "http://localhost:8003",
]

# QUORUM_R + QUORUM_W > REPLICATION_FACTOR to ensure consistency (for single key)
REPLICATION_FACTOR = 3
QURORUM_W = 2
QURORUM_R = 2

def get_all_nodes():
    return PEERS.append(SELF_URL)

def get_peers():
    return PEERS

def get_self_url():
    return SELF_URL