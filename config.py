NODE_ID = "node1"
PORT = 8001

SELF_URL = "http://localhost:8001"
PEERS = [
    "http://localhost:8002",
    "http://localhost:8003",
]

def get_all_nodes():
    return PEERS.append(SELF_URL)

def get_peers():
    return PEERS

def get_self_url():
    return SELF_URL