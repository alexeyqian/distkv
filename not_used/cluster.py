import httpx

class Cluster:
    def __init__(self, peers):
        self.peers = peers # list of URLs
        
    async def replicate(self, key, value, timestamp):
        async with httpx.AsyncClient() as client:
            for peer in self.peers:
                try:
                    await client.post(f"{peer}/replicate", json={
                        'key': key, 'value': value, 'timestamp': timestamp})
                except Exception:
                    pass #ignore failed nodes
    