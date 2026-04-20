import asyncio
import httpx
import time

MAX_HINTS = 1000

# Write → intended node is unavailable
# → store "hint" locally
# → replay later when node recovers
# for example: node a: up, node b: up, node c: down, when n=3
# a->ok, b->ok, c->fail+hint
#later when c comes back, handoff.replay() -> send missed writes
class HintedHandoff:
    def __init__(self, self_url):
        self.self_url = self_url
        #todo: in-memory to lost on restart, can persist to disk or use embedded db like sqlite
        self.hints = [] #in-memory queue(can persist later)

    def add_hint(self, target_node, key, value, vc):
        if len(self.hints) >= MAX_HINTS:
            # simple eviction policy: drop oldest hint
            self.hints.pop(0)

        self.hints.append({
            'target': target_node,
            'key': key,
            'value': value,
            'vc': vc,
            'timestamp': time.time()
        })

    async def replay_loop(self, interval=3):
        while True:
            await asyncio.sleep(interval)
            await self.replay()

    async def replay(self):
        if not self.hints:
            return

        remaining = []
        async with httpx.AsyncClient() as client:
            for hint in self.hints:
                try:
                    await client.post(
                        f"{hint['target']}/replicate",
                        json={
                            'key': hint['key'],
                            'value': hint['value'],
                            'vc': hint['vc']
                        },
                        timeout=5.0
                    )
                except Exception:
                    # still failed -> keep it
                    remaining.append(hint)
        self.hints = remaining