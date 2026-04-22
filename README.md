# distkv

A simple distributed system demonstration implemented in Python.

This project provides core building blocks commonly used in distributed systems:

Features:
Consistent hashing ✅
Replication ✅
Vector clocks ✅
Quorum R/W (prevents bad writes) ✅
Read repair (fixes on read) ✅
Ownership-based routing ✅
Hinted handoff (fixes during downtime) ✅
Gossip failure detection ✅

Current system (Dynamo-style like Amazon DynamoDB):
- Any side of a partition can accept writes
- Each side evolves independently
- Conflicts are resolved later (vector clocks)
👉 Result: split-brain is allowed by design
Consistency ❌
Availability ✅
eventually consistent + self-healing + failure-aware system

(
   Raft avoids split-brain by refusing to operate without a majority, and what it gives up is availability during partitions.
   Partition → stop if no majority
   Consistency ✅
   Availability ❌

   Split-brain is not prevented by magic.
   It’s prevented by: ❗ refusing to act without enough agreement
)

AP mindeset: (allow split-brain, like dynamodb, cassandra)
system must always respond
fix inconsistencies later

CP mindset: (avoid split-brain, such as etcd and ZooKeeper)
system must always be correct
refuse to respond if uncertain

KVStore:
- append-only log (like real systems)
- in-memory index
- recovery on restart
- simple compaction (optional)

Additional modules:

- store.py, node.py: minimal storage and node abstractions
- tests/: unit tests (pytest) — includes tests for VectorClock

Getting started

1. Install pytest (if needed):
   pip install pytest

2. Run tests:
   pytest -q

License

See LICENSE file in the repository.

Some Details:
1. What is Hinted Handoff?
When a replica is down:
Write → intended node is unavailable
→ store "hint" locally
→ replay later when node recovers

what is merkle tree sync?
what is ataptive quorum?
Add network partition simulator
add backpressure / rate limiting
change network protocol?

