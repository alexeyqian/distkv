# distkv

A simple distributed system demonstration implemented in Python.

This project provides core building blocks commonly used in distributed systems:

Features:
Consistent hashing ✅
Replication ✅
Vector clocks ✅
Quorum R/W ✅
Read repair ✅
Ownership-based routing ✅

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
