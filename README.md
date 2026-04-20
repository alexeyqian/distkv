# distkv

A simple distributed system demonstration implemented in Python.

This project provides two core building blocks commonly used in distributed systems:

- Vector clocks: causal versioning for tracking concurrent updates across nodes (vector_clock.py)
- Consistent hash ring: simple consistent hashing with virtual nodes for key distribution (hash_ring.py)

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
