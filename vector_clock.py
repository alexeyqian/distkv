class VectorClock:
    def __init__(self, clock=None):
        self.clock = clock or {}
    
    def increment(self, node_id):
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def update(self, other):
        for node, val in other.items():
            self.clock[node] = max(self.clock.get(node, 0), val)
    
    def copy(self):
        return dict(self.clock)
    
    @staticmethod
    def compare(vc1, vc2):
        """
        returns:
        1 -> vc1 > vc2 (happended after)
        -1 -> vc1 < vc2 (happended before)
        0 -> concurrent
        """
        greater = False
        less = False
        keys = set(vc1.keys()) | set(vc2.keys())
        for k in keys:
            v1 = vc1.get(k, 0)
            v2 = vc2.get(k, 0)
            if v1 > v2:
                greater = True
            elif v1 < v2:
                less = True
        if greater and not less:
            return 1
        elif less and not greater:
            return -1
        else:
            return 0