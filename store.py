import os
import json
import threading
from vector_clock import VectorClock

class KVStore:
    def __init__(self, file_path='kv.log'):
        self.file_path = file_path
        self.lock = threading.Lock()
        self.data = {} # key -> list of versions
        
        # open file in append+read mode, create if not exists
        self.file = open(self.file_path, 'a+', buffering=1)
        self._load() # rebuild state from disk

    def put(self, key, value, vc: VectorClock):
        new_version = {'value': value, 'vc': vc}
        if key not in self.data:
            self.data[key] = [new_version]
            return
        
        new_versions = []
        conflict = False
        for existing in self.data[key]:
            cmp = VectorClock.compare(vc, existing['vc'])
            if cmp == 1:
                #new dominates old -> drop old
                continue
            elif cmp == -1:
                # old dominates -> ignore new
                return
            else:
                #conflict 
                conflict = True
                new_versions.append(existing)

        new_versions.append(new_version)
        self.data[key] = new_versions
    
    def get(self, key):
        return self.data.get(key, [])