from lstore.lock import Lock

class lockEntry:
    def __init__(self, rid, lock):
        self.rid = rid
        self.lockInfo = lock

class LockManager:
    def __init__(self):
        self.locks = {} # lockEntry to Lock

    def insert_lock(self, entry):
        lock = Lock()
        self.locks[entry] = lock
        if entry.lockInfo == 's':
            self.locks[entry].acquire_read_lock()
        elif entry.lockInfo == 'x':
            self.locks[entry].acquire_write_lock()

    def release_lock(self, entry):
        if entry.lockInfo == 's':
            self.locks[entry].release_read_lock()
        elif entry.lockInfo == 'x':
            self.locks[entry].release_write_lock()
        del self.locks[entry]
