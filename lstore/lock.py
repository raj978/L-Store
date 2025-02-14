from lstore.lock_manager import lockEntry
import threading

class Lock:
    def __init__(self):
        self.read_count = 0
        self.write_count = 0
        self.mutex = threading.Lock()

    def acquire_read_lock(self):
        pass
    
    def acquire_write_lock(self):
        pass
        
    def release_read_lock(self):
        pass

    def release_write_lock(self):
        pass
