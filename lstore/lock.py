from lstore.lock_manager import lockEntry
import threading

class Lock:
    def __init__(self):
        self.read_count = 0
        self.write_count = 0
        self.mutux = threading.Lock()
        # self.shared_lock = threading.RLock()
        # self.exclusive_lock = threading.Lock()

    def acquire_read_lock(self):
        # pseudocode from milestone 3 slides
        self.mutex.acquire()
        if self.write_count == 0:
            self.read_count += 1
            self.mutex.release()
            return True
        else:
            self.mutex.release()
            return False
    
    def acquire_write_lock(self):
        # pseudocode from milestone 3 slides
        self.mutex.acquire()
        if self.write_count == 0 and self.read_count == 0:
            self.write_count += 1
            self.mutex.release()
            return True
        else:
            self.mutex.release()
            return False
        
    def release_read_lock(self):
        self.read_count -= 1
        self.mutex.release()

    def release_write_lock(self):
        self.write_count -= 1
        self.mutex.release()
