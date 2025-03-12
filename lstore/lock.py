import threading

class Lock:
    def __init__(self):
        self.read_count = 0
        self.write_count = 0
        # self.mutux = threading.Lock()
        self.shared_lock = threading.RLock()
        self.exclusive_lock = threading.Lock()

    def acquire_read_lock(self):
        # pseudocode from milestone 3 slides
        self.shared_lock.acquire()
        if self.write_count == 0:
            self.read_count += 1
            self.exclusive_lock.release()
            return True
        else:
            self.shared_lock.release()
            return False
    
    def acquire_write_lock(self):
        # pseudocode from milestone 3 slides
        self.exclusive_lock.acquire()
        if self.write_count == 0 and self.read_count == 0:
            self.write_count += 1
            self.shared_lock.release()
            return True
        else:
            self.exclusive_lock.release()
            return False
        
    def release_read_lock(self):
        self.read_count -= 1
        self.shared_lock.release()

    def release_write_lock(self):
        self.write_count -= 1
        self.exclusive_lock.release()
