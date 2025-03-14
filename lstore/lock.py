import threading

class Lock:
    """
    A simple lock class that allows
    multiple readers or a single writer
    to access a resource at a time.
    """
    def __init__(self):
        self._access_lock = threading.Lock()
        self._active_readers = 0
        self._is_writing = False

    def acquire_read_lock(self):
        """
        Acquire a read lock on the resource.
        If a write lock is held, this will
        block until the write lock is released.
        
        Returns:
            bool: True if the read lock was acquired, False otherwise.
        """
        with self._access_lock:
            if self._is_writing:
                return False
            self._active_readers += 1
            return True
    
    def acquire_write_lock(self):
        """
        Acquire a write lock on the resource.
        If any other locks are held, this will
        block until all locks are released.
        
        Returns:
            bool: True if the write lock was acquired, False otherwise.
        """
        with self._access_lock:
            if self._is_writing or self._active_readers > 0:
                return False
            self._is_writing = True
            return True
        
    def release_read_lock(self):
        """
        Release a read lock on the resource.
        """
        with self._access_lock:
            self._active_readers = max(0, self._active_readers - 1)

    def release_write_lock(self):
        """
        Release a write lock on the resource.
        """
        with self._access_lock:
            self._is_writing = False
