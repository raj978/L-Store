from lstore.lock import Lock

class LockManager:
    """
    A simple lock manager class that allows
    multiple readers or a single writer
    to access a resource at a time.
    """
    def __init__(self):
        self._lock_table = {}
        
    def acquire_read_lock(self, key):
        """
        Acquire a read lock on a key.
        If the key does not exist in the lock table,
        create a new lock for the key.
        
        Args:
            key (str): The key to acquire a read lock on.
        """
        if key not in self._lock_table:
            self._lock_table[key] = Lock()
        self._lock_table[key].acquire_read_lock()
    
    def acquire_write_lock(self, key):
        """
        Acquire a write lock on a key.
        If the key does not exist in the lock table,
        create a new lock for the key.
        
        Args:
            key (str): The key to acquire a write lock on.
        """
        if key not in self._lock_table:
            self._lock_table[key] = Lock()
        self._lock_table[key].acquire_write_lock()
    
    def release_read_lock(self, key):
        """
        Release a read lock on a key.
        
        Args:
            key (str): The key to release a read lock on.
        """
        if key in self._lock_table:
            self._lock_table[key].release_read_lock()
    
    def release_write_lock(self, key):
        """
        Release a write lock on a key.
        
        Args:
            key (str): The key to release a write lock on.
        """
        if key in self._lock_table:
            self._lock_table[key].release_write_lock()
    
    def get_lock_table(self):
        """
        Get the lock table.
        
        Returns:
            dict: The lock table.
        """
        return self._lock_table

    def delete_lock(self, key):
        """
        Delete a lock from the lock table.
        
        Args:
            key (str): The key of the lock to delete.
        """
        if key in self._lock_table:
            del self._lock_table[key]
            
    def has_key(self, key):
        """
        Check if the key is in the lock table.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the key is in the lock table, False otherwise.
        """
        return key in self._lock_table
