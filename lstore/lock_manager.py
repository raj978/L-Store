import threading

class LockManager:
    """
    Lock Manager class that implements Two-Phase Locking (2PL) for transaction isolation.
    Uses a no-wait policy: if a lock cannot be acquired, the transaction aborts immediately.
    """
    def __init__(self):
        # Dict of record_id -> lock_info
        # lock_info = {
        #   'shared': set of transaction IDs with shared locks
        #   'exclusive': transaction ID with exclusive lock or None
        #   'lock': threading.Lock() for synchronizing access to this record's lock data
        # }
        self.locks = {}
        # For synchronizing access to self.locks
        self.manager_lock = threading.Lock()
        
    def _get_lock_info(self, record_id):
        """
        Get or create a lock_info entry for the given record_id
        """
        with self.manager_lock:
            if record_id not in self.locks:
                self.locks[record_id] = {
                    'shared': set(),
                    'exclusive': None,
                    'lock': threading.Lock()
                }
            return self.locks[record_id]
    
    def acquire_shared(self, record_id, transaction_id):
        """
        Attempt to acquire a shared (read) lock on the specified record for the transaction.
        Returns True if the lock was acquired, False otherwise (no-wait policy).
        """
        lock_info = self._get_lock_info(record_id)
        
        with lock_info['lock']:
            # If there's an exclusive lock held by another transaction, fail immediately
            if lock_info['exclusive'] is not None and lock_info['exclusive'] != transaction_id:
                return False
            
            # If this transaction already has an exclusive lock, it can also read
            if lock_info['exclusive'] == transaction_id:
                return True
                
            # Grant the shared lock
            lock_info['shared'].add(transaction_id)
            return True
    
    def acquire_exclusive(self, record_id, transaction_id):
        """
        Attempt to acquire an exclusive (write) lock on the specified record for the transaction.
        Returns True if the lock was acquired, False otherwise (no-wait policy).
        """
        lock_info = self._get_lock_info(record_id)
        
        with lock_info['lock']:
            # Check if another transaction holds a lock (shared or exclusive)
            if (lock_info['exclusive'] is not None and lock_info['exclusive'] != transaction_id) or \
               (len(lock_info['shared']) > 0 and (len(lock_info['shared']) > 1 or transaction_id not in lock_info['shared'])):
                return False
            
            # If this transaction already has the exclusive lock, nothing to do
            if lock_info['exclusive'] == transaction_id:
                return True
            
            # If this transaction has a shared lock, upgrade to exclusive
            if transaction_id in lock_info['shared']:
                lock_info['shared'].remove(transaction_id)
                
            # Grant the exclusive lock
            lock_info['exclusive'] = transaction_id
            return True
    
    def release_lock(self, record_id, transaction_id):
        """
        Release any locks (shared or exclusive) held by the transaction on the record.
        """
        with self.manager_lock:
            if record_id not in self.locks:
                return
                
        lock_info = self.locks[record_id]
        
        with lock_info['lock']:
            # Release shared lock if held
            if transaction_id in lock_info['shared']:
                lock_info['shared'].remove(transaction_id)
            
            # Release exclusive lock if held
            if lock_info['exclusive'] == transaction_id:
                lock_info['exclusive'] = None
                
            # Clean up empty lock entries
            if len(lock_info['shared']) == 0 and lock_info['exclusive'] is None:
                with self.manager_lock:
                    # Double-check in case another thread acquired a lock in the meantime
                    if record_id in self.locks and \
                       len(self.locks[record_id]['shared']) == 0 and \
                       self.locks[record_id]['exclusive'] is None:
                        del self.locks[record_id]
    
    def release_all_locks(self, transaction_id):
        """
        Release all locks held by the transaction (called during commit/abort).
        """
        with self.manager_lock:
            record_ids = list(self.locks.keys())
            
        for record_id in record_ids:
            self.release_lock(record_id, transaction_id)
            
    def has_lock(self, record_id, transaction_id):
        """
        Check if the transaction has any lock on the record.
        """
        with self.manager_lock:
            if record_id not in self.locks:
                return False
                
        lock_info = self.locks[record_id]
        
        with lock_info['lock']:
            return transaction_id in lock_info['shared'] or lock_info['exclusive'] == transaction_id


# Keep the old lockEntry class for backward compatibility
class lockEntry:
    def __init__(self, rid, lock):
        self.rid = rid
        self.lockInfo = lock

# Keep a simplified version of the old lock_manager for backward compatibility
class lock_manager:
    def __init__(self):
        self.manager = []
        self.curIndex = 0
        self.lock_manager = LockManager()  # Use the new implementation internally
        
    def insert(self, rid, lock):
        self.manager.append(lockEntry(rid, lock))
        self.curIndex = len(self.manager) - 1
        
    def search(self, rid):
        for i in range(len(self.manager)):
            if self.manager[i].rid == rid:
                self.curIndex = i
                return True
        return False
        
    def get_lock(self, rid):
        for i in range(len(self.manager)):
            if self.manager[i].rid == rid:
                self.curIndex = i
                return self.manager[i].lockInfo
        return None
        
    def _search(self, rid):
        for i in range(len(self.manager)):
            if self.manager[i].rid == rid:
                return self.manager[i]
        return False
        
    def remove(self, rid):
        curNode = self._search(rid)
        if curNode != False:
            self.manager.remove(curNode)


