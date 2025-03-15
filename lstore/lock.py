"""
Lock class for Two-Phase Locking (2PL) protocol.
This is a simplified wrapper around our more complete LockManager.
"""
from .lock_manager import LockManager

# Global lock manager instance to be shared across the system
global_lock_manager = LockManager()

class Lock:
    """
    A simplified lock interface that uses the global lock manager 
    to provide record-level locking.
    """
    
    def __init__(self):
        self.transaction_id = None
        
    def acquire_read(self, rid, transaction_id):
        """
        Acquire a shared (read) lock on the record
        """
        success = global_lock_manager.acquire_shared(rid, transaction_id)
        if success:
            self.transaction_id = transaction_id
        return success
    
    def acquire_write(self, rid, transaction_id):
        """
        Acquire an exclusive (write) lock on the record
        """
        success = global_lock_manager.acquire_exclusive(rid, transaction_id)
        if success:
            self.transaction_id = transaction_id
        return success
    
    def release(self, rid, transaction_id):
        """
        Release any lock held by the transaction on the record
        """
        global_lock_manager.release_lock(rid, transaction_id)
        if self.transaction_id == transaction_id:
            self.transaction_id = None
        
    def release_all(self, transaction_id):
        """
        Release all locks held by the transaction
        """
        global_lock_manager.release_all_locks(transaction_id)
        if self.transaction_id == transaction_id:
            self.transaction_id = None
