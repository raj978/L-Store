from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import Lock
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.read_locks = set()
        self.write_locks = set()
        self.insert_locks = set()
        self.target_table = None
        self.aborted = False

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.target_table is None:
            self.target_table = table

    def run(self):
        if self.aborted:
            return False
            
        if not self.acquire_locks():
            return self.abort()
            
        try:
            return self.commit()
        except Exception as e:
            print(f"Transaction failed: {e}")
            return self.abort()

    def abort(self):
        self.aborted = True
        self.release_locks()
        return False

    def commit(self):
        try:
            for query, args in self.queries:
                key = args[0]
                result = query(*args)
                if result is False:  # If any query fails, abort the transaction
                    return self.abort()
                if query == Query.delete:
                    self.target_table.lock_manager.delete_lock(key)
                    self.write_locks.discard(key)
                    self.insert_locks.discard(key)
            
            self.release_locks()
            return True
        except Exception as e:
            print(f"Commit failed: {e}")
            return self.abort()

    def acquire_locks(self):
        """
        Acquire locks for all queries in the transaction
        """
        acquired_locks = set()
        try:
            for query, args in self.queries:
                key = args[0]
                
                # If it is an insert operation, it needs a write lock
                if not self.target_table.lock_manager.has_key(key):
                    if key not in self.insert_locks:
                        self.insert_locks.add(key)
                        acquired_locks.add(('insert', key))
                        
                # If it is an update or delete operation, it needs a write lock
                elif key not in self.write_locks and key not in self.insert_locks:
                    if not self.target_table.lock_manager.acquire_write_lock(key):
                        # If lock acquisition fails, rollback all acquired locks
                        self._rollback_locks(acquired_locks)
                        return False
                    self.write_locks.add(key)
                    acquired_locks.add(('write', key))
                    
                # If it is a select operation, it needs a read lock
                elif query == Query.select and key not in self.write_locks and key not in self.read_locks:
                    if not self.target_table.lock_manager.acquire_read_lock(key):
                        self._rollback_locks(acquired_locks)
                        return False
                    self.read_locks.add(key)
                    acquired_locks.add(('read', key))
                    
            return True
        except Exception as e:
            print(f"Lock acquisition failed: {e}")
            self._rollback_locks(acquired_locks)
            return False

    def _rollback_locks(self, acquired_locks):
        """
        Rollback locks that have been acquired
        
        :param acquired_locks: A set of locks that have been acquired
        """
        for lock_type, key in acquired_locks:
            if lock_type == 'read':
                self.target_table.lock_manager.release_read_lock(key)
                self.read_locks.discard(key)
            elif lock_type == 'write':
                self.target_table.lock_manager.release_write_lock(key)
                self.write_locks.discard(key)
            elif lock_type == 'insert':
                self.insert_locks.discard(key)

    def release_locks(self):
        """
        Release all locks that have been acquired
        """
        try:
            for key in self.read_locks:
                self.target_table.lock_manager.release_read_lock(key)
            for key in self.write_locks:
                self.target_table.lock_manager.release_write_lock(key)
            for key in self.insert_locks:
                self.target_table.lock_manager.release_write_lock(key)
            
            # Clear lock sets
            self.read_locks.clear()
            self.write_locks.clear()
            self.insert_locks.clear()
        except Exception as e:
            print(f"Lock release failed: {e}")
            # Clear lock sets
            self.read_locks.clear()
            self.write_locks.clear()
            self.insert_locks.clear()
