from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import Lock
import threading
import uuid

class Transaction:
    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.keys = []
        self.key_index = []
        self.table = None
        self.rollback = []
        self.queryIndex = 0
        self.lock = threading.Lock()
        self.type = None
        # Unique transaction ID - used for locking
        self.transaction_id = uuid.uuid4()
        # Lock object for handling record-level locks
        self.lock_manager = Lock()

    """
    # Adds the given query to this transaction
    """
    def add_query(self, query, table, *args):
        with self.lock:
            self.queries.append((query, args))
            self.table = table
            
            # Determine type based on the query and args
            if query.__name__ == 'insert':
                self.type = 'I'
            elif query.__name__ == 'update' and len(args) > 0:
                self.keys.append(args[0])
                self.type = 'U'
            elif query.__name__ == 'select' and len(args) > 1:
                self.keys.append(args[0])
                self.key_index.append(args[1])
                self.type = 'S'
            elif query.__name__ == 'delete' and len(args) > 0:
                self.keys.append(args[0])
                self.type = 'D'

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        try:
            with self.lock:
                if self.type == 'I':
                    for i, (query, args) in enumerate(self.queries):
                        try:
                            # Note: Insert doesn't need prior locking as it creates new records
                            # Pass parameters directly to the query
                            result = query(*args)
                            if result:
                                self.rollback.append(True)
                                self.queryIndex += 1
                            else:
                                return self.abort()
                        except Exception as e:
                            print(f"Transaction error during insert: {e}")
                            return self.abort()
                    return self.commit()
                
                elif self.type == 'U':
                    for i, (query, args) in enumerate(self.queries):
                        try:
                            if i < len(self.keys):
                                # Pass transaction_id to the update query for locking
                                args_with_txn = args + (False, self.transaction_id)
                                result = query(self.keys[i], *args_with_txn)
                                if result:
                                    self.rollback.append(True)
                                    self.queryIndex += 1
                                else:
                                    return self.abort()
                            else:
                                print(f"Warning: key index {i} out of range in update")
                                return self.abort()
                        except Exception as e:
                            print(f"Transaction error during update: {e}")
                            return self.abort()
                    return self.commit()
                
                elif self.type == 'S':
                    for i, (query, args) in enumerate(self.queries):
                        try:
                            if i < len(self.keys) and i < len(self.key_index):
                                # Pass transaction_id to the select query for locking
                                if len(args) > 0:
                                    args_with_txn = args + (self.transaction_id,)
                                else:
                                    args_with_txn = (self.transaction_id,)
                                result = query(self.keys[i], self.key_index[i], *args_with_txn)
                                if result is not False:
                                    self.queryIndex += 1
                                else:
                                    return self.abort()
                            else:
                                print(f"Warning: key or key_index {i} out of range in select")
                                return self.abort()
                        except Exception as e:
                            print(f"Transaction error during select: {e}")
                            return self.abort()
                    return self.commit()
                
                elif self.type == 'D':
                    for i, (query, args) in enumerate(self.queries):
                        try:
                            if i < len(self.keys):
                                # Pass transaction_id to the delete query for locking
                                args_with_txn = (self.transaction_id,)
                                result = query(self.keys[i], *args_with_txn)
                                if result:
                                    self.rollback.append(True)
                                    self.queryIndex += 1
                                else:
                                    return self.abort()
                            else:
                                print(f"Warning: key index {i} out of range in delete")
                                return self.abort()
                        except Exception as e:
                            print(f"Transaction error during delete: {e}")
                            return self.abort()
                    return self.commit()
                
                else:
                    print("Unknown transaction type")
                    return self.abort()
        except Exception as e:
            print(f"Unexpected transaction error: {e}")
            return self.abort()

    def abort(self):
        """
        Aborts the transaction, rolling back any changes and releasing locks
        """
        try:
            with self.lock:
                # Roll back operations if needed
                if self.type == 'I':
                    for i in reversed(range(self.queryIndex)):
                        if i < len(self.rollback) and self.rollback[i]:
                            self.queries[i][0](*self.queries[i][1], rollback=True)
                elif self.type == 'U':
                    for i in reversed(range(self.queryIndex)):
                        if i < len(self.rollback) and i < len(self.keys) and self.rollback[i]:
                            self.queries[i][0](self.keys[i], *self.queries[i][1], rollback=True)
                
                # Release all locks acquired by this transaction
                self.lock_manager.release_all(self.transaction_id)
        except Exception as e:
            print(f"Error during abort: {e}")
            # Ensure locks are released even if other abort logic fails
            try:
                self.lock_manager.release_all(self.transaction_id)
            except:
                pass
        return False
    
    def commit(self):
        """
        Commits the transaction, making all changes permanent and releasing locks
        """
        try:
            # Release all locks acquired by this transaction
            self.lock_manager.release_all(self.transaction_id)
            return True
        except Exception as e:
            print(f"Error during commit: {e}")
            return False

