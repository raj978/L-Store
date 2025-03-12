from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.num_queries_executed = 0
        self.lock_entries = [] # loop through Lock Manager, if the Lock matches then delete
        self.rids_inserted = []
        self.rids_updated = []
        self.deleted = []
        self.table = None
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting
        self.table = table

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # append recently added lock
            self.lock_entries.append(self.table.recently_added_lock_entry)

            # for insert abort
            if not self.table.recently_inserted_rid:
                self.rids_inserted.append(self.table.recently_inserted_rid.copy())
                self.table.recently_inserted_rid = None
            
            # for update abort
            if not self.table.recently_updated_rid:
                self.rids_updated.append(self.table.recently_updated_rid.copy())
                self.table.recently_updated_rid = None

            # for delete abort
            if not self.table.deleted_columns:
                self.deleted.append(self.table.deleted_columns.copy())
                self.table.deleted_columns = None
            
            self.num_queries_executed += 1
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # release all locks just acquired
        for lock_entry in self.lock_entries:
            if lock_entry in self.table.lock_manager.locks.keys():
                self.table.lock_manager.release_lock(lock_entry)

        # roll-back
        i = len(self.queries)-1
        while self.num_queries_executed > 0:
            query, args = self.queries[i]
            new_query = Query(self.table)
            if query == query.insert:
                new_query.delete(self.rids_inserted.pop(len(self.rids_inserted)-1))
            elif query == query.update:
                new_query.delete(self.rids_updated.pop(len(self.rids_updated)-1))
            elif query == query.delete:
                version = len(self.deleted[len(self.deleted)-1])-1
                new_query.insert(self.deleted[len(self.deleted)-1][version])
                version -= 1
                while not self.deleted:
                    new_query.update(self.deleted[len(self.deleted)-1][version])
                    version -= 1
                self.deleted.pop(len(self.deleted)-1)

            self.num_queries_executed -= 1
            i -= 1
        self.run()
        return False

    
    def commit(self):
        # TODO: commit to database

        # release all locks
        for lock_entry in self.lock_entries:
            if lock_entry in self.table.lock_manager.locks.keys():
                self.table.lock_manager.release_lock(lock_entry)

        return True

