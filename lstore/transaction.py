from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from lstore.bufferpool import Bufferpool

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.lock_entries = [] # array of lock entries
        # loop through Lock Manager, if the Lock matches then delete
        self.table = None
        self.old_table = None
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
        # save state of table before transaction
        bufferpool = Bufferpool(table.bufferpool.path)
        bufferpool.frames = list(table.bufferpool.frames)
        bufferpool.numFrames = table.bufferpool.numFrames
        bufferpool.frame_directory = list(table.bufferpool.frame_directory)
        bufferpool.frame_info = list(table.bufferpool.frame_info)
        bufferpool.page_ranges = dict(table.bufferpool.page_ranges)
        bufferpool.path = table.bufferpool.path
        self.old_table = Table(table.name, table.num_columns, table.key, bufferpool, False, table.path)
        self.old_table.page_directory = dict(table.page_directory)
        self.old_table.num_pageRanges = table.num_pageRanges
        self.old_table.page_range_index = table.page_range_index
        self.old_table.base_page_index = table.base_page_index
        self.old_table.record_id = table.record_id
        self.old_table.base_page_frame_index = table.base_page_frame_index
        self.old_table.tail_page_frame_index = table.tail_page_frame_index
        self.old_table.merge_thread = table.merge_thread
        self.old_table.path = table.path
        self.old_table.index = Index(self.old_table)

        self.table = table
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # append recently added lock
            self.locks.append(self.table.lock_manager.locks.keys()[len(self.table.lock_manager.locks)-1])
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # release all locks just acquired
        for lock_entry in self.lock_entries:
            if lock_entry in self.table.lock_manager.locks.values():
                del self.table.lock_manager.locks[lock_entry]
        for query in self.queries:
            query.table = self.old_table
        self.run()
        return False

    
    def commit(self):
        # TODO: commit to database

        # release all locks
        for lock_entry in self.lock_entries:
            if lock_entry in self.table.lock_manager.locks.values():
                del self.table.lock_manager.locks[lock_entry]

        return True

