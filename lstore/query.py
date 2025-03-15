from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import Lock
from datetime import datetime
import numpy as np
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key, transaction_id=None):
        """
        # Delete a record with specified primary key
        # Returns True upon successful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """
        try:
            primary_key_column = 0 
            rid = self.table.index.locate(primary_key_column, primary_key)
            if rid is None:
                # No record found with the given primary_key
                return False
            
            # Convert to tuple if it's a list
            if isinstance(rid, list):
                rid = tuple(rid)
                
            # If a transaction_id is provided, acquire an exclusive lock on the record
            if transaction_id is not None:
                lock = Lock()
                if not lock.acquire_write(rid, transaction_id):
                    # Could not acquire lock - another transaction has it
                    print(f"Could not acquire lock for record {rid} by transaction {transaction_id}")
                    return False
                    
            # Ensure rid is in page_directory before proceeding
            if rid not in self.table.page_directory:
                # Try alternate formats if direct lookup fails
                found = False
                
                # Try with a tuple if it's a list
                if isinstance(rid, list):
                    t_rid = tuple(rid)
                    if t_rid in self.table.page_directory:
                        rid = t_rid
                        found = True
                
                # If still not found, try with a list if it's a tuple
                elif isinstance(rid, tuple):
                    l_rid = list(rid)
                    if l_rid in self.table.page_directory:
                        rid = l_rid
                        found = True
                        
                if not found:
                    print(f"RID {rid} not found in page_directory")
                    
                    # Release lock if we had acquired one
                    if transaction_id is not None:
                        lock.release(rid, transaction_id)
                        
                    return False
                
            rid_info = self.table.page_directory[rid]
            frame_index = self.table.bufferpool.load_base_page(rid_info[0], rid_info[1], self.table.num_columns, self.table.name)
            key_directory = (rid_info[0], rid_info[1], 'b')
            self.table.bufferpool.frames[frame_index].indirection[rid_info[2]] = [0,0,0, 'd']
            
            # Mark the record as deleted in the page_directory
            # Keep the entry so other transactions know the RID exists but is deleted
            
            # If this is part of a transaction, the lock will be released when the transaction commits or aborts
            
            return True
        except Exception as e:
            print(f"Error in delete: {e}")
            # Release lock if there was an error and we had acquired one
            if transaction_id is not None:
                try:
                    lock.release(rid, transaction_id)
                except:
                    pass
            return False
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, rollback=False):
        try:
            if rollback == True:
                start_time = 0
                schema_encoding = ''
                self.table.insertRec(start_time, schema_encoding, *columns, rollback=True)
            else:
                start_time = datetime.now().strftime("%Y%m%d%H%M%S")
                schema_encoding = '0' * self.table.num_columns  #add '0000...' for schema_encoding
                rid = self.table.insertRec(start_time, schema_encoding, *columns) #call function in Table.py to insert record
                
                # Update indices for all columns that have an index
                for i in range(min(len(columns), len(self.table.index.indices))):
                    # Check if index exists before trying to access it
                    if i < len(self.table.index.indices) and self.table.index.indices[i] is not None:
                        self.table.index.add_node(i, columns[i], rid)
            return True
        except Exception as e:
            print(f"Error in insert: {str(e)}")
            return False
        

    # For select, gives only desired columns
    def modify_columns(self, record, projected_columns_index):
        new_record = []
        for i in range(len(record.columns)):
            if projected_columns_index[i] == 1:
                new_record.append(record.columns[i])
        
        return new_record
    
    def select(self, search_key, search_key_index, projected_columns_index, transaction_id=None):
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # :param transaction_id: Optional transaction ID for lock management
        # Returns a list of Record objects upon success
        # Returns an empty list if no records found
        # Returns False if record locked by TPL
        """
        try:
            # Normal select logic
            if search_key_index >= len(self.table.index.indices) or self.table.index.indices[search_key_index] is None:
                # Create index if it doesn't exist - this is important for primary key
                self.table.index.create_index(search_key_index)
            
            rids = self.table.index.locate(search_key_index, search_key)
            if rids is None:
                return []
                
            records = []
            locks_acquired = []  # Keep track of acquired locks to release on error
            
            if isinstance(rids, tuple):
                # Single RID case
                rid = rids
                
                # Convert to tuple if it's a list for consistency
                if isinstance(rid, list):
                    rid = tuple(rid)
                    
                # Acquire shared lock if transaction_id provided
                if transaction_id is not None:
                    lock = Lock()
                    if not lock.acquire_read(rid, transaction_id):
                        # Could not acquire lock - record is write-locked by another transaction
                        return False
                    locks_acquired.append(rid)
                    
                if rid not in self.table.page_directory:
                    # Try alternate formats if direct lookup fails
                    found = False
                    
                    # Try with list format if tuple lookup failed
                    l_rid = list(rid)
                    if l_rid in self.table.page_directory:
                        rid = l_rid
                        found = True
                        
                    if not found:
                        # Release any acquired locks and return empty
                        if transaction_id is not None:
                            for acquired_rid in locks_acquired:
                                lock.release(acquired_rid, transaction_id)
                        return []
                        
                try:
                    rid_info = self.table.page_directory[rid]
                    frame_index = self.table.bufferpool.load_base_page(rid_info[0], rid_info[1], self.table.num_columns, self.table.name)
                    
                    newrid = self.table.bufferpool.frames[frame_index].indirection[rid_info[2]]
                    if newrid == [0,0,0,'d']:
                        # Record is deleted
                        # Release any acquired locks and return empty
                        if transaction_id is not None:
                            for acquired_rid in locks_acquired:
                                lock.release(acquired_rid, transaction_id)
                        return []
                        
                    TPS = [0,0]
                    record = self.table.find_record(search_key, rid_info, projected_columns_index, TPS)
                    records.append(record)
                except Exception as e:
                    print(f"Error processing record: {e}")
                    # Release any acquired locks and return empty
                    if transaction_id is not None:
                        for acquired_rid in locks_acquired:
                            lock.release(acquired_rid, transaction_id)
                    return []
            else:
                # Multiple RIDs case - could be a list of RIDs
                if not isinstance(rids, list):
                    return []
                    
                for rid in rids:
                    try:
                        # Convert list to tuple if needed
                        if isinstance(rid, list):
                            rid = tuple(rid)
                            
                        # Acquire shared lock if transaction_id provided
                        if transaction_id is not None:
                            lock = Lock()
                            if not lock.acquire_read(rid, transaction_id):
                                # Skip this record if can't acquire lock
                                continue
                            locks_acquired.append(rid)
                            
                        if rid not in self.table.page_directory:
                            # Try alternate formats if direct lookup fails
                            found = False
                            
                            # Try with list format if tuple lookup failed
                            l_rid = list(rid)
                            if l_rid in self.table.page_directory:
                                rid = l_rid
                                found = True
                                
                            if not found:
                                # Release this lock and skip record
                                if transaction_id is not None:
                                    lock.release(rid, transaction_id)
                                    locks_acquired.remove(rid)
                                continue
                            
                        rid_info = self.table.page_directory[rid]
                        frame_index = self.table.bufferpool.load_base_page(rid_info[0], rid_info[1], self.table.num_columns, self.table.name)
                        
                        newrid = self.table.bufferpool.frames[frame_index].indirection[rid_info[2]]
                        if newrid == [0,0,0,'d']:
                            # Record is deleted, skip it
                            # Release this lock
                            if transaction_id is not None:
                                lock.release(rid, transaction_id)
                                locks_acquired.remove(rid)
                            continue
                            
                        TPS = [0,0]
                        record = self.table.find_record(search_key, rid_info, projected_columns_index, TPS)
                        records.append(record)
                    except Exception as e:
                        print(f"Error processing record: {e}")
                        # Release this lock and continue with next record
                        if transaction_id is not None:
                            lock.release(rid, transaction_id)
                            if rid in locks_acquired:
                                locks_acquired.remove(rid)
                        continue
            
            # NOTE: In a proper 2PL implementation, we would NOT release the locks here
            # They must be held until the transaction commits or aborts
            # The transaction should call release_all_locks() during commit/abort
            
            return records
            
        except Exception as e:
            print(f"Select error: {e}")
            # Release any acquired locks on error
            if transaction_id is not None:
                for acquired_rid in locks_acquired:
                    try:
                        lock.release(acquired_rid, transaction_id)
                    except:
                        pass
            return []

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction_id=None):
        """
        # Read a specific version of a record with the given search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # :param relative_version: the relative version of the record you need to retrieve.
        # :param transaction_id: Optional transaction ID for lock management
        # Returns a list of Record objects upon success
        # Returns empty list if no records found
        # Returns False if record locked by TPL
        """
        try:
            rid = self.table.index.locate(search_key_index, search_key)
            if rid is None:
                # No record found with the given search_key
                return []
                
            # Convert to tuple if it's a list for consistency
            if isinstance(rid, list):
                rid = tuple(rid)
                
            # Acquire shared lock if transaction_id provided
            if transaction_id is not None:
                lock = Lock()
                if not lock.acquire_read(rid, transaction_id):
                    # Could not acquire lock - record is write-locked by another transaction
                    print(f"Could not acquire read lock for record {rid} by transaction {transaction_id}")
                    return False
                
            # Check if rid is in page_directory
            if rid not in self.table.page_directory:
                # Try alternate formats if direct lookup fails
                found = False
                
                # Try with list format if tuple lookup failed
                l_rid = list(rid)
                if l_rid in self.table.page_directory:
                    rid = l_rid
                    found = True
                    
                if not found:
                    # Release lock if acquired and return empty
                    if transaction_id is not None:
                        lock.release(rid, transaction_id)
                    return []
                    
            baseRID = rid
            frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
            rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]] # converts base page rid to tail rid if any, else remains same  
            
            # Navigate to the desired version
            while relative_version != 0: 
                if(rid[3] == 'b'):
                    if(tuple(rid) != baseRID):
                        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                else: 
                    frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                    rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                relative_version += 1
                
            records = []
            record = self.table.find_record(search_key, rid, projected_columns_index, [0,0])
            records.append(record)
            
            # Note: In strict 2PL, we would NOT release the lock here
            # The transaction object should release all locks during commit/abort
            
            return records
        except Exception as e:
            print(f"Error in select_version: {e}")
            # Release lock if there was an error and we had acquired one
            if transaction_id is not None:
                try:
                    lock.release(rid, transaction_id)
                except:
                    pass
            return []

    
    def update(self, primary_key, *columns, rollback=False, transaction_id=None):
        """
        # Update a record with specified key and columns
        # Returns True if update is succesful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        try:
            # Locate the record by primary key
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                # No record found with the given primary_key
                return False
                
            # Convert list to tuple if needed to ensure it's hashable
            if isinstance(rid, list):
                rid = tuple(rid)
                
            # If a transaction_id is provided, acquire an exclusive lock on the record
            if transaction_id is not None:
                lock = Lock()
                if not lock.acquire_write(rid, transaction_id):
                    # Could not acquire lock - another transaction has it
                    print(f"Could not acquire lock for record {rid} by transaction {transaction_id}")
                    return False
            
            # Ensure rid is in page_directory before proceeding
            if rid not in self.table.page_directory:
                # Try alternate formats if direct lookup fails
                found = False
                
                # Try with a tuple if it's a list
                if isinstance(rid, list):
                    t_rid = tuple(rid)
                    if t_rid in self.table.page_directory:
                        rid = t_rid
                        found = True
                
                # If still not found, try with a list if it's a tuple
                elif isinstance(rid, tuple):
                    l_rid = list(rid)
                    if l_rid in self.table.page_directory:
                        rid = l_rid
                        found = True
                        
                if not found:
                    print(f"RID {rid} not found in page_directory")
                    
                    # Release lock if we had acquired one
                    if transaction_id is not None:
                        lock.release(rid, transaction_id)
                        
                    return False
                
            # Add values in columns to each index
            for i in range(len(columns)):
                if columns[i] is not None and i < len(self.table.index.indices) and self.table.index.indices[i] is not None:
                    self.table.index.indices[i][columns[i]] = rid
            
            if rollback == True:
                result = self.rollBackUpdate(primary_key, *columns)
            else:    
                BaseRID = rid
                rid_info = self.table.page_directory[rid]
                result = self.table.updateRec(rid_info, BaseRID, primary_key, *columns)
            
            return result
        except Exception as e:
            print(f"Error in update: {e}")
            # Release lock if there was an error and we had acquired one
            if transaction_id is not None:
                try:
                    lock.release(rid, transaction_id)
                except:
                    pass
            return False

        pass
    
    def rollBackUpdate(self, primary_key, *columns):
        rid = self.table.index.locate(0, primary_key)
        if rid is None:
            # No record found with the given primary_key
            return False
            
        baseRID = rid
        rollBackRID = rid
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        if(rid[3] == 'b'):
            if(tuple(rid) != baseRID):
                frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                rollBackRID = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        else: 
            frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns, self.table.name)
            rollBackRID = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        
        frame_index = self.table.bufferpool.load_base_page(baseRID[0], baseRID[1], self.table.num_columns, self.table.name)
        self.table.bufferpool.frames[frame_index].indirection[baseRID[2]] = rollBackRID
        return True
        
    
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            rids = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
            if not rids:
                return 0
                
            total = 0
            for rid in rids:
                # Convert list to tuple if needed to ensure it's hashable
                if isinstance(rid, list):
                    rid = tuple(rid)
                    
                # Skip if rid is not in page_directory
                if rid not in self.table.page_directory:
                    continue
                    
                rid_info = self.table.page_directory[rid]
                frame_index = self.table.bufferpool.load_base_page(rid_info[0], rid_info[1], self.table.num_columns, self.table.name)
                key_directory = (rid_info[0], rid_info[1], 'b')
                indirectrid = self.table.bufferpool.frames[frame_index].indirection[rid_info[2]]
                
                if indirectrid[3] == 't':
                    if self.table.greaterthan(
                        self.table.bufferpool.extractTPS(key_directory, self.table.num_columns),
                        [indirectrid[1], indirectrid[2]]
                    ):
                        data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, rid_info[2])
                    else:
                        # Update frame_index using the tail page
                        frame_index = self.table.bufferpool.load_tail_page(indirectrid[0], indirectrid[1],
                                                                           self.table.num_columns, self.table.name)
                        key_directory = (indirectrid[0], indirectrid[1], 't')
                        data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, indirectrid[2])
                else:
                    data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, rid_info[2])
                    
                if data and aggregate_column_index < len(data):
                    total += data[aggregate_column_index]
                    
            return total
        except Exception as e:
            print(f"Error in sum: {str(e)}")
            return 0

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction_id=None):
        """
        # Calculate sum for a specific version of records in the given range
        # :param start_range: Start of the key range to aggregate 
        # :param end_range: End of the key range to aggregate 
        # :param aggregate_column_index: Index of desired column to aggregate
        # :param relative_version: the relative version of the record you need to retrieve
        # :param transaction_id: Optional transaction ID for lock management
        """
        try:
            rids = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
            if not rids:
                return 0
                
            sum_value = 0
            records = []
            relative_version_copy = relative_version
            locks_acquired = []  # Keep track of locks to release on error
            
            for rid in rids: 
                try:
                    # Convert list to tuple if needed for consistent handling
                    if isinstance(rid, list):
                        rid = tuple(rid)
                    
                    # Acquire shared lock if transaction_id provided
                    if transaction_id is not None:
                        lock = Lock()
                        if not lock.acquire_read(rid, transaction_id):
                            # Skip this record if we can't acquire a lock
                            continue
                        locks_acquired.append(rid)
                    
                    # Ensure rid is in page_directory
                    if rid not in self.table.page_directory:
                        # Try alternate formats if direct lookup fails
                        found = False
                        
                        # Try with list format if tuple lookup failed
                        l_rid = list(rid)
                        if l_rid in self.table.page_directory:
                            rid = l_rid
                            found = True
                            
                        if not found:
                            # Release this lock and skip record
                            if transaction_id is not None:
                                lock.release(rid, transaction_id)
                                locks_acquired.remove(rid)
                            continue
                    
                    relative_version = relative_version_copy
                    baseRID = rid
                    frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns, self.table.name)
                    rid_value = self.table.bufferpool.frames[frame_index].indirection[rid[2]] # converts base page rid to tail rid if any, else remains same  
                    
                    # Navigate to the desired version
                    while relative_version != 0: 
                        if(rid_value[3] == 'b'):
                            if(tuple(rid_value) != baseRID):
                                frame_index = self.table.bufferpool.load_base_page(rid_value[0], rid_value[1], self.table.num_columns, self.table.name)
                                rid_value = self.table.bufferpool.frames[frame_index].indirection[rid_value[2]]
                        else: 
                            frame_index = self.table.bufferpool.load_tail_page(rid_value[0], rid_value[1], self.table.num_columns, self.table.name)
                            rid_value = self.table.bufferpool.frames[frame_index].indirection[rid_value[2]]
                        relative_version += 1
                    
                    # Create a full record with all columns (use [1,1,1,1,1] to get all columns)
                    record = self.table.find_record(0, rid_value, [1] * self.table.num_columns, [0,0])
                    
                    # Only add to sum if we have a valid record and the column is within bounds
                    if record and aggregate_column_index < len(record.columns):
                        sum_value += record.columns[aggregate_column_index]
                except Exception as e:
                    print(f"Error processing record {rid} for sum_version: {e}")
                    # Release lock for this record if there was an error
                    if transaction_id is not None and rid in locks_acquired:
                        try:
                            lock.release(rid, transaction_id)
                            locks_acquired.remove(rid)
                        except:
                            pass
            
            # Note: In strict 2PL, we would NOT release the locks here
            # The transaction object should release all locks during commit/abort
            
            return sum_value
        except Exception as e:
            print(f"Error in sum_version: {e}")
            # Release all acquired locks on error
            if transaction_id is not None:
                for acquired_rid in locks_acquired:
                    try:
                        lock.release(acquired_rid, transaction_id)
                    except:
                        pass
            return 0

    
    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
