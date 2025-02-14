from lstore.table import Table, Record
from lstore.index import Index
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
    def delete(self, primary_key):
        primary_key_column = 0 
        rid = self.table.index.locate(primary_key_column, primary_key)
        rid = self.table.page_directory[rid]
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
        newrid = []
        key_directory = (rid[0], rid[1], 'b')
        self.table.bufferpool.frames[frame_index].indirection[rid[2]] = [0,0,0, 'd']
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, rollback=False):
        if rollback:
            start_time = 0
            schema_encoding = ''
            self.table.insertRec(start_time, schema_encoding, *columns, rollback=True)
        else:
            start_time = datetime.now().strftime("%Y%m%d%H%M%S")
            schema_encoding = '0' * self.table.num_columns
            self.table.insertRec(start_time, schema_encoding, *columns)
            
            # Add entry to all column indices
            rid = self.table.createBP_RID()
            for i, value in enumerate(columns):
                self.table.index.add_node(i, value, rid)
                
        return True
        

    # For select, gives only desired columns
    def modify_columns(self, record, projected_columns_index):
        new_record = []
        for i in range(len(record.columns)):
            if projected_columns_index[i] == 1:
                new_record.append(record.columns[i])
        
        return new_record
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        rid = self.table.index.locate(column, key)
        if rid is None:
            return []
        rid = self.table.page_directory[rid]
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
        if frame_index is None:
            return []
            
        current_indirection = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        if current_indirection[3] == 't':
            frame_index = self.table.bufferpool.load_tail_page(current_indirection[0], current_indirection[1], self.table.num_columns)
            rid = current_indirection
        
        data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, rid[2])
        
        record = []
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                record.append(data[i])
                
        return [Record(rid, key, record)]
    

    
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
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
            rid = self.table.index.locate(search_key_index, search_key)
        # for rid in rids: 
            baseRID = rid
            frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
            rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]] # converts base page rid to tail rid if any, else remains same  
            while relative_version != 0: 
                if(rid[3] == 'b'):
                    if(tuple(rid) != baseRID):
                        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
                        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                else: 
                    frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns)
                    rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
                relative_version += 1
            records = []
        # for rid in rids:
            record = self.table.find_record(search_key, rid, projected_columns_index, [0,0])
            records.append(record)
            return records
            pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, rollback=False):
        rid = self.table.index.locate(self.table.key, primary_key)
        #Add values in columns to each index
        for i in range(len(columns)):
            self.table.index.indices[i][columns[i]] = rid
            #Need to delete the key:rid prior to update
        if(rollback == True):
            self.rollBackUpdate(primary_key, *columns)
        else:    
            BaseRID = rid
            rid = self.table.page_directory[rid]
            self.table.updateRec(rid, BaseRID, primary_key, *columns)
            return True

        pass
    
    def rollBackUpdate(self, primary_key, *columns):
        rid = self.table.index.locate(0, primary_key)
        baseRID = rid
        rollBackRID = rid
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
        rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        if(rid[3] == 'b'):
            if(tuple(rid) != baseRID):
                frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
                rollBackRID = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        else: 
            frame_index = self.table.bufferpool.load_tail_page(rid[0], rid[1], self.table.num_columns)
            rollBackRID = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        
        frame_index = self.table.bufferpool.load_base_page(baseRID[0], baseRID[1], self.table.num_columns)
        self.table.bufferpool.frames[frame_index].indirection[baseRID[2]] = rollBackRID 
        pass
        
    
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Sum records with keys in range [start_range, end_range] inclusive, in aggregate_column
        """
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return 0

        sum_result = 0
        for rid in rids:
            if rid in self.table.page_directory:
                record_rid = self.table.page_directory[rid]
                frame_index = self.table.bufferpool.load_base_page(record_rid[0], record_rid[1], self.table.num_columns)
                if frame_index is not None:
                    # Get current indirection
                    current_indirection = self.table.bufferpool.frames[frame_index].indirection[record_rid[2]]
                    # If record has been updated, use the tail record
                    if current_indirection[3] == 't':
                        frame_index = self.table.bufferpool.load_tail_page(current_indirection[0], current_indirection[1], self.table.num_columns)
                        record_rid = current_indirection
                        
                    data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, record_rid[2])
                    if data and len(data) > aggregate_column_index:
                        sum_result += data[aggregate_column_index]

        return sum_result

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if not rids:
            return 0

        sum_result = 0
        for rid in rids:
            if rid in self.table.page_directory:
                record_rid = self.table.page_directory[rid]
                frame_index = self.table.bufferpool.load_base_page(record_rid[0], record_rid[1], self.table.num_columns)
                if frame_index is not None:
                    version = relative_version
                    current_rid = record_rid
                    
                    while version < 0:
                        current_indirection = self.table.bufferpool.frames[frame_index].indirection[current_rid[2]]
                        if isinstance(current_indirection, tuple) and current_indirection[3] == 't':
                            frame_index = self.table.bufferpool.load_tail_page(current_indirection[0], current_indirection[1], self.table.num_columns)
                            current_rid = current_indirection
                            version += 1
                        else:
                            break
                            
                    data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, current_rid[2])
                    if data and len(data) > aggregate_column_index:
                        sum_result += data[aggregate_column_index]

        return sum_result

    
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
