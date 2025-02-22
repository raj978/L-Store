from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime

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
        if rid is None:
            return []
            
        baseRID = rid
        rid = self.table.page_directory[rid]
        if rid is None:
            return []
            
        frame_index = self.table.bufferpool.load_base_page(rid[0], rid[1], self.table.num_columns)
        if frame_index is None:
            return []
            
        # Get initial indirection
        current_rid = self.table.bufferpool.frames[frame_index].indirection[rid[2]]
        version = relative_version
        
        # Navigate version chain
        while version != 0:
            if current_rid[3] == 'b':
                if tuple(current_rid) != baseRID:
                    frame_index = self.table.bufferpool.load_base_page(current_rid[0], current_rid[1], self.table.num_columns)
                    current_rid = self.table.bufferpool.frames[frame_index].indirection[current_rid[2]]
            else:
                frame_index = self.table.bufferpool.load_tail_page(current_rid[0], current_rid[1], self.table.num_columns)
                current_rid = self.table.bufferpool.frames[frame_index].indirection[current_rid[2]]
            version += 1
            
        # Get data from final frame
        if current_rid[3] == 't':
            frame_index = self.table.bufferpool.load_tail_page(current_rid[0], current_rid[1], self.table.num_columns)
        else:
            frame_index = self.table.bufferpool.load_base_page(current_rid[0], current_rid[1], self.table.num_columns)
            
        data = self.table.bufferpool.extractdata(frame_index, self.table.num_columns, current_rid[2])
        
        # Create record with projected columns
        record = []
        for i in range(len(projected_columns_index)):
            if projected_columns_index[i] == 1:
                record.append(data[i])
                
        return [Record(current_rid, search_key, record)]
    

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, rollback=False):
        # Locate the record via the primary key index
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid is None:
            return False

        # Retrieve the old record so we know what to remove
        old_record = self.select(primary_key, self.table.key, [1] * self.table.num_columns)
        if not old_record or len(old_record) == 0:
            return False
        old_record = old_record[0].columns

        # Do not allow changing the primary key. (Assuming primary key is column 0)
        if columns[0] is not None and columns[0] != primary_key:
            return False

        # For each column that is changed (i.e. new value is not None)
        for i in range(len(columns)):
            if columns[i] is not None:
                # First remove the old mapping for this column value if it exists
                if old_record[i] in self.table.index.indices[i]:
                    del self.table.index.indices[i][old_record[i]]
                # Then add the new mapping: note that for nonprimary columns,
                # this index entry may already exist but should be updated to point to this record.
                self.table.index.indices[i][columns[i]] = rid

        if rollback:
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
        Sums the values in the aggregate_column for records with keys in range [start_range, end_range]
        """
        total = 0
        keys = range(start_range, end_range + 1)
        for key in keys:
            records = self.select(key, 0, [1] * self.table.num_columns)
            if records and len(records) > 0 and records[0]:
                total += records[0].columns[aggregate_column_index]
        return total

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
        """
        Sums the values in the aggregate_column for records with keys in range [start_range, end_range]
        at the specified relative version
        """
        total = 0
        keys = range(start_range, end_range + 1)
        for key in keys:
            records = self.select_version(key, 0, [1] * self.table.num_columns, relative_version)
            if records and len(records) > 0 and records[0]:
                total += records[0].columns[aggregate_column_index]
        return total

    
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
