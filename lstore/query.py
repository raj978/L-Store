from lstore.table import Table, Record
from lstore.index import Index
import threading
from lstore.lock_manager import lockEntry

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        self.table.insertRec(0, schema_encoding, *columns)
        return True

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = self.table.index.locate(0, primary_key)[0]
        if rid:
            # if record locked then abort
            # x is not compatible with s or x
            if lockEntry(rid, 's') in self.lock_manager.locks.keys() or lockEntry(rid, 'x') in self.lock_manager.locks.keys():
                return False

            # acquire x lock for record
            lock_entry = lockEntry(rid, 'x')
            self.lock_manager.insert_lock(lock_entry)
            self.recently_added_lock_entry = lock_entry

            # add to deleted columns
            version = 0
            record = self.table.get_version_record(rid, version)
            self.table.deleted_columns.append(record.columns)
            version -= 1
            new_record = self.table.get_version_record(rid, version)
            while record.rid != new_record.rid:
                self.table.deleted_columns.append(record.columns)
                record.rid = new_record.rid
                new_record = self.table.get_version_record(rid, version)

            self.table.page_directory.pop(rid, None)
            self.table.index.delete_node(0, primary_key, rid)

            return True
        return False

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
        rids = self.table.index.locate(column, key)
        if not rids:
            return []

        records = []
        for rid in rids:
            if rid in self.table.page_directory:
                record = self.table.get_record(rid)
                if record:
                    filtered_columns = []
                    for i, include in enumerate(query_columns):
                        if include:
                            filtered_columns.append(record.columns[i])
                        else:
                            filtered_columns.append(None)
                    records.append(Record(rid, key, filtered_columns))
        return records

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
    def select_version(self, key, column, query_columns, version):
        rids = self.table.index.locate(column, key)
        if not rids:
            return []

        records = []
        for rid in rids:
            if rid in self.table.page_directory:
                record = self.table.get_record_version(rid, version)
                if record:
                    filtered_columns = []
                    for i, include in enumerate(query_columns):
                        if include:
                            filtered_columns.append(record.columns[i])
                        else:
                            filtered_columns.append(None)
                    records.append(Record(rid, key, filtered_columns))
        return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        rids = self.table.index.locate(0, primary_key)
        if not rids:
            return False

        rid = rids[0]
        if rid not in self.table.page_directory:
            return False

        self.table.updateRec(rid, *columns)
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column):
        if start_range > end_range:
            return False

        column_sum = 0
        rids = self.table.index.locate_range(0, start_range, end_range)

        for rid in rids:
            if rid in self.table.page_directory:
                record = self.table.get_record(rid)
                if record:
                    val = record.columns[aggregate_column]
                    if val is None:
                        val = 0
                    column_sum += val

        return column_sum

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column, version):
        if start_range > end_range:
            return False

        column_sum = 0
        rids = self.table.index.locate_range(0, start_range, end_range)

        for rid in rids:
            if rid in self.table.page_directory:
                record = self.table.get_record_version(rid, version)
                if record:
                    val = record.columns[aggregate_column]
                    if val is None:
                        val = 0
                    column_sum += val

        return column_sum

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
        if r is not None:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            self.update(key, *updated_columns)
            return True
        return False
