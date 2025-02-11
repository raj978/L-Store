from lstore.table import Table, Record, Page, PageRange # imported Page class, PageRange class, and Entry class
from lstore.index import Index, Entry

from lstore.config import *

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
    # Divides a column into a list of smaller columns so they can fit into one base page
    # Returns a list of smaller columns
    """
    def _get_divided_columns(self, column, max_size):
        smaller_columns = []
        tiny_col = []
        for value in column:
            if len(tiny_col) == max_size:
                # smaller column has reached max size so append it
                smaller_columns.append(tiny_col)
                tiny_col = []
            tiny_col.append(value)
        if len(tiny_col) != 0:
            smaller_columns.append(tiny_col)
        return smaller_columns
    
    
    """
    # Returns the number of pages needed for however many columns there are
    """
    def _get_pages_needed(self, num_columns, max_size) -> int:
        return (num_columns // max_size) + 1 # this is to ceiling the value
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key, *columns):

        rid = self.table.key_to_rid[primary_key][0] # first rid is the base record
        entries: list[Entry] = self.table.page_directory[rid]

        record = self.table.get_record(rid, primary_key, entries)
        columns = record.columns
        columns = columns[KEY_INDEX+1:len(columns)] # remove key from columns

        # update indirections

        # split values
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS - 1) # subtract one to make space for the key
        divided_columns = self._get_divided_columns(columns, max_size)

        num_indirections = len(divided_columns)

        # get initial indirections
        initial_indirections = []
        for index in range(num_indirections):
            indirection_index = (index * OFFSET) + INDIRECTION_COLUMN
            indirection = self.table.get_value(entries[indirection_index])
            initial_indirections.append(indirection)

        for index in range(len(initial_indirections)):
            # go down that initial indirection
            indirection_index = (index * OFFSET) + INDIRECTION_COLUMN
            rid_index = (index * OFFSET) + RID_COLUMN
            entries = self.table.page_directory[rid] # reset entries
            indirection = initial_indirections[index]
            self.table.set_value(entries[indirection_index], RECORD_DELETED) # set indirection to invalid
            self.table.set_value(entries[rid_index], RECORD_DELETED) # set rid to invalid
            while indirection != LATEST_RECORD:
                entries = self.table.page_directory[indirection]
                indirection = self.table.get_value(entries[indirection_index])
                self.table.set_value(entries[indirection_index], RECORD_DELETED) # set indirection to invalid
                self.table.set_value(entries[rid_index], RECORD_DELETED) # set rid to invalid

        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # turn schema encoding into a decimal to store it and then convert it back to binary
        schema_encoding = '0' * self.table.num_columns

        # if table is full then return False

        # get key
        key = columns[KEY_INDEX]
        columns = columns[KEY_INDEX+1:len(columns)] # remove key from columns

        # split values
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS - 1) # subtract one to make space for the key
        divided_columns = self._get_divided_columns(columns, max_size)

        # check if there is a page range that has capacity
        page_range_index: int = self.table.get_nonempty_page_range()

        # append page range it is full or does not exist
        if page_range_index == len(self.table.page_ranges):
            self.table.append_page_range()

        page_range: PageRange = self.table.page_ranges[page_range_index]

        # check if there is enough base pages for the record in that page_range
        # the record can span multiple base pages in a page_range
        num_base_pages_needed = len(divided_columns)

        # append base pages
        offset = len(page_range.pages)
        for index in range(num_base_pages_needed - len(page_range.pages)):
            page_range.append_base_page(index + offset)

        # get nonempty base pages
        base_page_indices = page_range.get_nonempty_base_pages(num_base_pages_needed)

        # for each base page, insert values
        for index in range(len(base_page_indices)):
            current_base_page_index: int = base_page_indices[index]
            current_base_page: Page = page_range.pages[current_base_page_index]
            current_values = divided_columns[index]
            current_base_page.write(self.table, LATEST_RECORD, key, schema_encoding, page_range_index, current_base_page_index, current_values)
        
        # increment rid
        self.table.current_rid += 1

        return True
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        
        # get all records
        records: list[Record] = []
        for key in self.table.key_to_rid:
            list_of_rids = self.table.key_to_rid[key]
            for rid in list_of_rids:
                if rid != RECORD_DELETED:
                    entries = self.table.page_directory[rid]
                    record = self.table.get_record(rid, key, entries)
                    records.append(record)
        
        selected_records: list[Record] = []
        # select records that match search key and search key index
        for record in records:
            value = record.columns[search_key_index]
            if value == search_key:
                # use projected columns index to get what columns to return
                projected_columns = []
                for index in range(len(record.columns)):
                    if projected_columns_index[index] == 1:
                        projected_columns.append(record.columns[index])
                projected_record = Record(record.rid, record.key, projected_columns)
                selected_records.append(projected_record)

        return selected_records
    
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
        records: list[Record] = self.select(search_key, search_key_index, projected_columns_index)
        selected_records: list[Record] = []
        for record in records:
            version = self.table.get_version(record.rid, record.key)
            if version == relative_version:
                selected_records.append(record)
        
        # return a version greater than or equal to if version does not exist
        if len(selected_records) == 0:
            for record in records:
                version = self.table.get_version(record.rid, record.key)
                if version >= relative_version:
                    selected_records.append(record)

        return selected_records
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):

        # check if key exists
        if primary_key not in self.table.key_to_rid:
            return False

        # always do update on a base record

        rid = self.table.key_to_rid[primary_key][0] # first rid is the base record
        entries: list[Entry] = self.table.page_directory[rid]

        # split values
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS - 1) # subtract one to make space for the key
        divided_columns = self._get_divided_columns(columns, max_size)

        # get base record to update divided columns
        base_record = self.table.get_record(rid, primary_key, entries)
        base_divided_columns = self._get_divided_columns(base_record.columns, max_size)

        # update values in divided columns
        for i in range(len(divided_columns)):
            for j in range(len(divided_columns[i])):
                if divided_columns[i][j] == None:
                    divided_columns[i][j] = base_divided_columns[i][j]

        # update schema encoding
        schema_encoding = ''
        for index in range(len(base_record.columns)):
            current_value = base_record.columns[index]
            if current_value != base_record.columns[index]:
                schema_encoding += '1'
            else:
                schema_encoding += '0'

        num_indirections = len(divided_columns)

        # update indirections

        # get initial indirections
        initial_indirections = []
        for index in range(num_indirections):
            indirection_index = (index * OFFSET) + INDIRECTION_COLUMN
            indirection = self.table.get_value(entries[indirection_index])
            initial_indirections.append(indirection)

        for index in range(len(initial_indirections)):
            # go down that initial indirection
            indirection_index = (index * OFFSET) + INDIRECTION_COLUMN
            entries = self.table.page_directory[rid] # reset entries
            indirection = initial_indirections[index]
            if indirection == LATEST_RECORD:
                self.table.set_value(entries[indirection_index], self.table.current_rid) # point the indirection to the current rid to be inserted
            while indirection != LATEST_RECORD:
                indirection = self.table.get_value(entries[indirection_index]) # get rid of next tail record
                if indirection == LATEST_RECORD:
                    self.table.set_value(entries[indirection_index], self.table.current_rid) # point the indirection to the current rid to be inserted
                else:
                    entries = self.table.page_directory[indirection]

        # insert new tail record
        page_range_index = entries[0].page_range_index
        page_range: PageRange = self.table.page_ranges[page_range_index]

        tail_page_indices = []
        num_tail_pages_needed = len(divided_columns)
        for _ in range(num_tail_pages_needed):
            tail_page_indices.append(page_range.append_tail_page())

        # for each tail page insert values
        for index in range(len(tail_page_indices)):
            current_tail_page_index: int = tail_page_indices[index]
            current_tail_page: Page = page_range.pages[current_tail_page_index]
            current_values = divided_columns[index]
            current_tail_page.write(self.table, LATEST_RECORD, primary_key, schema_encoding, page_range_index, current_tail_page_index, current_values)
        
        # increment rid
        self.table.current_rid += 1
        
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

        # get records with a key in the range
        records: list[Record] = []
        for key in self.table.key_to_rid:
            if key >= start_range and key <= end_range:
                list_of_rids = self.table.key_to_rid[key]
                for rid in list_of_rids:
                    if rid != RECORD_DELETED:
                        entries = self.table.page_directory[rid]
                        record = self.table.get_record(rid, key, entries)
                        records.append(record)

        # return false if there are no records in the key range
        if len(records) == 0:
            return False

        # get sum on the column index
        sum = 0
        for record in records:
            sum += record.columns[aggregate_column_index]
        return sum
    
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

        # get records with a key in the range
        records: list[Record] = []
        for key in self.table.key_to_rid:
            if key >= start_range and key <= end_range:
                list_of_rids = self.table.key_to_rid[key]
                for rid in list_of_rids:
                    if rid != RECORD_DELETED:
                        # check version
                        version = self.table.get_version(rid, key)
                        if version == relative_version:
                            entries = self.table.page_directory[rid]
                            record = self.table.get_record(rid, key, entries)
                            records.append(record)

        # return a version greater than or equal to if version does not exist
        if len(records) == 0:
            records: list[Record] = []
            for key in self.table.key_to_rid:
                if key >= start_range and key <= end_range:
                    list_of_rids = self.table.key_to_rid[key]
                    for rid in list_of_rids:
                        if rid != RECORD_DELETED:
                            # check version
                            version = self.table.get_version(rid, key)
                            if version >= relative_version:
                                entries = self.table.page_directory[rid]
                                record = self.table.get_record(rid, key, entries)
                                records.append(record)

        # return false if there are no records in the key range
        if len(records) == 0:
            return False

        # get sum on the column index
        sum = 0
        for record in records:
            sum += record.columns[aggregate_column_index]
        return sum
    
    """
    incremenets one column of the record
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
