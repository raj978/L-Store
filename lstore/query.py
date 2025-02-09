from lstore.table import Table, Record, Page, PageRange, Entry # imported Page class, PageRange class, and Entry class
from lstore.index import Index

from lstore.config import MAX_PAGE_SIZE, MAX_COLUMN_SIZE, NUM_SPECIFIED_COLUMNS, KEY_INDEX, INDIRECTION_COLUMN, LATEST_RECORD, RECORD_DELETED, RID_COLUMN# import constants

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
    def _get_divided_columns(self, column):
        smaller_columns = []
        smaller_column = []
        max_data_columns: int = (MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS
        for value in column:
            if len(smaller_column) == max_data_columns:
                # smaller column has reached max size so append it
                smaller_columns.append(smaller_column)
                smaller_column = []
            smaller_column.append(value)
        if len(smaller_column) != 0:
            smaller_columns.append(smaller_column)
        return smaller_columns
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):

        entries: list[Entry] = self.table.page_directory[primary_key]
        # get number of indirections
        num_columns: int = MAX_PAGE_SIZE / MAX_COLUMN_SIZE
        num_indirections = (len(entries) // num_columns) + 1 # this is to ceiling the value

        for index in range(num_indirections):
            while self.table.get_value(entries[INDIRECTION_COLUMN + (index * num_columns)]) != LATEST_RECORD:
                rid = self.table.get_value(entries[INDIRECTION_COLUMN]) # get rid of next tail record
                self.table.set_value(entries[RID_COLUMN], RECORD_DELETED) # set rid to invalid
                self.table.set_value(entries[INDIRECTION_COLUMN], RECORD_DELETED) # set indirection to invalid
                entries = self.table.page_directory[rid]
        
        # update the page directory
        del self.table.page_directory[primary_key]

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # turn schema encoding into a decimal to store it and then convert it back to binary
        schema_encoding = '0' * self.table.num_columns

        # if table is full then return False
        
        key: int = columns[KEY_INDEX]

        # make a new record for each record passed in
        for index in range(len(columns)):

            # check if there is a page range that has capacity
            page_range_index: int = self.table.get_nonempty_page_range()

            # if page_range_index is the largest page index plus one 
            # then there is no nonempty page range or a page range does not exist so make a new page range
            if page_range_index == len(self.table.page_ranges):
                self.table.append_page_range()

            # select page_range to insert into
            page_range: PageRange = self.table.page_ranges[page_range_index]

            # check if there is enough base pages for the record in that page_range
            # the record can span multiple base pages in a page_range
            base_page_indices: list[int] = page_range.get_nonempty_base_pages()

            # get list of smaller columns based on the current column
            divided_columns = self._get_divided_columns(columns)

            for index in range(len(base_page_indices)):
                base_page_index: int = base_page_indices[index]
                if base_page_index == len(page_range): # there is no nonempty base page or the base page does not exist
                    # make a new base page
                    page_range.append_base_page(base_page_index)

                # append the values to the base page
                current_base_page: Page = page_range[base_page_index]
                smaller_column = divided_columns[index]
                current_base_page.write(self.table.current_rid, LATEST_RECORD, schema_encoding, key, smaller_column)

            # increment rid
            self.table.current_rid += 1

    
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
        records: list[Record] = []
        # records can span multiple base pages so split projected columns
        smaller_projected_columns_index = self._get_divided_columns(projected_columns_index)
        # index of smaller projected columns
        current_smaller_projected_columns: int = 0
        for rid in self.table.page_directory:
            entries: list[Entry] = self.table.page_directory[rid]
            for entry in entries:
                if search_key == entry.cell_index and search_key_index == entry.column_index:
                    record = self.table.get_record(entries, smaller_projected_columns_index[current_smaller_projected_columns])
                    current_smaller_projected_columns += 1
                    records.append(record)
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
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        records = self.select(search_key, search_key_index, projected_columns_index)
        records_with_correct_version = []
        for record in records:
            version = self.table.get_version(record.rid)
            if version >= relative_version:
                records_with_correct_version.append(record)
        return records_with_correct_version

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # always update latest version of a record
        

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
        pass

    
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
        pass

    
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
