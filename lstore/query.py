from lstore.table import Table, Record, Page, PageRange # imported Page class, PageRange class, and Entry class
from lstore.index import Index, Entry

from lstore.config import MAX_PAGE_SIZE, MAX_COLUMN_SIZE, NUM_SPECIFIED_COLUMNS, INDIRECTION_COLUMN, LATEST_RECORD, RECORD_DELETED, RID_COLUMN, MAX_BASE_PAGES, KEY_INDEX, OFFSET # import constants

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
        smaller_column = []
        for value in column:
            if len(smaller_column) == max_size:
                # smaller column has reached max size so append it
                smaller_columns.append(smaller_column)
                smaller_column = []
            smaller_column.append(value)
        if len(smaller_column) != 0:
            smaller_columns.append(smaller_column)
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

        record = self.table.get_record(rid, primary_key, entries, None)
        columns = record.columns
        columns = columns[KEY_INDEX+1:len(columns)] # remove key from columns

        # update indirections

        # split values
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS)
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

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # turn schema encoding into a decimal to store it and then convert it back to binary
        schema_encoding = '0' * self.table.num_columns

        # if table is full then return False

        key = columns[KEY_INDEX] # get key
        columns = columns[KEY_INDEX+1:len(columns)] # remove key from columns

        # split values
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS)
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
            current_base_page.write(self.table, LATEST_RECORD, schema_encoding, key, page_range_index, current_base_page_index, current_values)
        
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
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS)
        smaller_projected_columns_index = self._get_divided_columns(projected_columns_index, max_size)
        # index of smaller projected columns
        current_smaller_projected_columns: int = 0
        for rid in self.table.page_directory:
            entries: list[Entry] = self.table.page_directory[rid]
            for entry in entries:
                if search_key == entry.cell_index and search_key_index == entry.column_index:
                    record: Record = self.table.get_record(entries, smaller_projected_columns_index[current_smaller_projected_columns])
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
        records: list[Record] = []
        rids_visited = []
        version = 0
        # records can span multiple base pages so split projected columns
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS)
        smaller_projected_columns_index = self._get_divided_columns(projected_columns_index, max_size)

        current_smaller_projected_columns: int = 0
        for rid in self.table.page_directory:
            if rid in rids_visited:
                continue

            entries: list[Entry] = self.table.page_directory[rid]
            # get number of indirections
            num_columns: int = int(MAX_PAGE_SIZE / MAX_COLUMN_SIZE)
            num_indirections = int((len(entries) // num_columns) + 1) # this is to ceiling the value

            for index in range(num_indirections):
                while self.table.get_value(entries[INDIRECTION_COLUMN + (index * num_columns)]) != LATEST_RECORD:
                    for entry in entries:
                        if search_key == entry.cell_index and search_key_index == entry.column_index and version == relative_version:
                            record: Record = self.table.get_record(entries, smaller_projected_columns_index[current_smaller_projected_columns])
                            current_smaller_projected_columns += 1
                            records.append(record)
                        entries = self.table.page_directory[rid]
                    version -= 1
                    rids_visited.append(rid)
                version = 0
        
        return records
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # always do update on a base record

        rid = self.table.key_to_rid[primary_key][0] # first rid is the base record
        entries: list[Entry] = self.table.page_directory[rid]

        columns = columns[KEY_INDEX+1:len(columns)] # remove key from columns

        # update indirections

        # split values
        max_size: int = int((MAX_PAGE_SIZE / MAX_COLUMN_SIZE) - NUM_SPECIFIED_COLUMNS)
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

        # get base record
        base_record = self.table.get_record(rid, primary_key, entries, None)

        # update schema encoding
        schema_encoding = ''
        for index in range(len(base_record.columns)):
            current_value = base_record.columns[index]
            if current_value != columns[index]:
                schema_encoding += '1'
            else:
                schema_encoding += '0'

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
            current_tail_page.write(self.table, LATEST_RECORD, schema_encoding, primary_key, page_range_index, current_tail_page_index, current_values)
        
        # increment rid
        self.table.current_rid += 1
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        for rid in self.table.page_directory:
            entries: list[Entry] = self.table.page_directory[rid]
            if rid == start_range:
                sum = 0
                while rid <= end_range:
                    sum += self.table.get_value(entries[aggregate_column_index])
                    rid += 1
                    entries = self.table.page_directory[rid]
                return sum
        return False
    
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
        sum = 0
        rids_visited = []
        version = 0
        for rid in self.table.page_directory:
            if rid in rids_visited:
                continue

            entries: list[Entry] = self.table.page_directory[rid]
            # get number of indirections
            num_columns: int = int(MAX_PAGE_SIZE / MAX_COLUMN_SIZE)
            num_indirections = int((len(entries) // num_columns) + 1) # this is to ceiling the value

            for index in range(num_indirections):
                while self.table.get_value(entries[INDIRECTION_COLUMN + (index * num_columns)]) != LATEST_RECORD:
                    for entry in entries:
                        rid = self.table.get_value(entries[RID_COLUMN])
                        if version == relative_version and rid <= start_range and rid >= end_range:
                            sum += self.table.get_value(entries[aggregate_column_index])
                        entries = self.table.page_directory[rid]
                    version -= 1
                    rids_visited.append(rid)
                version = 0
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
