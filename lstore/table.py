from lstore.index import Index, Entry
from time import time

from lstore.page import Page # import Page class
from lstore.config import MAX_BASE_PAGES, NUM_SPECIFIED_COLUMNS, MAX_PAGE_SIZE, MAX_COLUMN_SIZE, INDIRECTION_COLUMN, LATEST_RECORD, OFFSET # import constants

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class PageRange:

    def __init__(self):
        """
        # a page range consists of a set of pages
        # the first MAX_PAGES are base pages
        # pages after the first MAX_PAGES pages are tail pages
        # pages = {page_index : Page Object}
        """
        self.pages: dict[int : Page] = {}

        self.has_capacity = False # checks if the page range has capacity
        self.num_tail_pages = 0 # keeps track of number of tail pages

    """
    # Checks if a page range is full
    # sets has_capacity to True if page range is full
    # sets has_capacity to False if page range is not full
    """
    def is_full(self) -> None:
        num_base_pages_full: int = 0
        for page_index in self.pages:
            current_page: Page = self.pages[page_index]
            if page_index < MAX_BASE_PAGES and not current_page.has_capacity():
                num_base_pages_full += 1
        if num_base_pages_full >= MAX_BASE_PAGES:
            self.has_capacity = False
        else:
            self.has_capacity = True

    """
    # Returns the indices of a nonempty base pages
    # Returns the largest page range index plus one if there is no nonempty base page
    """
    def get_nonempty_base_pages(self) -> int:
        for page_index in self.pages:
            current_page: Page = self.pages[page_index]
            if current_page.has_capacity:
                return page_index
        return len(self.pages)

    """
    # Appends a base page given an index
    """
    def append_base_page(self, base_page_index: int) -> None:
        self.pages[base_page_index] = Page()

    def get_nonempty_tail_pages(self):
        for page_index in self.pages:
            if page_index > MAX_BASE_PAGES:
                current_page = self.pages[MAX_BASE_PAGES + page_index]
                if current_page.has_capacity:
                    return MAX_BASE_PAGES + page_index
        return MAX_BASE_PAGES + len(self.pages)

    """
    # Appends a tail page given an index
    """
    def append_tail_page(self) -> None:
        self.pages[MAX_BASE_PAGES + self.num_tail_pages] = Page()
        self.num_tail_pages += 1
    
class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory: dict[int : list[Entry]] = {}
        self.index = Index(self)
        
        self.start_time = time() # record start time

        self.current_rid = 0
        """
        # a table has a set of page ranges
        # page_ranges = {page_range_index : PageRange object}
        """
        self.page_ranges: dict[int : PageRange] = {}

    def __merge(self):
        print("merge is happening")
        pass

    """
    # Returns the index of a nonempty page range
    # Returns the largest page range index plus one if there is no nonempty page range
    """
    def get_nonempty_page_range(self) -> int:
        for page_range_index in self.page_ranges:
            current_page_range: PageRange = self.page_ranges[page_range_index]
            if current_page_range.has_capacity:
                return page_range_index
        return len(self.page_ranges)

    """
    # Makes a new page range
    """
    def append_page_range(self) -> None:
        # length of page_ranges is largest page index plus one
        self.page_ranges[len(self.page_ranges)] = PageRange()

    """
    # Get a value from a column
    """
    def _column_to_val(self, page, column):
        if page.is_negative[column] == 1:
            is_negative = True
        else:
            is_negative = False
        column *= OFFSET
        val = bytearray(OFFSET)
        for index in range(len(OFFSET)):
            val[index] = page.data[column + index]
        return int.from_bytes(val, signed=is_negative)
    
    """
    # Insert integer into a column where each column is 8 bytes
    """
    def insert_int_to_column(self, page, value, column):
        column *= OFFSET
        if value < 0:
            is_negative = True
            page.is_negative.append(1)
        else:
            is_negative = False
            page.is_negative.append(0)
        value = value.to_bytes(OFFSET, signed=is_negative)
        for index in range(len(value)):
            page.data[column + index] = value[index]

    def get_value(self, entry: Entry):
        desired_page_range: PageRange = self.page_ranges[entry.page_range_index]
        desired_page: Page = desired_page_range.pages[entry.page_index]
        desired_col = entry.column_index
        return self._column_to_val(desired_page, desired_col)
    
    def set_value(self, entry: Entry, value):
        desired_page_range: PageRange = self.table.page_ranges[entry.page_range_index]
        desired_page: Page = desired_page_range.pages[entry.page_index]
        desired_col = entry.column_index
        return self.insert_int_to_column(desired_page, value, desired_col)

    def get_record(self, rid: int, entries: list[Entry], projected_columns_index) -> Record:
        record: Record = Record(rid, None, [])
        for entry_index in len(range(entries)):
            entry: Entry = entries[entry_index]
            if entry.column_index >= NUM_SPECIFIED_COLUMNS and (projected_columns_index[entry_index] == 1 or projected_columns_index == None):
                current_entry: Entry = entries[entry_index]
                value = self.get_value(current_entry)
                record.columns.append(value)
        return record
    
    def get_version(self, rid):
        version: int = 0
        entries: list[Entry] = self.page_directory[rid]
        while self.get_value(entries[INDIRECTION_COLUMN]) != LATEST_RECORD:
            entries: list[Entry] = self.page_directory[rid]
            version -= 1
        return version