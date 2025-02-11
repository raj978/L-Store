from lstore.index import Index, Entry
from time import time

from lstore.page import Page # import Page class
from lstore.config import * 

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
    """
    def get_nonempty_base_pages(self, num_base_pages_needed) -> list[int]:
        base_page_indices = []
        for page_index in self.pages:
            if len(base_page_indices) >= num_base_pages_needed:
                return base_page_indices
            current_page: Page = self.pages[page_index]
            if current_page.has_capacity:
                base_page_indices.append(page_index)
        return base_page_indices

    """
    # Appends a base page given an index
    """
    def append_base_page(self, base_page_index: int) -> None:
        self.pages[base_page_index] = Page()
        for _ in range(OFFSET):
            self.pages[base_page_index].is_negative.append(0)

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
    def append_tail_page(self) -> int:
        tail_page_index = MAX_BASE_PAGES + self.num_tail_pages
        self.pages[tail_page_index] = Page()
        for _ in range(OFFSET):
            self.pages[tail_page_index].is_negative.append(0)
        self.num_tail_pages += 1
        return tail_page_index
    
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
        self.key_to_rid: dict[int : int] = {} # maps key to rid

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
        for index in range(OFFSET):
            val[index] = page.data[column + index]
        return int.from_bytes(val, signed=is_negative)
    
    """
    # Insert integer into a column where each column is 8 bytes
    """
    def insert_int_to_column(self, page, value, column):
        if value < 0:
            is_negative = True
            page.is_negative[column] = 1
        else:
            is_negative = False
            page.is_negative[column] = 0
        column *= OFFSET
        value = value.to_bytes(OFFSET, signed=is_negative)
        for index in range(len(value)):
            page.data[column + index] = value[index]

    def get_value(self, entry: Entry):
        desired_page_range: PageRange = self.page_ranges[entry.page_range_index]
        desired_page: Page = desired_page_range.pages[entry.page_index]
        desired_col = entry.column_index
        return self._column_to_val(desired_page, desired_col)
    
    def set_value(self, entry: Entry, value):
        desired_page_range: PageRange = self.page_ranges[entry.page_range_index]
        desired_page: Page = desired_page_range.pages[entry.page_index]
        desired_col = entry.column_index
        return self.insert_int_to_column(desired_page, value, desired_col)

    def get_record(self, rid: int, key, entries: list[Entry]) -> Record:
        record = Record(rid, key, [])
        for entry_index in range(len(entries)):
            entry: Entry = entries[entry_index]
            if entry.column_index >= NUM_SPECIFIED_COLUMNS:
                current_entry: Entry = entries[entry_index]
                #does this mean that only stuff in the second base/tail page get added? #
                value = self.get_value(current_entry)
                if value != key:
                    record.columns.append(value)
                elif value == key and key not in record.columns:
                    record.columns.append(value) # append the key but only once
        return record
    
    def get_version(self, rid, key):
        list_of_rids = self.key_to_rid[key] # key_to_rid = {key: [rid 1, rid 2, etc.]}
        for index in range(len(list_of_rids)):
            if rid == list_of_rids[index]:
                return index * -1 # base record is 0, first tail record is -1, second tail record is -2, etc.