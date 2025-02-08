from lstore.index import Index
from time import time

from lstore.page import Page # import Page class
from lstore.config import INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN, MAX_PAGES, RECORD_DELETED # import constants

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Entry:

    """
    :param page_range_index: int         # index of page range in page_ranges in Table
    :param page_index: int               # index of page index in PageRange
    :param column_index: int             # index of column in Page
    :param cell_index: int               # index of cell in column
    """
    def __init__(self, page_range_index: int, page_index: int, column_index: int, cell_index: int):
        self.page_range_index: int = page_range_index
        self.page_index: int = page_index
        self.column_index: int = column_index
        self.cell_index: int = cell_index

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

    """
    # Checks if a page range is full
    # sets has_capacity to True if page range is full
    # sets has_capacity to False if page range is not full
    """
    def is_full(self) -> None:
        for page_index in self.pages:
            current_page: Page = self.pages[page_index]
            if current_page.has_capacity():
                self.has_capacity = False
        self.has_capacity = True

    def get_nonempty_base_pages(self):
        pass

    def append_base_page(self):
        pass

    def get_nonempty_tail_pages(self):
        pass

    def append_tail_page(self):
        pass
    
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

        # used for assigning rid to new records
        self.current_rid: int = 0

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
        for page_index in self.page_ranges:
            current_page_range: PageRange = self.page_ranges[page_index]
            if current_page_range.has_capacity:
                return page_index
        return len(self.page_ranges)

    """
    # Makes a new page range
    """
    def append_page_range(self) -> None:
        # length of page_ranges is largest page index plus one
        self.page_ranges[len(self.page_ranges)] = PageRange()
        