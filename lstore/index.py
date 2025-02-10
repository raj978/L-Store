
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
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
        
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):

        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):

        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
