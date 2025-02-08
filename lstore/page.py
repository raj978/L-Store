from lstore.config import MAX_PAGE_SIZE, MAX_COLUMN_SIZE # import constants

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)

        self.max_columns = MAX_PAGE_SIZE / MAX_COLUMN_SIZE # max columns in a page

        """
        # what column to write to
        # reset current_column to 0 if current row is full
        """
        self.current_column: int = 0

    def has_capacity(self):
        # each row of aligned columns is a record so MAX_COLUMN_SIZE rows of aligned columns is the max
        return self.num_records < MAX_COLUMN_SIZE

    def write(self, value):
        self.num_records += 1
        pass

    def row_is_full():
        pass

