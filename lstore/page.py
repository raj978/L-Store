
MAX_COLUMN_SIZE = 512

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
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

