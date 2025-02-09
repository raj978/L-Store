# from lstore.table import Table, Entry # import Table class and Entry class
from lstore.config import MAX_PAGE_SIZE, MAX_COLUMN_SIZE, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN, KEY_COLUMN, NUM_SPECIFIED_COLUMNS # import constants

from time import time

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)

    def has_capacity(self):
        # each row of aligned columns is a record so MAX_COLUMN_SIZE rows of aligned columns is the max
        return self.num_records < MAX_COLUMN_SIZE
    
    """
    # Convert schema encoding string to number in order to store it in schema encoding column
    """
    def _binary_to_decimal(binary_str: str) -> int:
        sum: int = 0
        for index in range(len(binary_str)):
            power: int = len(binary_str) - index
            digit: int = int(binary_str[index])
            sum += (2**power) * digit
        return sum

    """
    # Convert number in schema encoding column to schema encoding string
    """
    def decimal_to_binary(number: int) -> str:
        pass

    def write(self, table, indirection: int, schema_encoding: str, key, page_range_index: int, page_index: int, values):

        # insert specified columns
        self.data[INDIRECTION_COLUMN + self.num_records] = indirection # insert indirection
        self.data[RID_COLUMN + self.num_records] = table.current_rid # insert RID
        self.data[TIMESTAMP_COLUMN + self.num_records] = time() - table.start_time() # timestamp is the current time minus the start time of when the table is initialized
        schema_encoding = self._binary_to_decimal(schema_encoding) # convert schema encoding to number
        self.data[SCHEMA_ENCODING_COLUMN + self.num_records] = schema_encoding # insert schema encoding
        self.data[KEY_COLUMN + self.num_records] = key # insert key

        # insert values
        for column_index in range(len(values)):
            current_value: int = values[column_index]
            self.data[NUM_SPECIFIED_COLUMNS + column_index + self.num_records] = current_value
            # update page directory
            entry: Entry = Entry(page_range_index, page_index, column_index, self.num_records)

            # check if key is already in page directory
            if table.current_rid in table.page_directory:
                table.page_directory[self.num_records].append(entry)
            else:
                table.page_directory[self.num_records] = entry

        self.num_records += 1
