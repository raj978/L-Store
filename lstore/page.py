# from lstore.table import Table, Entry # import Table class and Entry class
from lstore.config import MAX_PAGE_SIZE, MAX_COLUMN_SIZE, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN, KEY_COLUMN, NUM_SPECIFIED_COLUMNS, OFFSET # import constants
from lstore.index import Entry

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
    def _binary_to_decimal(self, binary_str: str) -> int:
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

    """
    # Insert integer into a column where each column is 8 bytes
    """
    def insert_int_to_column(self, value, column):
        column *= OFFSET
        value = str(value)

        # add padding
        num_padding = OFFSET - len(value)
        padding = '0' * num_padding
        padding += value # prepend the padding to the value
        value = padding

        for index in range(len(value)):
            digit = value[index]
            self.data[column + index] = int(digit)

    def write(self, table, indirection: int, schema_encoding: str, key, page_range_index: int, page_index: int, values):

        # insert specified columns
        self.insert_int_to_column(indirection, INDIRECTION_COLUMN)
        self.insert_int_to_column(table.current_rid, RID_COLUMN)
        self.insert_int_to_column(int(time() - table.start_time), TIMESTAMP_COLUMN)
        self.insert_int_to_column(self._binary_to_decimal(schema_encoding), SCHEMA_ENCODING_COLUMN)
        self.insert_int_to_column(key, KEY_COLUMN)


        for index in range(len(values)):
            current_value = values[index]
            current_column = NUM_SPECIFIED_COLUMNS + index
            self.insert_int_to_column(current_value, current_column)
            # update page directory
            entry: Entry = Entry(page_range_index, page_index, current_column, self.num_records)

            # check if key is already in page directory
            if table.current_rid in table.page_directory:
                table.page_directory[self.num_records].append(entry)
            else:
                table.page_directory[self.num_records] = entry

        self.num_records += 1
