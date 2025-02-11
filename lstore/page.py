# from lstore.table import Table, Entry # import Table class and Entry class
from lstore.config import MAX_PAGE_SIZE, MAX_COLUMN_SIZE, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN, KEY_COLUMN, NUM_SPECIFIED_COLUMNS, OFFSET # import constants
from lstore.index import Entry

from time import time

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)

        self.is_negative = [] # keep track of what numbers are negative

    def has_capacity(self):
        # each column is OFFSET bytes
        return (self.num_records * OFFSET) < MAX_COLUMN_SIZE
    
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

    def write(self, table, indirection: int, schema_encoding: str, key, page_range_index: int, page_index: int, values):

        # get page to write to
        desired_page_range = self.table.page_ranges[page_range_index]
        desired_page = desired_page_range.pages[page_index]

        # insert specified columns
        table.insert_int_to_column(desired_page, indirection, INDIRECTION_COLUMN)
        table.insert_int_to_column(desired_page, table.current_rid, RID_COLUMN)
        table.insert_int_to_column(desired_page, int(time() - table.start_time), TIMESTAMP_COLUMN)
        table.insert_int_to_column(desired_page, self._binary_to_decimal(schema_encoding), SCHEMA_ENCODING_COLUMN)
        table.insert_int_to_column(desired_page, key, KEY_COLUMN)

        for index in range(len(values)):
            current_value = values[index]
            current_column = NUM_SPECIFIED_COLUMNS + index
            table.insert_int_to_column(desired_page, current_value, current_column)
            # update page directory
            entry: Entry = Entry(page_range_index, page_index, current_column)

            # check if key is already in page directory
            if table.current_rid not in table.page_directory:
                table.page_directory[table.current_rid] = []
            table.page_directory[table.current_rid].append(entry)

        self.num_records += 1
