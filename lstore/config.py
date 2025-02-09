INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
KEY_COLUMN = 4

KEY_INDEX = 0 # index of key in the record that is passed in
NUM_SPECIFIED_COLUMNS = 5 # this is to make space for columns that do not hold data

MAX_BASE_PAGES = 16 # max number of base pages in a page range
RECORD_DELETED = -1 # special value for deleted records
MAX_PAGE_SIZE = 4096
MAX_COLUMN_SIZE = 512

# version of base record is 0
# tail record one is -1, tail record two is -2, etc.
LATEST_RECORD = 1  # special value indicating that this record is the latest record