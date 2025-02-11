INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
KEY_COLUMN = 4

KEY_INDEX = 0 # modify this later for future milestones
NUM_SPECIFIED_COLUMNS = 5 # this is to make space for columns that do not hold data

MAX_BASE_PAGES = 16 # max number of base pages in a page range
RECORD_DELETED = -2 # special value for deleted records
MAX_PAGE_SIZE = 4096
MAX_COLUMN_SIZE = 512

LATEST_RECORD = -1  # special value indicating that this record is the latest record

OFFSET = 8 # there are 8 bytes in a column