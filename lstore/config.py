"""
Configuration constants for the L-Store database system
"""

# Page and record constants
PAGE_SIZE = 4096  # bytes
MAX_RECORDS_PER_PAGE = 512
MAX_BASEPAGES_PER_RANGE = 16

# Table indices
TABLEKEY = 0
TABLENUMCOL = 1
TABLECURPG = 2
TABLECURBP = 3
TABLECURREC = 4

# Bufferpool constants
FRAMECOUNT = 100  # Maximum number of frames in bufferpool
MERGE_INTERVAL = 60  # seconds between merge operations