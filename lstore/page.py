# UPDATE IN TESTING
#
# Following example in milestone1:
# PAGE_SIZE = 4096 bytes
# Base Page/ tail page has 1 page per column
# Page Range has 16 base pages + any tail pages
# Number of records per base page = 512


#for arrays and stuff
import numpy as np

MAX_RECORDS_PER_PAGE = 512
MAX_BASEPAGES_PER_RANGE = 16

#One Page for Every Column in Table (maybe 4k pages/columns per base page)
class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)  # 4096 bytes = 512 records * 8 bytes per record

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE

    def write(self, value):
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1
        
    def find_value(self, value):
        indexes = []
        for i in range(MAX_RECORDS_PER_PAGE):
            cur_val = int.from_bytes(self.data[i*8:(i+1)*8], byteorder='big')
            if cur_val == value:
                indexes.append(i)
        return indexes
        
    def get_value(self, index):
        return int.from_bytes(self.data[index*8:(index + 1)*8], 'big')

class BasePage:
    def __init__(self, numCols):
        self.rid = [None] * 512
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.pages = [Page() for _ in range(numCols)]
        self.num_records = 0
        self.num_cols = numCols

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE
            
    def insertRecBP(self, RID, start_time, schema_encoding, indirection, *columns):
        for i in range(self.num_cols):
            self.pages[i].write(columns[i])
        self.num_records += 1
        self.rid.append(RID)
        self.start_time.append(start_time)
        self.schema_encoding.append(schema_encoding)
        self.indirection.append(indirection)

class TailPage:
    def __init__(self, numCols):
        self.rid = []
        self.indirection = []
        self.pages = [Page() for _ in range(numCols)]
        self.schema_encoding = []
        self.BaseRID = []
        self.num_records = 0

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE

    def insertRecTP(self, record, rid, updateRID, currentRID, baseRID, baseFrameIndex, *columns):
        # Store the new tail rid, its previous version, and base rid
        self.rid.append(updateRID)
        self.indirection.append(currentRID)
        self.BaseRID.append(baseRID)
        schema = ''
        for j in range(len(columns)):
            if columns[j] is not None:
                self.pages[j].write(columns[j])
                schema += '1'
            else:
                self.pages[j].write(record.columns[j])
                schema += '0'
        self.schema_encoding.append(schema)
        self.num_records += 1

class PageRange:
    def __init__(self, numCols):
        self.num_base_pages = 0
        self.num_tail_pages = 0
        self.basePages = []
        self.tailPages = []
        self.TPS = [0,0]
        # Initialize with first base page
        self.add_base_page(numCols)
        
    def has_capacity(self):
        return self.num_base_pages < MAX_BASEPAGES_PER_RANGE
    
    def add_tail_page(self, numCols):
        new_tail_page = TailPage(numCols)
        self.tailPages.append(new_tail_page)
        self.num_tail_pages += 1
        return new_tail_page
    
    def add_base_page(self, numCols):
        if self.has_capacity():
            new_base_page = BasePage(numCols)
            self.basePages.append(new_base_page)
            self.num_base_pages += 1
            return new_base_page
        return None



