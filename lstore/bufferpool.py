from pickle import FALSE, TRUE
from lstore.page import Page, PageRange
from datetime import datetime

FRAMECOUNT = 100

class Bufferpool:
    def __init__(self):
        self.frames = []
        self.numFrames = 0
        self.frame_directory = []
        self.frame_info = [None] * 100
        self.page_ranges = {}  # In-memory storage of page ranges

    def has_capacity(self):
        return self.numFrames < FRAMECOUNT

    def allocate_page_range(self, num_columns, page_range_index):
        # Create new page range in memory
        if page_range_index not in self.page_ranges:
            self.page_ranges[page_range_index] = PageRange(num_columns)
            # Pre-initialize base pages
            for i in range(16):  # MAX_BASEPAGES_PER_RANGE
                d_key = (page_range_index, i, 'b')
                frame_index = self.get_empty_frame(num_columns)
                self.frame_info[frame_index] = d_key
                self.frames[frame_index].frameData = [Page() for _ in range(num_columns)]
        return self.page_ranges[page_range_index]

    def get_frame_index(self, key_directory):
        # First check if frame exists
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key_directory:
                return i
        
        # If not found, create a new frame
        page_range = self.page_ranges.get(key_directory[0])
        if not page_range:
            return None
            
        frame_index = self.get_empty_frame(page_range.basePages[0].num_cols if page_range.basePages else 0)
        self.frame_info[frame_index] = key_directory
        return frame_index

    def LRU(self):
        evict_index = 0
        for i in range(len(self.frames) - 1):
            if self.frames[i].lastAccess > self.frames[i + 1].lastAccess:
                if self.frames[i + 1].isPinned() == False:
                    evict_index = i + 1
        return evict_index
    
    def evict_page(self):
        return self.LRU()

    def get_empty_frame(self, numColumns):
        if not self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(numColumns))
            self.numFrames += 1
        return frame_index

    def load_base_page(self, page_range_index, base_page_index, numColumns):
        d_key = (page_range_index, base_page_index, 'b')
        if self.in_pool(d_key):
            return self.get_frame_index(d_key)

        frame_index = self.get_empty_frame(numColumns)
        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()
        self.frame_info[frame_index] = d_key
        
        # Get data from in-memory page range
        if page_range_index in self.page_ranges:
            page_range = self.page_ranges[page_range_index]
            if base_page_index < len(page_range.basePages):
                base_page = page_range.basePages[base_page_index]
                for i in range(numColumns):
                    cur_frame.frameData[i] = base_page.pages[i]
            
        cur_frame.unpin_page()
        return frame_index

    def load_tail_page(self, page_range_index, tail_page_index, numColumns):
        d_key = (page_range_index, tail_page_index, 't')
        
        # Check if page range exists
        if page_range_index not in self.page_ranges:
            self.allocate_page_range(numColumns, page_range_index)
            
        page_range = self.page_ranges[page_range_index]
        
        # Create tail page if it doesn't exist
        while len(page_range.tailPages) <= tail_page_index:
            page_range.add_tail_page(numColumns)
        
        frame_index = self.get_frame_index(d_key)
        if frame_index is None:
            frame_index = self.get_empty_frame(numColumns)
            self.frame_info[frame_index] = d_key
        
        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()
        
        # Initialize frame data if needed
        if cur_frame.frameData[0] is None:
            tail_page = page_range.tailPages[tail_page_index]
            cur_frame.frameData = [Page() for _ in range(numColumns)]
            for i in range(numColumns):
                cur_frame.frameData[i] = tail_page.pages[i]
        
        cur_frame.unpin_page()
        return frame_index

    def in_pool(self, key):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key:
                return True
        return False

    def insertRecBP(self, RID, start_time, schema_encoding, indirection, *columns, numColumns):
        frame_index = self.get_frame_index((RID[0], RID[1], 'b'))
        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()
        
        for i in range(numColumns):
            if cur_frame.frameData[i] is None:
                cur_frame.frameData[i] = Page()
            cur_frame.frameData[i].write(columns[i])
            
        cur_frame.numRecords += 1
        cur_frame.rid.append(RID)
        cur_frame.start_time.append(start_time)
        cur_frame.schema_encoding.append(schema_encoding)
        cur_frame.indirection.append(indirection)
        cur_frame.unpin_page()

    def insertRecTP(self, record, rid, updateRID, currentRID, baseRID, curFrameIndexBP, *columns):
        # First ensure the tail page exists
        frame_index = self.load_tail_page(updateRID[0], updateRID[1], len(columns))
        
        cur_frame = self.frames[frame_index]
        base_frame = self.frames[curFrameIndexBP]
        
        cur_frame.pin_page()
        base_frame.pin_page()
        
        schema = ''
        for j in range(len(columns)):
            if columns[j] is not None:
                if cur_frame.frameData[j] is None:
                    cur_frame.frameData[j] = Page()
                cur_frame.frameData[j].write(columns[j])
                schema += '1'
                base_frame.schema_encoding[j] = 1
            else:
                if cur_frame.frameData[j] is None:
                    cur_frame.frameData[j] = Page()
                cur_frame.frameData[j].write(record.columns[j])
                schema += '0'
                
        cur_frame.schema_encoding.append(schema)
        cur_frame.numRecords += 1
        cur_frame.indirection.append(currentRID)
        cur_frame.BaseRID.append(baseRID)
        cur_frame.rid.append(updateRID)
        
        base_frame.indirection[rid[2]] = updateRID
        
        cur_frame.unpin_page()
        base_frame.unpin_page()

    def extractdata(self, frame_index, num_columns, recordnumber):
        data = []
        cur_frame = self.frames[frame_index]
        for i in range(num_columns):
            if cur_frame.frameData[i] is not None:
                data.append(cur_frame.frameData[i].get_value(recordnumber))
            else:
                data.append(0)  # Default value for missing data
        return data

    def extractTPS(self, key_directory, num_columns):
        frame_index = self.get_frame_index(key_directory)
        cur_frame = self.frames[frame_index]
        try:
            if len(cur_frame.frameData) >= num_columns + 11:
                x = cur_frame.frameData[num_columns + 10].get_value(0)
                y = cur_frame.frameData[num_columns + 10].get_value(1)
            else:
                x = y = 0
        except:
            x = y = 0
        return [x, y]

    def close(self):
        self.frames = []
        self.numFrames = 0
        self.frame_directory = []
        self.frame_info = [None] * 100
        self.page_ranges = {}

class Frame:
    def __init__(self, numColumns):
        self.frameData = [None] * numColumns
        self.TPS = [0,0]
        self.numRecords = 0
        self.rid = []
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.BaseRID = []
        self.dirtyBit = False
        self.pinNum = 0
        self.numColumns = numColumns
        self.lastAccess = 0

    def has_capacity(self):
        if self.numRecords < 512:
            return True
        else: 
            return False
        
    def pin_page(self):
        self.pinNum += 1
        self.lastAccess = datetime.now()

    def unpin_page(self):
        self.pinNum -= 1

    def isPinned(self):
        if self.pinNum == 0:
            return False
        else:
            return True
