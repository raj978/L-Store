from lstore.config import *
from lstore.page import Page, PageRange
from datetime import datetime
import pickle
import os

class Bufferpool:
    def __init__(self):
        self.frames = []
        self.numFrames = 0
        self.frame_directory = []
        self.frame_info = [None] * FRAMECOUNT
        self.page_ranges = {}
        self.table_directories = {}

    def start_table_dir(self, tablename, numcols):
        self.table_directories[tablename] = {"numcols": numcols, "page_ranges": {}}

    def has_capacity(self):
        return self.numFrames < FRAMECOUNT

    def get_frame_index(self, key_directory, table_name):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == (key_directory, table_name):
                return i
        return None

    def in_pool(self, key, table_name):
        return self.get_frame_index(key, table_name) is not None

    def LRU(self):
        lru_time = datetime.max
        lru_index = 0
        for i in range(self.numFrames):
            if not self.frames[i].isPinned() and self.frames[i].lastAccess < lru_time:
                lru_time = self.frames[i].lastAccess
                lru_index = i
        return lru_index

    def evict_page(self, table_name):
        index = self.LRU()
        frame = self.frames[index]
        if frame.dirtyBit:
            self.write_to_disk(index, table_name)
        return index

    def get_empty_frame(self, numColumns, table_name):
        if not self.has_capacity():
            frame_index = self.evict_page(table_name)
            self.frames[frame_index] = Frame(numColumns)
            self.frame_info[frame_index] = None
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(numColumns))
            self.numFrames += 1
        return frame_index

    def write_to_disk(self, frame_index, table_name):
        frame = self.frames[frame_index]
        key_directory, _ = self.frame_info[frame_index]
        page_range_index, page_index, page_type = key_directory
        
        directory = self.table_directories[table_name]
        path = f"./ECS165/tables/{table_name}/pagerange{page_range_index}"
        if not os.path.exists(path):
            os.makedirs(path)
            
        filename = f"{path}/{'base' if page_type == 'b' else 'tail'}{page_index}.pkl"
        with open(filename, 'wb') as f:
            pickle.dump({
                'data': frame.frameData,
                'rid': frame.rid,
                'indirection': frame.indirection,
                'schema': frame.schema_encoding,
                'start_time': frame.start_time,
                'TPS': frame.TPS,
                'numRecords': frame.numRecords
            }, f)

    def read_from_disk(self, key_directory, table_name):
        page_range_index, page_index, page_type = key_directory
        path = f"./ECS165/tables/{table_name}/pagerange{page_range_index}"
        filename = f"{path}/{'base' if page_type == 'b' else 'tail'}{page_index}.pkl"
        
        if not os.path.exists(filename):
            return None
            
        with open(filename, 'rb') as f:
            data = pickle.load(f)
            return data

    def load_page(self, page_range_index, page_index, page_type, table_name):
        key_directory = (page_range_index, page_index, page_type)
        frame_index = self.get_frame_index(key_directory, table_name)
        
        if frame_index is not None:
            self.frames[frame_index].pin_page()
            return frame_index
            
        directory = self.table_directories[table_name]
        frame_index = self.get_empty_frame(directory["numcols"], table_name)
        
        data = self.read_from_disk(key_directory, table_name)
        if data:
            frame = self.frames[frame_index]
            frame.frameData = data['data']
            frame.rid = data['rid']
            frame.indirection = data['indirection']
            frame.schema_encoding = data['schema']
            frame.start_time = data['start_time']
            frame.TPS = data['TPS']
            frame.numRecords = data['numRecords']
            
        self.frame_info[frame_index] = (key_directory, table_name)
        self.frames[frame_index].pin_page()
        return frame_index

    def close(self, table_name):
        for i in range(self.numFrames):
            if self.frame_info[i] and self.frame_info[i][1] == table_name:
                if self.frames[i].dirtyBit:
                    self.write_to_disk(i, table_name)

    def allocate_page_range(self, num_columns, page_range_index, table_name):
        # Initialize base pages
        for i in range(MAX_BASEPAGES_PER_RANGE):
            frame_index = self.get_empty_frame(num_columns, table_name)
            self.frame_info[frame_index] = ((page_range_index, i, 'b'), table_name)
            frame = self.frames[frame_index]
            for j in range(num_columns):
                frame.frameData[j] = Page()
        
        # Initialize first tail page
        frame_index = self.get_empty_frame(num_columns, table_name)
        self.frame_info[frame_index] = ((page_range_index, 0, 't'), table_name)
        frame = self.frames[frame_index]
        for j in range(num_columns):
            frame.frameData[j] = Page()
        
class Frame:
    def __init__(self, numColumns):
        self.frameData = [None] * numColumns
        self.TPS = [0,0]
        self.numRecords = 0
        self.rid = []
        self.start_time = []
        self.schema_encoding = []
        self.indirection = []
        self.dirtyBit = False
        self.pinNum = 0
        self.numColumns = numColumns
        self.lastAccess = datetime.now()

    def has_capacity(self):
        return self.numRecords < 512

    def pin_page(self):
        self.pinNum += 1
        self.lastAccess = datetime.now()

    def unpin_page(self):
        self.pinNum = max(0, self.pinNum - 1)

    def isPinned(self):
        return self.pinNum > 0
