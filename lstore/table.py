from lstore.index import Index
from lstore.page import *
from lstore.config import *
import struct
import array
import os
import pickle
import threading
import time

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    def __init__(self, name, num_columns, key, bufferpool, isNew):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool = bufferpool
        self.num_pageRanges = 0
        self.curPageRange = 0
        self.curBP = 0
        self.curRecord = 0
        self.curFrameIndexBP = 0
        self.curFrameIndexTP = 0
        self.merge_thread = None
        self.path = './ECS165'  # Add path variable
        if isNew:
            self.add_page_range(self.num_columns)

    def add_page_range(self, numCols):
        self.bufferpool.allocate_page_range(self.num_columns, self.curPageRange, self.name)
        self.num_pageRanges += 1

    def updateCurBP(self):
        self.curBP += 1
        if self.curBP == -1:
            self.curBP = 0
    
    def getCurBP(self):
        return self.pageRange[self.curPageRange].basePages[self.curBP]

    def updateCurRecord(self):
        self.curRecord += 1

    def createBP_RID(self):
        return (self.curPageRange, self.curBP, self.curRecord, 'b')

    def createTP_RID(self, frame_index):
        cur_frame = self.bufferpool.frames[frame_index]
        return (self.curPageRange, len(cur_frame.rid), self.curRecord, 't')

    def find_record(self, key, rid, projected_columns_index, TPS):
        if rid[3] == 't':
            frame_index = self.bufferpool.load_tail_page(rid[0], rid[1], self.num_columns)
            data = self.bufferpool.extractdata(frame_index, self.num_columns, rid[2])
        
        if rid[3] == 'b':
            self.curFrameIndexBP = self.bufferpool.load_base_page(rid[0], rid[1], self.num_columns)
            data = self.bufferpool.extractdata(self.curFrameIndexBP, self.num_columns, rid[2])

        record = []
        for i in range(len(projected_columns_index)):
            if projected_columns_index[i] == 1:
                record.append(data[i])
        return Record(key, rid, record)

    def curBP_has_Capacity(self):
        return self.bufferpool.frames[self.curFrameIndexBP].numRecords < 512

    def curPR_has_Capacity(self):
        return self.bufferpool.frames[15].numRecords < 512

    def insertRec(self, start_time, schema_encoding, *columns, rollback=False):
        if rollback:
            self.insertRollback(*columns)
            return
        
        self.curFrameIndexBP = self.bufferpool.load_page(self.curPageRange, self.curBP, 'b', self.name)
        RID = self.createBP_RID()
        self.page_directory[RID] = RID
        indirection = RID
        
        cur_frame = self.bufferpool.frames[self.curFrameIndexBP]
        for i in range(self.num_columns):
            if cur_frame.frameData[i] is None:
                cur_frame.frameData[i] = Page()
            cur_frame.frameData[i].write(columns[i])
            
        record_index = self.curRecord % MAX_RECORDS_PER_PAGE
        # RID Error fixing

        # Initialize lists if they are None
        if cur_frame.rid is None:
            cur_frame.rid = [None] * MAX_RECORDS_PER_PAGE
        if cur_frame.start_time is None:
            cur_frame.start_time = [None] * MAX_RECORDS_PER_PAGE
        if cur_frame.schema_encoding is None:
            cur_frame.schema_encoding = [None] * MAX_RECORDS_PER_PAGE
        if cur_frame.indirection is None:
            cur_frame.indirection = [None] * MAX_RECORDS_PER_PAGE

        # Extend lists if the current record index is out of range
        if record_index >= len(cur_frame.rid):
            needed = record_index + 1 - len(cur_frame.rid)
            cur_frame.rid.extend([None] * needed)
            cur_frame.start_time.extend([None] * needed)
            cur_frame.schema_encoding.extend([None] * needed)
            cur_frame.indirection.extend([None] * needed)


        cur_frame.rid[record_index] = RID
        cur_frame.start_time[record_index] = start_time
        cur_frame.schema_encoding[record_index] = schema_encoding
        cur_frame.indirection[record_index] = indirection
        cur_frame.numRecords += 1
        cur_frame.dirtyBit = True
        cur_frame.unpin_page()
        
        self.updateCurRecord()
        
        if not cur_frame.has_capacity():
            if self.curRecord == MAX_RECORDS_PER_PAGE * MAX_BASEPAGES_PER_RANGE:
                self.curPageRange += 1
                self.curRecord = 0
                self.curBP = 0
                self.add_page_range(self.num_columns)
            else:
                self.updateCurBP()
                
        for i in range(len(columns)):
            self.index.add_node(i, columns[i], RID)

    def updateRec(self, rid, *columns):
        # Load base page
        self.curFrameIndexBP = self.bufferpool.load_page(rid[0], rid[1], 'b', self.name)
        base_frame = self.bufferpool.frames[self.curFrameIndexBP]
        base_frame.pin_page()
        
        # Initialize arrays if they don't exist
        if base_frame.indirection is None:
            base_frame.indirection = [None] * MAX_RECORDS_PER_PAGE
        if base_frame.rid is None:
            base_frame.rid = [None] * MAX_RECORDS_PER_PAGE
            
        # Get current record's indirection
        record_idx = rid[2]
        if record_idx >= len(base_frame.indirection):
            base_frame.indirection.extend([None] * (record_idx + 1 - len(base_frame.indirection)))
            base_frame.rid.extend([None] * (record_idx + 1 - len(base_frame.rid)))
            
        currentRID = base_frame.indirection[record_idx]
        if currentRID is None:
            currentRID = rid
        baseRID = rid

        # Find or create appropriate tail page
        numTPS = 0
        pagerange_path = f"{self.path}/tables/{self.name}/pagerange{rid[0]}/tailPages"
        if os.path.exists(pagerange_path):
            numTPS = len([f for f in os.listdir(pagerange_path) if f.startswith('tail') and f.endswith('.pkl')])
            if numTPS > 0:
                numTPS -= 1

        # Load tail page
        self.curFrameIndexTP = self.bufferpool.load_page(rid[0], numTPS, 't', self.name)
        tail_frame = self.bufferpool.frames[self.curFrameIndexTP]
        
        # Create new tail page if current is full
        if not tail_frame.has_capacity():
            numTPS += 1
            tail_frame.unpin_page()
            self.curFrameIndexTP = self.bufferpool.load_page(rid[0], numTPS, 't', self.name)
            tail_frame = self.bufferpool.frames[self.curFrameIndexTP]
        
        # Create new tail record RID
        updateRID = (rid[0], numTPS, tail_frame.numRecords, 't')
        
        # Initialize tail frame arrays if needed
        if tail_frame.frameData is None:
            tail_frame.frameData = [None] * self.num_columns
        if tail_frame.indirection is None:
            tail_frame.indirection = [None] * MAX_RECORDS_PER_PAGE
        if tail_frame.rid is None:
            tail_frame.rid = [None] * MAX_RECORDS_PER_PAGE
        if tail_frame.schema_encoding is None:
            tail_frame.schema_encoding = [None] * MAX_RECORDS_PER_PAGE
            
        # Update tail record data
        schema = ''
        for j in range(len(columns)):
            if tail_frame.frameData[j] is None:
                tail_frame.frameData[j] = Page()
                
            if columns[j] is not None:
                tail_frame.frameData[j].write(columns[j])
                schema += '1'
            else:
                # Get value from current record
                if currentRID[3] == 't':
                    cur_frame_idx = self.bufferpool.load_page(currentRID[0], currentRID[1], 't', self.name)
                    cur_frame = self.bufferpool.frames[cur_frame_idx]
                    value = cur_frame.frameData[j].get_value(currentRID[2])
                    cur_frame.unpin_page()
                else:
                    value = base_frame.frameData[j].get_value(currentRID[2])
                tail_frame.frameData[j].write(value)
                schema += '0'
        # RiD error fixing
        # Ensure arrays are not None
        if tail_frame.rid is None:
            tail_frame.rid = []
        if tail_frame.schema_encoding is None:
            tail_frame.schema_encoding = []
        if tail_frame.indirection is None:
            tail_frame.indirection = []
        
        # If numRecords is at or beyond current array size, extend them
        if tail_frame.numRecords >= len(tail_frame.rid):
            needed = tail_frame.numRecords + 1 - len(tail_frame.rid)
            tail_frame.rid.extend([None] * needed)
            tail_frame.schema_encoding.extend([None] * needed)
            tail_frame.indirection.extend([None] * needed)

        # Now safely assign
        tail_frame.rid[tail_frame.numRecords] = updateRID
        tail_frame.schema_encoding[tail_frame.numRecords] = schema
        tail_frame.indirection[tail_frame.numRecords] = currentRID
        tail_frame.numRecords += 1
        tail_frame.dirtyBit = True
        
        # Update base record indirection
        base_frame.indirection[record_idx] = updateRID
        base_frame.dirtyBit = True
        
        # Update page directory
        self.page_directory[updateRID] = updateRID
        
        # Update indices
        for i in range(len(columns)):
            if columns[i] is not None:
                self.index.update_node(i, columns[i], rid)
        
        # Cleanup
        tail_frame.unpin_page()
        base_frame.unpin_page()
        
        # Consider merging if we have many updates
        if tail_frame.numRecords >= MAX_RECORDS_PER_PAGE:
            self.start_merge_thread()

    def greaterthan(self, a, b):
        """Compare two RID tuples for ordering"""
        if a[0] > b[0]:
            return True
        elif a[0] == b[0]:
            if a[1] > b[1]:
                return True
        return False

    def merge(self, page_range_index=None):
        """Consolidate a page range's updates into base pages"""
        if page_range_index is None:
            page_range_index = self.curPageRange - 1
            
        if page_range_index < 0:
            return
            
        base_frames = {}
        tail_frames = {}
        
        # Load base pages and pin them
        for bp_index in range(MAX_BASEPAGES_PER_RANGE):
            frame_idx = self.bufferpool.load_page(page_range_index, bp_index, 'b', self.name)
            if frame_idx is not None:
                base_frames[bp_index] = self.bufferpool.frames[frame_idx]
                base_frames[bp_index].pin_page()
        
        # Find and load all tail pages
        tail_page_path = f"{self.path}/tables/{self.name}/pagerange{page_range_index}/tailPages"
        if os.path.exists(tail_page_path):
            tp_files = [f for f in os.listdir(tail_page_path) if f.startswith('tail') and f.endswith('.pkl')]
            for tp_file in tp_files:
                tp_index = int(tp_file.replace("tail", "").replace(".pkl", ""))
                frame_idx = self.bufferpool.load_page(page_range_index, tp_index, 't', self.name)
                if frame_idx is not None:
                    tail_frames[tp_index] = self.bufferpool.frames[frame_idx]
                    tail_frames[tp_index].pin_page()
        
        # For each base page
        for bp_index, base_frame in base_frames.items():
            for record_idx in range(base_frame.numRecords):
                if base_frame.indirection is None or record_idx >= len(base_frame.indirection) or base_frame.indirection[record_idx] is None:
                    continue  # Skip if indirection is not properly set
                
                if base_frame.rid is None or record_idx >= len(base_frame.rid) or base_frame.rid[record_idx] is None:
                    continue  # Skip if RID is not properly set
                    
                current_rid = base_frame.indirection[record_idx]
                base_rid = base_frame.rid[record_idx]
                
                if current_rid != base_rid:  # Record has updates
                    latest_values = [None] * self.num_columns
                    schema = '0' * self.num_columns
                    
                    # Follow indirection chain to get latest values
                    while current_rid != base_rid:
                        if current_rid[3] != 't':
                            break
                            
                        # Safely get tail frame or load it if not available
                        if current_rid[1] not in tail_frames:
                            frame_idx = self.bufferpool.load_page(current_rid[0], current_rid[1], 't', self.name)
                            if frame_idx is None:
                                break  # Can't load this tail page, stop the chain
                            tail_frames[current_rid[1]] = self.bufferpool.frames[frame_idx]
                            tail_frames[current_rid[1]].pin_page()
                            
                        tail_frame = tail_frames[current_rid[1]]
                        
                        # Validate tail frame data
                        if (tail_frame.schema_encoding is None or 
                            tail_frame.indirection is None or
                            current_rid[2] >= len(tail_frame.schema_encoding) or
                            current_rid[2] >= len(tail_frame.indirection)):
                            break  # Invalid data, stop the chain
                        
                        record_schema = tail_frame.schema_encoding[current_rid[2]]
                        
                        # Only update values that haven't been set yet
                        for col in range(self.num_columns):
                            if record_schema[col] == '1' and latest_values[col] is None:
                                if tail_frame.frameData[col] is None:
                                    tail_frame.frameData[col] = Page()
                                latest_values[col] = tail_frame.frameData[col].get_value(current_rid[2])
                                schema = schema[:col] + '1' + schema[col+1:]
                                
                        current_rid = tail_frame.indirection[current_rid[2]]
                    
                    # Update base record with merged values
                    for col in range(self.num_columns):
                        if latest_values[col] is not None:
                            if base_frame.frameData[col] is None:
                                base_frame.frameData[col] = Page()
                            base_frame.frameData[col].update(record_idx, latest_values[col])
                    
                    # Reset base record's indirection to point to itself
                    base_frame.indirection[record_idx] = base_rid
                    base_frame.schema_encoding[record_idx] = schema
                    base_frame.dirtyBit = True
                    
                    # Update index for changed values
                    for col in range(self.num_columns):
                        if latest_values[col] is not None:
                            self.index.update_node(col, latest_values[col], base_rid)
        
        # Cleanup
        for frame in base_frames.values():
            if frame.dirtyBit:
                frame.unpin_page()
        
        for frame in tail_frames.values():
            frame.unpin_page()

    def start_merge_thread(self):
        if self.merge_thread is None or not self.merge_thread.is_alive():
            self.merge_thread = threading.Thread(target=self._background_merge)
            self.merge_thread.daemon = True
            self.merge_thread.start()

    def _background_merge(self):
        while True:
            self.merge()
            time.sleep(MERGE_INTERVAL)  # Sleep between merges

    def savemetadata(self, path):
        arr = array.array('i', [self.key, self.num_columns, self.curPageRange, self.curBP, self.curRecord])
        with open(path, 'wb') as file:
            arr.tofile(file)

    def pullpagerangesfromdisk(self, path):
        pagerange_dir = path + "/pagerange"
        if not os.path.exists(pagerange_dir):
            return
            
        for entry in os.listdir(pagerange_dir):
            if entry.startswith("pagerange"):
                pr_index = int(entry.replace("pagerange", ""))
                pr_path = os.path.join(pagerange_dir, entry)
                
                # Load base pages
                for bp_file in os.listdir(pr_path):
                    if bp_file.startswith("base"):
                        bp_index = int(bp_file.replace("base", "").replace(".pkl", ""))
                        self.bufferpool.load_page(pr_index, bp_index, 'b', self.name)
                        
                # Load tail pages
                for tp_file in os.listdir(pr_path):
                    if tp_file.startswith("tail"):
                        tp_index = int(tp_file.replace("tail", "").replace(".pkl", ""))
                        self.bufferpool.load_page(pr_index, tp_index, 't', self.name)

    def get_record(self, rid):
        if rid not in self.page_directory:
            return None
            
        frame_index = self.bufferpool.load_page(rid[0], rid[1], rid[3], self.name)
        frame = self.bufferpool.frames[frame_index]
        
        record_columns = []
        for col in range(self.num_columns):
            if frame.frameData[col] is None:
                frame.frameData[col] = Page()
            value = frame.frameData[col].get_value(rid[2])
            record_columns.append(value)
            
        frame.unpin_page()
        return Record(rid, record_columns[self.key], record_columns)

    def get_record_version(self, rid, version):
        if rid not in self.page_directory:
            return None
            
        base_frame_index = self.bufferpool.load_page(rid[0], rid[1], 'b', self.name)
        base_frame = self.bufferpool.frames[base_frame_index]
        
        if version == -1:  # Original version
            record_columns = []
            for col in range(self.num_columns):
                if base_frame.frameData[col] is None:
                    base_frame.frameData[col] = Page()
                value = base_frame.frameData[col].get_value(rid[2])
                record_columns.append(value)
            base_frame.unpin_page()
            return Record(rid, record_columns[self.key], record_columns)
            
        # For other versions, traverse the indirection chain
        current_rid = rid if version <= -2 else base_frame.indirection[rid[2]]
        updates_seen = 0
        target_version = abs(version) if version < 0 else version
        
        while current_rid != rid and updates_seen < target_version:
            updates_seen += 1
            frame_index = self.bufferpool.load_page(current_rid[0], current_rid[1], 't', self.name)
            frame = self.bufferpool.frames[frame_index]
            current_rid = frame.indirection[current_rid[2]]
            frame.unpin_page()
            
        frame_index = self.bufferpool.load_page(current_rid[0], current_rid[1], 
                                              't' if current_rid != rid else 'b', 
                                              self.name)
        frame = self.bufferpool.frames[frame_index]
        
        record_columns = []
        for col in range(self.num_columns):
            if frame.frameData[col] is None:
                frame.frameData[col] = Page()
            value = frame.frameData[col].get_value(current_rid[2])
            record_columns.append(value)
            
        frame.unpin_page()
        base_frame.unpin_page()
        return Record(rid, record_columns[self.key], record_columns)
