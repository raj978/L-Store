import array
import os
import threading
import time

from lstore.bufferpool import Bufferpool
from lstore.config import *
from lstore.index import Index
from lstore.page import *
from lstore.lock_manager import LockManager


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    def __init__(self, name, num_columns, key, bufferpool, isNew, path):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool: Bufferpool = bufferpool
        self.num_pageRanges = 0
        self.page_range_index = 0
        self.base_page_index = 0
        self.record_id = 0
        self.base_page_frame_index = 0
        self.tail_page_frame_index = 0
        self.merge_thread = None
        self.path = path
        if isNew:
            self.add_page_range(self.num_columns)

        if not os.path.exists(f"{self.path}/{self.name}"):
            os.mkdir(f"{self.path}/{self.name}")

        self.lock_manager = LockManager()

    def add_page_range(self, numCols):

        self.bufferpool.allocate_page_range(self.num_columns, self.page_range_index)
        self.num_pageRanges += 1

    def updateCurBP(self):
        self.base_page_index += 1
        self.record_id = 0

    def getCurBP(self):
        return self.pageRange[self.page_range_index].basePages[self.base_page_index]

    def updateCurRecord(self):
        self.record_id += 1

    def createBP_RID(self):
        result = (self.page_range_index, self.base_page_index, self.record_id, 'b')
        return result

    def createTP_RID(self, frame_index):
        cur_frame = self.bufferpool.frames[frame_index]
        result = (self.page_range_index, len(cur_frame.rid), self.record_id, 't')
        return result

    def find_record(self, key, rid, projected_columns_index, TPS):
        if rid[3] == 't':
            frame_index = self.bufferpool.load_tail_page(rid[0], rid[1], self.num_columns)
            data = self.bufferpool.extract_data(frame_index, self.num_columns, rid[2])

        if rid[3] == 'b':
            self.base_page_frame_index = self.bufferpool.load_base_page(rid[0], rid[1], self.num_columns)
            data = self.bufferpool.extract_data(self.base_page_frame_index, self.num_columns, rid[2])

        record = []
        for i in range(len(projected_columns_index)):
            if projected_columns_index[i] == 1:
                record.append(data[i])
        return Record(key, rid, record)

    def curBP_has_Capacity(self):
        return self.bufferpool.frames[self.base_page_frame_index].numRecords < MAX_RECORDS_PER_PAGE

    def curPR_has_Capacity(self):
        return self.bufferpool.frames[15].numRecords < MAX_RECORDS_PER_PAGE

    def insertRec(self, start_time, schema_encoding, *columns):

        # if self.base_page_index locked then abort

        # acquire x lock for record and ix for page

        self.base_page_frame_index = self.bufferpool.get_frame_index((self.page_range_index, self.base_page_index, 'b'))
        if self.base_page_frame_index is None:
            raise Exception(f"Error: Could not find frame for base page with index {self.base_page_index}")

        RID = self.createBP_RID()
        self.page_directory[RID] = None
        origin_rid = RID

        cur_frame = self.bufferpool.insertRecBP(RID, start_time, schema_encoding, origin_rid, *columns, numColumns=self.num_columns)
        self.updateCurRecord()

        if not cur_frame.has_capacity():
            if self.record_id >= MAX_RECORDS_PER_PAGE * MAX_BASEPAGES_PER_RANGE:
                self.page_range_index += 1
                self.record_id = 0
                self.base_page_index = 0
                self.add_page_range(self.num_columns)
            else:
                self.updateCurBP()

        for i in range(len(columns)):
            self.index.add_node(i, columns[i], RID)

    def updateRec(self, current_rid, *columns):
        page_range_index, page_index, record_id, mark = current_rid
    
        # if page_index locked then abort

        # acquire x lock for record and ix for page

        # Load base page
        self.base_page_frame_index = self.bufferpool.get_frame_index((page_range_index, page_index, 'b'))
        base_frame = self.bufferpool.frames[self.base_page_frame_index]
        origin_rid = base_frame.get_indirection(record_id)

        # update data
        origin_columns = []
        new_columns = []
        for j in range(len(columns)):
            origin_columns.append(base_frame.read_data(j, record_id))
            if columns[j] is not None:
                base_frame.update_data(j, record_id, columns[j])
            new_columns.append(base_frame.read_data(j, record_id))
        # Find or create appropriate tail page
        numTPS = 0
        pagerange_path = f"{self.path}/{self.name}/pagerange{page_range_index}/tailPages"
        if os.path.exists(pagerange_path):
            numTPS = len([f for f in os.listdir(pagerange_path) if f.startswith('tail') and f.endswith('.pkl')])
            if numTPS > 0:
                numTPS -= 1

        # Load tail page
        self.tail_page_frame_index = self.bufferpool.get_frame_index((page_range_index, numTPS, 't'))
        tail_frame = self.bufferpool.frames[self.tail_page_frame_index]

        # Create new tail page if current is full
        if not tail_frame.has_capacity():
            numTPS += 1
            tail_frame.pin_page()
            self.tail_page_frame_index = self.bufferpool.get_frame_index((page_range_index, numTPS, 't'))
            tail_frame = self.bufferpool.frames[self.tail_page_frame_index]
            tail_frame.unpin_page()

        # Create new tail record RID
        new_rid = (page_range_index, numTPS, tail_frame.numRecords, 't')
        self.bufferpool.insertRecTP(new_rid, current_rid, origin_rid, self.base_page_frame_index, *origin_columns)

        # Update page directory
        self.page_directory[new_rid] = None

        # Update indices
        for i in range(len(columns)):
            if columns[i] is not None:
                self.index.update_node(i, columns[i], current_rid)

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
            page_range_index = self.page_range_index - 1

        if page_range_index < 0:
            return

        base_frames = {}
        tail_frames = {}

        # Load base pages and pin them
        for bp_index in range(MAX_BASEPAGES_PER_RANGE):
            frame_idx = self.bufferpool.load_page((page_range_index, bp_index, None, 'b'))
            if frame_idx is not None:
                base_frames[bp_index] = self.bufferpool.frames[frame_idx]
                base_frames[bp_index].pin_page()

        # Find and load all tail pages
        tail_page_path = f"{self.path}/tables/{self.name}/pagerange{page_range_index}/tailPages"
        if os.path.exists(tail_page_path):
            tp_files = [f for f in os.listdir(tail_page_path) if f.startswith('tail') and f.endswith('.pkl')]
            for tp_file in tp_files:
                tp_index = int(tp_file.replace("tail", "").replace(".pkl", ""))
                frame_idx = self.bufferpool.load_page((page_range_index, tp_index, None, 't'))
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
                            frame_idx = self.bufferpool.load_page((current_rid[0], current_rid[1], None, 't'))
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
                                latest_values[col] = tail_frame.read_data(col, current_rid[2])
                                schema = schema[:col] + '1' + schema[col + 1:]

                        current_rid = tail_frame.indirection[current_rid[2]]

                    # Update base record with merged values
                    for col in range(self.num_columns):
                        base_frame.update_data(col, record_idx, latest_values[col])

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
        arr = array.array('i', [self.key, self.num_columns, self.page_range_index, self.base_page_index, self.record_id])
        with open(path, 'wb') as file:
            arr.tofile(file)

    def pullpagerangesfromdisk(self, path):
        pagerange_dir = path + "/pagerange"
        if not os.path.exists(pagerange_dir):
            return

        for entry in os.listdir(pagerange_dir):
            if entry.startswith("pagerange"):
                page_range_index = int(entry.replace("pagerange", ""))
                pr_path = os.path.join(pagerange_dir, entry)

                # Load base pages
                for bp_file in os.listdir(pr_path):
                    if bp_file.startswith("base"):
                        bp_index = int(bp_file.replace("base", "").replace(".pkl", ""))
                        self.bufferpool.load_page((page_range_index, bp_index, None, 'b'))

                # Load tail pages
                for tp_file in os.listdir(pr_path):
                    if tp_file.startswith("tail"):
                        tp_index = int(tp_file.replace("tail", "").replace(".pkl", ""))
                        self.bufferpool.load_page((page_range_index, tp_index, None, 't'))

    def get_record(self, rid):

        # if page_index locked then abort

        # acquire s lock for record and is for page

        if rid not in self.page_directory:
            return None

        frame_index = self.bufferpool.load_page(rid)
        frame = self.bufferpool.frames[frame_index]

        record_columns = []
        for col in range(self.num_columns):
            value = frame.read_data(col, rid[2])
            record_columns.append(value)

        frame.unpin_page()
        return Record(rid, record_columns[self.key], record_columns)

    def get_record_version(self, rid, version):

        # if page_index locked then abort

        # acquire s lock for record and is for page

        if rid not in self.page_directory:
            return None

        page_range_index, page_index, record_id, mark = rid
        base_frame_index = self.bufferpool.get_frame_index((page_range_index, page_index, 'b'))

        if version == 0:  # current version
            record_columns = self.bufferpool.extract_data(base_frame_index, self.num_columns, record_id)
            # print(f"record_columns: {record_columns}")
            record_key = record_columns[self.key]
            return Record(rid, record_key, record_columns)
        elif version == -1:  # Original version
            base_frame = self.bufferpool.frames[base_frame_index]
            version_rid = base_frame.indirection[record_id]  # Start from the indirection of the base frame

            frame_index = base_frame_index
            frame = base_frame
            # Traverse the indirection chain to find the original (earliest) version
            while version_rid is not None and version_rid != rid:
                # print("get_record_version with version == -1, get version_rid: ", version_rid)
                version_page_range_index, version_page_index, version_record_id, version_mark = version_rid
                frame_index = self.bufferpool.load_page((version_page_range_index, version_page_index, version_record_id, 't'))
                frame = self.bufferpool.frames[frame_index]

                # If there is no further indirection, we've found the original version
                next_rid = frame.get_indirection(version_record_id)
                if next_rid is None or next_rid == rid:  # No further indirection, original version reached
                    break
                version_rid = next_rid

            # Retrieve the record from the original version's frame
            version_page_range_index, version_page_index, version_record_id, version_mark = version_rid
            record_columns = self.bufferpool.extract_data(frame_index, self.num_columns, version_record_id)
            record_key = record_columns[self.key]
            return Record(version_rid, record_key, record_columns)
        else:
            base_frame = self.bufferpool.frames[base_frame_index]
            updates_seen = 0
            version_rid = base_frame.indirection[record_id]  # Start from the indirection of the base frame

            target_version = abs(version) if version < 0 else version

            frame_index = base_frame_index
            frame = base_frame
            # Traverse the indirection chain to find the original (earliest) version
            while version_rid != rid and updates_seen < target_version:
                updates_seen += 1
                # print("get_record_version with version == -1, get version_rid: ", version_rid)
                version_page_range_index, version_page_index, version_record_id, version_mark = version_rid
                frame_index = self.bufferpool.load_page((version_page_range_index, version_page_index, version_record_id, 't'))
                frame = self.bufferpool.frames[frame_index]

            # Retrieve the record from the original version's frame
            version_page_range_index, version_page_index, version_record_id, version_mark = version_rid
            record_columns = self.bufferpool.extract_data(frame_index, self.num_columns, version_record_id)
            record_key = record_columns[self.key]
            return Record(version_rid, record_key, record_columns)
