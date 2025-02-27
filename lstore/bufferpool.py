import os
from datetime import datetime
from typing import Optional

from lstore.config import *
from lstore.page import Page, PageRange


class Bufferpool:
    def __init__(self, path):
        self.frames: list[Frame] = []
        self.numFrames = 0
        self.frame_directory = []
        self.frame_info: list[Optional[tuple]] = [None] * FRAMECOUNT
        self.page_ranges = {}  # In-memory storage of page ranges
        self.path = path

    def has_capacity(self):
        return self.numFrames < FRAMECOUNT

    def allocate_page_range(self, num_columns, page_range_index):
        # Create new page range in memory
        if page_range_index not in self.page_ranges:
            self.page_ranges[page_range_index] = PageRange(num_columns)
            # Pre-initialize base pages
            for base_page_index in range(MAX_BASEPAGES_PER_RANGE):  # MAX_BASEPAGES_PER_RANGE
                key_directory = (page_range_index, base_page_index, 'b')
                frame_index = self.get_empty_frame(num_columns)
                self.frame_info[frame_index] = key_directory
                self.frames[frame_index].initialize_page()
        return self.page_ranges[page_range_index]

    def get_frame_index(self, key_directory):
        # First check if frame exists
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key_directory:
                return i

        page_range_index, page_index, mark = key_directory
        # If not found, create a new frame
        page_range = self.page_ranges.get(page_range_index)
        if not page_range:
            print(f"error in get_frame_index, page_range not found, detail: {key_directory}")
            return None

        frame_index = self.get_empty_frame(page_range.basePages[0].num_cols if page_range.basePages else 0)
        self.frame_info[frame_index] = key_directory
        return frame_index

    def LRU(self):
        evict_index = 0
        for i in range(len(self.frames) - 1):
            if self.frames[i].lastAccess > self.frames[i + 1].lastAccess:
                if not self.frames[i + 1].is_pinned():
                    evict_index = i + 1
        return evict_index

    def evict_page(self):
        evict_index = self.LRU()
        evicted_frame = self.frames[evict_index]

        if evicted_frame.dirtyBit:
            self.write_to_disk(evicted_frame)

        self.frames[evict_index] = Frame(evicted_frame.numColumns)
        return evict_index

    def get_empty_frame(self, numColumns):
        if not self.has_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(numColumns)
        else:
            frame_index = self.numFrames
            self.frames.append(Frame(numColumns))
            self.numFrames += 1
        return frame_index

    def load_page(self, RID, numColumns=0):
        page_range_index, page_index, record_id, mark = RID
        if mark == 'b':
            return self.load_base_page(page_range_index, page_index, numColumns)
        elif mark == 't':
            return self.load_tail_page(page_range_index, page_index, numColumns)
        else:
            raise Exception(f"error in load_page, mark is invalid, detail: {RID}")

    def load_base_page(self, page_range_index, base_page_index, numColumns=0):
        key_directory = (page_range_index, base_page_index, 'b')
        if self.in_pool(key_directory):
            return self.get_frame_index(key_directory)
        else:
            if numColumns == 0:
                raise Exception(f"error in load_base_page, numColumns is 0, detail: {key_directory}")
            frame_index = self.get_empty_frame(numColumns)
            cur_frame = self.frames[frame_index]
            cur_frame.pin_page()
            self.frame_info[frame_index] = key_directory

            # Get data from in-memory page range
            if page_range_index in self.page_ranges:
                page_range = self.page_ranges[page_range_index]
                if base_page_index < len(page_range.basePages):
                    base_page = page_range.basePages[base_page_index]
                    for i in range(numColumns):
                        cur_frame.update_data(i, None, base_page.pages[i])
            cur_frame.unpin_page()
            return frame_index

    def load_tail_page(self, page_range_index, tail_page_index, numColumns):
        key_directory = (page_range_index, tail_page_index, 't')

        # Check if page range exists
        if page_range_index not in self.page_ranges:
            self.allocate_page_range(numColumns, page_range_index)

        page_range = self.page_ranges[page_range_index]

        # Create tail page if it doesn't exist
        while len(page_range.tailPages) <= tail_page_index:
            page_range.add_tail_page(numColumns)

        frame_index = self.get_frame_index(key_directory)
        if frame_index is None:
            frame_index = self.get_empty_frame(numColumns)
            self.frame_info[frame_index] = key_directory

        cur_frame = self.frames[frame_index]
        cur_frame.pin_page()

        # Initialize frame data if needed
        if cur_frame.need_initialize():
            tail_page = page_range.tailPages[tail_page_index]
            cur_frame.initialize_page()
            for i in range(numColumns):
                cur_frame.update_data(i, None, tail_page.pages[i])

        cur_frame.unpin_page()
        return frame_index

    def in_pool(self, key):
        for i in range(len(self.frame_info)):
            if self.frame_info[i] == key:
                return True
        return False

    def insertRecBP(self, RID, start_time, schema_encoding, origin_rid, *columns, numColumns):
        # print(f"insertRecBP with RID: {RID}, start_time: {start_time}, schema_encoding: {schema_encoding}, indirection: {indirection}, numColumns: {numColumns}")
        page_range_index, base_page_index, record_id, mark = RID
        if mark != 'b':
            raise Exception(f"error in insertRecBP, mark is invalid, detail: {RID}")

        frame_index = self.get_frame_index((page_range_index, base_page_index, mark))
        cur_frame = self.frames[frame_index]
        # print(f"frame_index: {frame_index}, cur_frame.numRecords: {cur_frame.numRecords}")
        if not cur_frame.has_capacity():
            return None

        for i in range(numColumns):
            cur_frame.write_data(i, columns[i])
        cur_frame.pin_page()

        cur_frame.numRecords += 1
        cur_frame.rid.append(RID)
        cur_frame.start_time.append(start_time)
        cur_frame.schema_encoding.append(schema_encoding)
        cur_frame.indirection.append(origin_rid)

        cur_frame.unpin_page()
        return cur_frame

    def insertRecTP(self, new_rid, current_rid, origin_rid, base_page_frame_index, *columns):
        # print(f"insertRecTP with new_rid: {new_rid}, current_rid: {current_rid}, origin_rid: {origin_rid}, base_page_frame_index: {base_page_frame_index}, columns: {columns}")
        new_page_range_index, new_tail_page_index, new_record_id, new_mark = new_rid
        current_page_range_index, current_tail_page_index, current_record_id, current_mark = current_rid
        # First ensure the tail page exists
        new_frame_index = self.load_tail_page(new_page_range_index, new_tail_page_index, len(columns))

        new_frame = self.frames[new_frame_index]
        base_frame = self.frames[base_page_frame_index]

        new_frame.pin_page()
        base_frame.pin_page()

        if new_frame.numRecords >= MAX_RECORDS_PER_PAGE:
            new_frame.unpin_page()
            frame_index = self.load_tail_page(new_page_range_index, new_tail_page_index + 1, len(columns))
            new_frame = self.frames[frame_index]
            new_frame.pin_page()

        schema = ''
        new_data = []
        for j in range(len(columns)):
            if columns[j] is not None:
                new_frame.write_data(j, columns[j])
                schema += '1'
                base_frame.set_schema_encoding(j, 1)
            else:
                new_frame.write_data(j, columns[j])
                schema += '0'
            new_data.append(columns[j])

        new_frame.schema_encoding.append(schema)
        new_frame.numRecords += 1
        new_frame.set_indirection(new_record_id, current_rid)
        new_frame.BaseRID.append(origin_rid)
        new_frame.rid.append(new_rid)

        base_frame.set_indirection(current_record_id, new_rid)

        new_frame.unpin_page()
        base_frame.unpin_page()

    def extract_data(self, frame_index, num_columns, record_id):
        data = []
        cur_frame = self.frames[frame_index]
        for frame_index in range(num_columns):
            result = cur_frame.read_data(frame_index, record_id)
            data.append(result)
        return data

    def extractTPS(self, key_directory, num_columns):
        frame_index = self.get_frame_index(key_directory)
        cur_frame = self.frames[frame_index]
        try:
            if len(cur_frame.frameData) >= num_columns + 11:
                x = cur_frame.read_data(num_columns + 10, 0)
                y = cur_frame.read_data(num_columns + 10, 1)
            else:
                x = y = 0
        except:
            x = y = 0
        return [x, y]

    def write_to_disk(self, frame):
        disk_directory = f"{self.path}/pages"
        if not os.path.exists(disk_directory):
            os.makedirs(disk_directory)
        disk_filename = f"{disk_directory}/page_{id(frame)}.txt"

        with open(disk_filename, 'w') as f:
            for i in range(len(frame.frameData)):
                data = frame.read_data(i, 0)
                if data is not None:
                    f.write(f"Column {i}: {data}\n")
            # print(f"Frame data written to {disk_filename}")

        frame.reset_dirty()

    def close(self):
        for frame in self.frames:
            if frame.dirtyBit:
                self.write_to_disk(frame)
        self.frames = []
        self.numFrames = 0
        self.frame_directory = []
        self.frame_info = [None] * 100
        self.page_ranges = {}


class Frame:
    def __init__(self, numColumns):
        self.frameData: list[Page] = [None] * numColumns  # List of pages in the frame
        self.TPS = [0, 0]  # Transaction timestamps, if applicable
        self.numRecords = 0  # Number of records in the frame
        self.rid = []  # Record IDs for the records in the frame
        self.start_time = []  # Start times for the records
        self.schema_encoding = []  # Schema encoding for the records
        self.indirection = []  # Indirection pointers
        self.BaseRID = []  # Base Record IDs for updates
        self.dirtyBit = False  # Indicates whether the frame has been modified
        self.pinNum = 0  # The number of times this frame has been pinned
        self.numColumns = numColumns  # Number of columns per page/frame
        self.lastAccess = 0  # Timestamp of last access

    def need_initialize(self):
        return self.frameData[0] is None

    def initialize_page(self):
        self.frameData = [Page() for _ in range(self.numColumns)]

    def set_indirection(self, record_id, data):
        self.pin_page()
        if record_id >= len(self.indirection):
            self.indirection.extend([None] * (record_id + 1 - len(self.indirection)))
        self.indirection[record_id] = data
        self.unpin_page()

    def set_schema_encoding(self, record_id, data):
        self.pin_page()
        if record_id >= len(self.schema_encoding):
            self.schema_encoding.extend([None] * (record_id + 1 - len(self.schema_encoding)))
        self.schema_encoding[record_id] = data
        self.unpin_page()

    def get_indirection(self, record_id):
        if record_id >= len(self.indirection):
            return None

        self.pin_page()
        result = self.indirection[record_id]
        self.unpin_page()
        return result

    def set_rid(self, record_id, data):
        self.pin_page()
        if record_id >= len(self.rid):
            self.rid.extend([None] * (record_id + 1 - len(self.rid)))
        self.rid[record_id] = data
        self.unpin_page()

    def has_capacity(self):
        if self.numRecords < MAX_RECORDS_PER_PAGE:
            return True
        else:
            return False

    def pin_page(self):
        self.pinNum += 1
        self.lastAccess = datetime.now()

    def unpin_page(self):
        self.pinNum -= 1

    def is_pinned(self):
        if self.pinNum == 0:
            return False
        else:
            return True

    def mark_dirty(self):
        """Mark the frame as dirty if modified."""
        self.dirtyBit = True

    def reset_dirty(self):
        """Reset the dirty flag if changes are written back."""
        self.dirtyBit = False

    def write_data(self, column_index: int, data):
        """Write data to a specific column and mark the frame as dirty."""
        self.pin_page()
        if data is None:
            return 0
        if self.frameData[column_index] is None:
            self.frameData[column_index] = Page()  # Initialize if needed
        self.frameData[column_index].write(data)  # Write data to the page
        self.mark_dirty()  # Mark the frame as dirty since it's been modified
        self.unpin_page()
        return 1

    def update_data(self, column_index: int, record_id, data):
        """Write data to a specific column and mark the frame as dirty."""
        self.pin_page()
        if data is None:
            return 0
        if self.frameData[column_index] is None:
            self.frameData[column_index] = Page()  # Initialize if needed
        if record_id is not None:
            self.frameData[column_index].update(record_id, data)  # Write data to the page
        else:
            self.frameData[column_index] = data
        self.mark_dirty()  # Mark the frame as dirty since it's been modified
        self.unpin_page()
        return 1

    def read_data(self, column_index, record_id):
        """Read data from a specific column."""
        self.pin_page()
        result = None
        if self.frameData[column_index] is not None:
            result = self.frameData[column_index].get_value(record_id)
        self.unpin_page()
        return result
