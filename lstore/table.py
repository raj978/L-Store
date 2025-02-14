from lstore.index import Index
from lstore.page import Page, PageRange
from time import time
from lstore.bufferpool import *
from lstore.lock import *
import numpy as np

TABLEKEY = 0
TABLENUMCOL = 1
TABLECURPG = 2
TABLECURBP = 3
TABLECURREC = 4

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
        if isNew:
            self.add_page_range(self.num_columns)

    def add_page_range(self, numCols):
        self.bufferpool.allocate_page_range(self.num_columns, self.curPageRange)
        self.num_pageRanges += 1

    def get_page_range(self):
        if self.pageRange[self.curPageRange].has_capacity():
            return self.pageRange[self.curPageRange]
        else:
            self.add_page_range(self.num_columns)
            return self.pageRange[self.curPageRange]

    def updateCurBP(self):
        self.curBP += 1
        if self.curBP == -1:
            self.curBP = 0
    
    def getCurBP(self):
        return self.pageRange[self.curPageRange].basePages[self.curBP]

    def updateCurRecord(self):
        self.curRecord += 1

    def createBP_RID(self):
        tupleRID = (self.curPageRange, self.curBP, self.bufferpool.frames[self.curFrameIndexBP].numRecords, 'b')
        return tupleRID
    
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
        
        self.curFrameIndexBP = self.bufferpool.load_base_page(self.curPageRange, self.curBP, self.num_columns)
        RID = self.createBP_RID()
        self.page_directory[RID] = RID
        indirection = RID
        
        self.bufferpool.insertRecBP(RID, start_time, schema_encoding, indirection, *columns, numColumns=self.num_columns)
        self.updateCurRecord()
        
        if not self.bufferpool.frames[self.curFrameIndexBP].has_capacity():
            if self.curRecord == 8192:
                self.curPageRange += 1
                self.bufferpool.allocate_page_range(self.num_columns, self.curPageRange)
                self.curRecord = 0
                self.curBP = 0
            else:
                self.updateCurBP()
                
        for i in range(len(columns)):
            self.index.add_node(i, columns[i], RID)

    def updateRec(self, rid, baseRID, primary_key, *columns):
        projected_columns_index = [1] * self.num_columns
        self.curFrameIndexBP = self.bufferpool.load_base_page(rid[0], rid[1], self.num_columns)
        if self.curFrameIndexBP is None:
            raise ValueError("Could not load base page")
            
        currentRID = self.bufferpool.frames[self.curFrameIndexBP].indirection[rid[2]]
        
        # Get the current record data
        try:    
            record = self.find_record(primary_key, currentRID, projected_columns_index, 
                                    self.bufferpool.frames[self.curFrameIndexBP].TPS[rid[2]])
        except:
            record = self.find_record(primary_key, currentRID, projected_columns_index, (0, 0))

        # Get tail page from bufferpool
        page_range = self.bufferpool.page_ranges.get(rid[0])
        if page_range is None:
            # Create new page range if it doesn't exist
            page_range = self.bufferpool.allocate_page_range(self.num_columns, rid[0])
            
        numTPS = len(page_range.tailPages)
        tail_page_index = numTPS - 1 if numTPS > 0 else 0
        
        self.curFrameIndexTP = self.bufferpool.load_tail_page(rid[0], tail_page_index, self.num_columns)
        if self.curFrameIndexTP is None:
            raise ValueError("Could not load tail page")

        if not self.bufferpool.frames[self.curFrameIndexTP].has_capacity():
            # Add new tail page if current one is full
            page_range.add_tail_page(self.num_columns)
            tail_page_index = len(page_range.tailPages) - 1
            self.curFrameIndexTP = self.bufferpool.load_tail_page(rid[0], tail_page_index, self.num_columns)

        updateRID = (rid[0], tail_page_index, self.bufferpool.frames[self.curFrameIndexTP].numRecords, 't')
        self.bufferpool.insertRecTP(record, rid, updateRID, currentRID, baseRID, self.curFrameIndexBP, *columns)
        self.page_directory[updateRID] = updateRID

    def greaterthan(self, a, b):
        """Compare two RID tuples for ordering"""
        if a[0] > b[0]:
            return True
        elif a[0] == b[0]:
            if a[1] > b[1]:
                return True
        return False

    def __merge(self, PageRangeIndex):
        # Simplified merge operation for in-memory implementation
        pass
