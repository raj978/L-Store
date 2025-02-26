from lstore import bufferpool
from lstore.table import Table
from lstore.bufferpool import *
import csv
import os
import pickle
import array, struct

TABLEKEY = 0
TABLENUMCOL = 1
TABLECURPG = 2
TABLECURBP = 3
TABLECURREC = 4

class Database():

    def __init__(self):
        self.tables = []
        self.tablenames = []
        self.bufferpool = None
        self.path = ''

    def open(self, path):
        print("opening DB")
        self.bufferpool = Bufferpool() 
        self.path = path
        
        if not os.path.exists(self.path):
            os.makedirs(path + "/tables")
            return
            
        # pull all tables, update metadata
        path = f"{self.path}/tables"
        if not os.path.exists(path):
            return
            
        for entry in os.listdir(path):
            specifictablepath = os.path.join(path, entry)
            if os.path.isdir(specifictablepath):
                metadata_path = specifictablepath + "/metadata.bin"
                if not os.path.exists(metadata_path):
                    continue
                    
                with open(metadata_path, 'rb') as file:
                    binary_data = file.read()
                arr = array.array('i', struct.unpack('i'* (len(binary_data)//struct.calcsize('i')), binary_data))
                tableName = entry
                num_columns = arr[TABLENUMCOL]
                table_key = arr[TABLEKEY]
                self.bufferpool.start_table_dir(tableName, num_columns)
                table = Table(tableName, num_columns, table_key, self.bufferpool, False)
                table.curPageRange = arr[TABLECURPG]
                table.curBP = arr[TABLECURBP]
                table.curRecord = arr[TABLECURREC]
                self.tables.append(table)
                
                # Load indices and page directory if they exist
                if os.path.exists(specifictablepath + "/indices.pkl"):
                    table.index.load_index(specifictablepath + "/indices.pkl")
                if os.path.exists(specifictablepath + "/pagedirectory.pkl"):
                    with open(specifictablepath + "/pagedirectory.pkl", 'rb') as file:
                        table.page_directory = pickle.load(file)
                        
                # Load page ranges
                table.pullpagerangesfromdisk(specifictablepath)

    def close(self):
        for table in self.tables:
            picklepath = self.path + f"/tables/{table.name}/indices.pkl"
            table.index.close_and_save(picklepath)
            directorypath = self.path + f"/tables/{table.name}/pagedirectory.pkl"
            with open(directorypath, 'wb') as file:
                pickle.dump(table.page_directory, file)
            metadatapath = self.path + f"/tables/{table.name}/metadata.bin"
            table.savemetadata(metadatapath)
            table.bufferpool.close(table.name)

    def create_table(self, name, num_columns, key_index):
        if self.bufferpool is None:
            raise ValueError("Bufferpool is not initialized. Call open() first.")
        self.bufferpool.start_table_dir(name, num_columns)
        table = Table(name, num_columns, key_index, self.bufferpool, True)
        if not os.path.exists(self.path + f"/tables/{table.name}"):
            os.mkdir(self.path + f"/tables/{table.name}")
        self.tables.append(table)
        self.tablenames.append(name)
        return table

    def drop_table(self, name):
        for table in self.tables: 
            if table.name == name: 
                self.tables.remove(table)
        print(self.tables)

    def get_table(self, name):
        for table in self.tables: 
            if table.name == name: 
                return table
