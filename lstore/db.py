import array
import os
import pickle
import struct

from lstore.bufferpool import *
from lstore.table import Table

TABLEKEY = 0
TABLENUMCOL = 1
TABLECURPG = 2
TABLECURBP = 3
TABLECURREC = 4


class Database:

    def __init__(self):
        self.tables: list[Table] = []
        self.tablenames = []
        self.bufferpool: Optional[Bufferpool] = None
        self.path = '.'
        self.tables_path = self.path + "/tables"

    def open(self, path):
        print("opening DB")

        # pull all tables, update metadata
        self.path = path
        self.bufferpool = Bufferpool(self.path)
        self.tables_path = self.path + "/tables"
        if not os.path.exists(self.tables_path):
            os.makedirs(self.tables_path)

        # Load bufferpool
        if os.path.exists(self.path + "/buffer.pkl"):
            with open(self.path + "/buffer.pkl", 'rb') as file:
                self.bufferpool = pickle.load(file)

        for entry in os.listdir(self.tables_path):
            specific_table_path = os.path.join(self.tables_path, entry)
            if os.path.isdir(specific_table_path):
                metadata_path = specific_table_path + "/metadata.bin"
                if os.path.exists(metadata_path):
                    with open(metadata_path, 'rb') as file:
                        binary_data = file.read()
                    arr = array.array('i', struct.unpack('i' * (len(binary_data) // struct.calcsize('i')), binary_data))
                    tableName = entry
                    num_columns = arr[TABLENUMCOL]
                    table_key = arr[TABLEKEY]
                    table = Table(tableName, num_columns, table_key, self.bufferpool, False, self.tables_path)
                    table.page_range_index = arr[TABLECURPG]
                    table.base_page_index = arr[TABLECURBP]
                    table.record_id = arr[TABLECURREC]
                    self.tables.append(table)

                    # Load indices and page directory if they exist
                    if os.path.exists(specific_table_path + "/indices.pkl"):
                        table.index.load_index(specific_table_path + "/indices.pkl")
                    if os.path.exists(specific_table_path + "/pagedirectory.pkl"):
                        with open(specific_table_path + "/pagedirectory.pkl", 'rb') as file:
                            table.page_directory = pickle.load(file)

                    # Load page ranges
                    table.pullpagerangesfromdisk(specific_table_path)

        print("open DB finished")

    def close(self):
        directory_path = self.path + f"/buffer.pkl"
        with open(directory_path, 'wb') as file:
            pickle.dump(self.bufferpool, file)
        for table in self.tables:
            pickle_path = self.path + f"/tables/{table.name}/indices.pkl"
            table.index.close_and_save(pickle_path)
            directory_path = self.path + f"/tables/{table.name}/pagedirectory.pkl"
            with open(directory_path, 'wb') as file:
                pickle.dump(table.page_directory, file)
            metadata_path = self.path + f"/tables/{table.name}/metadata.bin"
            table.savemetadata(metadata_path)
            table.bufferpool.close()

    def create_table(self, name, num_columns, key_index):
        if self.bufferpool is None:
            self.open(self.path)
        # self.bufferpool.start_table_dir(name, num_columns)
        table = Table(name, num_columns, key_index, self.bufferpool, True, self.tables_path)
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
