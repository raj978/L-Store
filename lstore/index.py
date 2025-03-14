"""
A data strucutre holding indices for various columns of a table.
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""
from collections import defaultdict
from BTrees.OOBTree import OOBTree
import pickle

class Index:
    def __init__(self, table):
        self.table = table
        self.indices = [OOBTree() for _ in range(table.num_columns)]
    
    def get_index(self, column_number):
        return self.indices[column_number]

    def locate(self, column, value):
        """
        Find RID of first record with value in given column
        """
        if value in self.indices[column]:
            return self.indices[column][value]
        return []
    
    def locate_range(self, column, start, end):
        """
        Find all RIDs of records with values in range [start, end] in given column
        """
        result = []
        for key in self.indices[column].keys(min=start, max=end):
            result.extend(self.indices[column][key])
        return result

    def create_index(self, column_number):
        if not isinstance(self.indices[column_number], OOBTree):
            self.indices[column_number] = OOBTree()

    def drop_index(self, column_number):
        self.indices[column_number] = OOBTree()

    def add_node(self, column, value, rid):
        """
        Add RID to index for given column and key value
        """
        if value not in self.indices[column]:
            self.indices[column][value] = []
        self.indices[column][value].append(rid)

    def update_node(self, column, value, rid):
        # First remove the old mapping if it exists
        self.delete_node(column, None, rid)
        # Add the new mapping
        self.add_node(column, value, rid)

    def delete_node(self, column, value, rid):
        if value is not None and value in self.indices[column]:
            if rid in self.indices[column][value]:
                self.indices[column][value].remove(rid)
                if not self.indices[column][value]:
                    del self.indices[column][value]
        else:
            # If value not provided, search all values
            for value in list(self.indices[column].keys()):
                if rid in self.indices[column][value]:
                    self.indices[column][value].remove(rid)
                    if not self.indices[column][value]:
                        del self.indices[column][value]
                    break

    def close_and_save(self, path):
        with open(path, 'wb') as f:
            pickle.dump(self.indices, f)

    def load_index(self, path):
        with open(path, 'rb') as f:
            self.indices = pickle.load(f)
