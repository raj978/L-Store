"""
A data strucutre holding indices for various columns of a table.
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""
from collections import defaultdict

class Index:
    def __init__(self, table):
        # One index for each column. All are empty initially.
        self.indices = [defaultdict(list) for _ in range(table.num_columns)]
    
    def get_index(self, column_number):
        return self.indices[column_number]

    def locate(self, column, value):
        """
        Find RID of first record with value in given column
        """
        index = self.indices[column]
        records = index.get(value, [])
        return records[0] if records else None
    
    def locate_range(self, begin, end, column):
        """
        Find all RIDs of records with values in range [begin, end] in given column
        """
        index = self.indices[column]
        result = []
        for value in range(begin, end + 1):
            result.extend(index.get(value, []))
        return result

    def create_index(self, column_number):
        if not isinstance(self.indices[column_number], defaultdict):
            self.indices[column_number] = defaultdict(list)

    def drop_index(self, column_number):
        self.indices[column_number] = defaultdict(list)

    def add_node(self, col, key, rid):
        """
        Add RID to index for given column and key value
        """
        self.indices[col][key].append(rid)
        
    def update_index(self, rid, *columns):
        for key in columns:
            self.add_node(key, rid)
