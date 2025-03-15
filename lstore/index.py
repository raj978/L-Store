"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import pickle
from BTrees._OOBTree import OOBTree
import threading

class Index:
    def __init__(self, table):
        # One index for each column. All are empty initially.
        self.indices = [None] * table.num_columns
        self.lock = threading.RLock()  # Use RLock to allow nested acquisitions
        
    def load_index(self, picklepath): 
        with self.lock:
            with open(picklepath, 'rb') as file:
                self.indices = pickle.load(file)
    
    def get_index(self, column_number):
        with self.lock:
            if 0 <= column_number < len(self.indices):
                return self.indices[column_number]
            return None
    
    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        with self.lock:
            if column >= len(self.indices):
                return None
                
            tree = self.indices[column]
            if tree is None:
                # Create index if it doesn't exist
                self.create_index(column)
                tree = self.indices[column]
                # Return empty result since newly created index won't have data yet
                return None
            return tree.get(value, None)
    
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        with self.lock:
            if column >= len(self.indices) or self.indices[column] is None:
                return []
                
            tree = self.indices[column]
            RID_arr = []
            try:
                for RID in list(tree.values(min=begin, max=end)):
                    RID_arr.append(RID)
            except Exception as e:
                print(f"Error in locate_range: {str(e)}")
                return []
            return RID_arr
    
    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        with self.lock:
            if column_number >= len(self.indices):
                # Extend indices list if needed
                self.indices.extend([None] * (column_number - len(self.indices) + 1))
                
            if self.indices[column_number] is None:
                self.indices[column_number] = OOBTree()
                return True
            else:
                print(f"Column {column_number} index already created")
                return False
    
    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        with self.lock:
            if 0 <= column_number < len(self.indices):
                self.indices[column_number] = None
                return True
            return False
    
    def add_node(self, col, key, rid):
        with self.lock:
            # Convert rid to tuple if it's a list to ensure it's hashable
            if isinstance(rid, list):
                rid = tuple(rid)
            
            # Create index if it doesn't exist
            if col >= len(self.indices):
                # Extend indices list if needed
                self.indices.extend([None] * (col - len(self.indices) + 1))
                
            if self.indices[col] is None:
                self.create_index(col)
                
            # Add node to index by either creating a new key entry or appending to existing one
            existing_rids = self.indices[col].get(key)
            if existing_rids is None:
                self.indices[col][key] = rid
            elif isinstance(existing_rids, tuple):
                # If we already have one RID, convert to list
                self.indices[col][key] = [existing_rids, rid]
            else:
                # If we already have a list of RIDs, append to it
                existing_rids.append(rid)
                
    def update_index(self, rid, *columns):
        with self.lock:
            # Convert rid to tuple if it's a list to ensure it's hashable
            if isinstance(rid, list):
                rid = tuple(rid)
                
            for i, key in enumerate(columns):
                if key is not None:
                    self.add_node(i, key, rid)
    
    def close_and_save(self, picklepath): 
        with self.lock:
            with open(picklepath, 'wb') as file:
                pickle.dump(self.indices, file)
