from lstore import bufferpool
from lstore.table import Table
from lstore.bufferpool import Bufferpool

class Database():
    def __init__(self):
        """Initialize database with in-memory bufferpool"""
        self.tables = {}  # In-memory tables
        self.bufferpool = Bufferpool()  # Initialize bufferpool immediately

    def open(self, path=None):
        """Kept for backwards compatibility, does nothing since we're fully in-memory"""
        pass

    def close(self):
        """Clear all in-memory data"""
        if self.bufferpool:
            self.bufferpool.close()
        self.tables = {}

    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")
            
        table = Table(name, num_columns, key_index, self.bufferpool, True)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]

    def get_table(self, name):
        return self.tables.get(name)
