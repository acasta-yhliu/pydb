import os
from pydb.fio import FileManager
from pydb.metasystem.meta import DBMeta, TableMeta
import pickle

# this handles a single file of the meta file
# it should contain methods to deal with various modification to the database
# although this is a manager, it only deals with single file


class MetaManager:
    def __init__(self, fm: FileManager,  dbname: str) -> None:
        self.fm = fm
        self.dbname = dbname
        with open(self.get_filename(), 'rb') as f:
            self.meta: DBMeta = pickle.load(f)

    def get_filename(self):
        return os.path.join(self.dbname, f'{self.dbname}.meta')

    def close(self):
        with open(self.get_filename(), 'wb') as f:
            pickle.dump(self.meta, f)

    # table operation
    def add_table(self, table: TableMeta):
        if not self.meta.has_table(table.name):
            self.meta.add_table(table)
        else:
            raise Exception(f'table "{table.name}" already exists')

    def pop_table(self, name: str):
        if self.meta.has_table(name):
            self.meta.pop_table(name)
        else:
            raise Exception(f'table "{name}" does not exist')

    def get_table(self, name: str) -> TableMeta:
        if self.meta.has_table(name):
            return self.meta.get_table(name)
        else:
            raise Exception(f'table "{name}" does not exist')

    def has_table(self, name: str):
        return self.meta.has_table(name)

    # index operation
    def add_index(self, table: str, col: str):
        table_info = self.get_table(table)
        if table_info.has_index(col):
            raise Exception(f'index "{table}.{col}" already exists')
        else:
            table_info.add_index(col)

    def pop_index(self, table: str, col: str):
        table_info = self.get_table(table)
        if not table_info.has_index(col):
            raise Exception(f'index "{table}.{col}" does not exist')
        else:
            table_info.pop_index(col)

    def has_index(self, table: str, col: str):
        return self.get_table(table).has_index(col)
