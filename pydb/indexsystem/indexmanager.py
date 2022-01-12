from pydb.config import PAGE_SIZE
from pydb.fio import FileManager, dump_header
from pydb.indexsystem.indexhandle import IndexHandle
import os
import numpy as np


class IndexManager:
    def __init__(self, fm: FileManager, dbname: str) -> None:
        self.fm = fm
        self.dbname = dbname
        self.handles: dict[tuple[str, str], IndexHandle] = {}

    def get_filename(self, table: str, col: str):
        return os.path.join(self.dbname, f'{table}.{col}.index')

    def create_index_file(self, table: str, col: str):
        index_filename = self.get_filename(table, col)
        self.fm.create_file(index_filename)
        f = self.fm.open_file(index_filename)
        self.fm.new_page(f, dump_header({
            'table': table,
            'column': col,
            'root_id': 1,
        }))
        self.fm.new_page(f, np.zeros(PAGE_SIZE, dtype=np.uint8))
        self.fm.close_file(f)

    def has_index_file(self, table: str, col: str):
        return os.path.exists(self.get_filename(table, col))

    def pop_index_file(self, table: str, col: str):
        self.close_index_file(table, col)
        self.fm.remove_file(self.get_filename(table, col))

    def open_index_file(self, table: str, col: str) -> IndexHandle:
        if (table, col) in self.handles:
            return self.handles[table, col]
        else:
            f = self.fm.open_file(self.get_filename(table, col))
            handle = IndexHandle(self.fm, f)
            self.handles[table, col] = handle
            return handle

    def close_index_file(self, table: str, col: str):
        if (table, col) in self.handles:
            self.handles[table, col].close()
            self.handles.pop((table, col))

    def close(self):
        for handle in self.handles.values():
            handle.close()
        self.handles.clear()
