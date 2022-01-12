from .recordhandle import *
import logging
import os

# this manager actually with the records (tables) of a single database


class RecordManager:
    def __init__(self, fm: FileManager, dbname: str) -> None:
        self.fm = fm
        self.dbname = dbname
        self.handles: dict[str, RecordHandle] = {}

    def get_filename(self, fname: str):
        return os.path.join(self.dbname, fname + '.db')

    @staticmethod
    def bitmap_size(rec_per_page: int):
        return (rec_per_page + 7) >> 3

    @staticmethod
    def records_per_page(record_length: int):
        remain = PAGE_SIZE - RE_OFFSET
        x = remain // record_length
        while RecordManager.bitmap_size(x) + x * record_length > remain:
            x -= 1
        return x

    @staticmethod
    def initial_header(record_length: int):
        rpp = RecordManager.records_per_page(record_length)
        return {
            'record_length': record_length,
            'records_per_page': rpp,
            'page_count': 1,
            'record_count': 0,
            'next_page': 0,
            'bitmap_length': RecordManager.bitmap_size(rpp)
        }

    def create_record_file(self, table: str, record_length: int):
        table_file = self.get_filename(table)
        self.fm.create_file(table_file)

        f = self.fm.open_file(table_file)
        self.fm.new_page(f, dump_header(
            RecordManager.initial_header(record_length)))
        self.fm.close_file(f)

    def has_record_file(self, table: str):
        return os.path.exists(self.get_filename(table))

    def pop_record_file(self, table: str):
        self.close_record_file(table)
        self.fm.remove_file(self.get_filename(table))

    def open_record_file(self, table: str):
        if table in self.handles:
            return self.handles[table]
        else:
            f = self.fm.open_file(self.get_filename(table))
            handle = RecordHandle(self.fm, f)
            self.handles[table] = handle
            return handle

    def close_record_file(self, table: str):
        if table in self.handles:
            self.handles[table].close()
            self.handles.pop(table)

    def close(self):
        for handle in self.handles.values():
            handle.close()
        self.handles.clear()
