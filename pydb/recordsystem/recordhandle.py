from numpy import record
from pydb.config import PAGE_SIZE
from pydb.fio import FileManager, load_header, dump_header
from .record import *
import numpy as np

NP_OFFSET = 0
BM_OFFSET = 4
RE_OFFSET = 4

# this class handles a file as an record file
# | next_page | bitmaps... | records...


class RecordHandle:
    def __init__(self, fm: FileManager, fid: int) -> None:
        self.fm = fm
        self.fid = fid
        # load the header from the first page
        self.header = load_header(fm.get_page(fid, 0))

    def get_page(self, page_id: int):
        return self.fm.get_page(self.fid, page_id)

    def put_page(self, page_id: int, data: np.ndarray):
        self.fm.put_page(self.fid, page_id, data)

    # close the record page
    def close(self):
        # flush back the header of the record page
        self.put_page(0, dump_header(self.header))
        # totally close the page
        self.fm.close_file(self.fid)

    # unpack the bitmap from the page
    # BM_OFFSET = 4
    def get_bitmap(self, page: np.ndarray):
        return np.unpackbits(page[BM_OFFSET: BM_OFFSET + self.header['bitmap_length']])[:self.header['records_per_page']]

    # pack the bitmap back to the page
    def set_bitmap(self, page: np.ndarray, bitmap: np.ndarray):
        page[BM_OFFSET:BM_OFFSET +
             self.header['bitmap_length']] = np.packbits(bitmap)

    # | next_page |
    def get_next(self, page: np.ndarray):
        return int.from_bytes(page[NP_OFFSET:NP_OFFSET + 4].tobytes(), 'big')

    # | next_page |
    def set_next(self, page: np.ndarray, id: int):
        page[NP_OFFSET:NP_OFFSET +
             4] = np.frombuffer(id.to_bytes(4, 'big'), dtype=np.uint8)

    # offset of the certain slot
    def get_slot_offset(self, slot_id: int):
        return RE_OFFSET + self.header['bitmap_length'] + self.header['record_length'] * slot_id

    def get_record(self, rid: RID):
        offset = self.get_slot_offset(rid.slot_id)
        return Record(rid, self.get_page(rid.page_id)[offset:offset+self.header['record_length']])

    # find a empty slot from the bitmap and set it
    def find_slots(self, page: np.ndarray):
        bitmap = self.get_bitmap(page)
        free_slots = np.where(bitmap)[0]
        find = free_slots[0]  # first empty slot id
        bitmap[find] = 0
        self.set_bitmap(page, bitmap)
        return find, len(free_slots) - 1  # remain slots after this one

    # allocate an empty page, if no empty page, then add a new page
    def alloc_page(self):
        page_id = self.header['next_page']
        if page_id == 0:
            # append a new page
            page = np.full(PAGE_SIZE, -1, dtype=np.uint8)  # -1 to 0xFF
            self.set_next(page, 0)
            page_id = self.fm.new_page(self.fid, page)
            self.header['page_count'] += 1
            self.header['next_page'] = page_id
        return (page_id, self.get_page(page_id))

    # add a new record to the record file
    def add_record(self, data: np.ndarray):
        pageid, page = self.alloc_page()
        slotid, remain = self.find_slots(page)
        offset = self.get_slot_offset(slotid)
        page[offset:offset+self.header['record_length']] = data
        self.header['record_count'] += 1
        # page become full
        if remain == 0:
            self.header['next_page'] = self.get_next(page)
            self.set_next(page, pageid)
        self.put_page(pageid, page)
        return RID(pageid, slotid)

    # pop the certain record with the given location
    def pop_record(self, rid: RID):
        page = self.get_page(rid.page_id)
        bitmap = self.get_bitmap(page)
        bitmap[rid.slot_id] = 1
        self.set_bitmap(page, bitmap)
        self.header['record_count'] -= 1
        if self.get_next(page) == rid.page_id:
            self.set_next(page, self.header['next_page'])
            self.header['next_page'] = rid.page_id
        self.put_page(rid.page_id, page)

    # update the certain record with the given location
    def update_record(self, record: Record):
        page = self.get_page(record.rid.page_id)
        offset = self.get_slot_offset(record.rid.slot_id)
        page[offset:offset + self.header['record_Length']] = record.data
        self.put_page(record.rid.page_id, page)

    def iterator(self):
        return RecordHandleScan(self)


class RecordHandleScan:
    def __init__(self, handle: RecordHandle) -> None:
        self.handle = handle

    def __iter__(self):
        for page_id in range(1, self.handle.header['page_count']):
            page = self.handle.get_page(page_id)
            bitmap = self.handle.get_bitmap(page)
            for slot in np.where(bitmap == 0)[0]:
                yield self.handle.get_record(RID(page_id, slot))
