import numpy as np
import os
import json
import shutil

from pydb.config import *


def pack_pid(fid: int, pid: int):
    return fid | (pid << FILE_ID_BITS)


def unpack_pid(pid: int):
    return pid & ((1 << FILE_ID_BITS) - 1), pid >> FILE_ID_BITS


class LinkList:
    def __init__(self, capacity, list_number):
        self._capacity = capacity
        self._list_number = list_number
        self._next = np.arange(capacity + list_number)
        self._last = np.arange(capacity + list_number)

    def _link(self, last_node, next_node):
        self._last[next_node] = last_node
        self._next[last_node] = next_node

    def remove(self, index):
        if self._last[index] == index:
            return
        self._link(self._last[index], self._next[index])
        self._last[index] = index
        self._next[index] = index

    def append(self, list_id, index):
        self.remove(index)
        head = list_id + self._capacity
        self._link(self._last[head], index)
        self._link(index, head)

    def insert_first(self, list_id, index):
        self.remove(index)
        head = list_id + self._capacity
        first = self._next[head]
        self._link(head, index)
        self._link(index, first)

    def get_first(self, list_id):
        return self._next[list_id + self._capacity]

    def get_next(self, index):
        return self._next[index]

    def is_head(self, index):
        return index >= self._capacity

    def is_alone(self, index):
        # this function won't be used in system
        return self._next[index] == index


class FindReplace:
    def __init__(self, capacity):
        self._capacity = capacity
        self.list = LinkList(capacity, 1)
        for i in range(capacity - 1, 0, -1):
            self.list.insert_first(0, i)

    def find(self):
        index = self.list.get_first(0)
        self.list.remove(index)
        self.list.append(0, index)
        return index

    def free(self, index):
        self.list.insert_first(0, index)

    def access(self, index):
        self.list.append(0, index)


class FileManager:
    try:
        FILE_OPEN_MODE = os.O_RDWR | os.O_BINARY
    except AttributeError as exception:
        FILE_OPEN_MODE = os.O_RDWR

    def __init__(self):
        self.file_cache_pages = {}
        self.file_id_to_name = {}
        self.file_name_to_id = {}
        self.page_buffer = byte_array((CACHE_SIZE, PAGE_SIZE))
        self.dirty = bool_array(CACHE_SIZE)
        self.index_to_file_page = np.full(
            CACHE_SIZE, DEFAULT_ID, dtype=np.int64)
        self.replace = FindReplace(CACHE_SIZE)
        self.file_page_to_index = {}
        # self.file_size = {}
        self.last = DEFAULT_ID

    def _access(self, index):
        if index == self.last:
            return
        self.replace.access(index)
        self.last = index

    def mark_dirty(self, index):
        self.dirty[index] = True
        self._access(index)

    def _write_back(self, index):
        if self.dirty[index]:
            self.write_page(
                *unpack_pid(self.index_to_file_page[index]), self.page_buffer[index])
        self._release(index)

    def _release(self, index):
        self.dirty[index] = False
        self.replace.free(index)
        file_page = self.index_to_file_page[index]
        self.file_cache_pages[unpack_pid(file_page)[0]].remove(index)
        self.file_page_to_index.pop(file_page)
        self.index_to_file_page[index] = DEFAULT_ID

    @staticmethod
    def create_file(filename: str):
        open(filename, 'w').close()

    @staticmethod
    def touch_file(filename: str):
        open(filename, 'a').close()

    @staticmethod
    def remove_file(filename: str):
        os.remove(filename)

    @staticmethod
    def exists_file(filename: str):
        return os.path.exists(filename)

    @staticmethod
    def move_file(source: str, dest: str):
        return os.rename(source, dest)

    @staticmethod
    def create_dir(dirname: str):
        os.mkdir(dirname)

    @staticmethod
    def remove_dir(dirname: str):
        shutil.rmtree(dirname)

    def open_file(self, filename: str):
        if filename in self.file_name_to_id:
            raise IOError(f"File {filename} has been opened")
        file_id = os.open(filename, FileManager.FILE_OPEN_MODE)
        if file_id == DEFAULT_ID:
            raise IOError("Can't open file " + filename)
        self.file_cache_pages[file_id] = set()
        self.file_name_to_id[filename] = file_id
        self.file_id_to_name[file_id] = filename
        return file_id

    def close_file(self, file_id: int):
        pages = self.file_cache_pages.pop(file_id, {})
        for index in pages:
            file_page = self.index_to_file_page[index]
            self.index_to_file_page[index] = DEFAULT_ID
            self.file_page_to_index.pop(file_page)
            self.replace.free(index)
            if self.dirty[index]:
                self.write_page(*unpack_pid(file_page),
                                self.page_buffer[index])
                self.dirty[index] = False
        os.close(file_id)
        filename = self.file_id_to_name.pop(file_id)
        self.file_name_to_id.pop(filename)

    @staticmethod
    def read_page(file_id, page_id) -> bytes:
        offset = page_id << PAGE_SIZE_BITS
        os.lseek(file_id, offset, os.SEEK_SET)
        data = os.read(file_id, PAGE_SIZE)
        if not data:
            raise IOError(
                f"Can't read page {page_id} from file {file_id}")
        return data

    @staticmethod
    def write_page(file_id, page_id, data: np.ndarray):
        offset = page_id << PAGE_SIZE_BITS
        os.lseek(file_id, offset, os.SEEK_SET)
        os.write(file_id, data.tobytes())

    @staticmethod
    def new_page(file_id, data: np.ndarray) -> int:
        pos = os.lseek(file_id, 0, os.SEEK_END)
        os.write(file_id, data.tobytes())
        return pos >> PAGE_SIZE_BITS

    def put_page(self, file_id, page_id, data: np.ndarray):
        file_page = pack_pid(file_id, page_id)
        index = self.file_page_to_index.get(file_page)
        if index is None:
            self.get_page(file_id, page_id)
            index = self.file_page_to_index.get(file_page)
        self.page_buffer[index] = data
        self.dirty[index] = True
        self.replace.access(index)

    def _get_page(self, file_id, page_id) -> np.ndarray:
        file_page = pack_pid(file_id, page_id)
        index = self.file_page_to_index.get(file_page)

        # if index is not None, then just past to
        if index is not None:
            self._access(index)
            return self.page_buffer[index]

        # else we should get a position in cache
        index = self.replace.find()
        last_id = self.index_to_file_page[index]

        # if this position is occupied, we should remove it first
        if last_id != DEFAULT_ID:
            self._write_back(index)

        # now save the new page info
        self.file_page_to_index[file_page] = index
        self.file_cache_pages[file_id].add(index)
        self.index_to_file_page[index] = file_page
        data = self.read_page(file_id, page_id)
        data = np.frombuffer(data, np.uint8, PAGE_SIZE)
        self.page_buffer[index] = data
        return self.page_buffer[index]

    def get_page_reference(self, file_id, page_id) -> np.ndarray:
        page = self._get_page(file_id, page_id)
        self.mark_dirty(
            self.file_page_to_index[pack_pid(file_id, page_id)])
        return page

    def get_page(self, file_id, page_id) -> np.ndarray:
        return self._get_page(file_id, page_id).copy()

    def release_cache(self):
        for index in np.where(self.dirty)[0]:
            self._write_back(index)
        self.page_buffer.fill(0)
        self.dirty.fill(False)
        self.index_to_file_page.fill(DEFAULT_ID)
        self.file_page_to_index.clear()
        self.last = DEFAULT_ID

    def shutdown(self):
        self.release_cache()
        while self.file_cache_pages:
            self.close_file(self.file_cache_pages.popitem()[0])


def load_header(data: np.ndarray):
    return json.loads(data.tobytes().decode('utf-8').rstrip('\0'))


def dump_header(header):
    data = list(json.dumps(header, ensure_ascii=False).encode('utf-8'))
    p = byte_array(PAGE_SIZE)
    p[:len(data)] = data
    return p
