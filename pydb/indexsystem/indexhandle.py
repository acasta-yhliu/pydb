from numpy import dtype
from pydb.config import PAGE_SIZE
from pydb.fio import FileManager, dump_header, load_header
import numpy as np
import bisect
from pydb.recordsystem.record import RID


TYPE_DEFAULT = -1
TYPE_LEAF = 1
TYPE_INTERNAL = 0


class BTreeNode:
    def __init__(self, page_id: int, parent_id: int, type: int, keys: list[int], vals: list, handle):
        self.page_id: int = page_id
        self.parent_id: int = parent_id
        self.keys = keys
        self.vals = vals
        self.type = type
        self.handle = handle

    def split(self):
        mid = len(self.keys) // 2
        half_key, half_val = self.keys[mid:], self.vals[mid:]
        self.keys, self.vals = self.keys[:mid], self.vals[:mid]
        return half_key, half_val, half_key[0]

    # first one >= itemkey
    def lower_bound(self, itemkey: int):
        return bisect.bisect_left(self.keys, itemkey)

    # first one > itemkey
    def upper_bound(self, itemkey: int):
        return bisect.bisect_right(self.keys, itemkey)

    def page_size(self) -> int:
        raise NotImplemented()

    def insert(self, key: int, val: RID) -> int:
        raise NotImplemented()

    def remove(self, key: int, val: RID) -> int:
        raise NotImplemented()

    def serialize(self) -> np.ndarray:
        raise NotImplemented()

    def range(self, low: int, high: int) -> list[RID]:
        raise NotImplemented()

    def select(self, operator: str, key) -> list[RID]:
        raise NotImplemented()

    def print(self, indent: int = 0):
        raise NotImplemented()


class LeafNode(BTreeNode):
    def __init__(self, page_id: int, parent_id: int, keys: list[int], vals: list[RID], handle):
        super().__init__(page_id, parent_id, TYPE_LEAF, keys, vals, handle)
        self.vals: list[RID]

    def insert(self, key: int, val: RID):
        id = self.lower_bound(key)
        self.keys.insert(id, key)
        self.vals.insert(id, val)
        return self.keys[0]

    def remove(self, key: int, val: RID):
        for i in range(self.lower_bound(key), self.upper_bound(key)):
            if self.keys[i] == key and self.vals[i] == val:
                self.keys.pop(i)
                self.vals.pop(i)
                return None if len(self.keys) == 0 else self.keys[0]
        return self.keys[0]

    def page_size(self) -> int:
        # int, rid
        return 3 * 8 + len(self.keys) * 24

    def serialize(self) -> np.ndarray:
        page = np.zeros(PAGE_SIZE // 8, np.int64)
        page[0:3] = [TYPE_LEAF, self.parent_id, len(self.keys)]
        for i in range(len(self.keys)):
            page[3 * i + 3: 3 * i + 6] = [self.keys[i],
                                          self.vals[i].page_id, self.vals[i].slot_id]
        page.dtype = np.uint8
        return page

    def range(self, low: int, high: int) -> list[RID]:
        return (self.vals[self.lower_bound(low): self.upper_bound(high)])

    def select(self, operator: str, key) -> list[RID]:
        if operator == '=':
            return self.range(key, key)
        elif operator == '<>':
            return [self.vals[i] for i in range(len(self.keys)) if self.keys[i] != key]
        elif operator == '>':
            return (self.vals[self.upper_bound(key):])
        elif operator == '>=':
            return (self.vals[self.lower_bound(key):])
        elif operator == '<':
            return (self.vals[:self.lower_bound(key)])
        elif operator == '<=':
            return (self.vals[:self.upper_bound(key)])
        else:
            raise Exception(f'unknown query operator {operator}')

    def print(self, indent: int = 0):
        print(' ' * indent + f'===== LeafNode : {self.page_id} =====')
        for i in range(len(self.keys)):
            print(' ' * (indent + 2) + f'> {self.keys[i]} : {self.vals[i]}')


class InternalNode(BTreeNode):
    def __init__(self, page_id: int, parent_id: int, keys: list[int], vals: list[BTreeNode], handle):
        super().__init__(page_id, parent_id, TYPE_INTERNAL, keys, vals, handle)
        self.vals: list[BTreeNode]

    def key_lower(self, key: int):
        id = self.lower_bound(key)
        if id > 0:
            id -= 1
        return id

    def key_upper(self, key: int):
        return self.upper_bound(key)

    # split, is that correct ?
    def insert(self, key: int, val: RID):
        if len(self.keys) == 0:
            node = LeafNode(self.handle.new_page(),
                            self.page_id, [], [], self.handle)
            node.insert(key, val)
            self.keys.append(key)
            self.vals.append(node)
            return self.keys[0]
        else:
            pos = self.key_lower(key)
            self.keys[pos] = self.vals[pos].insert(key, val)
            if self.vals[pos].page_size() > PAGE_SIZE:
                nkeys, nvals, mid = self.vals[pos].split()
                self.keys.insert(pos + 1, mid)
                page_id = self.handle.new_page()
                if self.vals[pos].type == TYPE_INTERNAL:
                    new_node = InternalNode(
                        page_id, self.page_id, nkeys, nvals, self.handle)
                else:
                    new_node = LeafNode(
                        page_id, self.page_id, nkeys, nvals, self.handle)
                self.vals.insert(pos + 1, new_node)
            return self.keys[0]

    def remove(self, key: int, val: RID):
        empty_pages = set()
        for i in range(self.key_lower(key), self.key_upper(key)):
            ret_val = self.vals[i].remove(key, val)
            if ret_val is None:
                empty_pages.add(i)
            else:
                self.keys[i] = ret_val
        # here, bug
        self.keys = [self.keys[i]
                     for i in range(len(self.keys)) if i not in empty_pages]
        self.vals = [self.vals[i]
                     for i in range(len(self.vals)) if i not in empty_pages]
        return None if len(self.keys) == 0 else self.keys[0]

    def page_size(self) -> int:
        return 3 * 8 + len(self.keys) * 16

    def serialize(self) -> np.ndarray:
        page = np.zeros(PAGE_SIZE // 8, dtype=np.int64)
        page[0:3] = [TYPE_INTERNAL,
                     self.parent_id, len(self.keys)]
        for i in range(len(self.keys)):
            page[2 * i + 3: 2 * i + 5] = [self.keys[i], self.vals[i].page_id]
        page.dtype = np.uint8
        return page

    # TODO rearrange
    def range(self, low, high) -> list[RID]:
        records = []
        for i in range(self.key_lower(low), self.key_upper(high)):
            records += (self.vals[i].range(low, high))
        return records

    def select(self, operator: str, key) -> list[RID]:
        if operator == '=':
            child_range = range(self.key_lower(key), self.key_upper(key))
        elif operator == '<>':
            child_range = range(len(self.keys))
        elif operator == '>':
            child_range = range(self.key_lower(key), len(self.keys))
        elif operator == '>=':
            child_range = range(self.key_lower(key), len(self.keys))
        elif operator == '<':
            child_range = range(self.key_upper(key))
        elif operator == '<=':
            child_range = range(self.key_upper(key))
        else:
            raise Exception(f'unknown query operator {operator}')

        records = []
        for i in child_range:
            records += (self.vals[i].select(operator, key))
        return records

    def print(self, indent: int = 0):
        print(' ' * indent + f'== Internal Node : {self.page_id}')
        for i in range(len(self.keys)):
            print(' ' * (indent + 2) +
                  f'> {self.keys[i]} : {self.vals[i].page_id}')
            self.vals[i].print(indent + 2)


class IndexHandle:
    def __init__(self, fm: FileManager, fid: int) -> None:
        self.fm = fm
        self.fid = fid
        # load the header from the first page
        self.header = load_header(self.get_page(0))
        self.root = self.load_node(self.get_root_id())

    def get_root_id(self) -> int:
        return self.header['root_id']

    def set_root_id(self, root_id: int):
        self.header['root_id'] = root_id

    def get_page(self, page_id: int):
        return self.fm.get_page(self.fid, page_id)

    def put_page(self, page_id: int, page: np.ndarray):
        return self.fm.put_page(self.fid, page_id, page)

    def new_page(self):
        return self.fm.new_page(self.fid, np.zeros(PAGE_SIZE, dtype=np.uint8))

    def close(self):
        # flush back the header, which is in the first page
        self.put_page(0, dump_header(self.header))
        # dump the tree back into the file
        self.dump()
        # totally close the file
        self.fm.close_file(self.fid)

    def load_node(self, page_id: int):
        page = self.get_page(page_id)
        page.dtype = np.int64
        page_type, parent_id, length = page[0], page[1], page[2]
        if page_type == TYPE_LEAF:
            keys = [page[3 * i + 3] for i in range(length)]
            vals = [RID(int(page[3 * i + 4]), int(page[3 * i + 5]))
                    for i in range(length)]
            node = LeafNode(page_id, parent_id, keys, vals, self)
        else:
            keys = [page[2 * i + 3] for i in range(length)]
            vals = [self.load_node(page[2 * i + 4]) for i in range(length)]
            node = InternalNode(page_id,
                                parent_id, keys, vals, self)
        return node

    def build(self, keys, rids):
        for i in range(len(keys)):
            self.insert(keys[i], rids[i])

    def dump(self):
        q: list[BTreeNode] = [self.root]
        while len(q) > 0:
            node = q.pop(0)
            page_id = node.page_id
            if isinstance(node, InternalNode):
                for i in node.vals:
                    q.append(i)
            self.put_page(page_id, node.serialize())

    def insert(self, key: int, rid: RID):
        self.root.insert(key, rid)
        if self.root.page_size() > PAGE_SIZE:
            old_rt = self.root
            pid = self.new_page()
            self.root = InternalNode(pid, pid, [], [], self)
            self.set_root_id(pid)

            old_rt.parent_id = self.root.page_id
            nkeys, nvals, nkey = old_rt.split()

            self.root.keys = [old_rt.keys[0], nkey]
            self.root.vals = [old_rt, InternalNode(
                self.new_page(), self.root.page_id, nkeys, nvals, self)]

    def remove(self, key: int, val: RID):
        self.root.remove(key, val)

    def range(self, low: int, high: int) -> set[RID]:
        return set(self.root.range(low, high))

    def select(self, operator: str, key) -> set[RID]:
        return set(self.root.select(operator, key))

    def print(self):
        self.root.print()
