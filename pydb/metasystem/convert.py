from pydb.config import *
from pydb.recordsystem import Record
import struct


class Convert:
    CONVERT_FORMAT = {'INT': '<q', 'FLOAT': '<d'}

    @staticmethod
    def encode(sizes: list[int], types: list[str], total_size: int, values: list):
        off, data = 0, np.zeros(total_size, dtype=np.uint8)
        for size, type, value in zip(sizes, types, values):
            data[off:off + size] = list(value.encode('utf-8').ljust(size, b'\0')) if type == 'VARCHAR' else list(
                struct.pack(Convert.CONVERT_FORMAT[type], NULL_FIELD if value is None else value))
            off += size
        return data

    @staticmethod
    def decode(sizes: list[int], types: list[str], total_size: int, record: Record):
        def decode_field(data: np.ndarray, type: str):
            if type == 'VARCHAR':
                s = data.tobytes().rstrip(b'\x00').decode('utf-8')
                return None if len(s) == 0 else s
            else:
                value = struct.unpack(Convert.CONVERT_FORMAT[type], data)[0]
                return None if value == NULL_FIELD else value
        off, res = 0, []
        for size, type in zip(sizes, types):
            res.append(decode_field(record.data[off:off+size], type))
            off += size
        return res
