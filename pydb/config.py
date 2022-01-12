import math
import numpy as np

PAGE_SIZE = 8192
PAGE_SIZE_BITS = int(math.log2(PAGE_SIZE))
CACHE_SIZE = 1024 * 16
FILE_ID_BITS = 16
DEFAULT_ID = -1

NULL_FIELD = -1 << 32

REBUILD_TRIGGER = 1000

def byte_array(size):
    return np.zeros(size, dtype=np.uint8)


def bool_array(size):
    return np.zeros(size, dtype=np.bool8)
