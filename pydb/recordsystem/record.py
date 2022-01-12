import numpy as np


class RID:
    def __init__(self, page_id: int, slot_id: int) -> None:
        self.page_id = page_id
        self.slot_id = slot_id

    def __str__(self) -> str:
        return f'(page_id: {self.page_id}, slot_id: {self.slot_id})'

    def __eq__(self, o: object) -> bool:
        return self.page_id == o.page_id and self.slot_id == o.slot_id

    def __hash__(self) -> int:
        return (self.page_id, self.slot_id).__hash__()


class Record:
    def __init__(self, rid: RID, data: np.ndarray) -> None:
        self.rid = rid
        self.data = data
