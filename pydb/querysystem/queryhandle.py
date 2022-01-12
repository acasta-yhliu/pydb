from typing import Any, Callable, ValuesView
from pydb.config import NULL_FIELD, REBUILD_TRIGGER
from pydb.indexsystem.indexhandle import IndexHandle
from pydb.metasystem.meta import ACC_TYPE, TableMeta
from pydb.recordsystem import RecordManager, recordhandle
from pydb.indexsystem import IndexManager
import functools
import tqdm

from pydb.recordsystem.record import RID


class Operator:
    LT = '<'
    LE = '<='
    GT = '>'
    GE = '>='
    EQ = '='
    NE = '<>'

    # we suggest NULL is smaller than anyone else
    @staticmethod
    def compare(operator: str) -> Callable[[Any, Any], bool]:
        if operator == Operator.LT:
            return lambda a, b: (a == None and b != None) or (a != None and b != None and a < b)
        elif operator == Operator.LE:
            return lambda a, b:  (a == None) or (a != None and b != None and a <= b)
        elif operator == Operator.GT:
            return lambda a, b: (b == None and a != None) or (a != None and b != None and a > b)
        elif operator == Operator.GE:
            return lambda a, b:  (b is None) or (a != None and b != None and a >= b)
        elif operator == Operator.EQ:
            return lambda a, b: a == b
        elif operator == Operator.NE:
            return lambda a, b: a != b
        else:
            raise Exception(f'unknown operator "{operator}"')

# query handler of a single table


class QueryHandle:
    def __init__(self, tbname: str, rm: RecordManager, im: IndexManager, table_info: TableMeta) -> None:
        self.tbname = tbname
        self.table_info = table_info
        self.rm, self.im = rm, im

    # query a single index
    def query_index(self, column: str, op: str, value: Any) -> set[RID]:
        if not self.table_info.has_index(column):
            self.add_index(column)
            print(
                f'Warning: no index for column "{column}" and it would be created')
        index_h = self.im.open_index_file(self.tbname, column)
        result = index_h.select(op, value)
        return result

    def condition_typecheck(self, conditions: dict[str, tuple[str, Any]]):
        for column, cond in conditions.items():
            # check if column exists
            if column not in self.table_info.col_name:
                raise Exception(
                    f'column "{column}" does not exist in table "{self.tbname}"')
            col_info = self.table_info.col_name[column]

            # type check
            acc_type = ACC_TYPE[col_info.type]
            act_type = type(cond[1])
            if act_type not in acc_type:
                raise Exception(
                    f'column "{column}" of table "{self.tbname}" expects to have type "{acc_type}", but got "{act_type}"')

    def values_typecheck(self, values: list[Any], res_check: bool = True):
        if len(values) != len(self.table_info.columns):
            raise Exception(
                f'table "{self.tbname}" has {len(self.table_info.columns)} columns, but only got {len(values)}')
        # single field type check
        for i in range(len(values)):
            col_info = self.table_info.columns[i]
            if type(values[i]) not in ACC_TYPE[col_info.type]:
                raise Exception(
                    f'column "{col_info.name}" of table "{self.tbname}" expects to have type "{col_info.type}", but got type "{type(values[i])}"')
            if col_info.type == 'VARCHAR' and values[i] is not None and len(str(values[i]).encode('utf-8')) > col_info.size:
                raise Exception(
                    f'column "{col_info.name}" of table "{self.tbname}" expects to have VARCHAR({col_info.size}), but string is too long')
            if values[i] is None:
                if col_info.default is None and not col_info.null:
                    raise Exception(
                        f'column "{col_info.name}" of table "{self.tbname}" is NOT NULL and does not have DEFAULT, but got NULL')
                elif col_info.default is not None:
                    values[i] = col_info.default

                if col_info.type == 'VARCHAR':
                    values[i] = ''

        # check primary key
        if res_check and len(self.table_info.primaries) > 0:
            cond = []
            for col_name in self.table_info.primaries:
                col_id = self.table_info.get_column_id(col_name)
                if values[col_id] is None:
                    raise Exception(
                        f'column "{col_name}" of table "{self.tbname}" is PRIMARY, it does not accept NULL')
                cond.append((col_name, Operator.EQ, values[col_id]))
            if len(self.select_rid(cond)) != 0:
                raise Exception(
                    f'table "{self.tbname}" encounters duplicate PRIMARY KEY of value "{values}"')
        return values

    def all_indexed(self, columns):
        columns = list(columns)
        if len(columns) == 0:
            return False
        for column in columns:
            if not self.table_info.has_index(column):
                return False
        return True

    def build_condition(self, conditions: list[tuple[str, str, Any]]):
        if len(conditions) == 0:
            return lambda x: True
        condition_funcs = []
        for column, op, value in conditions:
            col_id = self.table_info.get_column_id(column)
            condition_funcs.append((Operator.compare(op), col_id, value))

        def condition(x):
            p = True
            for cmp, col_id, value in condition_funcs:
                p = p and cmp(x[col_id], value)
            return p
        return condition

    def add_index(self, column: str, progress: bool = False):
        if self.table_info.has_index(column):
            raise Exception(f'index "{self.tbname}.{column}" already exists')
        if self.table_info.col_name[column].type != 'INT':
            print(
                f'Warning: index could be only create on INT, but got {self.table_info.col_name[column].type}, nothing would be done')
            return
        self.table_info.add_index(column)
        self.im.create_index_file(self.tbname, column)
        index_h = self.im.open_index_file(self.tbname, column)
        iter = self.rm.open_record_file(self.tbname).iterator()
        if progress:
            iter = tqdm.tqdm(iter)
        for record in iter:
            index_h.insert(self.table_info.decode(record)[
                           self.table_info.get_column_id(column)], record.rid)

    def drop_index(self, column: str):
        if not self.table_info.has_index(column):
            raise Exception(f'index "{self.tbname}.{column}" does not exist')
        self.table_info.pop_index(column)
        self.im.pop_index_file(self.tbname, column)

    # directly set field to primary
    def set_primary(self, primaries: set[str]):
        self.table_info.set_primary(primaries)
        for p in primaries:
            if not self.table_info.has_index(p):
                self.add_index(p)

    def insert(self, values):
        rid = self.rm.open_record_file(self.tbname).add_record(
            self.table_info.encode(values))
        for index in self.table_info.indexes:
            col_id = self.table_info.get_column_id(index)
            self.im.open_index_file(
                self.tbname, index).insert(values[col_id], rid)

    def select(self, conditions: list[tuple[str, str, Any]]) -> list:
        record_h = self.rm.open_record_file(self.tbname)
        results = []

        if not self.all_indexed(map(lambda x: x[0], conditions)):
            # build condition functions
            cond_func = self.build_condition(conditions)
            record_iter = record_h.iterator()
            for record in record_iter:
                values = self.table_info.decode(record)
                if cond_func(values):
                    results.append(values)
        else:
            rids: set[RID] | None = None
            for column, op, value in conditions:
                if rids is None:
                    rids = self.query_index(column, op, value)
                else:
                    rids.intersection_update(
                        self.query_index(column, op, value))
            # got rids, shift to result otherwise
            if rids is None:
                rids = set()
            for rid in rids:
                results.append(self.table_info.decode(
                    record_h.get_record(rid)))
        return results

    def select_rid(self, conditions: list[tuple[str, str, Any]]) -> set[RID]:
        record_h = self.rm.open_record_file(self.tbname)
        rids: set[RID] | None = None
        if not self.all_indexed(map(lambda x: x[0], conditions)):
            rids = set()
            cond_func = self.build_condition(conditions)
            record_iter = record_h.iterator()
            for record in record_iter:
                values = self.table_info.decode(record)
                if cond_func(values):
                    rids.add(record.rid)
        else:
            for column, op, value in conditions:
                if rids is None:
                    rids = self.query_index(column, op, value)
                else:
                    rids.intersection_update(
                        self.query_index(column, op, value))
        if rids is None:
            rids = set()
        return rids

    def drop(self, conditions: list[tuple[str, str, Any]]):
        rids = list(self.select_rid(conditions))
        record_h = self.rm.open_record_file(self.tbname)

        # reorder rids for faster delete
        rids.sort(key=lambda x: x.page_id)

        if len(rids) > REBUILD_TRIGGER:
            for rid in tqdm.tqdm(rids):
                record_h.pop_record(rid)
            for index in self.table_info.indexes.copy():
                self.drop_index(index)
                self.add_index(index)
        else:
            dropping_indexes = [(self.table_info.get_column_id(
                index), self.im.open_index_file(self.tbname, index)) for index in self.table_info.indexes]
            for rid in tqdm.tqdm(rids):
                record = record_h.get_record(rid)
                record_h.pop_record(rid)
                values = self.table_info.decode(record)
                for col_id, index_h in dropping_indexes:
                    index_h.remove(values[col_id], rid)

    def select_all(self):
        return self.rm.open_record_file(self.tbname).iterator()
