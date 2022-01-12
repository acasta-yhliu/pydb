from functools import reduce
from typing import Any, cast

from numpy import where
from numpy.lib.arraysetops import isin
from pydb.fio import FileManager
from pydb.indexsystem import IndexManager
from pydb.metasystem.meta import ForeignKey
from pydb.querysystem.queryhandle import Operator
from pydb.recordsystem import RecordManager
from pydb.metasystem import MetaManager, TableMeta
from pydb.querysystem import QueryHandle
from itertools import product
import tqdm

from pydb.recordsystem.record import RID


class Aggregator:
    Count = 'COUNT'
    Average = 'AVERAGE'
    Max = 'MAX'
    Min = 'MIN'
    Sum = 'SUM'


class Selector:
    pass


class AnySelector(Selector):
    def __init__(self) -> None:
        super().__init__()


class ColumnSelector(Selector):
    def __init__(self, column_selector: tuple[str, str]) -> None:
        super().__init__()
        self.table, self.column = column_selector


class CountSelector(Selector):
    def __init__(self) -> None:
        super().__init__()


class AggregatorSelector(Selector):
    def __init__(self, aggregator: str, column: tuple[str, str]) -> None:
        super().__init__()
        self.aggregator = aggregator
        self.table, self.column = column


class WhereClause:
    pass


class ValueWhere(WhereClause):
    def __init__(self, column: tuple[str, str], operator: str, value: Any) -> None:
        super().__init__()
        self.table, self.column = column
        self.operator = operator
        self.value = value


class JoinWhere(WhereClause):
    def __init__(self, column1: tuple[str, str], operator: str, column2: tuple[str, str]) -> None:
        super().__init__()
        self.table1, self.column1 = column1
        self.operator = operator
        self.table2, self.column2 = column2
        self.t1id, self.t2id = 0, 0
        self.c1id, self.c2id = 0, 0
        self.compare = Operator.compare(self.operator)

    def get_column(self, table: str):
        return self.column1 if table == self.table1 else self.column2


class DBHandle:
    def __init__(self, dbname: str, fm: FileManager) -> None:
        self.dbname = dbname
        self.fm = fm
        self.rm = RecordManager(fm, dbname)
        self.im = IndexManager(fm, dbname)
        self.mm = MetaManager(fm, dbname)

    def close(self):
        self.mm.close()
        self.im.close()
        self.rm.close()
        # should not shutdown the filemanager, let it done with dbmanager

    def show_tables(self) -> list[str]:
        return list(self.mm.meta.tables.keys())

    def show_indexes(self) -> list[str]:
        indexes = []
        for table_info in self.mm.meta.tables.values():
            for index in table_info.indexes:
                indexes.append(f'{table_info.name}.{index}')
        return indexes

    def get_table_info(self, table: str):
        return self.mm.get_table(table)

    def create_foreign_index(self, foreign_key: ForeignKey):
        for ref_col in foreign_key.ref_columns():
            if not self.get_table_info(foreign_key.ref_table).has_index(ref_col):
                self.get_table(foreign_key.ref_table).add_index(ref_col)

    def add_foreign(self, table: str, name: str, foreign_key: ForeignKey):
        self.mm.get_table(table).add_foreign(
            name, foreign_key)
        self.create_foreign_index(foreign_key)

    def create_table(self, table_info: TableMeta):
        self.mm.add_table(table_info)
        self.rm.create_record_file(table_info.name, table_info.record_size)
        for primary in table_info.primaries:
            self.get_table(table_info.name).add_index(primary)
        for _, foreign_key in table_info.foreigns.items():
            self.create_foreign_index(foreign_key)

    def get_table(self, table: str) -> QueryHandle:
        table_info = self.mm.get_table(table)
        return QueryHandle(table, self.rm, self.im, table_info)

    def drop_table(self, table: str):
        for ori_table, ori_table_info in self.mm.meta.tables.items():
            ref_tables = list(
                map(lambda x: x.ref_table, ori_table_info.foreigns.values()))
            if table in ref_tables:
                raise Exception(
                    f'unable to drop table "{table}" since it is referenced by table "{ori_table}" as foreign key')

        table_info = self.get_table_info(table)
        self.mm.pop_table(table)
        self.rm.pop_record_file(table)
        for index in table_info.indexes:
            self.im.pop_index_file(table, index)

    def insert(self, table: str, values: list[list], res_check: bool = True):
        table_info = self.get_table_info(table)
        tvalues = []
        if res_check and len(table_info.primaries) > 0:
            # primary key check, make sure the value your insert dont have save primary key
            prims = set()
            for value in tvalues:
                pkey = tuple(value[table_info.get_column_id(i)]
                             for i in table_info.primaries)
                if pkey in prims:
                    raise Exception(
                        f'table "{table_info.name}" encounters duplicate PRIMARY KEY of value "{value}"')
                else:
                    prims.add(pkey)
        for value in values:
            tvalue = self.get_table(table).values_typecheck(value, res_check)
            # type check the foreign fields
            if res_check:
                # check for foreign key contraint, for each foreign key constraint,
                # make sure the foreign key actually exists
                for name, foreign_key in table_info.foreigns.items():
                    # build foreign key query condition for each value
                    cond = []
                    for ori_col, ref_col in foreign_key.references:
                        # query with the reference column, it should contain the value we want to insert
                        cond.append(
                            (ref_col, Operator.EQ, tvalue[table_info.get_column_id(ori_col)]))
                    # use select rid for faster select
                    if len(self.get_table(foreign_key.ref_table).select_rid(cond)) == 0:
                        raise Exception(
                            f'foreign key "{name}" is broken since the foreign key is not appeared')
            tvalues.append(tvalue)
        table_query = self.get_table(table)
        for tvalue in tqdm.tqdm(tvalues):
            table_query.insert(tvalue)

    def drop(self, table: str, conditions: list[tuple[str, str, Any]]):
        table_info = self.get_table_info(table)
        to_drop = self.get_table(table).select(conditions)
        # remove those foreign ones
        for ori_table, ori_table_info in self.mm.meta.tables.items():
            # if the table reference this table as foreign key
            for name, foreign in filter(lambda x: x[1].ref_table == table, ori_table_info.foreigns.items()):
                for value in to_drop:
                    cond = []
                    my_cond = []
                    for ori_col, ref_col in foreign.references:
                        cond.append(
                            (ori_col, Operator.EQ, value[table_info.get_column_id(ref_col)]))
                        my_cond.append(
                            (ref_col, Operator.EQ, value[table_info.get_column_id(ref_col)]))
                    if len(self.get_table(ori_table).select_rid(cond)) != 0 and len(self.get_table(table).select_rid(my_cond)) == 0:
                        raise Exception(
                            f'drop break the foreign key constraint “{name}” of table "{ori_table}", stop dropping!')
        self.get_table(table).drop(conditions)

    def update(self, table: str, set_clauses: list[tuple[str, Any]], conditions: list[tuple[str, str, Any]]):
        to_drop = self.get_table(table).select(conditions)
        table_info = self.get_table_info(table)

        id_set_clauses = {self.get_table_info(
            table).get_column_id(x[0]): x[1] for x in set_clauses}
        modify_cols = set(map(lambda x: x[0], set_clauses))

        # first, we need to drop value, so check foreign key as constraint
        for ori_table, ori_table_info in self.mm.meta.tables.items():
            # if the table reference this table as foreign key
            for name, foreign in filter(lambda x: x[1].ref_table == table, ori_table_info.foreigns.items()):
                check_foreign = reduce(lambda a, b: a or b, [
                    i in modify_cols for i in foreign.ref_columns()])
                if check_foreign:
                    for value in to_drop:
                        my_cond = []
                        cond = []
                        for ori_col, ref_col in foreign.references:
                            cond.append(
                                (ori_col, Operator.EQ, value[table_info.get_column_id(ref_col)]))
                            my_cond.append(
                                (ref_col, Operator.EQ, value[table_info.get_column_id(ref_col)]))
                        # if len(cond) > 0, then there's modification in reference key
                        if len(cond) > 0 and len(self.get_table(ori_table).select_rid(cond)) != 0 and len(self.get_table(table).select_rid(my_cond)) == 1:
                            raise Exception(
                                f'update break the foreign key constraint “{name}” of table "{ori_table}", stop updating!')

        def setter(value):
            for i in range(len(value)):
                if i in id_set_clauses:
                    value[i] = id_set_clauses[i]
            return value

        to_update = list(map(setter, to_drop))

        # then, we need to insert back, first do primary check with all the update items
        prims = set()
        for tvalue in to_update:
            if len(table_info.primaries) > 0:
                pkey = tuple(tvalue[table_info.get_column_id(i)]
                             for i in table_info.primaries)
                if pkey in prims:
                    raise Exception(
                        f'table "{table_info.name}" encounters duplicate PRIMARY KEY of value "{tvalue}"')
                else:
                    prims.add(pkey)

            # then foreign key check, the thing we update need to exist
            for name, foreign_key in table_info.foreigns.items():
                cond = []
                for ori_col, ref_col in foreign_key.references:
                    cond.append(
                        (ref_col, Operator.EQ, tvalue[table_info.get_column_id(ori_col)]))
                if len(self.get_table(foreign_key.ref_table).select_rid(cond)) == 0:
                    raise Exception(
                        f'foreign key "{name}" is broken since the foreign key is not appeared')

        # then, commit
        self.get_table(table).drop(conditions)

        for tvalue in tqdm.tqdm(to_update):
            self.get_table(table).insert(tvalue)

    def select(self, selectors: Selector | list[Selector], tables: list[str], where_clause: list[WhereClause], __grouping: tuple[str, str] | None, __limit: int | None, __offset: int | None):
        # filter out aggregator
        if isinstance(selectors, AnySelector):
            # select '*'
            if len(tables) != 1:
                raise Exception(
                    f'unable to select * from more than 1 table')
            table = tables[0]
            # construct where to condition
            cond = []
            for clause in where_clause:
                if isinstance(clause, ValueWhere):
                    if clause.table != table:
                        raise Exception(
                            f'table "{clause.table}" in where is not defined')
                    cond.append((clause.column, clause.operator, clause.value))
                else:
                    raise Exception(f'unable to join with only 1 table')
            # select from table
            records = self.get_table(table).select(cond)
            return {'records': records, 'columns': [
                x.name for x in self.get_table_info(table).columns]}
        elif isinstance(selectors, CountSelector):
            join_wheres: list[JoinWhere] = []

            # first, query with join_table and target_table
            conds = {i: [] for i in tables}
            for clause in where_clause:
                if isinstance(clause, ValueWhere):
                    conds[clause.table].append((clause.column,
                                                clause.operator, clause.value))
                elif isinstance(clause, JoinWhere):
                    join_wheres.append(clause)

            select_results = {i: self.get_table(
                i).select(conds[i]) for i in tables}

            # prepare for join
            tbname2id = {tbname: i for i,
                         tbname in enumerate(select_results.keys())}
            for jwhere in join_wheres:
                jwhere.t1id = tbname2id[jwhere.table1]
                jwhere.t2id = tbname2id[jwhere.table2]
                jwhere.c1id = self.get_table_info(
                    jwhere.table1).get_column_id(jwhere.column1)
                jwhere.c2id = self.get_table_info(
                    jwhere.table2).get_column_id(jwhere.column2)

            # perform join
            joined = 0
            total_joined = reduce(
                lambda a, b: a * b, map(lambda x: len(x), select_results.values()))
            for rec in tqdm.tqdm(product(*select_results.values()), total=total_joined):
                passed = True
                for jwhere in join_wheres:
                    v1 = rec[jwhere.t1id][jwhere.c1id]
                    v2 = rec[jwhere.t2id][jwhere.c2id]
                    if not jwhere.compare(v1, v2):
                        passed = False
                        break
                if passed:
                    joined += 1
            return {'records': [[joined]], 'columns': ['COUNT(*)']}
        elif isinstance(selectors, list):
            columns = []
            for i in selectors:
                if isinstance(i, ColumnSelector):
                    if i.column in columns:
                        raise Exception(f'duplicate column in joining')
                    columns.append(i.column)
                else:
                    raise Exception(f'unsupported selector')

            join_wheres: list[JoinWhere] = []

            # first, query with join_table and target_table
            conds = {i: [] for i in tables}
            for clause in where_clause:
                if isinstance(clause, ValueWhere):
                    conds[clause.table].append((clause.column,
                                                clause.operator, clause.value))
                elif isinstance(clause, JoinWhere):
                    join_wheres.append(clause)

            select_results = {i: self.get_table(
                i).select(conds[i]) for i in tables}

            # prepare for join
            tbname2id = {tbname: i for i,
                         tbname in enumerate(select_results.keys())}
            for jwhere in join_wheres:
                jwhere.t1id = tbname2id[jwhere.table1]
                jwhere.t2id = tbname2id[jwhere.table2]
                jwhere.c1id = self.get_table_info(
                    jwhere.table1).get_column_id(jwhere.column1)
                jwhere.c2id = self.get_table_info(
                    jwhere.table2).get_column_id(jwhere.column2)

            selections = []
            for i in selectors:
                if isinstance(i, ColumnSelector):
                    selections.append((tbname2id[i.table], self.get_table_info(
                        i.table).get_column_id(i.column)))

            # perform join
            joined = []
            total_joined = reduce(
                lambda a, b: a * b, map(lambda x: len(x), select_results.values()))
            for rec in tqdm.tqdm(product(*select_results.values()), total=total_joined):
                passed = True
                for jwhere in join_wheres:
                    v1 = rec[jwhere.t1id][jwhere.c1id]
                    v2 = rec[jwhere.t2id][jwhere.c2id]
                    if not jwhere.compare(v1, v2):
                        passed = False
                        break
                if passed:
                    joined.append([rec[sel[0]][sel[1]] for sel in selections])

            return {'records': joined, 'columns': columns}
        else:
            raise Exception(f'unknown selector type')


def list_at(list: list, ids: list[int]):
    return [list[i] for i in ids]
