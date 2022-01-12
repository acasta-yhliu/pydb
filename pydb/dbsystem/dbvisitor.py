from typing import Any, cast

from numpy import result_type
from pydb.metasystem.meta import ACC_TYPE, ColumnMeta, TableMeta
from pydb.sqlsystem import SQLVisitor, SQLParser, SQLLexer
import antlr4
from .dbmanager import DBManager
from .dbhandle import *
from prettytable import PrettyTable
import csv
import tqdm


def to_str(node: antlr4.TerminalNode):
    return node.__str__()


class VisitResult(BaseException):
    def __init__(self, result: dict[str, list]) -> None:
        self.results = result

    def __str__(self) -> str:
        x = PrettyTable()
        for field, values in self.results.items():
            x.add_column(field, values)
        return x.__str__()


class QueryResult(VisitResult):
    def __init__(self, result: dict[str, list]) -> None:
        super().__init__(result)

    def __str__(self) -> str:
        x = PrettyTable()
        x.field_names = self.results['columns']
        for record in self.results['records']:
            x.add_row(list(map(lambda x: x if x is not None else 'NULL', record)))
        return x.__str__() + f'\n{len(self.results["records"])} items in total'


class DBVisitor(SQLVisitor):
    def __init__(self, dm: DBManager) -> None:
        self.dm = dm
        super().__init__()

    def optional(self, a, default):
        return default if a is None else self.visit(a)

    # BEGIN db_statement

    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        self.dm.create_db(str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        self.dm.drop_db(str(ctx.Identifier()))

    def visitShow_dbs(self, ctx: SQLParser.Show_dbsContext):
        dbs = self.dm.show_dbs()
        raise VisitResult({'databases': dbs})

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        self.dm.use_db(str(ctx.Identifier()))

    def visitShow_tables(self, ctx: SQLParser.Show_tablesContext):
        tables = self.dm.get_db().show_tables()
        raise VisitResult({'tables': tables})

    def visitShow_indexes(self, ctx: SQLParser.Show_indexesContext):
        indexes = self.dm.get_db().show_indexes()
        raise VisitResult({'indexes': indexes})

    # BEGIN io_statement

    def visitLoad_data(self, ctx: SQLParser.Load_dataContext):
        table_info = self.dm.get_db().get_table_info(str(ctx.Identifier()))

        def filter_csv_reader(reader):
            data = []
            for row in reader:
                if len(row) != len(table_info.columns):
                    raise Exception(f'unexpected row count')
                for i in range(len(table_info.columns)):
                    col_info = table_info.columns[i]
                    if row[i] == 'NULL':
                        row[i] = None
                    if col_info.type == 'INT':
                        row[i] = int(row[i])
                    elif col_info.type == 'FLOAT':
                        row[i] = float(row[i])
                data.append(row)
            return data

        with open(f'{str(ctx.String())[1:-1]}', 'r') as f:
            reader = csv.reader(f)
            self.dm.get_db().insert(str(ctx.Identifier()), filter_csv_reader(reader), False)

    def visitStore_data(self, ctx: SQLParser.Store_dataContext):
        table_info = self.dm.get_db().get_table_info(str(ctx.Identifier()))
        with open(f'{str(ctx.String())[1:-1]}', 'w', newline="") as f:
            writer = csv.writer(f)
            for record in self.dm.get_db().get_table(str(ctx.Identifier())).select_all():
                writer.writerow(
                    map(lambda x: 'NULL' if x is None else x, table_info.decode(record)))

    # BEGIN table_statement

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        columns, primaries, foreigns = self.visit(ctx.field_list())
        table_meta = TableMeta(str(ctx.Identifier()), columns)
        table_meta.set_primary(primaries)
        for name, foreign_key in foreigns.items():
            rinfo = self.dm.get_db().get_table_info(foreign_key.ref_table)
            self.type_check_foreign_key(
                table_meta, rinfo, foreign_key.ori_columns(), foreign_key.ref_columns())
        table_meta.foreigns = foreigns
        self.dm.get_db().create_table(table_meta)

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        self.dm.get_db().drop_table(str(ctx.Identifier()))

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        description = self.dm.get_db().get_table_info(str(ctx.Identifier())).describe()
        raise VisitResult(description)

    def visitInsert_into_table(self, ctx: SQLParser.Insert_into_tableContext):
        self.dm.get_db().insert(str(ctx.Identifier()), self.visit(ctx.value_lists()))

    def visitDelete_from_table(self, ctx: SQLParser.Delete_from_tableContext):
        table = str(ctx.Identifier())
        table_info = self.dm.get_db().get_table_info(table)
        where_and_clause: list[WhereClause] = self.visit(
            ctx.where_and_clause())
        cond = []
        for clause in where_and_clause:
            if isinstance(clause, ValueWhere):
                if clause.table != table:
                    raise Exception(
                        f'table "{clause.table}" in where is not defined')
                if clause.column not in table_info.col_name:
                    raise Exception(
                        f'table "{table}" does not have field "{clause.column}"')
                column_info = table_info.col_name[clause.column]
                if type(clause.value) not in ACC_TYPE[column_info.type]:
                    raise Exception(
                        f'field "{clause.table}.{clause.column}" is of type {column_info.type}, but got {type(clause.value)}')
                if column_info.type == 'VARCHAR' and clause.value is not None and len(str(clause.value).encode('utf-8')) > column_info.size:
                    raise Exception(
                        f'field {clause.table}.{clause.column} is of VARCHAR({column_info.size}), but value is too long')
                cond.append((clause.column, clause.operator, clause.value))
            else:
                raise Exception(
                    f'where clause in delete could only be COLUMN OP VALUE, but got "{clause}"')
        self.dm.get_db().drop(table, cond)

    def visitUpdate_table(self, ctx: SQLParser.Update_tableContext):
        table = str(ctx.Identifier())
        table_info = self.dm.get_db().get_table_info(table)

        set_clause: list[tuple[str, Any]] = self.visit(ctx.set_clause())
        for col, value in set_clause:
            if col not in table_info.col_name:
                raise Exception(
                    f'table "{table}" does not have field "{col}"')
            col_info = table_info.col_name[col]
            if type(value) not in ACC_TYPE[col_info.type]:
                raise Exception(
                    f'field "{table}.{col}" is of type {col_info.type}, but got {type(value)}')
            if col_info.type == 'VARCHAR' and value is not None and len(str(value).encode('utf-8')) > col_info.size:
                raise Exception(
                    f'field {table}.{col} is of VARCHAR({col_info.size}), but value is too long')

        where_and_clause: list[WhereClause] = self.visit(
            ctx.where_and_clause())
        cond = []
        for clause in where_and_clause:
            if isinstance(clause, ValueWhere):
                if clause.table != table:
                    raise Exception(
                        f'table "{clause.table}" in where is not defined')
                if clause.column not in table_info.col_name:
                    raise Exception(
                        f'table "{table}" does not have field "{clause.column}"')
                column_info = table_info.col_name[clause.column]
                if type(clause.value) not in ACC_TYPE[column_info.type]:
                    raise Exception(
                        f'field "{clause.table}.{clause.column}" is of type {column_info.type}, but got {type(clause.value)}')
                if column_info.type == 'VARCHAR' and clause.value is not None and len(str(clause.value).encode('utf-8')) > column_info.size:
                    raise Exception(
                        f'field {clause.table}.{clause.column} is of VARCHAR({column_info.size}), but value is too long')
                cond.append((clause.column, clause.operator, clause.value))
            else:
                raise Exception(
                    f'where clause in delete could only be COLUMN OP VALUE, but got "{clause}"')

        self.dm.get_db().update(table, set_clause, cond)

    def visitSet_clause(self, ctx: SQLParser.Set_clauseContext):
        return list(zip([str(i) for i in cast(list, ctx.Identifier())], [self.visit(i) for i in cast(list, ctx.value())]))

    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        selectors: Selector | list[Selector] = self.visit(ctx.selectors())
        tables: list[str] = self.visit(ctx.identifiers())
        where_clauses: list[WhereClause] = self.optional(
            ctx.where_and_clause(), [])
        grouping: tuple[str, str] | None = self.optional(ctx.column(), None)

        limit: int | None = None if ctx.Integer(0) is None else int(
            ctx.Integer(0).__str__())
        offset: int | None = None if ctx.Integer(1) is None else int(
            ctx.Integer(1).__str__())

        # do type check for selectors and where_clause and grouping
        for table in tables:
            self.dm.get_db().get_table_info(table)

        if isinstance(selectors, list):
            for selector in selectors:
                if isinstance(selector, CountSelector):
                    if len(selectors) != 0:
                        raise Exception(
                            f'selector Count(*) cannot use with other selectors')
                    break

                if isinstance(selector, ColumnSelector) or isinstance(selector, AggregatorSelector):
                    if selector.table not in tables:
                        raise Exception(
                            f'selection table "{selector.table}" is not in query tables "{tables}"')
                    if selector.column not in self.dm.get_db().get_table_info(selector.table).col_name:
                        raise Exception(
                            f'field "{selector.column}" does not exist in table "{selector.table}"')

                    if isinstance(selector, AggregatorSelector):
                        column_info = self.dm.get_db().get_table_info(
                            selector.table).col_name[selector.column]
                        if column_info.type == 'VARCHAR':
                            raise Exception(
                                f'aggregate field "{selector.table}.{selector.column}" is of type VARCHAR, cannot use aggregator')

        for clause in where_clauses:
            if isinstance(clause, ValueWhere):
                if clause.table not in tables:
                    raise Exception(
                        f'where table "{clause.table}" is not in query tables')
                if clause.column not in self.dm.get_db().get_table_info(clause.table).col_name:
                    raise Exception(
                        f'field "{clause.column} does not exist in table "{clause.table}""')
                column_info = self.dm.get_db().get_table_info(
                    clause.table).col_name[clause.column]
                if type(clause.value) not in ACC_TYPE[column_info.type]:
                    raise Exception(
                        f'field "{clause.table}.{clause.column}" is of type {column_info.type}, but got {type(clause.value)}')
                if column_info.type == 'VARCHAR' and clause.value is not None and len(str(clause.value).encode('utf-8')) > column_info.size:
                    raise Exception(
                        f'field {clause.table}.{clause.column} is of VARCHAR({column_info.size}), but value is too long')
            elif isinstance(clause, JoinWhere):
                if clause.table1 not in tables:
                    raise Exception(
                        f'where table "{clause.table1}" is not in query tables')
                if clause.table2 not in tables:
                    raise Exception(
                        f'where table "{clause.table2}" is not in query tables')
                if clause.column1 not in self.dm.get_db().get_table_info(clause.table1).col_name:
                    raise Exception(
                        f'field "{clause.column1} does not exist in table "{clause.table1}""')
                if clause.column2 not in self.dm.get_db().get_table_info(clause.table2).col_name:
                    raise Exception(
                        f'field "{clause.column2} does not exist in table "{clause.table2}""')
        raise QueryResult(self.dm.get_db().select(
            selectors, tables, where_clauses, grouping, limit, offset))
    # BEGIN alter_statement

    def visitAlter_add_index(self, ctx: SQLParser.Alter_add_indexContext):
        table_name = str(ctx.Identifier())
        for column in self.visit(ctx.identifiers()):
            self.dm.get_db().get_table(table_name).add_index(column, True)

    def visitAlter_drop_index(self, ctx: SQLParser.Alter_drop_indexContext):
        table_name = str(ctx.Identifier())
        for column in self.visit(ctx.identifiers()):
            self.dm.get_db().get_table(table_name).drop_index(column)

    def visitAlter_table_drop_pk(self, ctx: SQLParser.Alter_table_drop_pkContext):
        table_info = self.dm.get_db().get_table_info(str(ctx.Identifier()))
        table_info.set_primary(set(), False)

    def visitAlter_table_drop_foreign_key(self, ctx: SQLParser.Alter_table_drop_foreign_keyContext):
        self.dm.get_db().get_table_info(str(ctx.Identifier(0))
                                        ).pop_foreign(str(ctx.Identifier(1)))

    def visitAlter_table_add_pk(self, ctx: SQLParser.Alter_table_add_pkContext):
        primaries_visited = self.visit(ctx.identifiers())
        if len(primaries_visited) != len(set(primaries_visited)):
            raise Exception(f'duplicate field in "{primaries_visited}"')
        self.dm.get_db().get_table(str(ctx.Identifier())
                                   ).set_primary(set(primaries_visited))

    def type_check_foreign_key(self, tinfo: TableMeta, rinfo: TableMeta, oris: list[str], refs: list[str]):
        for i in range(len(oris)):
            if tinfo.col_name[oris[i]].type != rinfo.col_name[refs[i]].type or tinfo.col_name[oris[i]].size != rinfo.col_name[refs[i]].size:
                raise Exception(f'foreign key field type mismatch')

    def visitAlter_table_add_foreign_key(self, ctx: SQLParser.Alter_table_add_foreign_keyContext):
        table_name = str(ctx.Identifier(0))
        name = str(ctx.Identifier(1))
        ref_table = str(ctx.Identifier(2))
        oris: list[str] = self.visit(ctx.identifiers(0))
        refs: list[str] = self.visit(ctx.identifiers(1))
        # type check
        # type check the reference
        db = self.dm.get_db()
        tinfo = db.get_table_info(table_name)
        rinfo = db.get_table_info(ref_table)
        for i, ref_field in enumerate(refs):
            if ref_field not in rinfo.col_name:
                raise Exception(
                    f'table "{ref_table}" does not have field "{ref_field}"')
        if len(oris) != len(refs):
            raise Exception(
                f'FOREIGN KEY must have same length when union')
        self.type_check_foreign_key(tinfo, rinfo, oris, refs)
        self.dm.get_db().add_foreign(table_name, name,
                                     ForeignKey(ref_table, list(zip(oris, refs))))

    def visitAlter_table_add_unique(self, ctx: SQLParser.Alter_table_add_uniqueContext):
        raise NotImplemented()

    # ----- misc -----

    def visitField_list(self, ctx: SQLParser.Field_listContext):
        self.fkid = 0
        columns: list[ColumnMeta] = []
        primaries: set[str] = set()
        foreigns: dict[str, ForeignKey] = {}
        fields = cast(list, ctx.field())
        for field in fields:
            i = self.visit(field)
            if isinstance(i, ColumnMeta):
                for col in columns:
                    if i.name == col.name:
                        raise Exception(
                            f'duplicate field name "{i.name}" in creating table')
                columns.append(i)
            elif isinstance(i, list):  # list of identifiers, that is primary key
                for col in i:
                    if col in primaries:
                        raise Exception(
                            f'duplicate field in PRIMARY KEY "{col}"')
                    primaries.add(col)
            elif isinstance(i, tuple):  # foreign key reference
                col = i[0]
                if col in foreigns:
                    raise Exception(f'duplicate field in FOREIGN KEY "{col}"')
                foreigns[col] = i[1]

        def find_column(name, cls):
            for col in cls:
                if col.name == name:
                    return True
            return False

        for pri in primaries:
            if not find_column(pri, columns):
                raise Exception(
                    f'PRIMARY KEY "{pri}" is not defined in creating table')

        for name, fk in foreigns.items():
            for col, ref_col in fk.references:
                if not find_column(col, columns):
                    raise Exception(
                        f'FOREIGN KEY "{name}" column "{col}" is not defined in creating table')
                if ref_col not in self.dm.get_db().get_table_info(fk.ref_table).col_name:
                    raise Exception(
                        f'FOREIGN KEY "{name}" column "{col}" reference "{fk.ref_table}.{ref_col}" does not exist')

        return columns, primaries, foreigns

    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        t, size = self.visit(ctx.type_())
        default: Any = None if ctx.value() is None else self.visit(ctx.value())
        null = ctx.Null() is None
        return ColumnMeta(str(ctx.Identifier()), t, null, size, default)

    def visitPrimary_key_field(self, ctx: SQLParser.Primary_key_fieldContext):
        return self.visit(ctx.identifiers())

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        if len(cast(list, ctx.Identifier())) == 1:
            name = f'FK{self.fkid}'
            self.fkid += 1
            ref_table = str(ctx.Identifier(0))
        else:
            name = str(ctx.Identifier(0))
            ref_table = str(ctx.Identifier(1))
        oris: list[str] = self.visit(ctx.identifiers(0))
        refs: list[str] = self.visit(ctx.identifiers(1))
        if len(oris) != len(refs):
            raise Exception(f'foreign key should have same length when union')
        return (name, ForeignKey(ref_table, list(zip(oris, refs))))

    def visitType_(self, ctx: SQLParser.Type_Context):
        if ctx.Integer() is None:
            return ctx.getText(), None
        else:
            length = int(ctx.Integer().__str__())
            if length == 0:
                raise Exception(f'unexpected VARCHAR length: 0')
            return 'VARCHAR', length

    def visitValue_lists(self, ctx: SQLParser.Value_listsContext):
        return [self.visit(i) for i in cast(list, ctx.value_list())]

    def visitValue_list(self, ctx: SQLParser.Value_listContext):
        return [self.visit(i) for i in cast(list, ctx.value())]

    def visitValue(self, ctx: SQLParser.ValueContext):
        if ctx.Integer() is not None:
            return int(ctx.Integer().__str__())
        if ctx.String() is not None:
            return ctx.String().__str__()[1:-1]  # trim '
        if ctx.Float() is not None:
            return float(ctx.Float().__str__())
        return None

    # selector
    def visitSelectors(self, ctx: SQLParser.SelectorsContext):
        if ctx.getText() == '*':
            return AnySelector()
        elif ctx.Count() is not None:
            return CountSelector()
        else:
            return [self.visit(i) for i in cast(list, ctx.selector())]

    def visitSelector(self, ctx: SQLParser.SelectorContext):
        if ctx.aggregator() is not None:
            return AggregatorSelector(self.visit(ctx.aggregator()), self.visit(ctx.column()))
        elif ctx.column() is not None:
            return ColumnSelector(self.visit(ctx.column()))
        else:
            return CountSelector()

    def visitColumn(self, ctx: SQLParser.ColumnContext):
        return (str(ctx.Identifier(0)), str(ctx.Identifier(1)))

    def visitAggregator(self, ctx: SQLParser.AggregatorContext):
        return ctx.getText()

    # where clause

    def visitWhere_and_clause(self, ctx: SQLParser.Where_and_clauseContext):
        return [self.visit(i) for i in cast(list, ctx.where_clause())]

    def visitWhere_operator_expression(self, ctx: SQLParser.Where_operator_expressionContext):
        column = self.visit(ctx.column())
        operator = self.visit(ctx.operate())
        expression = self.visit(ctx.expression())
        if isinstance(expression, tuple):
            return JoinWhere(column, operator, cast(tuple[str, str], expression))
        else:
            return ValueWhere(column, operator, expression)

    def visitIdentifiers(self, ctx: SQLParser.IdentifiersContext):
        return [str(i) for i in cast(list, ctx.Identifier())]

    def visitOperate(self, ctx: SQLParser.OperateContext):
        return ctx.getText()
