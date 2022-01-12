from pydb.metasystem.convert import Convert
from pydb.recordsystem.record import Record


ACC_TYPE = {
    'INT': (int, type(None)), 'FLOAT': (int, float, type(None)), 'VARCHAR': (str, type(None))
}


class ColumnMeta:
    def __init__(self, name: str, ttype: str, null: bool, size: int = 8, default=None) -> None:
        self.type = ttype
        self.name = name
        self.size = size
        self.default = default
        if default is not None:
            if type(default) not in ACC_TYPE[ttype]:
                raise Exception(
                    f'field "{name}" of type "{ttype}" expects default value of type "{ACC_TYPE[ttype]}", but got "{type(default)}"')
        self.null = null

    def get_size(self) -> int:
        return self.size if self.type == 'VARCHAR' else 8

    def desc(self):
        return {
            'Column': self.name,
            'Type': '{}{}'.format(self.type, f"({self.size})" if self.size else ""),
            'Null': 'NULL' if self.null else 'NOT NULL',
            'PRIMARY': '',
            'FOREIGN': '',
            'Default': 'NULL' if self.default is None else self.default,
        }


class ForeignKey:
    def __init__(self, ref_table: str, references: list[tuple[str, str]]) -> None:
        self.ref_table = ref_table
        self.references = references

    def ori_columns(self):
        return list(map(lambda x: x[0], self.references))

    def ref_columns(self):
        return list(map(lambda x: x[1], self.references))

    def get_ref(self, col: str):
        for o, r in self.references:
            if o == col:
                return r
        return None


class TableMeta:
    def __init__(self, name: str, columns: list[ColumnMeta]) -> None:
        self.name = name
        self.columns = columns
        self.sizes: list[int] = []
        self.types: list[str] = []
        self.record_size: int = 0
        self.col_name = {}
        self.col_index = {}
        self.indexes = set()

        self.primaries: set[str] = set()
        self.foreigns: dict[str, ForeignKey] = {}
        self.update()

    def update(self):
        self.col_name = {x.name: x for x in self.columns}
        self.col_index = {x.name: i for i, x in enumerate(self.columns)}
        self.sizes = list(map(ColumnMeta.get_size, self.columns))
        self.types = list(map(lambda x: x.type, self.columns))
        self.record_size = sum(self.sizes)

    def set_primary(self, primary: set[str], check:bool = True):
        if check and len(self.primaries) != 0:
            raise Exception(f'table "{self.name}" already has PRIMARY KEY')
        for p in primary:
            if p not in self.col_name:
                raise Exception(
                    f'table "{self.name}" does not have primary field "{p}"')
            self.col_name[p].null = False
        self.primaries = primary

    def add_foreign(self, name: str, foreign: ForeignKey):
        if name in self.foreigns:
            raise Exception(
                f'table "{self.name}" already has foreign key "{name}"')
        for col in foreign.ori_columns():
            if col not in self.col_name:
                raise Exception(
                    f'table "{self.name}" does not have field "{col}"')
        self.foreigns[name] = foreign

    def pop_foreign(self, name: str):
        if name not in self.foreigns:
            raise Exception(
                f'table "{self.name}" does not have foreign key "{name}"')
        self.foreigns.pop(name)

    def encode(self, values: list):
        return Convert.encode(self.sizes, self.types, self.record_size, values)

    def decode(self, record: Record):
        return Convert.decode(self.sizes, self.types, self.record_size, record)

    def get_column_id(self, col: str):
        return self.col_index[col]

    def add_index(self, col: str):
        if col not in self.col_name:
            raise Exception(
                f'table "{self.name}" does not have column "{col}"')
        self.indexes.add(col)

    def has_index(self, col: str):
        if col not in self.col_name:
            raise Exception(
                f'table "{self.name}" does not have column "{col}"')
        return col in self.indexes

    def pop_index(self, col: str):
        if col not in self.col_name:
            raise Exception(
                f'table "{self.name}" does not have column "{col}"')
        self.indexes.remove(col)

    def describe(self) -> dict[str, list[str]]:
        desc = {}
        for col in self.columns:
            col_desc = col.desc()
            if col.name in self.primaries:
                col_desc['PRIMARY'] = 'TRUE'
            foreigns = [f'{name}: {foreign_key.ref_table}.{foreign_key.get_ref(col.name)}' for name,
                        foreign_key in self.foreigns.items() if foreign_key.get_ref(col.name) is not None]
            col_desc['FOREIGN'] = '\n'.join(foreigns)
            for key in col_desc.keys():
                if key not in desc:
                    desc[key] = []
                desc[key].append(col_desc[key])
        return desc


class DBMeta:
    def __init__(self, name: str, tables: list[TableMeta]) -> None:
        self.name = name
        self.tables = {x.name: x for x in tables}

    def add_table(self, table: TableMeta):
        self.tables[table.name] = table

    def pop_table(self, table: str):
        self.tables.pop(table)

    def get_table(self, table: str):
        return self.tables.get(table)

    def has_table(self, table: str):
        return table in self.tables
