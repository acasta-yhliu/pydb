from pydb.fio import FileManager

from pydb.metasystem.meta import DBMeta
from .dbhandle import DBHandle
import os
import pickle


class DBManager:
    def __init__(self, fm: FileManager) -> None:
        self.fm = fm
        self.db: DBHandle | None = None

    def close(self):
        if self.db != None:
            self.db.close()
        self.fm.shutdown()

    def get_db(self):
        if self.db is None:
            raise Exception(f'no database is using')
        else:
            return self.db

    def close_db(self):
        if self.db is not None:
            self.db.close()

    def use_db(self, dbname: str):
        self.close_db()
        self.db = DBHandle(dbname, self.fm)

    def has_db(self, dbname: str):
        return self.fm.exists_file(dbname) and self.fm.exists_file(os.path.join(dbname, dbname + '.meta'))

    def create_db(self, dbname: str):
        if self.has_db(dbname):
            raise Exception(f'databse "{dbname}" already exists')
        self.fm.create_dir(dbname)
        # write the initial meta information for the new database
        with open(os.path.join(dbname, dbname + '.meta'), 'wb') as f:
            pickle.dump(DBMeta(dbname, []), f)

    def drop_db(self, dbname: str):
        if self.db is not None and self.db.dbname == dbname:
            print("Warning: drop current using database!")
            self.db.close()
            self.db = None
        if self.has_db(dbname):
            self.fm.remove_dir(dbname)
        else:
            raise Exception(f'database "{dbname}" does not exist')

    def show_dbs(self) -> list[str]:
        for _, dirs, _ in os.walk('.'):
            return list(filter(self.has_db, dirs))
        return []
