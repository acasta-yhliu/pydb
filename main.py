from pydb.dbsystem import DBSystem
from prompt_toolkit import prompt
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import InMemoryHistory
from pygments.lexers.sql import SqlLexer
import argparse
import time
from colorama import Fore

parser = argparse.ArgumentParser()

parser.add_argument('-f', '--file', help='input .sql file', type=str)
parser.add_argument(
    '-d', '--debug', help='open debug mode to print more exception trace log', type=bool, default=False)

sql_completer = WordCompleter(['CREATE', 'DATABASE', 'DROP', 'SHOW', 'DATABASES', 'USE', 'TABLES', 'INDEXES', 'LOAD', 'DATA', 'INFILE', 'INTO', 'TABLE', 'STORE', 'OUTFILE', 'FROM', 'DESC', 'INSERT', 'VALUES', 'DELETE', 'WHERE', 'UPDATE',
                              'SET', 'SELECT', 'GROUP', 'BY', 'LIMIT', 'OFFSET', 'ALTER', 'ADD', 'INDEX', 'PRIMARY', 'KEY', 'FOREIGN', 'CONSTRAINT', 'REFERENCES', 'UNIQUE', 'NOT', 'DEFAULT', 'INT', 'VARCHAR', 'FLOAT', 'AND', 'COUNT', 'AVG', 'MAX', 'MIN', 'SUM', 'NULL'])
sql_history = InMemoryHistory()

def repl(debugging: bool):
    dbsys = DBSystem(debugging)
    try:
        while True:
            input_sql = prompt(
                f'PYDB @[{dbsys.dm.db.dbname if dbsys.dm.db != None else ""}] > ', lexer=PygmentsLexer(SqlLexer), completer=sql_completer, history=sql_history)
            start = time.time()
            if input_sql == '.exit':
                dbsys.close()
                return
            elif input_sql == '.help':
                with open('pydb/SQL.g4') as f:
                    print(f.read())
            else:
                result = dbsys.run(input_sql)
                end = time.time()
                if result != None:
                    print(result)
                print(Fore.RESET + f'Time Usage: {end - start}s')
    except KeyboardInterrupt:
        print("\nWarning: unexpected shutting down because of SIGINT")
        dbsys.close()


def print_welcome_message():
    print(
        """===== PYDB Database System =====
    > Welcome to use PYDB Database System
    > To exit, enter '.exit'
    > For information, enter '.help'
""")


if __name__ == '__main__':
    args = parser.parse_args()
    if args.debug:
        print('[ info ] Started in debug mode')
    if args.file is not None:
        with open(args.file, 'r') as f:
            dbsys = DBSystem(args.debug)
            dbsys.run(f.read())
            dbsys.close()
    else:
        print_welcome_message()
        repl(args.debug)
