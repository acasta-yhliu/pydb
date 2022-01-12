from pydb.dbsystem import DBVisitor
from antlr4 import InputStream, CommonTokenStream
from antlr4.error.ErrorListener import ErrorListener
from pydb.dbsystem.dbmanager import DBManager
from pydb.dbsystem.dbvisitor import VisitResult
from pydb.fio import FileManager

from pydb.sqlsystem.SQLLexer import SQLLexer
from pydb.sqlsystem.SQLParser import SQLParser
import traceback
from colorama import Fore

class SyntaxErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offending_symbol, line, column, msg, e):
        raise Exception(
            "line " + str(line) + ":" + str(column) + " " + msg)


class DBSystem:
    def __init__(self, debugging: bool) -> None:
        self.dm = DBManager(FileManager())
        self.visitor = DBVisitor(self.dm)
        self.debugging = debugging

    def close(self):
        self.dm.close()

    def run(self, sql: str):
        input_stream = InputStream(sql)
        lexer = SQLLexer(input_stream)
        lexer.removeErrorListeners()
        lexer.addErrorListener(SyntaxErrorListener())
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        parser.removeErrorListeners()
        parser.addErrorListener(SyntaxErrorListener())
        try:
            tree = parser.program()
        except Exception as e:
            print(Fore.RED + f'Syntax Error: {e}')
            return None

        try:
            self.visitor.visit(tree)
        except VisitResult as result:
            return result
        except Exception as e:
            if self.debugging:
                traceback.print_exception(e)
            else:
                print(Fore.RED + f'Execution Error: {e}')