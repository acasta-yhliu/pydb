# Generated from SQL.g4 by ANTLR 4.9.3
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SQLParser import SQLParser
else:
    from SQLParser import SQLParser

# This class defines a complete generic visitor for a parse tree produced by SQLParser.

class SQLVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by SQLParser#program.
    def visitProgram(self, ctx:SQLParser.ProgramContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#statement.
    def visitStatement(self, ctx:SQLParser.StatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#create_db.
    def visitCreate_db(self, ctx:SQLParser.Create_dbContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#drop_db.
    def visitDrop_db(self, ctx:SQLParser.Drop_dbContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#show_dbs.
    def visitShow_dbs(self, ctx:SQLParser.Show_dbsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#use_db.
    def visitUse_db(self, ctx:SQLParser.Use_dbContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#show_tables.
    def visitShow_tables(self, ctx:SQLParser.Show_tablesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#show_indexes.
    def visitShow_indexes(self, ctx:SQLParser.Show_indexesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#load_data.
    def visitLoad_data(self, ctx:SQLParser.Load_dataContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#store_data.
    def visitStore_data(self, ctx:SQLParser.Store_dataContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#create_table.
    def visitCreate_table(self, ctx:SQLParser.Create_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#drop_table.
    def visitDrop_table(self, ctx:SQLParser.Drop_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#describe_table.
    def visitDescribe_table(self, ctx:SQLParser.Describe_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#insert_into_table.
    def visitInsert_into_table(self, ctx:SQLParser.Insert_into_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#delete_from_table.
    def visitDelete_from_table(self, ctx:SQLParser.Delete_from_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#update_table.
    def visitUpdate_table(self, ctx:SQLParser.Update_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#select_table_.
    def visitSelect_table_(self, ctx:SQLParser.Select_table_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#select_table.
    def visitSelect_table(self, ctx:SQLParser.Select_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_add_index.
    def visitAlter_add_index(self, ctx:SQLParser.Alter_add_indexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_drop_index.
    def visitAlter_drop_index(self, ctx:SQLParser.Alter_drop_indexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_table_drop_pk.
    def visitAlter_table_drop_pk(self, ctx:SQLParser.Alter_table_drop_pkContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_table_drop_foreign_key.
    def visitAlter_table_drop_foreign_key(self, ctx:SQLParser.Alter_table_drop_foreign_keyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_table_add_pk.
    def visitAlter_table_add_pk(self, ctx:SQLParser.Alter_table_add_pkContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_table_add_foreign_key.
    def visitAlter_table_add_foreign_key(self, ctx:SQLParser.Alter_table_add_foreign_keyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_table_add_unique.
    def visitAlter_table_add_unique(self, ctx:SQLParser.Alter_table_add_uniqueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#field_list.
    def visitField_list(self, ctx:SQLParser.Field_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#normal_field.
    def visitNormal_field(self, ctx:SQLParser.Normal_fieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#primary_key_field.
    def visitPrimary_key_field(self, ctx:SQLParser.Primary_key_fieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#foreign_key_field.
    def visitForeign_key_field(self, ctx:SQLParser.Foreign_key_fieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#type_.
    def visitType_(self, ctx:SQLParser.Type_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value_lists.
    def visitValue_lists(self, ctx:SQLParser.Value_listsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value_list.
    def visitValue_list(self, ctx:SQLParser.Value_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value.
    def visitValue(self, ctx:SQLParser.ValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#where_and_clause.
    def visitWhere_and_clause(self, ctx:SQLParser.Where_and_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#where_operator_expression.
    def visitWhere_operator_expression(self, ctx:SQLParser.Where_operator_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#column.
    def visitColumn(self, ctx:SQLParser.ColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#expression.
    def visitExpression(self, ctx:SQLParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#set_clause.
    def visitSet_clause(self, ctx:SQLParser.Set_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#selectors.
    def visitSelectors(self, ctx:SQLParser.SelectorsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#selector.
    def visitSelector(self, ctx:SQLParser.SelectorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#identifiers.
    def visitIdentifiers(self, ctx:SQLParser.IdentifiersContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#operate.
    def visitOperate(self, ctx:SQLParser.OperateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#aggregator.
    def visitAggregator(self, ctx:SQLParser.AggregatorContext):
        return self.visitChildren(ctx)



del SQLParser