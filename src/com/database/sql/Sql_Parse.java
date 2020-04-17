package com.database.sql;

import com.database.RAtree.RA_Tree;
import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Sql_Parse implements StatementVisitor {

    public static void print(DB_Iterator input) {
        Object[] row = input.next();
        if (row != null) {
            do {
                int i;
                i = 0;
                while (i < row.length - 1) {
                    if (row[i] instanceof StringValue) {
                        System.out.print(((StringValue) row[i]).getNotExcapedValue() + "|");
                    } else
                        System.out.print(row[i] + "|");
                    i++;
                }
                if (row[i] instanceof StringValue) {
                    System.out.print(((StringValue) row[i]).getNotExcapedValue());
                } else
                    System.out.print(row[i]);
                System.out.println();
                row = input.next();
            } while (row != null);
        }
    }


    @Override
    public void visit(Select select) {
        Organizer extractor = new Organizer(select.getSelectBody());
        Shared_Variables.column_used = extractor.columns;
        Build_Tree treeBuilder = new Build_Tree(select.getSelectBody());
        RA_Tree root = treeBuilder.getRoot();
        Optimize.selectionpushdown(root);
        print(root.get_iterator());
        System.out.println("=");
    }

    @Override
    public void visit(Delete delete) {

    }

    @Override
    public void visit(Update update) {

    }

    @Override
    public void visit(Insert insert) {

    }

    @Override
    public void visit(Replace replace) {

    }

    @Override
    public void visit(Drop drop) {

    }

    @Override
    public void visit(Truncate truncate) {

    }

    @Override
    public void visit(CreateTable createTable) {
        String tableName = createTable.getTable().getName();
        LinkedHashMap<String, Integer> cols = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        if (!Shared_Variables.list_tables.containsKey(tableName)) {
            List<ColumnDefinition> lists = createTable.getColumnDefinitions();
            int i = 0;
            for (ColumnDefinition list : lists) {
                cols.put(tableName + "." + list.getColumnName(), i);
                dataType.add(list.getColDataType().toString());
                i++;
            }
            Shared_Variables.list_tables.put(tableName, cols);
            Shared_Variables.schema_store.put(tableName, dataType);
        }
    }
}
