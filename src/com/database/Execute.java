package com.database;

import com.database.helpers.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class Execute {


    public static DB_Iterator executeSelect(DB_Iterator op, Table table, Expression where, Expression condition, List<SelectItem> list, ArrayList<Table> joins, boolean allColumns) {
        ArrayList<Function> aggregator = new ArrayList<>();
        boolean isAggregate = false;
        DB_Iterator oper = op;
        Global.column_used = new ArrayList<>();
        if (joins != null && !joins.isEmpty()) {
            for (Table jointly : joins) {
                oper = new CrossProductIterator(oper, jointly, table);
                table = oper.getTable();
            }
            table = oper.getTable();
        }
        if (where != null)
            oper = new SelectionIterator(oper, Global.list_tables.get(table.getAlias()), where);
        if (condition != null)
            oper = new SelectionIterator(oper, Global.list_tables.get(table.getAlias()), condition);
        if (!allColumns) {


            for (SelectItem selectItem : list) {
                if (!(selectItem instanceof AllTableColumns)) {
                    SelectExpressionItem selectitem = (SelectExpressionItem) selectItem;
                    if (selectitem.getExpression() instanceof Function) {
                        aggregator.add((Function) selectitem.getExpression());
                        isAggregate = true;
                    }
                }
            }
        }
        if (isAggregate)
            oper = new AggregateIterator(oper, aggregator, table);
        else
            oper = new ProjectionIterator(oper, list, table, allColumns);
        return oper;
    }


    public static void print(DB_Iterator input) {
        Object[] row = input.next();
        while (row != null) {
            int i;
            for (i = 0; i < row.length - 1; i++) {
                if (row[i] instanceof StringValue)
                    System.out.print(((StringValue) row[i]).getNotExcapedValue() + "|");
                else
                    System.out.print(row[i] + "|");
            }
            if (row[i] instanceof StringValue)
                System.out.print(((StringValue) row[i]).getNotExcapedValue());
            else
                System.out.print(row[i]);
            System.out.println();
            row = input.next();
        }
    }

    public static DB_Iterator executeUnion(DB_Iterator current, DB_Iterator operator) {
        DB_Iterator output = new UnionIterator(current, operator);
        output = new DistinctIterator(output);
        return output;
    }
}
