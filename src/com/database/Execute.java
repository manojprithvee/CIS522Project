package com.database;

import com.database.helpers.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Execute {


    public static DB_Iterator select_tree(DB_Iterator op, Expression where, Expression condition, List<SelectItem> list, Table table, boolean allColumns, ArrayList<Table> joins, List<OrderByElement> orderByElements, Limit limit, List<Column> groupByColumnReferences, Expression having) throws SQLException {
        boolean ifagg = false;
        Shared_Variables.column_used = new ArrayList<>();
        var aggregator = new ArrayList<Function>();
        if (!allColumns) {
            for (Iterator<SelectItem> iterator = list.iterator(); iterator.hasNext(); ) {
                SelectItem item = iterator.next();
                if (!(item instanceof AllTableColumns)) {
                    SelectExpressionItem exp_item = (SelectExpressionItem) item;
                    if (exp_item.getExpression() instanceof Function) {
                        aggregator.add((Function) exp_item.getExpression());
                        ifagg = true;
                    }
                }
            }
        }
        if (joins != null && !joins.isEmpty()) {
            for (Table jointly : joins) {
                op = new Cross_Product_Iterator(op, jointly, table);
                table = op.getTable();
            }
            table = op.getTable();
        }
        if (where != null)
            op = new Selection_Iterator(op, where, Shared_Variables.list_tables.get(table.getAlias()));
        if (condition != null)
            op = new Selection_Iterator(op, condition, Shared_Variables.list_tables.get(table.getAlias()));

        if (groupByColumnReferences != null) {
            op = new Group_By_Iterator(op, table, list, aggregator, groupByColumnReferences, having);
//            op = new Projection_Iterator(op, list, table, allColumns);
        } else if (ifagg)
            op = new Aggregate_Iterator(op, aggregator, table);
        else
            op = new Projection_Iterator(op, list, table, allColumns);
        if (orderByElements != null)
            op = new Order_By_Iterator(op, orderByElements, table);
        if (limit != null)
            op = new Limit_Iterator(op, limit);
        return op;
    }


    public static void print(DB_Iterator input) throws SQLException {
        Object[] row = input.next();
//        var count =0;
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
//                count++;
//                if(count>10) break;
            } while (row != null);
        }
    }

    public static DB_Iterator union_tree(DB_Iterator current, DB_Iterator operator) {
        DB_Iterator output = new Union_Iterator(current, operator);
        output = new Distinct_Iterator(output);
        return output;
    }
}
