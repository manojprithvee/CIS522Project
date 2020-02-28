package com.database;

import com.database.helpers.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class Execute {
    public static ItratorImp executeSelect(ItratorImp op, Table table, Expression condition, List<SelectItem> list, ArrayList<Table> joins, ArrayList<Column> groupByColumnReferences, Expression having, boolean allColumns, Limit limit) {

        ItratorImp oper = op;
        if (joins != null) {
            for (int i = 0; i < joins.size(); i++) {
                Table jointly = joins.get(i);
                oper = new CrossProductItrator(oper, jointly, table);
                table = oper.getTable();
            }
            table = oper.getTable();
        }
        if (condition != null)
            oper = new SelectionItrator(oper, Global.tables.get(table.getAlias()), condition);
        oper = new ProjectionItrator(oper, list, table, allColumns);
        return oper;
    }



    public static void dump(ItratorImp input) {
        Object[] row = input.next();
        while (row != null) {
            int i = 0;
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

    public static ItratorImp executeUnion(ItratorImp current, ItratorImp operator) {
        ItratorImp output = new UnionItator(current, operator);
        output = new DistinctItrator(output);
        return output;
    }
}
