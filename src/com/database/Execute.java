package com.database;

import com.database.helpers.HelperImp;
import com.database.helpers.SelectionHelper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class Execute {
    public static HelperImp executeSelect(HelperImp op, Table table, Expression condition, List<SelectItem> list, ArrayList<Join> joins, ArrayList<Column> groupByColumnReferences, Expression having, boolean allColumns, Limit limit) {
        //todo should complete it
        if (condition != null)
            op = new SelectionHelper(op, Global.tables.get(table.getAlias()), condition);
        return op;
    }

    public static void print(HelperImp input) {
        Object[] row = input.read();
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
            row = input.read();
        }
    }

}
