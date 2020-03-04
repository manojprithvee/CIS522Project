package com.database.helpers;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.ArrayList;

public class AggregateIterator implements DB_Iterator {
    final DB_Iterator oper;
    final ArrayList<Function> aggregator;
    final Table table;

    public AggregateIterator(DB_Iterator oper, ArrayList<Function> aggregator, Table table) {
        this.oper = oper;
        this.aggregator = aggregator;
        this.table = table;
    }

    @Override
    public void reset() {
        oper.reset();
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] obj;
        obj = new Object[aggregator.size()];
        int i = 0;
        while (i < aggregator.size()) {
            Object l;
            Object[] row = oper.next();
            if (row == null)
                l = null;
            else {
                int count;
                count = 0;
                count++;
                row = oper.next();
                while (row != null) {
                    count++;
                    row = oper.next();
                }
                l = new LongValue(Integer.toString(count));
            }
            if (l == null) return null;
            obj[i] = l;
            i++;
        }
        return obj;
    }

    @Override
    public Table getTable() {
        return table;
    }
}
