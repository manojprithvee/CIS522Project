package com.database.helpers;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Table;

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
    public Object[] next() {
        Object[] obj = new Object[aggregator.size()];
        for (int i = 0; i < aggregator.size(); i++) {
            Object l = count();
            if (l == null)
                return null;
            obj[i] = l;
        }
        return obj;
    }

    @Override
    public Table getTable() {
        return table;
    }

    private Object count() {
        Object[] row;
        row = oper.next();
        int count = 0;
        if (row == null)
            return null;

        do {
            count++;
            row = oper.next();

        } while (row != null);


        return new LongValue(Integer.toString(count));
    }
}
