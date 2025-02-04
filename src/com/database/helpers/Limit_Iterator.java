package com.database.helpers;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Limit;

public class Limit_Iterator implements DB_Iterator {
    DB_Iterator op;
    Long limit;
    Long count = Long.valueOf(0);

    public Limit_Iterator(DB_Iterator op, Limit limit) {
        this.op = op;
        this.limit = limit.getRowCount();
    }

    @Override
    public void reset() {

    }

    @Override
    public Object[] next() {
        Object[] row = op.next();
        if (row != null && count < limit) {
            count += 1;
            return row;
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
