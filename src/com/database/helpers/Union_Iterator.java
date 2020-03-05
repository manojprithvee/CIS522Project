package com.database.helpers;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;

public class Union_Iterator implements DB_Iterator {
    final DB_Iterator left;
    final DB_Iterator right;
    boolean left_done = false;

    public Union_Iterator(DB_Iterator left, DB_Iterator right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void reset() {
        left.reset();
        right.reset();
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] lout;
        if (!left_done)
            lout = left.next();
        else
            lout = null;
        if (lout == null) {
            left_done = true;
            return right.next();
        } else {
            return lout;
        }
    }

    @Override
    public Table getTable() {
        return null;
    }
}

