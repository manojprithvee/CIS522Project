package com.database.helpers;

import net.sf.jsqlparser.schema.Table;

public class UnionIterator implements DB_Iterator {
    final DB_Iterator left;
    final DB_Iterator right;
    boolean leftdone = false;

    public UnionIterator(DB_Iterator left, DB_Iterator right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void reset() {
        left.reset();
        right.reset();
    }

    @Override
    public Object[] next() {
        Object[] lout;
        if (!leftdone)
            lout = left.next();
        else
            lout = null;
        if (lout == null) {
            leftdone = true;
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

