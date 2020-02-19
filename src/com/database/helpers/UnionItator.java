package com.database.helpers;

import net.sf.jsqlparser.schema.Table;

public class UnionItator implements ItratorImp {
    ItratorImp left, right;
    boolean leftdone = false;

    public UnionItator(ItratorImp left, ItratorImp right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void reset() {

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
            Object[] rout = right.next();
            return rout;
        } else {
            return lout;
        }
    }

    @Override
    public Table getTable() {
        return null;
    }
}

