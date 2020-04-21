package com.database.helpers;

import com.database.RAtree.RA_Tree;
import com.database.Shared_Variables;
import net.sf.jsqlparser.schema.Table;

public class Cross_Product_Iterator implements DB_Iterator {

    Table table;
    DB_Iterator leftIterator, rightIterator;
    private int size;
    private Table righttable, lefttable;
    private Object[] temp1;

    public Cross_Product_Iterator(RA_Tree left, RA_Tree right) {
        this.leftIterator = left.get_iterator();
        this.rightIterator = right.get_iterator();
        temp1 = leftIterator.next();
    }

    @Override
    public void reset() {
        leftIterator.reset();
        rightIterator.reset();
        temp1 = leftIterator.next();
    }

    @Override
    public Object[] next() {

        Object[] temp2 = rightIterator.next();
        if (temp2 == null) {
            temp1 = leftIterator.next();

            if (temp1 == null) {

                return null;
            }
            rightIterator.reset();
            temp2 = rightIterator.next();
        }
        return Shared_Variables.create_row(temp1, temp2);
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}