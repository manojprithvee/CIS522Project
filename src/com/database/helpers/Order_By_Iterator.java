package com.database.helpers;

import com.database.RAtree.RA_Tree;
import com.database.sql.Row_Compare;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

public class Order_By_Iterator implements DB_Iterator {
    private final RA_Tree left;
    ArrayList<Object[]> buffer;
    List<OrderByElement> orderByElements;
    Table table;
    DB_Iterator left_iterator;
    private Iterator<Object[]> ite;

    public Order_By_Iterator(RA_Tree op, List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
        this.left = op;
        this.left_iterator = op.get_iterator();
        reset();
    }

    @Override
    public void reset() {
        buffer = new ArrayList<Object[]>();
        left_iterator.reset();

        Object[] row = left_iterator.next();
        while (row != null) {
            buffer.add(row);
            row = left_iterator.next();
        }

        Comparator<Object[]> main_compare = null;
        for (OrderByElement orderbyElement : orderByElements) {
            if (main_compare == null) {
                main_compare = new Row_Compare(orderbyElement, table, left.getSchema());
            }
            main_compare = main_compare.thenComparing(new Row_Compare(orderbyElement, table, left.getSchema()));
        }
        Collections.sort(buffer, main_compare);
        ite = buffer.iterator();
    }

    @Override
    public Object[] next() {
        if (ite.hasNext())
            return ite.next();
        else
            return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
