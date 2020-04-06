package com.database.helpers;

import com.database.sql.Row_Compare;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

public class Order_By_Iterator implements DB_Iterator {
    ArrayList<Object[]> buffer;
    List<OrderByElement> orderByElements;
    Table table;
    DB_Iterator op;
    private Iterator<Object[]> ite;

    public Order_By_Iterator(DB_Iterator op, List<OrderByElement> orderByElements, Table table) {
        this.orderByElements = orderByElements;
        this.table = table;
        this.op = op;
        reset();
    }

    @Override
    public void reset() {
        buffer = new ArrayList<Object[]>();
        op.reset();

            Object[] row = op.next();
            while (row != null) {
                buffer.add(row);
                row = op.next();
            }

        Comparator<Object[]> main_compare = null;
        for (OrderByElement orderbyElement : orderByElements) {
            if (main_compare == null) {
                main_compare = new Row_Compare(orderbyElement, table);
            }
            main_compare = main_compare.thenComparing(new Row_Compare(orderbyElement, table));
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
