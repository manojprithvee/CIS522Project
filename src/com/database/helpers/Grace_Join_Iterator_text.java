package com.database.helpers;

import com.database.RAtree.RA_Tree;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;


public class Grace_Join_Iterator_text implements DB_Iterator {
    private static final int NONE = 0;
    private static final int LONG = 1;
    private static final int DOUBLE = 2;
    private static final int DATE = 3;
    private static final int STRING = 4;
    private final DB_Iterator leftIterator;
    private final DB_Iterator rightIterator;
    private final int leftIndex;
    private final int rightIndex;
    private HashMap<Object, LinkedList<Object[]>> table;
    private int type;
    private Object[] right;
    private Iterator<Object[]> current;

    public Grace_Join_Iterator_text(RA_Tree left, RA_Tree right,
                                    int leftIndex,
                                    int rightIndex) {
        this.leftIterator = right.get_iterator();
        this.rightIterator = left.get_iterator();
        this.leftIndex = rightIndex;
        this.rightIndex = leftIndex;
        reset();
    }

    public Object[] next() {
        if (table == null) return null;
        right = rightIterator.next();
//        System.out.println(Arrays.deepToString(right));
        if (current == null && right == null) {
            clear();
            return null;
        }
        if (current == null) {
            while (right != null) {
                PrimitiveValue leaf = (PrimitiveValue) right[rightIndex];
                Object key = getKey(leaf);
                if (table.containsKey(key)) {
                    current = table.get(key).iterator();
                    Object[] next = create_row(current.next(), right);
                    if (!current.hasNext()) current = null;
                    return next;
                }
                right = rightIterator.next();
            }
            clear();
            return null;
        }
        Object[] next = create_row(current.next(), right);
        if (!current.hasNext()) current = null;
        return next;
    }


    @Override
    public Table getTable() {
        return null;
    }

    private Object[] create_row(Object[] left, Object[] right) {
        if (left == null || right == null) return null;
        Object[] new_row = new Object[left.length + right.length];
        int index = 0;
        for (Object o : left) {
            new_row[index] = o;
            index++;
        }
        for (Object o : right) {
            new_row[index] = o;
            index++;
        }
        return new_row;
    }

    public boolean delete() {
        if (table == null) return false;
        table = null;
        type = NONE;
        return false;
    }

    public void clear() {
        table = null;
        type = NONE;
    }

    public void reset() {
        if (table != null) return;
        table = new HashMap<Object, LinkedList<Object[]>>();
        type = NONE;
        Object[] left = leftIterator.next();
        while (left != null) {
            PrimitiveValue leaf = (PrimitiveValue) left[leftIndex];

            if (type == NONE) {
                if (leaf instanceof LongValue) type = LONG;
                else if (leaf instanceof DoubleValue) type = DOUBLE;
                else if (leaf instanceof DateValue) type = DATE;
                else type = STRING;
            }

            Object key = getKey(leaf);

            LinkedList<Object[]> values;
            if (table.containsKey(key)) {
                values = table.get(key);
            } else {
                values = new LinkedList<Object[]>();
                table.put(key, values);
            }
            values.add(left);
            left = leftIterator.next();
        }
    }

    private Object getKey(PrimitiveValue leaf) {
        try {
            switch (type) {
                case LONG:
                    return leaf.toLong();
                case DOUBLE:
                    return leaf.toDouble();
                case DATE:
                    return ((DateValue) leaf).getValue().getTime();
                case STRING:
                default:
                    return leaf.toString();
            }
        } catch (PrimitiveValue.InvalidPrimitive e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }
}
