package com.database.helpers;

import com.database.RAtree.RA_Tree;
import net.sf.jsqlparser.schema.Table;

import java.util.*;

public class Grace_Join_Iterator implements DB_Iterator {
    private final DB_Iterator rightIterator;
    private final Map<Object, ArrayList<Object[]>> map;
    private final Integer leftIndex, rightIndex;
    DB_Iterator leftIterator;
    Object current_group;
    private String type = "NONE";
    private Table table;
    private Object[] temp2;
    private int size;
    private Iterator<Object> group;
    private Iterator<Object[]> current_group_iterator;


    public Grace_Join_Iterator(RA_Tree left, RA_Tree right,
                               int leftindex, int rightindex) {
        this.rightIterator = right.get_iterator();
        this.leftIterator = left.get_iterator();
        Object[] row = this.leftIterator.next();
        this.rightIndex = rightindex;
        this.leftIndex = leftindex;
        map = new HashMap<>();
        while (row != null) {
            Object key = row[leftindex];
            ArrayList<Object[]> abc;
            if (map.containsKey(key)) {
                abc = map.get(key);
            } else {
                abc = new ArrayList<>();
            }
            abc.add(row);
            map.put(key, abc);
            row = this.leftIterator.next();
        }
        temp2 = rightIterator.next();
    }

    public Object[] create_row(Object[] left, Object[] right, int size) {
        Object[] new_row = new Object[left.length + right.length];
        int index = 0;
        if (left == null || right == null) return null;
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

    @Override
    public void reset() {
        leftIterator.reset();
        rightIterator.reset();
        temp2 = rightIterator.next();
    }

    @Override
    public Object[] next() {
        Object[] temp1 = null;
        if (current_group_iterator != null)
            if (current_group_iterator.hasNext())
                temp1 = current_group_iterator.next();
        if (temp1 == null) {
            do {
                if (temp2 == null) {
                    return null;
                }
                if (map.containsKey(temp2[rightIndex])) {
                    current_group = temp2[rightIndex];
                    current_group_iterator = map.get(current_group).iterator();
                    temp1 = current_group_iterator.next();

                }
                if (temp1 == null) {
                    temp2 = rightIterator.next();
                }
            } while (temp1 == null);
        }
        Object[] output = create_row(temp2, temp1, size);
        if (!current_group_iterator.hasNext())
            temp2 = rightIterator.next();
        return output;
    }
    @Override
    public Table getTable() {
        return this.table;
    }
}