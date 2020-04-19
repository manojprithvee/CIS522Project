package com.database.helpers;

import com.database.RAtree.RA_Tree;
import net.sf.jsqlparser.schema.Table;

import java.util.*;

public class External_Join_Iterator implements DB_Iterator {
    private final DB_Iterator rightIterator;
    private final Map<Object, ArrayList<Object[]>> leftmap, rightmap;
    private final ArrayList<Object> keys;
    private final Iterator<Object[]> bufferintrator;
    DB_Iterator leftIterator;
    private Iterator<Object[]> left_list;
    private Iterator<Object[]> right_list;
    private Object key;


    public External_Join_Iterator(RA_Tree left, RA_Tree right,
                                  int leftindex, int rightindex) {
        this.leftIterator = left.get_iterator();
        this.rightIterator = right.get_iterator();
        Object[] row = this.leftIterator.next();
        leftmap = new HashMap<>();
        while (row != null) {
            Object key = row[leftindex];
            ArrayList<Object[]> abc;
            if (leftmap.containsKey(key)) {
                abc = leftmap.get(key);
            } else {
                abc = new ArrayList<>();
            }
            abc.add(row);
            leftmap.put(key, abc);
            row = this.leftIterator.next();
        }
//        System.out.println(leftmap);
        row = this.rightIterator.next();
        rightmap = new HashMap<>();
        while (row != null) {
            Object key = row[rightindex];
            ArrayList<Object[]> abc;
            if (rightmap.containsKey(key)) {
                abc = rightmap.get(key);
            } else {
                abc = new ArrayList<>();
            }
            abc.add(row);
            rightmap.put(key, abc);
            row = this.rightIterator.next();
        }
//        System.out.println(rightmap);
        leftmap.keySet().retainAll(rightmap.keySet());
        rightmap.keySet().retainAll(leftmap.keySet());
        Set<Object> setkeys = leftmap.keySet();
        ArrayList<Object[]> buffer = new ArrayList<>();
        keys = new ArrayList<Object>(setkeys);
        for (Object key : keys) {
            left_list = leftmap.get(key).iterator();
            right_list = rightmap.get(key).iterator();
            while (left_list.hasNext()) {
                Object[] leftrow = left_list.next();
                while (right_list.hasNext()) {
                    Object[] rightrow = right_list.next();
                    buffer.add(create_row(leftrow, rightrow));
                }
                right_list = rightmap.get(key).iterator();
            }
            leftmap.remove(key);
            rightmap.remove(key);
        }
        bufferintrator = buffer.iterator();
    }

    public Object[] create_row(Object[] left, Object[] right) {

        int index = 0;
        if (left == null || right == null) return null;
        Object[] new_row = new Object[left.length + right.length];
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
    }

    @Override
    public Object[] next() {
        if (bufferintrator.hasNext())
            return bufferintrator.next();
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}