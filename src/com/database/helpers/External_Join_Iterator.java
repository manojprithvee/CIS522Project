package com.database.helpers;

import com.database.RAtree.RA_Tree;
import com.database.Shared_Variables;
import net.sf.jsqlparser.schema.Table;

import java.util.*;

public class External_Join_Iterator implements DB_Iterator {
    private final LinkedList<Object[]> buffer;
    private Iterator<Object[]> bufferintrator;


    public External_Join_Iterator(RA_Tree left, RA_Tree right,
                                  int leftindex, int rightindex) {
        DB_Iterator leftIterator = left.get_iterator();
        DB_Iterator rightIterator = right.get_iterator();
        Object[] row = leftIterator.next();
        Map<Object, ArrayList<Object[]>> leftmap = new HashMap<>();
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
            row = leftIterator.next();
        }
        row = rightIterator.next();

        Map<Object, ArrayList<Object[]>> rightmap = new HashMap<>();
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
            row = rightIterator.next();
        }

        leftIterator = null;
        rightIterator = null;

        leftmap.keySet().retainAll(rightmap.keySet());
        rightmap.keySet().retainAll(leftmap.keySet());
        Set<Object> setkeys = leftmap.keySet();
        buffer = new LinkedList<>();
        ArrayList<Object> keys = new ArrayList<Object>(setkeys);
        for (Object key : keys) {
            Iterator<Object[]> left_list = leftmap.get(key).iterator();
            Iterator<Object[]> right_list = rightmap.get(key).iterator();
            while (left_list.hasNext()) {
                Object[] leftrow = left_list.next();
                while (right_list.hasNext()) {
                    Object[] rightrow = right_list.next();
                    buffer.add(Shared_Variables.create_row(leftrow, rightrow));
                }
                right_list = rightmap.get(key).iterator();
            }
            leftmap.remove(key);
            rightmap.remove(key);
        }
        leftmap = null;
        rightmap = null;

        bufferintrator = buffer.iterator();
    }

    public Object[] create_row(Object[] left, Object[] right) {
        if (left == null || right == null) return null;
        Object[] new_row = new Object[left.length + right.length];
        System.arraycopy(left, 0, new_row, 0, left.length);
        System.arraycopy(right, 0, new_row, left.length, right.length);
        return new_row;
    }

    @Override
    public void reset() {
        bufferintrator = buffer.iterator();
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