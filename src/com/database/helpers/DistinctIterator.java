package com.database.helpers;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class DistinctIterator implements DB_Iterator {

    private final DB_Iterator DB_Iterator;
    private final HashSet<List<Object>> buffer = new HashSet<>();

    public DistinctIterator(DB_Iterator DB_Iterator) {
        this.DB_Iterator = DB_Iterator;
    }

    @Override
    public void reset() {

    }

    @Override
    public Object[] next() throws SQLException {
        Object[] nextRow = DB_Iterator.next();
        if (nextRow == null) return null;
        if (buffer.contains(Arrays.asList(nextRow))) {
            return next();
        } else {
            buffer.add(Arrays.asList(nextRow));
            return nextRow;
        }
    }

    @Override
    public Table getTable() {
        return null;
    }
}
