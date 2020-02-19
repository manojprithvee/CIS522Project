package com.database.helpers;

import net.sf.jsqlparser.schema.Table;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class DistinctHelper implements HelperImp {

    private final HelperImp itrator;
    private HashSet<List<Object>> buffer = new HashSet<>();

    public DistinctHelper(HelperImp itrator) {
        this.itrator = itrator;
    }

    @Override
    public void reset() {

    }


    @Override
    public Object[] read() {
        Object[] nextRow = itrator.read();
        if (nextRow == null) {
            return null;
        }
        if (!buffer.contains(Arrays.asList(nextRow))) {
            buffer.add(Arrays.asList(nextRow));
            return nextRow;
        } else {
            return read();
        }
    }

    @Override
    public Table getTable() {
        return null;
    }
}
