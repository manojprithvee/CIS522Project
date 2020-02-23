package com.database.helpers;

import net.sf.jsqlparser.schema.Table;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class DistinctItrator implements ItratorImp {

    private final ItratorImp itrator;
    private HashSet<List<Object>> buffer = new HashSet<>();
    public DistinctItrator(ItratorImp itrator) {
        this.itrator = itrator;
    }
    @Override
    public void reset() {

    }

    @Override
    public Object[] next() {
        Object[] nextRow = itrator.next();
        if (nextRow == null) {
            return null;
        }
        if (!buffer.contains(Arrays.asList(nextRow))) {
            buffer.add(Arrays.asList(nextRow));
            return nextRow;
        } else {
            return next();
        }
    }

    @Override
    public Table getTable() {
        return null;
    }
}
