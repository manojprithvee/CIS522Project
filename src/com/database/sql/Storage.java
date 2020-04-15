package com.database.sql;

import com.database.aggregators.Aggregator;

public class Storage {
    public final int id;
    public final Aggregator function;

    public Storage(int id, Aggregator function) {
        this.id = id;
        this.function = function;
    }
}
