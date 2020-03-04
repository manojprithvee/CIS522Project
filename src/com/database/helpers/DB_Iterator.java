package com.database.helpers;

        import net.sf.jsqlparser.schema.Table;


public interface DB_Iterator {

    void reset();

    Object[] next();

    Table getTable();
}
