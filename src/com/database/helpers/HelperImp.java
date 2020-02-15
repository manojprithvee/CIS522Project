package com.database.helpers;

import net.sf.jsqlparser.schema.Table;


public interface HelperImp {

    void reset();

    Object[] read();

    Table getTable();
}
