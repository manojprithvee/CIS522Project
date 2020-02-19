package com.database.helpers;

import net.sf.jsqlparser.schema.Table;


public interface ItratorImp {

    void reset();

    Object[] next();

    Table getTable();
}
