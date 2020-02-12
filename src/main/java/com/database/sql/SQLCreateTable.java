package com.database.sql;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SQLCreateTable {
    private CreateTable sql;
    public SQLCreateTable(CreateTable stmt) {
        this.sql = stmt;
    }
    public String getResult(){
        return(sql.getColumnDefinitions().toString());
    }
}
