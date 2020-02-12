package com.database.sql;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.Arrays;

public class SQLSelect {
    private Select sql;
    public SQLSelect(Select stmt) {
        this.sql = stmt;
    }
    public String getResult(){
        PlainSelect select = (PlainSelect) sql.getSelectBody();
        return(Arrays.toString(new String[]{select.getFromItem().toString(), select.getSelectItems().toString()}));
    }
}
