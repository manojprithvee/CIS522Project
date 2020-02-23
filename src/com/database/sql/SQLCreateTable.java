package com.database.sql;

import com.database.Global;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


public class SQLCreateTable {
    private CreateTable sql;

    public SQLCreateTable(CreateTable stmt) {
        this.sql = stmt;
    }

    public void getResult() throws Exception {
        String tableName = sql.getTable().getName().toUpperCase();
        LinkedHashMap<String, Integer> cols = new LinkedHashMap<String, Integer>();
        ArrayList<String> dataType = new ArrayList<String>();
        if (Global.tables != null && !Global.tables.containsKey(tableName)) {
            List<ColumnDefinition> list = sql.getColumnDefinitions();
            for (int i = 0; i < list.size(); i++) {
                ColumnDefinition temp = list.get(i);
                cols.put(tableName + "." + temp.getColumnName(), i);
                dataType.add(temp.getColDataType().toString());
            }
            Global.tables.put(tableName, cols);
            Global.tableSchema.put(tableName, dataType);
        }

    }
}
