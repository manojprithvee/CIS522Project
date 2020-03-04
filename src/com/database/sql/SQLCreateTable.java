package com.database.sql;

import com.database.Global;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


public class SQLCreateTable {
    private final CreateTable sql;

    public SQLCreateTable(CreateTable stmt) {
        this.sql = stmt;
    }

    public void getResult() {
        String tableName = sql.getTable().getName();
        LinkedHashMap<String, Integer> cols = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        if (!Global.list_tables.containsKey(tableName)) {
            List<ColumnDefinition> lists = sql.getColumnDefinitions();
            int i = 0;
            for (ColumnDefinition list : lists) {
                cols.put(tableName + "." + list.getColumnName(), i);
                dataType.add(list.getColDataType().toString());
                i++;
            }
            Global.list_tables.put(tableName, cols);
            Global.schema_store.put(tableName, dataType);
        }

    }
}
