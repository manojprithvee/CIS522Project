package com.database;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class Shared_Variables {
    public static final HashMap<String, ArrayList<String>> schema_store = new HashMap<>();
    public static final HashMap<String, LinkedHashMap<String, Integer>> list_tables = new HashMap<>();
    public static LinkedHashMap<String, Integer> current_schema = new LinkedHashMap<String, Integer>();
    public static int table = 0;
    public static File table_location = new File("data/");
    public static HashMap<String, Expression> rename = new HashMap<>();

    public static int get_index(Column main_column, HashMap<String, Integer> structure) {
        String table;
        int id = Integer.MAX_VALUE;
        if ((main_column.getTable() != null) && (main_column.getTable().getName() != null)) {
            table = main_column.getTable().getName();
            if (!structure.containsKey(table + "." + main_column.getColumnName()))
                id = columnchange(id, main_column.getTable() + "." + main_column.getColumnName(), structure);
            else id = structure.get(table + "." + main_column.getColumnName());
        } else if (!Shared_Variables.rename.containsKey(main_column.getColumnName()))
            id = columnchange(id, main_column.getColumnName(), structure);
        else if (structure.containsKey(main_column.getColumnName())) id = structure.get(main_column.getColumnName());
        else if (structure.containsKey(Shared_Variables.rename.get(main_column.getColumnName()).toString()))
            id = structure.get(Shared_Variables.rename.get(main_column.getColumnName()).toString());
        else id = columnchange(id, main_column.getColumnName(), structure);
        return id;
    }

    public static int columnchange(int id, String columnName, HashMap<String, Integer> structure) {
        for (Iterator<String> iterator = structure.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(columnName)) id = structure.get(column);
        }
        return id;
    }
}
