package com.database.sql;

import com.database.Shared_Variables;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;


public class Evaluator extends Eval {

    private final HashMap<String, Integer> structure;
    private Object[] row;

    public Evaluator(HashMap<String, Integer> table, Object[] row) {
        this.structure = table;
        this.row = row;
    }

    public Evaluator(LinkedHashMap<String, Integer> schema) {
        this.structure = schema;
    }

    public void setTuple(Object[] row) {
        this.row = row;
    }

    public PrimitiveValue eval(Column main_column) {
        String table;
        int id = 0;
        if ((main_column.getTable() != null) && (main_column.getTable().getName() != null)) {
            table = main_column.getTable().getName();
            if (!structure.containsKey(table + "." + main_column.getColumnName()))
                id = columnchange(id, main_column.getTable() + "." + main_column.getColumnName());
            else id = structure.get(table + "." + main_column.getColumnName());
        } else if (!Shared_Variables.rename.containsKey(main_column.getColumnName()))
            id = columnchange(id, main_column.getColumnName());
        else if (structure.containsKey(main_column.getColumnName())) id = structure.get(main_column.getColumnName());
        else if (structure.containsKey(Shared_Variables.rename.get(main_column.getColumnName()).toString()))
            id = structure.get(Shared_Variables.rename.get(main_column.getColumnName()).toString());
        else id = columnchange(id, main_column.getColumnName());
        return (PrimitiveValue) row[id];
    }

    public int columnchange(int id, String columnName) {
        for (Iterator<String> iterator = structure.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(columnName)) id = structure.get(column);
        }
        return id;
    }
}
