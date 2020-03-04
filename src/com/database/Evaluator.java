package com.database;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;


public class Evaluator extends Eval {

    private final HashMap<String, Integer> schema;
    private Object[] row;

    public Evaluator(HashMap<String, Integer> table, Object[] row) {
        this.schema = table;
        this.row = row;
    }

    public void setTuple(Object[] row) {
        this.row = row;
    }

    public PrimitiveValue eval(Column maincolumn) {
        String table;
        int id = 0;
        if (maincolumn.getTable() != null && maincolumn.getTable().getName() != null) {
            table = maincolumn.getTable().getName();
            if (schema.containsKey(table + "." + maincolumn.getColumnName())) {
                id = schema.get(table + "." + maincolumn.getColumnName());
            } else {
                for (String key : schema.keySet()) {
                    String x = key.substring(key.indexOf(".") + 1);
                    if (x.equals(maincolumn.getTable() + "." + maincolumn.getColumnName())) {
                        id = schema.get(key);
                    }
                }
            }
        } else {
            if (Global.rename.containsKey(maincolumn.getColumnName())) {
                if (schema.containsKey(Global.rename.get(maincolumn.getColumnName()).toString()))
                    id = schema.get(Global.rename.get(maincolumn.getColumnName()).toString());
                else if (schema.containsKey(maincolumn.getColumnName())) {
                    id = schema.get(maincolumn.getColumnName());
                } else {
                    for (String column : schema.keySet()) {
                        String x = column.substring(column.indexOf(".") + 1);
                        if (x.equals(maincolumn.getColumnName())) {
                            id = schema.get(column);
                        }
                    }
                }
            } else {
                for (String column : schema.keySet()) {
                    String x = column.substring(column.indexOf(".") + 1);
                    if (x.equals(maincolumn.getColumnName())) {
                        id = schema.get(column);
                    }
                }
            }

        }
        return (PrimitiveValue) row[id];
    }
}
