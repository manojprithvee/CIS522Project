package com.database;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;


public class Evaluator extends Eval {

    private HashMap<String, Integer> schema;
    private Object[] tuple;

    public Evaluator(HashMap<String, Integer> table, Object[] tuple) {
        this.schema = table;
        this.tuple = tuple;
    }

    public PrimitiveValue eval(Column c) {
        String t = "";
        int columnID = 0;
        if (c.getTable() != null && c.getTable().getName() != null) {
            t = c.getTable().getName();
            if (schema.containsKey(t + "." + c.getColumnName())) {
                columnID = schema.get(t + "." + c.getColumnName());
            } else {
                for (String key : schema.keySet()) {
                    String x = key.substring(key.indexOf(".") + 1);
                    if (x.equals(c.getTable() + "." + c.getColumnName())) {
                        columnID = schema.get(key);
                    }
                }
            }
            return (PrimitiveValue) tuple[columnID];
        } else {
            if (Global.alias != null && Global.alias.containsKey(c.getColumnName())) {
                if (schema.containsKey(Global.alias.get(c.getColumnName()).toString()))
                    columnID = schema.get(Global.alias.get(c.getColumnName()).toString());
                else if (schema.containsKey(c.getColumnName())) {

                    columnID = schema.get(c.getColumnName());
                } else {
                    for (String key : schema.keySet()) {
                        String x = key.substring(key.indexOf(".") + 1);
                        if (x.equals(c.getColumnName())) {
                            columnID = schema.get(key);
                        }
                    }
                }
                return (PrimitiveValue) tuple[columnID];
            } else {
                for (String key : schema.keySet()) {
                    String x = key.substring(key.indexOf(".") + 1);
                    if (x.equals(c.getColumnName())) {
                        columnID = schema.get(key);
                    }
                }
            }
            return (PrimitiveValue) tuple[columnID];
        }
    }
}
