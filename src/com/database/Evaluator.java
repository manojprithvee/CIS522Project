package com.database;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;


public class Evaluator extends Eval {

    private final HashMap<String, Integer> structure;
    private Object[] row;

    public Evaluator(HashMap<String, Integer> table, Object[] row) {
        this.structure = table;
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
            if (!structure.containsKey(table + "." + maincolumn.getColumnName())) {
                for (String key : structure.keySet()) {
                    String x = key.substring(key.indexOf(".") + 1);
                    if (x.equals(maincolumn.getTable() + "." + maincolumn.getColumnName())) id = structure.get(key);
                }

            } else {
                id = structure.get(table + "." + maincolumn.getColumnName());
            }
        } else {
            if (!Global.rename.containsKey(maincolumn.getColumnName())) {
                for (String column : structure.keySet()) {
                    String x = column.substring(column.indexOf(".") + 1);
                    if (x.equals(maincolumn.getColumnName())) id = structure.get(column);
                }
            } else {
                if (structure.containsKey(maincolumn.getColumnName())) id = structure.get(maincolumn.getColumnName());
                else if (structure.containsKey(Global.rename.get(maincolumn.getColumnName()).toString()))
                    id = structure.get(Global.rename.get(maincolumn.getColumnName()).toString());
                else {
                    for (String column : structure.keySet()) {
                        String x = column.substring(column.indexOf(".") + 1);
                        if (x.equals(maincolumn.getColumnName())) id = structure.get(column);
                    }
                }
            }

        }
        return (PrimitiveValue) row[id];
    }
}
