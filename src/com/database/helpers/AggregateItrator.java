package com.database.helpers;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class AggregateItrator implements ItratorImp {
    ItratorImp oper;
    LinkedHashMap<String, Integer> schema;
    String fname;
    ArrayList<Function> functions;
    Table table;

    public AggregateItrator(ItratorImp oper, LinkedHashMap<String, Integer> stringIntegerLinkedHashMap, ArrayList<Function> functions, Table table) {
        this.oper = oper;
        this.schema = stringIntegerLinkedHashMap;
        this.functions = functions;
        this.table = table;
    }

    @Override
    public void reset() {
        oper.reset();
    }

    @Override
    public Object[] next() {
        Object[] obj = new Object[functions.size()];
        for (int i = 0; i < functions.size(); i++) {
            String fname = functions.get(i).getName();
            Object l = computeCount();
            if (l == null)
                return null;
            obj[i] = l;
        }
        return obj;
    }

    @Override
    public Table getTable() {
        return table;
    }

    private Object computeCount() {
        Object[] tuple = null;
        tuple = oper.next();
        Integer count = 0;
        if (tuple == null)
            return null;

        do {
            count++;
            tuple = oper.next();

        } while (tuple != null);


        return new LongValue(count.toString());
    }
}
