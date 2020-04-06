package com.database.helpers;

import com.database.Shared_Variables;
import com.database.aggregators.Aggregator;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

public class Aggregate_Iterator implements DB_Iterator {
    final DB_Iterator oper;
    final ArrayList<Function> aggregator;
    final Table table;
    boolean output = true;

    public Aggregate_Iterator(DB_Iterator oper, ArrayList<Function> aggregator, Table table) {
        this.oper = oper;
        this.aggregator = aggregator;
        this.table = table;
    }

    @Override
    public void reset() {
        oper.reset();
    }

    @Override
    public Object[] next() {

        Object[] result = new Object[aggregator.size()];
        int count = 0;
        for (Function function : aggregator) {
            if (function instanceof Function) {
                Aggregator abc = Aggregator.get_agg(function, Shared_Variables.current_schema);
                PrimitiveValue output = null;
                Object[] tuple = oper.next();
                while (tuple != null) {
                    output = abc.get_results(tuple);
                    tuple = oper.next();
                }
                result[count] = output;
                count++;
                oper.reset();
            }
        }
        if (output) {
            output = false;
            return result;
        } else
            return null;
    }

    @Override
    public Table getTable() {
        return table;
    }
}
