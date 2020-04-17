package com.database.helpers;

import com.database.aggregators.Aggregator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class Aggregate_Iterator implements DB_Iterator {
    final DB_Iterator oper;
    final ArrayList<Expression> aggregator;
    private final LinkedHashMap<String, Integer> lastschema;
    boolean output = true;

    public Aggregate_Iterator(DB_Iterator oper, ArrayList<Expression> aggregator, LinkedHashMap<String, Integer> new_schema, LinkedHashMap<String, Integer> lastschema) {
        this.oper = oper;
        this.aggregator = aggregator;
        this.lastschema = lastschema;
    }

    @Override
    public void reset() {
        oper.reset();
        output = true;
    }

    @Override
    public Object[] next() {

        Object[] result = new Object[aggregator.size()];
        int count = 0;
        for (Expression function : aggregator) {
            if (function instanceof Function) {
                Aggregator abc = Aggregator.get_agg((Function) function, lastschema);
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
        return null;
    }
}
