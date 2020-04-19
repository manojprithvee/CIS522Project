package com.database.aggregators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.LinkedHashMap;

public class Count extends Aggregator {

    Long count = 0L;

    public Count(Expression expression, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
        output = new LongValue(0L);
    }

    public PrimitiveValue get_results(Object[] row) {
        if (row != null) {
            if (count == null) {
                count = 1L;
            } else {
                count = count + 1L;
            }
        }
        output = new LongValue(count);
        return output;
    }
}
