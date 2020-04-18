package com.database.aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Average extends Aggregator {


    private final LinkedHashMap<String, Integer> schema;

    public Average(Expression expression, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
        this.schema = schema;
    }

    public PrimitiveValue get_results(Object[] row) {
        evaluator.setTuple(row);
        try {
            PrimitiveValue sum = new Sum(expression, schema).get_results(row);
            PrimitiveValue count = new Count(expression, schema).get_results(row);
            if (sum instanceof DoubleValue) {
                output = new DoubleValue(((DoubleValue) sum).getValue() / count.toLong());
            } else {
                output = new LongValue(((LongValue) sum).getValue() / count.toLong());
            }
            return output;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
