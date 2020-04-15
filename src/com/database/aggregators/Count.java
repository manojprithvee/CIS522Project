package com.database.aggregators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Count extends Aggregator {
    PrimitiveValue count = new LongValue(0);

    public Count(Expression expression, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
    }

    public PrimitiveValue get_results(Object[] row) {
        if (row != null) {
            evaluator.setTuple(row);
            if (count == null) return new LongValue(1);
            try {
                output = evaluator.eval(new Addition(count, new LongValue(1)));
                count = evaluator.eval(new Addition(count, new LongValue(1)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return count;
        }
        return new LongValue(0);
    }
}
