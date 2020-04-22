package com.database.aggregators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Sum extends Aggregator {
    Boolean isLong = null;

    public Sum(Expression expression, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
    }

    public PrimitiveValue get_results(Object[] row) {

        if (row != null) {
            evaluator.setTuple(row);

            if (output == null) {
                try {
                    output = evaluator.eval(expression);
                    return output;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            try {
                output = evaluator.eval(new Addition(expression, output));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return output;
        }
        return new LongValue(0);
    }
}
