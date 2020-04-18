package com.database.aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

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
            if (isLong == null) isLong = output instanceof LongValue;
            try {
                if (isLong) {
                    ((LongValue) output).setValue(((LongValue) output).getValue() + ((LongValue) evaluator.eval(expression)).getValue());
                } else {
                    ((DoubleValue) output).setValue(((DoubleValue) output).getValue() + ((DoubleValue) evaluator.eval(expression)).getValue());
                }
            } catch (PrimitiveValue.InvalidPrimitive throwables) {
                throwables.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return output;
        }
        return new LongValue(0);
    }
}
