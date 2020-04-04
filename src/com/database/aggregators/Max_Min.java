package com.database.aggregators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Max_Min extends Aggregator {
    private final int i;
    private PrimitiveValue output;

    public Max_Min(Expression expression, int i, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
        this.i = i;
    }

    public PrimitiveValue get_results(Object[] row) throws SQLException {
        if (row != null) {
            evaluator.setTuple(row);
            PrimitiveValue data = evaluator.eval(expression);
            if (data == null) return data;
            if (i == 0) {
//              min
                if ((output == null) || (evaluator.eval(new MinorThan(data, output)).toBool())) {
                    output = data;
                    return data;
                }
                return output;
            } else {
//              max
                if ((output == null) || (evaluator.eval(new GreaterThan(data, output)).toBool())) {
                    output = data;
                    return data;
                }
                return output;
            }
        } else {
            return new LongValue(0);
        }
    }
}
