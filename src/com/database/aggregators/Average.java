package com.database.aggregators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Average extends Aggregator {


    private final LinkedHashMap<String, Integer> schema;
    private Sum sum;
    private Count count;

    public Average(Expression expression, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
        this.schema = schema;
        sum = new Sum(expression, schema);
        count = new Count(expression, schema);
    }

    public PrimitiveValue get_results(Object[] row) {

        evaluator.setTuple(row);
        try {
            output = evaluator.eval(new Division(sum.get_results(row), count.get_results(row)));
            return output;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}

