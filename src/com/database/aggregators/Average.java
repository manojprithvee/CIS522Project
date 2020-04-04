package com.database.aggregators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Average extends Aggregator {


    private final LinkedHashMap<String, Integer> schema;

    public Average(Expression expression, LinkedHashMap<String, Integer> schema) {
        super(expression, schema);
        this.schema = schema;
    }

    public PrimitiveValue get_results(Object[] row) throws SQLException {
        evaluator.setTuple(row);
        return evaluator.eval(new Division(new Sum(expression, schema).get_results(row), new Count(expression, schema).get_results(row)));
    }
}
