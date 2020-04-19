package com.database.aggregators;

import com.database.Evaluator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.LinkedHashMap;

public abstract class Aggregator {

    protected final Expression expression;
    protected Evaluator evaluator;
    public PrimitiveValue output;


    public Aggregator(Expression e, LinkedHashMap<String, Integer> schema) {
        evaluator = new Evaluator(schema);
        expression = e;
    }

    public static Aggregator get_agg(Function f, LinkedHashMap<String, Integer> schema) {
        String fname = f.getName();
        if (fname.contains("SUM")) {
            return new Sum(f.getParameters().getExpressions().get(0), schema);
        } else if (fname.contains("MAX")) {
            return new Max_Min(f.getParameters().getExpressions().get(0), 1, schema);
        } else if (fname.contains("MIN")) {
            return new Max_Min(f.getParameters().getExpressions().get(0), 0, schema);
        } else if (fname.contains("AVG")) {
            return new Average(f.getParameters().getExpressions().get(0), schema);
        } else if (fname.contains("COUNT")) {
            if (f.isAllColumns()) {
                return new Count(null, schema);
            }
            return new Count(f.getParameters().getExpressions().get(0), schema);
        }
        return null;
    }

    public abstract PrimitiveValue get_results(Object[] row);
}
