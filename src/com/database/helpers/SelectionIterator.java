package com.database.helpers;


import com.database.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.HashMap;


public class SelectionIterator implements DB_Iterator {

    final DB_Iterator op;
    final HashMap<String, Integer> schema;
    final Expression condition;

    public SelectionIterator(DB_Iterator input, HashMap<String, Integer> schema, Expression condition) {

        this.op = input;
        this.schema = schema;
        this.condition = condition;

    }

    @Override
    public void reset() {
        op.reset();
    }

    @Override
    public Object[] next() {
        Object[] row = op.next();
        Evaluator eval = new Evaluator(schema, row);
        while (row != null) {
            try {
                if (((BooleanValue) eval.eval(condition)).getValue()) {
                    return row;
                }
            } catch (SQLException e) {
                System.out.println("Error");
            }
            row = op.next();
            eval.setTuple(row);
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
