package com.database.helpers;


import com.database.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.HashMap;


public class SelectionItrator implements ItratorImp {

    ItratorImp op;
    HashMap<String, Integer> schema;
    Expression condition;

    public SelectionItrator(ItratorImp input, HashMap<String, Integer> schema, Expression condition) {

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
        Object[] tuple = op.next();
        Evaluator eval = new Evaluator(schema, tuple);
        while (tuple != null) {
            try {
                if (((BooleanValue) eval.eval(condition)).getValue()) {
                    return tuple;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Exception occured in SelectionOperator.readOneTuple()");
            }
            tuple = op.next();
            eval.setTuple(tuple);
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
