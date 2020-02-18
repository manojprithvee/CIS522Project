package com.database.helpers;


import com.database.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.HashMap;


public class SelectionHelper implements HelperImp {

    HelperImp op;
    HashMap<String, Integer> schema;
    Expression condition;

    public SelectionHelper(HelperImp input, HashMap<String, Integer> schema, Expression condition) {

        this.op = input;
        this.schema = schema;
        this.condition = condition;

    }

    @Override
    public void reset() {
        op.reset();
    }


    @Override
    public Object[] read() {
        Object[] tuple = null;
        tuple = op.read();
        if (tuple == null) {
            return null;
        }
        Evaluator eval = new Evaluator(schema, tuple);
        try {
            if (((BooleanValue) eval.eval(condition)).getValue()) {
                return tuple;
            } else {
                tuple = read();
                return tuple;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Exception occured in SelectionOperator.readOneTuple()");
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
