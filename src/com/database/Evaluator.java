package com.database;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;


public class Evaluator extends Eval {
    public Evaluator() {

    }

    public PrimitiveValue eval(Column c) throws SQLException {
        return null;
    }
}
