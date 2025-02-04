package com.database.helpers;

import com.database.Evaluator;
import com.database.aggregators.Aggregator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.*;


public class Group_By_Iterator implements DB_Iterator {
    private final LinkedHashMap<String, Integer> inSchema;
    private final ArrayList<Expression> newprojection;
    private final DB_Iterator left;
    private final Evaluator evaluator;
    private HashMap<List<Object>, HashMap<Integer, Aggregator>> groups;

    public Group_By_Iterator(DB_Iterator left,
                             ArrayList<Expression> outExpressions,
                             LinkedHashMap<String, Integer> inSchema) {
        this.left = left;
        this.newprojection = outExpressions;
        this.inSchema = inSchema;
        evaluator = new Evaluator(inSchema);
        reset();
    }

    @Override
    public Object[] next() {
        if (groups == null) return null;
        if (groups.isEmpty()) {
            groups = null;
            return null;
        } else {
            List<Object> row = groups.keySet().iterator().next();
            HashMap<Integer, Aggregator> pairs = groups.get(row);
            groups.remove(row);
            for (Integer pair : pairs.keySet()) {
                row.set(pair, pairs.get(pair).output);
            }
            return row.toArray();
        }
    }

    @Override
    public Table getTable() {
        return null;
    }

    @Override
    public void reset() {
        if (groups != null) return;
        groups = new HashMap<>();
        Object[] leftrows = left.next();
        while (leftrows != null) {
            List<Object> rightrows = Arrays.asList(new Object[newprojection.size()]);
            ArrayList<Integer> indexes = new ArrayList<>();
            evaluator.setTuple(leftrows);
            for (int i = 0; i < newprojection.size(); i++) {
                if (newprojection.get(i) instanceof Column) {
                    try {
                        rightrows.set(i, evaluator.eval(newprojection.get(i)));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    indexes.add(i);
                }
            }
            if (groups.containsKey(rightrows)) {
                for (Integer group : groups.get(rightrows).keySet())
                    groups.get(rightrows).get(group).get_results(leftrows);
            } else {
                HashMap<Integer, Aggregator> rows = new HashMap<>();
                for (Integer i : indexes) {
                    Aggregator function = Aggregator.get_agg((Function) newprojection.get(i), inSchema);
                    if (function != null) {
                        function.get_results(leftrows);
                        rows.put(i, function);
                    }
                }
                groups.put(rightrows, rows);
            }
            leftrows = left.next();
        }
    }
}
