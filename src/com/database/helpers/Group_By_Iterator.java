package com.database.helpers;

import com.database.aggregators.Aggregator;
import com.database.sql.Evaluator;
import com.database.sql.Storage;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.*;


public class Group_By_Iterator implements DB_Iterator {
    private final LinkedHashMap<String, Integer> inSchema;
    private final ArrayList<Expression> outExpressions;
    private final DB_Iterator iterator;
    private final Evaluator evaluator;
    private HashMap<List<Object>, ArrayList<Storage>> buffer;

    public Group_By_Iterator(DB_Iterator iterator,
                             ArrayList<Expression> outExpressions,
                             LinkedHashMap<String, Integer> inSchema) {
        this.iterator = iterator;
        this.outExpressions = outExpressions;
        this.inSchema = inSchema;
        evaluator = new Evaluator(inSchema);
        reset();
    }

    @Override
    public Object[] next() {
        if (buffer == null) return null;
        if (buffer.isEmpty()) {
            buffer = null;
            return null;
        } else {
            List<Object> row = buffer.keySet().iterator().next();
            ArrayList<Storage> pairs = buffer.get(row);
            buffer.remove(row);

            for (Storage pair : pairs) {
                row.set(pair.id, pair.function.output);
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
        if (buffer != null) return;
        buffer = new HashMap<>();
        Object[] inRow = this.iterator.next();
        while (inRow != null) {
            List<Object> outRow = Arrays.asList(new Object[outExpressions.size()]);
            ArrayList<Integer> indexes = new ArrayList<>();
            evaluator.setTuple(inRow);

            for (int i = 0; i < outExpressions.size(); i++) {
                if (outExpressions.get(i) instanceof Column) {
                    try {
                        outRow.set(i, evaluator.eval(outExpressions.get(i)));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    indexes.add(i);
                }
            }
            if (buffer.containsKey(outRow)) {
                for (Storage pair : buffer.get(outRow))
                    pair.function.get_results(inRow);
                inRow = this.iterator.next();
                continue;
            }

            ArrayList<Storage> pairs = new ArrayList<Storage>();

            for (Integer i : indexes) {
                Aggregator aggregate = Aggregator.get_agg((Function) outExpressions.get(i), inSchema);
                aggregate.get_results(inRow);
                Storage pair = new Storage(i, aggregate);
                pairs.add(pair);
            }

            buffer.put(outRow, pairs);
            inRow = this.iterator.next();
        }
    }
}
