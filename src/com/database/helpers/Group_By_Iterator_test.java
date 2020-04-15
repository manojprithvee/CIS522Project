package com.database.helpers;

import com.database.aggregators.Aggregator;
import com.database.sql.Evaluator;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Group_By_Iterator_test implements DB_Iterator {

    private final ArrayList<Integer> indexes;
    private final Map<List<Object>, List<Object[]>> bufferHash;
    private final Iterator<List<Object>> bufferHashitrator;
    private final LinkedHashMap<String, Integer> newschema;
    private final List<SelectItem> list;
    private final LinkedHashMap<String, Integer> new_schema;


    public Group_By_Iterator_test(DB_Iterator oper, List<SelectItem> list, List<Column> groupByColumnReferences, LinkedHashMap<String, Integer> new_schema, LinkedHashMap<String, Integer> lastschema) {

        this.list = list;
        this.new_schema = new_schema;
        ArrayList<Object[]> buffer = new ArrayList<Object[]>();

        Object[] row = oper.next();
        while (row != null) {
            buffer.add(row);
            row = oper.next();
        }

        this.indexes = new ArrayList<Integer>();
        newschema = new LinkedHashMap<>();
        int count = 0;

        //getting all the index for the group by elements in the table
        for (Column column : groupByColumnReferences) {
            int index = 0;
            String column_name = "";
            if (lastschema.get(column.getWholeColumnName()) != null) {
                column_name = column.getWholeColumnName();
                index = lastschema.get(column.getWholeColumnName());
            } else {
                for (var columnname : lastschema.keySet()) {
                    String x = columnname.substring(columnname.indexOf(".") + 1);
                    if (x.equals(column.getColumnName())) {
                        column_name = columnname;
                        index = lastschema.get(columnname);
                    }
                }
            }
            newschema.put(column_name, count);
            count += 1;
            indexes.add(index);
        }

        bufferHash = buffer.stream().collect(Collectors.groupingBy(w -> grouping(w)));
        buffer = null;
        ArrayList<Double> function_results = new ArrayList<>();
        bufferHashitrator = bufferHash.keySet().iterator();
    }

    public List<Object> grouping(Object[] w) {
        ArrayList<Object> output = new ArrayList<Object>();
        for (Integer index : indexes) {
            output.add(w[index]);
        }
        return output;
    }

    @Override
    public void reset() {

    }

    @Override
    public Object[] next() {
        List<Object> group;

        if (bufferHashitrator.hasNext()) {
            group = bufferHashitrator.next();
            Object[] result = new Object[list.size()];
            int count = 0;

            for (SelectItem f : list) {
                if (((SelectExpressionItem) f).getExpression() instanceof Function) {
                    Function function = (Function) ((SelectExpressionItem) f).getExpression();
                    Aggregator abc = Aggregator.get_agg(function, new_schema);
                    PrimitiveValue output = null;
                    for (var tuple : bufferHash.get(group)) {
                        output = abc.get_results(tuple);
                    }
                    result[count] = output;
                } else {
                    Evaluator eval = new Evaluator(newschema, group.toArray());
                    try {
                        result[count] = eval.eval(((SelectExpressionItem) f).getExpression());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                count++;
            }
            return result;
        } else
            return null;
    }


    @Override
    public Table getTable() {
        return null;
    }
}
