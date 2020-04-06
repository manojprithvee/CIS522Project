package com.database.helpers;

import com.database.Shared_Variables;
import com.database.aggregators.Aggregator;
import com.database.sql.Evaluator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Group_By_Iterator implements DB_Iterator {

    private final DB_Iterator oper;
    private final Table table;
    private final List<Column> groupByColumnReferences;
    private final ArrayList<Object[]> buffer;
    private final ArrayList<Integer> indexes;
    private final Map<List<Object>, List<Object[]>> bufferHash;
    private final Iterator<List<Object>> bufferHashitrator;
    private final LinkedHashMap<String, Integer> newschema;
    private final List<SelectItem> list;

    {
    }

    public Group_By_Iterator

    public Group_By_Iterator(DB_Iterator oper, ArrayList<Expression> list, List<Column> groupByColumnReferences) {

        this.oper = oper;
        this.table = table;
        this.list = list;
        this.groupByColumnReferences = groupByColumnReferences;
        buffer = new ArrayList<Object[]>();
//        schema = Shared_Variables.list_tables.get(table.getAlias());

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
            if (Shared_Variables.current_schema.get(column.getWholeColumnName()) != null) {
                column_name = column.getWholeColumnName();
                index = Shared_Variables.current_schema.get(column.getWholeColumnName());
            } else if (Shared_Variables.current_schema.get(table.getAlias() + "." + column.getWholeColumnName()) != null) {
                column_name = table.getAlias() + "." + column.getWholeColumnName();
                index = Shared_Variables.current_schema.get(table.getAlias() + "." + column.getWholeColumnName());
            } else {
                for (var columnname : Shared_Variables.current_schema.keySet()) {
                    String x = columnname.substring(columnname.indexOf(".") + 1);
                    if (x.equals(column.getColumnName())) {
                        column_name = columnname;
                        index = Shared_Variables.current_schema.get(columnname);
                    }
                }
            }
            newschema.put(column_name, count);
            count += 1;
            indexes.add(index);
        }

//        finalschema = new LinkedHashMap<>();
//        count = 0;
//        for (SelectItem f : list) {
//            if (((SelectExpressionItem) f).getExpression() instanceof Function) {
//                Function function = (Function) ((SelectExpressionItem) f).getExpression();
//                if (((SelectExpressionItem) f).getAlias() != null) {
//                    finalschema.put(table.getName() + "." + ((SelectExpressionItem) f).getAlias(), count);
//                } else {
//                    finalschema.put(table.getName() + "." + function.getName(), count);
//                }
//            } else {
//                Column main_column = (Column) ((SelectExpressionItem) f).getExpression();
//                int id = 0;
//                HashMap<Integer, String> names;
//                String finalcolumn = "";
//                if (!Shared_Variables.rename.containsKey(main_column.getColumnName())) {
//                    names = columnchange(id, main_column.getColumnName());
//                    finalcolumn = (String) names.values().toArray()[0];
//                    id = (int) names.keySet().toArray()[0];
//                } else if (newschema.containsKey(main_column.getColumnName())) {
//                    finalcolumn = main_column.getColumnName();
//                    id = newschema.get(main_column.getColumnName());
//                } else if (newschema.containsKey(Shared_Variables.rename.get(main_column.getColumnName()).toString())) {
//                    finalcolumn = Shared_Variables.rename.get(main_column.getColumnName()).toString();
//                    id = newschema.get(Shared_Variables.rename.get(main_column.getColumnName()).toString());
//                } else {
//                    names = columnchange(id, main_column.getColumnName());
//                    finalcolumn = (String) names.values().toArray()[0];
//                    id = (int) names.keySet().toArray()[0];
//                }
//                finalschema.put(finalcolumn, count);
//            }
//            count++;
//        }

//        Shared_Variables.current_schema = finalschema;

        bufferHash = this.buffer.stream().collect(Collectors.groupingBy(w -> grouping(w)));
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

            for (Expression f : inExpressions) {
                if (((SelectExpressionItem) f).getExpression() instanceof Function) {
                    Function function = (Function) ((SelectExpressionItem) f).getExpression();
                    Aggregator abc = Aggregator.get_agg(function, Shared_Variables.current_schema);
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
