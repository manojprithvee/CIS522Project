package com.database.sql;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;


public class Row_Compare implements Comparator<Object[]> {
    private final LinkedHashMap<String, Integer> schema;
    OrderByElement orderByElement;
    Table table;

    public Row_Compare(OrderByElement orderByElement, Table table, LinkedHashMap<String, Integer> schema) {
        this.orderByElement = orderByElement;
        this.table = table;
        this.schema = schema;
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
        Evaluator eval1 = new Evaluator(schema, o1);
        PrimitiveValue left = eval1.eval((Column) orderByElement.getExpression());
        eval1.setTuple(o2);
        PrimitiveValue right = eval1.eval((Column) orderByElement.getExpression());
        int sortDirection = (orderByElement.isAsc()) ? 1 : -1;
        try {
            if (left instanceof StringValue) return left.toString().compareTo(right.toString()) * sortDirection;
            if (left instanceof DoubleValue) return (int) ((left.toDouble() - right.toDouble()) * sortDirection);
            if (left instanceof LongValue) return (int) ((left.toLong() - right.toLong()) * sortDirection);
            if (left instanceof DateValue) {
                SimpleDateFormat data_format = new SimpleDateFormat("yyyy-mm-dd");
                Date left_date = data_format.parse(String.valueOf(left));
                Date right_date = data_format.parse(String.valueOf(right));
                return ((left_date.compareTo(right_date)) * sortDirection);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
