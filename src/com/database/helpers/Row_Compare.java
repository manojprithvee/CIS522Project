package com.database.helpers;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;


public class Row_Compare implements Comparator<Object[]> {
    private final LinkedHashMap<String, Integer> schema;
    List<OrderByElement> orderByElements;

    public Row_Compare(List<OrderByElement> orderByElement, LinkedHashMap<String, Integer> schema) {
        this.orderByElements = orderByElement;
        this.schema = schema;
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
        for (OrderByElement orderByElement : orderByElements) {
            Column column = (Column) orderByElement.getExpression();
            Integer index = schema.get(column.getWholeColumnName());
            PrimitiveValue left = (PrimitiveValue) o1[index];
            PrimitiveValue right = (PrimitiveValue) o2[index];
            int sortDirection = (orderByElement.isAsc()) ? 1 : -1;
            try {
                if (left instanceof StringValue) return left.toString().compareTo(right.toString()) * sortDirection;
                else if (left instanceof DoubleValue)
                    return (int) ((left.toDouble() - right.toDouble()) * sortDirection);
                else if (left instanceof LongValue) return (int) ((left.toLong() - right.toLong()) * sortDirection);
                else if (left instanceof DateValue)
                    return (int) ((((DateValue) left).getValue().getTime() - ((DateValue) right).getValue().getTime()) * sortDirection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 0;
    }
}
