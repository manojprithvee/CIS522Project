package com.database.sql;

import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;


public class Row_Compare implements Comparator<Object[]> {
    OrderByElement orderByElement;
    Table table;

    public Row_Compare(OrderByElement orderByElement, Table table) {
        this.orderByElement = orderByElement;
        this.table = table;
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
        LinkedHashMap<String, Integer> schema = Shared_Variables.list_tables.get(table.getAlias());
        Column column = (Column) orderByElement.getExpression();
        int sortDirection = (orderByElement.isAsc()) ? 1 : -1;
        int index = 0;
        if (schema.get(column.getWholeColumnName()) != null) {
            index = schema.get(column.getWholeColumnName());
        } else if (schema.get(table.getAlias() + "." + column.getWholeColumnName()) != null) {
            index = schema.get(table.getAlias() + "." + column.getWholeColumnName());
        } else {
            for (var columnname : schema.keySet()) {
                String x = columnname.substring(columnname.indexOf(".") + 1);
                if (x.equals(column.getColumnName())) index = schema.get(columnname);
            }
        }
        PrimitiveValue left = (PrimitiveValue) o1[index];
        PrimitiveValue right = (PrimitiveValue) o2[index];
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
