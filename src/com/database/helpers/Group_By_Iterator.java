package com.database.helpers;

import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Group_By_Iterator implements DB_Iterator {

    private final Expression having;
    private final DB_Iterator oper;
    private final Table table;
    private final List<SelectItem> list;
    private final ArrayList<Function> aggregator;
    private final List<Column> groupByColumnReferences;
    private final ArrayList<Object[]> buffer;
    private final ArrayList<Integer> indexes;
    private final Map<List<Object>, List<Object[]>> bufferHash;
    private final Iterator<List<Object>> bufferHashitrator;

    public Group_By_Iterator(DB_Iterator oper, Table table, List<SelectItem> list, ArrayList<Function> aggregator, List<Column> groupByColumnReferences, Expression having) {
        this.oper = oper;
        this.table = table;
        this.list = list;
        this.aggregator = aggregator;
        this.groupByColumnReferences = groupByColumnReferences;
        this.having = having;
        buffer = new ArrayList<Object[]>();
        try {
            Object[] row = oper.next();
            while (row != null) {
                buffer.add(row);
                row = oper.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.indexes = new ArrayList<Integer>();
        for (Column column : groupByColumnReferences) {
            int index = 0;
            LinkedHashMap<String, Integer> schema = Shared_Variables.list_tables.get(table.getAlias());
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
            indexes.add(index);
        }

        bufferHash = this.buffer.stream().collect(Collectors.groupingBy(w -> abc(w)));
        bufferHashitrator = bufferHash.keySet().iterator();
    }

    public List<Object> abc(Object[] w) {
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
    public Object[] next() throws SQLException {
        if (bufferHashitrator.hasNext())
            return bufferHashitrator.next().toArray();
        else
            return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
