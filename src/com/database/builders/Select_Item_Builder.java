package com.database.builders;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class Select_Item_Builder implements SelectItemVisitor {

    private final LinkedHashMap<String, Integer> schema;
    ArrayList<SelectExpressionItem> list = new ArrayList<SelectExpressionItem>();
    private Table table;

    public Select_Item_Builder(Table table, SelectItem item, LinkedHashMap<String, Integer> schema) {
        this.table = table;
        this.schema = schema;
        item.accept(this);
    }

    @Override
    public void visit(AllColumns allColumns) {
        for (String i : schema.keySet()) {
            SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
            String[] a = i.split("\\.");
            selectExpressionItem.setExpression(new Column(new Table(a[0]), a[1]));
            list.add(selectExpressionItem);
        }
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        Table tab = allTableColumns.getTable();
        for (Iterator<String> iterator = schema.keySet().iterator(); iterator.hasNext(); ) {
            String j;
            j = iterator.next();
            SelectExpressionItem expItem;
            expItem = new SelectExpressionItem();
            j = j.substring(j.indexOf(".") + 1);
            expItem.setAlias(j);
            expItem.setExpression(new Column(tab, j));
            list.add(expItem);
        }
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        list.add(selectExpressionItem);
    }

    public ArrayList<SelectExpressionItem> getitems() {
        return list;
    }
}
