package com.database.builders;

import com.database.Shared_Variables;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.Iterator;

public class Select_Item_Builder implements SelectItemVisitor {

    ArrayList<SelectExpressionItem> list = new ArrayList<SelectExpressionItem>();
    private Table table;

    public Select_Item_Builder(Table table, SelectItem item) {
        this.table = table;
        item.accept(this);
    }

    @Override
    public void visit(AllColumns allColumns) {
        for (String i : Shared_Variables.current_schema.keySet()) {
            SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
            selectExpressionItem.setExpression(new Column(table, i));
            list.add(selectExpressionItem);
        }
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        Table tab = allTableColumns.getTable();
        for (Iterator<String> iterator = Shared_Variables.current_schema.keySet().iterator(); iterator.hasNext(); ) {
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
