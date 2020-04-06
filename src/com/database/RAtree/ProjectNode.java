package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Aggregate_Iterator;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Group_By_Iterator;
import com.database.helpers.Projection_Iterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ProjectNode extends RA_Tree {
    private final ArrayList<Expression> inExpressions = new ArrayList<>();
    private final PlainSelect body;
    private final Table table;
    boolean isagg = false;
    boolean isattribule = false;

    LinkedHashMap<String, Integer> new_schema = new LinkedHashMap<>();
    private boolean allColumns = false;

    public ProjectNode(PlainSelect plainSelect, Table t) {
        this.body = plainSelect;
        this.table = t;
        ArrayList<SelectExpressionItem> items = new ArrayList<>();
        allColumns = ((plainSelect.getSelectItems().get(0) instanceof AllColumns));
        for (SelectItem item : plainSelect.getSelectItems()) {
            items.addAll(new Select_Item_Builder(t, item).getitems());
        }

        Table table = new Table(String.valueOf(Shared_Variables.table));
        table.setAlias(String.valueOf(Shared_Variables.table));
        Shared_Variables.table += 1;
        int count = 0;
        for (SelectExpressionItem item : items) {
            Expression expression = item.getExpression();
            inExpressions.add(expression);
            if (expression instanceof Column) isattribule = true;
            if (expression instanceof Function) isagg = true;
            String alias = item.getAlias();
            if (expression instanceof Column && alias == null) {
                System.out.println(expression);
                if (((Column) expression).getTable() == null) {
                    new_schema.put(table.getName() + "." + ((Column) expression).getColumnName(), count);
                } else {
                    new_schema.put(((Column) expression).getWholeColumnName(), count);
                }
            } else {
                if (alias == null) alias = expression.toString();
                new_schema.put(table.getName() + "." + alias, count);
            }
            count++;
        }


    }

    @Override
    public DB_Iterator get_iterator() {

        if (allColumns) return this.getLeft().get_iterator();
        List<SelectItem> inSchema = null;
        if (isattribule && isagg) {
            return new Group_By_Iterator(left.get_iterator(), body.getSelectItems(), body.getGroupByColumnReferences(), new_schema);
        } else if (!isattribule && isagg) {
            return new Aggregate_Iterator(left.get_iterator(), inExpressions, new_schema);
        } else {
            return new Projection_Iterator(
                    left.get_iterator(),
                    body.getSelectItems(),
                    allColumns, new_schema);
        }
    }
}
