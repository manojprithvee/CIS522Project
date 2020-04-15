package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Aggregate_Iterator;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Group_By_Iterator;
import com.database.helpers.Projection_Iterator;
import com.database.sql.Select_Item_Builder;
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

public class Project_Node extends RA_Tree {
    private final ArrayList<Expression> inExpressions = new ArrayList<>();
    private final PlainSelect body;
    private final Table table;
    boolean isagg = false;
    boolean isattribule = false;

    LinkedHashMap<String, Integer> new_schema = new LinkedHashMap<>();
    private boolean allColumns = false;

    public Project_Node(RA_Tree left, PlainSelect plainSelect, Table t) {
        this.left = left;
        this.body = plainSelect;
        this.table = t;
    }

    @Override
    public DB_Iterator get_iterator() {
        ArrayList<SelectExpressionItem> items = new ArrayList<>();
        allColumns = ((body.getSelectItems().get(0) instanceof AllColumns));
        for (SelectItem item : body.getSelectItems()) {
            items.addAll(new Select_Item_Builder(table, item).getitems());
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
        schema = new_schema;
        if (allColumns) return this.getLeft().get_iterator();
        if (isattribule && isagg) {
            return new Group_By_Iterator(left.get_iterator(), inExpressions, left.getSchema());
        } else if (!isattribule && isagg) {
            return new Aggregate_Iterator(left.get_iterator(), inExpressions, new_schema, left.getSchema());
        } else {
            return new Projection_Iterator(
                    left,
                    body.getSelectItems(),
                    allColumns);
        }
    }

    @Override
    public String toString() {
        return "Project_Node{" +
                "inExpressions=" + inExpressions +
                ", body=" + body +
                ", table=" + table +
                ", isagg=" + isagg +
                ", isattribule=" + isattribule +
                ", new_schema=" + new_schema +
                ", allColumns=" + allColumns +
                '}';
    }
}
