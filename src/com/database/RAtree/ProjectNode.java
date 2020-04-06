package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Group_By_Iterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ProjectNode extends RA_Tree {
    private final ArrayList<Expression> inExpressions = new ArrayList<>();
    boolean isagg = false;
    boolean isattribule = false;
    LinkedHashMap<String, Integer> new_schema = new LinkedHashMap<>();

    public ProjectNode(RA_Tree o, List<SelectItem> selectItems, Table t,) {
        super(o);
        ArrayList<SelectExpressionItem> items = new ArrayList<>();
        for (SelectItem item : selectItems) {
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
                new_schema.put(table.getName() + "." + ((Column) expression).getColumnName(), count);

            } else {
                if (alias == null) alias = expression.toString();
                new_schema.put(table.getName() + "." + alias, count);
            }
        }
    }


    @Override
    public Group_By_Iterator get_iterator() {
        if (allColumns) return this.getLeft().get_iterator();
        List<SelectItem> inSchema = null;
        if (isattribule && isagg) {
            return new Group_By_Iterator(left.get_iterator(), inExpressions, inSchema);
        } else if (!isattribule && isagg) {
            return new AggregateIterator((RowIterator) left.iterator(), inExpressions, inSchema);
        } else {
            return new NonAggregateIterator((RowIterator) left.iterator(), inExpressions, inSchema);
        }
        return null;
    }
}
