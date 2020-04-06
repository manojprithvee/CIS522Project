package com.database.RAtree;

import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.LinkedHashMap;
import java.util.List;

public class Build_Tree implements SelectVisitor {
    private final boolean print;
    private RA_Tree root;
    private LinkedHashMap<String, Integer> schema;

    public Build_Tree(SelectBody selectBody) {
        print = true;
        selectBody.accept(this);

    }

    public Build_Tree(SelectBody selectBody, boolean print) {
        this.print = print;
        selectBody.accept(this);

    }

    public LinkedHashMap<String, Integer> getSchema() {
        return schema;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        Table t = null;
        if (!(plainSelect.getFromItem() instanceof SubSelect))
            t = (Table) plainSelect.getFromItem();
        RA_Tree output = build_from_joins(plainSelect.getFromItem(), plainSelect.getJoins());

        if (plainSelect.getWhere() != null) {
            RA_Tree selectTree = new SelectNode(plainSelect.getWhere());
            selectTree.setLeft(output);
            output = selectTree;
        }
        RA_Tree projectTree = new ProjectNode(plainSelect, t);
        projectTree.setLeft(output);
        output = projectTree;

        if (plainSelect.getHaving() != null) {
            RA_Tree selectTree = new SelectNode(plainSelect.getHaving());
            selectTree.setLeft(output);
            output = selectTree;
        }

        if (plainSelect.getOrderByElements() != null) {
            List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
            RA_Tree orderByTree = new OrderByNode(orderByElements, t);
            orderByTree.setLeft(output);
            output = orderByTree;
        }

        if (plainSelect.getDistinct() != null) {
            RA_Tree distinctTree = new DistinctNode();
            distinctTree.setLeft(output);
            output = distinctTree;
        }

        if (plainSelect.getLimit() != null) {
            RA_Tree limitTree = new LimitNode(plainSelect.getLimit());
            limitTree.setLeft(output);
            output = limitTree;
        }
        root = output;

        schema = Shared_Variables.current_schema;
    }

    public RA_Tree getRoot() {
        return root;
    }


    @Override
    public void visit(Union union) {

    }

    public RA_Tree build_from_joins(FromItem fromItem, List<Join> joins) {

        RA_Tree output = null;
        Table table = null;
        if (joins == null) {
            return new FromItems_Builder(fromItem).getCurrent();
        }
        for (Join join : joins) {
            RA_Tree left;
            if (output != null) {
                left = output;
            } else {
                left = new FromItems_Builder(fromItem).getCurrent();
                table = left.get_iterator().getTable();
                if (table == null) {
                    if (join.getRightItem().getAlias() != null) {
                        table = new Table(join.getRightItem().getAlias());
                        table.setAlias(join.getRightItem().getAlias());
                    } else {
                        table = new Table(String.valueOf(Shared_Variables.table));
                        table.setAlias(String.valueOf(Shared_Variables.table));
                        Shared_Variables.table += 1;
                    }
                }
            }
            Expression expression = join.getOnExpression();
            RA_Tree right = new FromItems_Builder(join.getRightItem()).getCurrent();
            Table t;
            if (join.getRightItem() instanceof Table) {
                t = ((Table) join.getRightItem());
            } else {
                if (join.getRightItem().getAlias() != null) {
                    t = new Table(join.getRightItem().getAlias());
                    t.setAlias(join.getRightItem().getAlias());
                } else {
                    t = new Table(String.valueOf(Shared_Variables.table));
                    t.setAlias(String.valueOf(Shared_Variables.table));
                    Shared_Variables.table += 1;
                }
            }
            output = new CartesianNode(left, right, t, table);
            table = output.get_iterator().getTable();
            if (expression != null) {
                output = new SelectNode(expression);
            }
        }

        return output;
    }


}