package com.database.RAtree;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.List;

public class Build_Tree implements SelectVisitor {
    private RA_Tree root;

    public Build_Tree(SelectBody selectBody) {
        selectBody.accept(this);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        Table t = (Table) plainSelect.getFromItem();
        RA_Tree output = build_from_joins(plainSelect.getFromItem(), plainSelect.getJoins());
        RA_Tree projectTree = new ProjectNode(null, output, plainSelect.getSelectItems());

        if (plainSelect.getWhere() != null) {
            RA_Tree selectTree = new SelectNode(projectTree, plainSelect.getWhere());
            selectTree.setLeft(projectTree.getLeft());
            projectTree.setLeft(selectTree);
        }

        if (plainSelect.getHaving() != null) {
            RA_Tree selectTree = new SelectNode(null, plainSelect.getHaving());
            selectTree.setLeft(output);
            output = selectTree;
        }

        if (plainSelect.getDistinct() != null) {
            RA_Tree distinctTree = new DistinctNode(null);
            distinctTree.setLeft(output);
            output = distinctTree;
        }

        if (plainSelect.getOrderByElements() != null) {
            List<OrderByElement> orderByElements = plainSelect.getOrderByElements();

            RA_Tree orderByTree = new OrderByNode(null, orderByElements, t);
            orderByTree.setLeft(output);
            output = orderByTree;
        }

        if (plainSelect.getLimit() != null) {
            RA_Tree limitTree = new LimitNode(null, plainSelect.getLimit());
            limitTree.setLeft(output);
            output = limitTree;
        }
        root = output;
    }

    public RA_Tree getRoot() {
        return root;
    }


    @Override
    public void visit(Union union) {

    }

    public RA_Tree build_from_joins(FromItem fromItem, List<Join> joins) {
        RA_Tree output = null;
        if (joins == null) {
            return new FromItems_Builder(fromItem).getCurrent();
        }
        for (Join join : joins) {
            RA_Tree left;
            if (output != null) {
                left = output;
            } else {
                left = new FromItems_Builder(fromItem).getCurrent();
            }
            Expression expression = join.getOnExpression();
            RA_Tree right = new FromItems_Builder(join.getRightItem()).getCurrent();
            output = new CartesianNode(null, left, right);
            if (expression != null) {
                output = new SelectNode(output, expression);
            }
        }
        return output;
    }


}