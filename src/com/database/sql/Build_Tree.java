package com.database.sql;

import com.database.RAtree.*;
import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class Build_Tree implements SelectVisitor {
    private RA_Tree root;

    public Build_Tree(SelectBody selectBody) {
        selectBody.accept(this);
    }

    public static void manage_renaming(SelectBody body) {
        ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) ((PlainSelect) body).getSelectItems();
        Shared_Variables.rename = new HashMap<>();
        for (SelectItem a : selectItems) {
            if ((a instanceof AllTableColumns) || (a instanceof AllColumns))
                return;
            SelectExpressionItem s = (SelectExpressionItem) a;
            String alias = s.getAlias();
            if (alias == null) {
                s.setAlias(s.getExpression().toString());
            }
            Shared_Variables.rename.put(s.getAlias(), s.getExpression());
        }
    }

    public LinkedHashMap<String, Integer> getSchema() {
        return root.getSchema();
    }

    public RA_Tree getRoot() {
        return root;
    }


    @Override
    public void visit(Union union) {
        RA_Tree output = null;
        for (PlainSelect plainselect : union.getPlainSelects()) {
            if (output == null) {
                output = new Build_Tree(plainselect).getRoot();
            } else {
                output = new Union_Node(output, new Build_Tree(plainselect).getRoot());
            }
        }
        root = output;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        manage_renaming(plainSelect);
        Table t = null;
        if (!(plainSelect.getFromItem() instanceof SubSelect))
            t = (Table) plainSelect.getFromItem();
        root = build_from_joins(plainSelect.getFromItem(), plainSelect.getJoins());
        if (plainSelect.getWhere() != null) root = new Select_Node(root, plainSelect.getWhere());
        root = new Project_Node(root, plainSelect, t);
        if (plainSelect.getHaving() != null) root = new Select_Node(root, plainSelect.getHaving());
        if (plainSelect.getOrderByElements() != null)
            root = new Order_By_Node(root, plainSelect.getOrderByElements(), t);
        if (plainSelect.getDistinct() != null) root = new Distinct_Node(root);
        if (plainSelect.getLimit() != null) root = new Limit_Node(root, plainSelect.getLimit());
    }

    public RA_Tree build_from_joins(FromItem fromItem, List<Join> joins) {
        RA_Tree output = null;
        if (joins == null) {
            return new FromItems_Builder(fromItem).getCurrent();
        }
        for (Join join : joins) {
            RA_Tree right;
            if (output != null) {
                right = output;
            } else {
                right = new FromItems_Builder(fromItem, true).getCurrent();
            }
            Expression expression = join.getOnExpression();
            RA_Tree left = new FromItems_Builder(join.getRightItem()).getCurrent();
            if (expression != null) {
                output = new Join_Node(right, left, (BinaryExpression) expression);
            } else {
                output = new Cross_Product_Node(left, right);
            }
            left.setParent(output);
            right.setParent(output);
        }

        return output;
    }


}