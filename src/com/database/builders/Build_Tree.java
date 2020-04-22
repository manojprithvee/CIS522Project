package com.database.builders;

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
        if (plainSelect.getWhere() != null)
            root = new Select_Node(root, plainSelect.getWhere());
        root = new Project_Node(root, plainSelect, t);
        if (plainSelect.getHaving() != null)
            root = new Select_Node(root, plainSelect.getHaving());
        if (plainSelect.getOrderByElements() != null)
            root = new Order_By_Node(root, plainSelect.getOrderByElements(), t);
        if (plainSelect.getDistinct() != null)
            root = new Distinct_Node(root);
        if (plainSelect.getLimit() != null)
            root = new Limit_Node(root, plainSelect.getLimit());
    }

    public RA_Tree build_from_joins(FromItem fromItem, List<Join> joins) {
        RA_Tree output = null;
        List<List_Tables> tables = new ArrayList<>();
        FromItems_Builder node = new FromItems_Builder(fromItem);
        List_Tables temp = new List_Tables(node.getCost(), node.getCurrent(), null);
        tables.add(temp);
        if (joins != null) {
            for (Join join : joins) {
                node = new FromItems_Builder(join.getRightItem());
                temp = new List_Tables(node.getCost(), node.getCurrent(), join.getOnExpression());
                tables.add(temp);
            }
        }
        for (List_Tables table : tables) {
            RA_Tree left, right;
            if (output != null) {
                left = output;
                right = table.current;
                if (table.expression != null) {
                    output = new Join_Node(left, right, (BinaryExpression) table.expression);
                } else {
                    output = new Cross_Product_Node(left, right);
                }
            } else {
                output = table.current;
            }
        }
        return output;
    }
}

class List_Tables implements Comparable {
    Long cost;
    RA_Tree current;
    Expression expression;

    public List_Tables(Long cost, RA_Tree current, Expression expression) {
        this.cost = cost;
        this.current = current;
        this.expression = expression;
    }

    @Override
    public int compareTo(Object o) {
        List_Tables old = (List_Tables) o;
        if (this.cost > old.cost) {
            return 1;
        }
        return -1;
    }
}