package com.database.sql;

import com.database.RAtree.*;
import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class Build_Tree implements SelectVisitor {
    private RA_Tree root;
    private LinkedHashMap<String, Integer> schema;

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
        return schema;
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
        RA_Tree output = build_from_joins(plainSelect.getFromItem(), plainSelect.getJoins());

        if (plainSelect.getWhere() != null) {
            RA_Tree selectTree = new Select_Node(output, plainSelect.getWhere(), t);
            output.setParent(selectTree);
            output = selectTree;
        }
        RA_Tree projectTree = new Project_Node(output, plainSelect, t);
        output.setParent(projectTree);
        output = projectTree;

        if (plainSelect.getHaving() != null) {
            RA_Tree selectTree = new Select_Node(output, plainSelect.getHaving(), t);
            output.setParent(selectTree);
            output = selectTree;
        }

        if (plainSelect.getOrderByElements() != null) {
            List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
            RA_Tree orderByTree = new Order_By_Node(output, orderByElements, t);
            output.setParent(orderByTree);
            output = orderByTree;
        }

        if (plainSelect.getDistinct() != null) {
            RA_Tree distinctTree = new Distinct_Node(output);
            output.setParent(distinctTree);
            output = distinctTree;
        }

        if (plainSelect.getLimit() != null) {
            RA_Tree limitTree = new Limit_Node(output, plainSelect.getLimit());
            output.setParent(limitTree);
            output = limitTree;
        }
        root = output;
        schema = Shared_Variables.current_schema;
    }

    public RA_Tree build_from_joins(FromItem fromItem, List<Join> joins) {

        RA_Tree output = null;
        Table table = null;
        if (joins == null) {
            return new FromItems_Builder(fromItem).getCurrent();
        }
        for (Join join : joins) {
            RA_Tree right;
            if (output != null) {
                right = output;
            } else {
                right = new FromItems_Builder(fromItem, true).getCurrent();
                table = right.get_iterator().getTable();
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
            RA_Tree left = new FromItems_Builder(join.getRightItem()).getCurrent();
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


            if (expression != null) {
                output = new Join_Node(right, left, expression);
            } else {
                output = new Cross_Product_Node(left, right);
            }
            left.setParent(output);
            right.setParent(output);
            table = output.get_iterator().getTable();
        }

        return output;
    }


}