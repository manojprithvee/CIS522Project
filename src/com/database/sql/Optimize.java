package com.database.sql;

import com.database.RAtree.Cross_Product_Node;
import com.database.RAtree.Join_Node;
import com.database.RAtree.RA_Tree;
import com.database.RAtree.Select_Node;
import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class Optimize {
    public static void selectionpushdown(RA_Tree abc) {
        List<RA_Tree> selectNodes = Optimize.getnodes(abc, Select_Node.class);
        for (RA_Tree node : selectNodes) {
            Select_Node select = (Select_Node) node;
            if (select.where instanceof BinaryExpression) {
                BinaryExpression where = (BinaryExpression) select.where;
                for (Expression expression : splitconditions(where)) {
                    List<Column> column_used = getcolumnused(expression);
                    RA_Tree lowestChild = getLowestChild(select.getLeft(), column_used);
                    Select_Node new_select = new Select_Node(lowestChild, expression, true);
                    RA_Tree parent = lowestChild.getParent();
                    if (parent.getLeft() == lowestChild) {
                        parent.setLeft(new_select);
                    } else {
                        parent.setRight(new_select);
                    }
                    new_select.setParent(parent);
                    lowestChild.setParent(new_select);
                    new_select.setSchema(lowestChild.getSchema());
                }
            }
            select.getParent().setLeft(select.getLeft());
            select.getLeft().setParent(select.getParent());
        }
        List<RA_Tree> crossNodes = Optimize.getnodes(abc, Cross_Product_Node.class);
        for (RA_Tree node : crossNodes) {
            if (!(node instanceof Cross_Product_Node)) continue;
            Select_Node selectnode = getSelectParent(node.getParent());
            if (selectnode == null) continue;
            RA_Tree parent = selectnode.getParent();
            RA_Tree child = selectnode.getLeft();
            if (parent != null) {
                if (parent.getLeft() == selectnode) parent.setLeft(child);
                else parent.setRight(child);
            }
            if (child != null) child.setParent(parent);
            parent = node.getParent();
            child = node.getLeft();
            if (parent != null) {
                if (parent.getLeft() == node) parent.setLeft(child);
                else parent.setRight(child);
            }
            if (child != null) child.setParent(parent);
            parent = node.getLeft().getParent();
            Join_Node join = new Join_Node(node.getLeft(), node.getRight(), (BinaryExpression) selectnode.where);
            join.setParent(parent);
            if (join.getParent() != null) {
                if (join.getParent().getLeft() == join.getLeft()) join.getParent().setLeft(join);
                else join.getParent().setRight(join);
            }
            if (join.getLeft() != null) join.getLeft().setParent(join);
            if (join.getRight() != null) join.getRight().setParent(join);
        }
//        printtree(abc, true, "");

    }

    public static Select_Node getSelectParent(RA_Tree parent) {
        if (parent == null) return null;
        if (!(parent instanceof Select_Node)) return null;
        if (((Select_Node) parent).where instanceof EqualsTo) return (Select_Node) parent;
        else return getSelectParent(parent.getParent());
    }


    public static void printtree(RA_Tree root, boolean a, String text) {
        if (root != null) {
            if (a)
                System.out.println(text + root.toString());
            if ((root.getLeft() != null) && (root.getRight() != null)) {
                String[] abc = {root.getLeft().toString() + "left", root.getRight().toString() + "right"};
                System.out.println(Arrays.deepToString(abc));
                printtree(root.getLeft(), false, "left");
                printtree(root.getRight(), false, "right");
            } else if (root.getLeft() != null) {
                printtree(root.getLeft(), true, text);
            }
        }
    }


    public static List<RA_Tree> getnodes(RA_Tree root, Class<?> type) {
        List<RA_Tree> node = new ArrayList<>();
        if (root != null) {
            if (root.getClass() == type) node.add(root);
            if (root.getLeft() != null) node.addAll(getnodes(root.getLeft(), type));
            if (root.getRight() != null) node.addAll(getnodes(root.getRight(), type));
        }
        return node;
    }

    public static List<Column> getcolumnused(Expression givenexpression) {
        List<Column> list = new ArrayList<>();
        if (givenexpression instanceof BinaryExpression) {
            BinaryExpression expression = (BinaryExpression) givenexpression;
            if (expression.getLeftExpression() instanceof Column) {
                Column leftcolumn = (Column) expression.getLeftExpression();
                list.add(leftcolumn);
            } else {
                if (expression.getLeftExpression() instanceof BinaryExpression) {
                    list.addAll(getcolumnused(expression.getLeftExpression()));
                }
            }
            if (givenexpression instanceof BinaryExpression) {
                if (expression.getRightExpression() instanceof Column) {
                    Column rightcolumn = (Column) expression.getRightExpression();
                    list.add(rightcolumn);
                } else {
                    if (expression.getRightExpression() instanceof BinaryExpression) {
                        list.addAll(getcolumnused(expression.getRightExpression()));
                    }
                }
            }
        }
        if (givenexpression instanceof InExpression) {
            InExpression expression = (InExpression) givenexpression;
            if (expression.getLeftExpression() instanceof Column) {
                Column leftcolumn = (Column) expression.getLeftExpression();
                list.add(leftcolumn);
            }
        }
        return list;
    }

    public static RA_Tree getLowestChild(RA_Tree childNode, List<Column> columns) {
        RA_Tree lowest = childNode;
        RA_Tree[] children = {childNode.getLeft(), childNode.getRight()};

        for (RA_Tree child : children) {

            if (child != null) {
                LinkedHashMap<String, Integer> schema = child.getSchema();
                boolean containsAll = true;
                for (Column column : columns) {
                    if (!get(schema, column)) containsAll = false;
                }
                if (containsAll) {
                    lowest = getLowestChild(child, columns);
                }
            }
        }

        return lowest;
    }

    public static List<Expression> splitconditions(Expression where) {
        List<Expression> list = new ArrayList<>();
//        System.out.println(where);
        if (where instanceof AndExpression) {
            list.addAll(splitconditions(((BinaryExpression) where).getLeftExpression()));
            list.addAll(splitconditions(((BinaryExpression) where).getRightExpression()));
        } else {
            list.add(where);
        }
        return (list);

    }

    public static boolean get(LinkedHashMap structure, Column main_column) {
        String table;
        int id = Integer.MAX_VALUE;
        if ((main_column.getTable() != null) && (main_column.getTable().getName() != null)) {
            table = main_column.getTable().getName();
            if (!structure.containsKey(table + "." + main_column.getColumnName()))
                id = columnchange(structure, id, main_column.getTable() + "." + main_column.getColumnName());
            else id = (int) structure.get(table + "." + main_column.getColumnName());
        } else if (!Shared_Variables.rename.containsKey(main_column.getColumnName()))
            id = columnchange(structure, id, main_column.getColumnName());
        else if (structure.containsKey(main_column.getColumnName()))
            id = (int) structure.get(main_column.getColumnName());
        else if (structure.containsKey(Shared_Variables.rename.get(main_column.getColumnName()).toString()))
            id = (int) structure.get(Shared_Variables.rename.get(main_column.getColumnName()).toString());
        else id = columnchange(structure, id, main_column.getColumnName());
        return id != Integer.MAX_VALUE;
    }

    public static int columnchange(LinkedHashMap structure, int id, String columnName) {
        for (Iterator<String> iterator = structure.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(columnName)) id = (int) structure.get(column);
        }
        return id;
    }
}
