package com.database;

import com.database.RAtree.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class Optimize {
    public static RA_Tree selectionpushdown(RA_Tree abc) {
        List<RA_Tree> selectNodes = Optimize.getnodes(abc, Select_Node.class);
//                printtree(abc, true, "");
        List<Select_Node> or_only = new ArrayList<>();
        for (RA_Tree node : selectNodes) {
            Select_Node select = (Select_Node) node;
            if (select.where instanceof BinaryExpression) {
                BinaryExpression where = (BinaryExpression) select.where;
//                List<Expression> new_ands = splitconditions(where);
                for (Expression expression : splitconditions(where)) {
                    List<Column> column_used = getcolumnused(expression);
                    RA_Tree lowestChild = getLowestChild(select.getLeft(), column_used);
                    Select_Node new_select = new Select_Node(lowestChild, expression, true);
                    if (expression instanceof OrExpression) {
                        or_only.add(new_select);
                    }
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
            if (select.getParent() != null) {
                select.getParent().setLeft(select.getLeft());
                select.getLeft().setParent(select.getParent());
            } else {
                select.getLeft().setParent(null);
                abc = select.getLeft();
            }
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
            } else {
                abc = child;
                child.setParent(null);
            }
            if (child != null) child.setParent(parent);
            parent = node.getParent();
            child = node.getLeft();
            boolean join_flag = true;
            Join_Node join = null;
            if (parent != null) {
                if (parent.getLeft() == node) parent.setLeft(child);
                else parent.setRight(child);
            } else {
                abc = new Join_Node(node.getLeft(), node.getRight(), (BinaryExpression) selectnode.where);
                join = (Join_Node) abc;
                join_flag = false;
            }
            if (child != null) child.setParent(parent);
            parent = node.getLeft().getParent();
            if (join_flag) {
                join = new Join_Node(node.getLeft(), node.getRight(), (BinaryExpression) selectnode.where);
            }
            join.setParent(parent);
            if (join.getParent() != null) {
                if (join.getParent().getLeft() == join.getLeft()) join.getParent().setLeft(join);
                else join.getParent().setRight(join);
            }
        }

        List<Expression> list = new ArrayList<>();
        for (Select_Node node : or_only) {
            RA_Tree parent = node.getParent();

            RA_Tree output = null;
            for (Expression i : split_or_conditions(node.getWhere())) {
                RA_Tree new_tree = copytree(node.getLeft());
                Select_Node new_select = new Select_Node(new_tree, i);
                RA_Tree new_select_optimized = selectionpushdown(new_select);
                if (output == null) {
                    output = new_select_optimized;
                } else {
                    output = new Union_Or_Node(output, new_select);
                }

            }
            if (parent.getLeft() == node) {
                parent.setLeft(output);
                output.setParent(parent);
            } else {
                parent.setRight(output);
                output.setParent(parent);
            }
        }
//        TreePrinter.print(abc);
        return abc;

//        System.exit(9);
    }

    public static Select_Node getSelectParent(RA_Tree parent) {
        if (parent == null) return null;
        if (!(parent instanceof Select_Node)) return null;
        if ((((Select_Node) parent).where instanceof EqualsTo) && ((((EqualsTo) ((Select_Node) parent).where).getLeftExpression() instanceof Column) && (((EqualsTo) ((Select_Node) parent).where).getRightExpression() instanceof Column)))
            return (Select_Node) parent;
        else return getSelectParent(parent.getParent());
    }

    public static RA_Tree copytree(RA_Tree root) {
        if (root instanceof Cross_Product_Node) {
            return new Cross_Product_Node(Objects.requireNonNull(copytree(root.getLeft())), Objects.requireNonNull(copytree(root.getRight())));
        } else if (root instanceof Distinct_Node) {
            return new Distinct_Node(Objects.requireNonNull(copytree(root.getLeft())));
        } else if (root instanceof Join_Node) {
            return new Join_Node(Objects.requireNonNull(copytree(root.getLeft())), Objects.requireNonNull(copytree(root.getRight())), ((Join_Node) root).getExpression());
        } else if (root instanceof Limit_Node) {
            return new Limit_Node(Objects.requireNonNull(copytree(root.getLeft())), ((Limit_Node) root).getLimit());
        } else if (root instanceof Order_By_Node) {
            return new Order_By_Node(Objects.requireNonNull(copytree(root.getLeft())), ((Order_By_Node) root).getOrderByElements(), ((Order_By_Node) root).getTable());
        } else if (root instanceof Project_Node) {
            return new Project_Node(Objects.requireNonNull(copytree(root.getLeft())), ((Project_Node) root).getBody(), ((Project_Node) root).getTable());
        } else if (root instanceof Select_Node) {
            return new Select_Node(Objects.requireNonNull(copytree(root.getLeft())), ((Select_Node) root).getWhere());
        } else if (root instanceof Union_Node) {
            return new Union_Node(Objects.requireNonNull(copytree(root.getLeft())), Objects.requireNonNull(copytree(root.getRight())));
        } else if (root instanceof Scan_Node) {
            return new Scan_Node(((Scan_Node) root).getTable(), ((Scan_Node) root).isFlag());
        } else if (root instanceof Union_Or_Node) {
            return new Union_Or_Node(Objects.requireNonNull(copytree(root.getLeft())), Objects.requireNonNull(copytree(root.getRight())));
        }
        return null;
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

    public static List<Expression> split_or_conditions(Expression where) {
        List<Expression> list = new ArrayList<>();
        Expression right = ((BinaryExpression) where).getRightExpression();
        Expression left = ((BinaryExpression) where).getLeftExpression();
        if (left instanceof OrExpression)
            list.addAll(split_or_conditions(left));
        else
            list.add(left);
        if (right instanceof OrExpression)
            list.addAll(split_or_conditions(right));
        else
            list.add(right);
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
