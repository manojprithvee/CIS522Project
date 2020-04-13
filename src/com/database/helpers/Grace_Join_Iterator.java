package com.database.helpers;

import com.database.RAtree.RA_Tree;
import com.database.Shared_Variables;
import com.database.sql.Evaluator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.*;

import static com.database.Shared_Variables.get_index;

public class Grace_Join_Iterator implements DB_Iterator {
    private final Table righttable, lefttable;
    private final DB_Iterator rightIterator;
    private final List<Column> columnused;
    private final LinkedHashMap<String, Integer> newschama;
    private final Map<ArrayList<Object>, ArrayList<Object[]>> map;
    private final Evaluator eval;
    DB_Iterator leftIterator;
    Expression expression;
    ArrayList<Object> current_group;
    private Table table;
    private Object[] temp2;
    private int size;
    private Iterator<ArrayList<Object>> group;
    private Iterator<Object[]> current_group_iterator;


    public Grace_Join_Iterator(RA_Tree left, RA_Tree right, Table lefttable, Table righttable,
                               BinaryExpression expression) {
        this.lefttable = left.get_iterator().getTable();
        this.righttable = right.get_iterator().getTable();
        if (righttable.getAlias() == null) righttable.setAlias(righttable.getName());
        if (lefttable.getAlias() == null) lefttable.setAlias(lefttable.getName());
        this.rightIterator = right.get_iterator();
        this.leftIterator = left.get_iterator();
        this.expression = expression;
        this.columnused = getcolumnused(expression);
        Set<Column> leftcolumn = new HashSet<Column>();
        Set<Column> rightcolumn = new HashSet<Column>();
        Set<Integer> leftindex = new HashSet<Integer>();
        Set<Integer> rightindex = new HashSet<Integer>();
        LinkedHashMap<String, Integer> newleftschema = new LinkedHashMap<>();
        int count = 0;
        for (Column c : columnused) {
            if (c.getTable().getName() == this.lefttable.getName()) {
                if (!leftcolumn.contains(c)) {
                    leftcolumn.add(c);
                    leftindex.add(get_index(c, Shared_Variables.list_tables.get(lefttable.getName())));
                    newleftschema.put(new Column(this.lefttable, c.getColumnName()).getWholeColumnName(), count);
                    count++;
                }
            } else if (c.getTable().getName() == this.righttable.getName()) {
                rightcolumn.add(c);
                rightindex.add(get_index(c, Shared_Variables.list_tables.get(this.righttable.getName())));
            } else {
                int index = get_index(c, Shared_Variables.list_tables.get(this.lefttable.getName()));
                if (index != Integer.MAX_VALUE) {
                    if (!leftcolumn.contains(c)) {
                        leftcolumn.add(c);
                        leftindex.add(index);
                        newleftschema.put(new Column(this.lefttable, c.getColumnName()).getWholeColumnName(), count);
                        count++;
                    }
                }
                index = get_index(c, Shared_Variables.list_tables.get(this.righttable.getName()));
                if (index != Integer.MAX_VALUE) {
                    rightcolumn.add(c);
                    rightindex.add(index);
                }
            }
        }
        newschama = buildschema(newleftschema, right.getSchema());
        Object[] row = this.leftIterator.next();
        map = new HashMap<>();
        while (row != null) {
            ArrayList<Object> key = grouping(row, leftindex);
            ArrayList<Object[]> abc;
            if (map.containsKey(key)) {
                abc = map.get(grouping(row, leftindex));
            } else {
                abc = new ArrayList<>();
            }
            abc.add(row);
            map.put(key, abc);
            row = this.leftIterator.next();
        }
        group = map.keySet().iterator();
        eval = new Evaluator(newschama);
        main(lefttable, righttable);
    }

    public ArrayList<Object> grouping(Object[] w, Set<Integer> indexes) {
        ArrayList<Object> output = new ArrayList<Object>();
        for (Integer index : indexes) {
            output.add(w[index]);
        }
        return output;
    }

    private void main(Table lefttable, Table righttable) {
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = String.valueOf(Shared_Variables.table);
        Shared_Variables.table += 1;
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = create_new_schema(newSchema, righttable, lefttable, dataType);
        Shared_Variables.list_tables.put(newTableName, newSchema);
        Shared_Variables.schema_store.put(newTableName, dataType);
        temp2 = rightIterator.next();
        size = newSchema.size();
        Shared_Variables.current_schema = (LinkedHashMap<String, Integer>) newSchema.clone();
    }


    ArrayList<String> create_new_schema(HashMap<String, Integer> newSchema, Table lefttable, Table righttable, ArrayList<String> dataType) {
        LinkedHashMap<String, Integer> oldschema = Shared_Variables.list_tables.get(lefttable.getAlias());
        dataType.addAll(Shared_Variables.schema_store.get(lefttable.getName()));
        int sizes = 0;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        sizes = newSchema.size();
        oldschema = Shared_Variables.list_tables.get(righttable.getAlias());
        dataType.addAll(Shared_Variables.schema_store.get(righttable.getName()));
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        return dataType;
    }

    public LinkedHashMap<String, Integer> buildschema(HashMap<String, Integer> left, HashMap<String, Integer> right) {
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        int sizes = 0;
        for (String col : left.keySet()) {
            newSchema.put(col, left.get(col) + sizes);
        }
        sizes = newSchema.size();
        for (String col : right.keySet()) {
            newSchema.put(col, right.get(col) + sizes);
        }
        return newSchema;
    }

    public Object[] create_row(Object[] left, Object[] right, int size) {
        Object[] new_row = new Object[size];
        int index = 0;
        if (left == null || right == null) return null;
        for (Object o : left) {
            new_row[index] = o;
            index++;
        }
        for (Object o : right) {
            new_row[index] = o;
            index++;
        }
        return new_row;
    }

    public List<Column> getcolumnused(BinaryExpression expression) {
        List<Column> list = new ArrayList<>();
        if (expression.getLeftExpression() instanceof Column) {
            Column leftcolumn = (Column) expression.getLeftExpression();
            list.add(leftcolumn);
        } else {
            if (expression.getLeftExpression() instanceof BinaryExpression) {
                list.addAll(getcolumnused((BinaryExpression) expression.getLeftExpression()));
            }
        }
        if (expression.getRightExpression() instanceof Column) {
            Column rightcolumn = (Column) expression.getRightExpression();
            list.add(rightcolumn);
        } else {
            if (expression.getRightExpression() instanceof BinaryExpression) {
                list.addAll(getcolumnused((BinaryExpression) expression.getRightExpression()));
            }
        }
        return list;
    }

    @Override
    public void reset() {
        leftIterator.reset();
        rightIterator.reset();
        temp2 = rightIterator.next();
    }

    @Override
    public Object[] next() {
        Object[] temp1 = null;
        if (current_group_iterator != null)
            if (current_group_iterator.hasNext())
                temp1 = current_group_iterator.next();
        if (temp1 == null) {
            boolean flag = true;
            boolean flag2 = true;
            while (flag2) {
                while (group.hasNext()) {
                    current_group = group.next();

                    Object[] row = create_row(current_group.toArray(), temp2, current_group.size() + temp2.length);

                    eval.setTuple(row);
                    try {
                        if (((BooleanValue) eval.eval(expression)).getValue()) {
                            current_group_iterator = map.get(current_group).iterator();
                            temp1 = current_group_iterator.next();
                            flag = false;
                            flag2 = false;
                            break;
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (flag) {
                    temp2 = rightIterator.next();
                    group = map.keySet().iterator();
                    if (temp2 == null) {
                        return null;
                    }
                }
            }

        }

        return create_row(temp2, temp1, size);
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}
