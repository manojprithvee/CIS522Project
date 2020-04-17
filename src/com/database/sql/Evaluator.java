package com.database.sql;

import com.database.RAtree.Distinct_Node;
import com.database.RAtree.RA_Tree;
import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Scan_Iterator;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.io.*;
import java.sql.SQLException;
import java.util.*;


public class Evaluator extends Eval {

    private final HashMap<String, Integer> structure;
    private Object[] row;

    public Evaluator(HashMap<String, Integer> table, Object[] row) {
        this.structure = table;
        this.row = row;
    }

    public Evaluator(LinkedHashMap<String, Integer> schema) {
        this.structure = schema;
    }

    public HashMap<String, Integer> getStructure() {
        return structure;
    }

    public void setTuple(Object[] row) {
        this.row = row;
    }

    public PrimitiveValue eval(Column main_column) {
        String table;
        int id = 0;

        if ((main_column.getTable() != null) && (main_column.getTable().getName() != null)) {
            table = main_column.getTable().getName();
            if (!structure.containsKey(table + "." + main_column.getColumnName()))
                id = columnchange(id, main_column.getTable() + "." + main_column.getColumnName());
            else id = structure.get(table + "." + main_column.getColumnName());
        } else if (!Shared_Variables.rename.containsKey(main_column.getColumnName()))
            id = columnchange(id, main_column.getColumnName());
        else if (structure.containsKey(main_column.getColumnName())) id = structure.get(main_column.getColumnName());
        else if (structure.containsKey(Shared_Variables.rename.get(main_column.getColumnName()).toString()))
            id = structure.get(Shared_Variables.rename.get(main_column.getColumnName()).toString());
        else id = columnchange(id, main_column.getColumnName());
        return (PrimitiveValue) row[id];
    }


    public PrimitiveValue eval(InExpression inExpression) throws SQLException {
        ItemsList i = inExpression.getItemsList();
        Expression left = inExpression.getLeftExpression();
        if (i instanceof ExpressionList) {
            BinaryExpression a = null;
            for (var c : ((ExpressionList) i).getExpressions()) {
                if (eval(new EqualsTo(left, c)).toBool()) {
                    return BooleanValue.TRUE;
                }
            }
            return BooleanValue.FALSE;
        } else {
            Scan_Iterator scan = null;
            if (Shared_Variables.Sub_Select.get(i.toString()) == null) {
                Set<String> abc = Shared_Variables.column_used;
                Organizer extractor = new Organizer(((SubSelect) i).getSelectBody());
                ((SubSelect) i).getSelectBody().accept(extractor);
                Shared_Variables.column_used = extractor.columns;
                Build_Tree Build_Tree = new Build_Tree(((SubSelect) i).getSelectBody());
                RA_Tree current = Build_Tree.getRoot();
                RA_Tree distinctTree = new Distinct_Node(current);
                current.setParent(distinctTree);
                current = distinctTree;
                Optimize.selectionpushdown(current);
                DB_Iterator itrator = current.get_iterator();
                FileWriter fileWriter = null;
                BufferedWriter bufferedWriter = null;
                PrintWriter printWriter = null;
                String newtable = Shared_Variables.table_location.toString() + File.separator + "temp.dat";
                new File(newtable).delete();
                ArrayList<String> datatype = new ArrayList<>();
                try {
                    fileWriter = new FileWriter(newtable, true);
                    bufferedWriter = new BufferedWriter(fileWriter);
                    printWriter = new PrintWriter(bufferedWriter);
                    Object[] row = itrator.next();

                    if (row[0] instanceof LongValue) {
                        datatype.add("INT");
                    } else if (row[0] instanceof DoubleValue) {
                        datatype.add("DOUBLE");
                    } else if (row[0] instanceof DateValue) {
                        datatype.add("DATE");
                    } else {
                        datatype.add("STRING");
                    }
                    while (row != null) {
                        printWriter.println(row[0]);
                        row = itrator.next();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        printWriter.close();
                        bufferedWriter.close();
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                LinkedHashMap<String, Integer> schema = new LinkedHashMap<>();
                schema.put(left.toString(), 0);
                Shared_Variables.list_tables.put("TEMP", schema);
                Shared_Variables.schema_store.put("TEMP", datatype);
                scan = new Scan_Iterator(new File(newtable), "TEMP", schema);
                Shared_Variables.Sub_Select.put(i.toString(), scan);
                Shared_Variables.column_used = abc;
            } else {
                scan = Shared_Variables.Sub_Select.get(i.toString());
                scan.reset();
            }

            Object[] row = scan.next();
            while (row != null) {
                if (eval(new EqualsTo((PrimitiveValue) row[0], left)).toBool()) {
                    return BooleanValue.TRUE;
                }
                row = scan.next();
            }
            return BooleanValue.FALSE;
        }
    }

    public int columnchange(int id, String columnName) {
        for (Iterator<String> iterator = structure.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(columnName)) id = structure.get(column);
        }
        return id;
    }
}
