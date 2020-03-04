package com.database.helpers;

import com.database.Evaluator;
import com.database.Global;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProjectionIterator implements DB_Iterator {

    final DB_Iterator op;
    final Table table;
    ArrayList<SelectItem> toProject;
    final HashMap<String, Integer> schema;
    final boolean allColumns;
    Object[] row;

    public ProjectionIterator(DB_Iterator op, List<SelectItem> p, Table table, boolean allColumns) {

        this.op = op;
        this.row = new Object[p.size()];
        this.toProject = (ArrayList<SelectItem>) p;
        this.table = table;
        this.schema = Global.list_tables.get(table.getAlias());
        this.allColumns = allColumns;
    }

    @Override
    public void reset() {


    }

    @Override
    public Object[] next() {

        Object[] temp = op.next();
        Evaluator eval = new Evaluator(schema, temp);

        int index = 0;
        if (temp == null)
            return null;
        if (allColumns) {
            return temp;
        }
        ArrayList<SelectItem> list = new ArrayList<>();
        for (SelectItem f : toProject) {
            if (f instanceof AllTableColumns) {
                AllTableColumns a = (AllTableColumns) f;
                Table tab = a.getTable();
                for (String j : Global.list_tables.get(tab.getName()).keySet()) {
                    SelectExpressionItem expItem = new SelectExpressionItem();
                    j = j.substring(j.indexOf(".") + 1);
                    expItem.setAlias(j);
                    expItem.setExpression(new Column(tab, j));
                    list.add(expItem);
                }
            } else {
                list.add(f);
            }
        }
        toProject = list;
        row = new Object[toProject.size()];
        for (SelectItem f : toProject) {
            try {
                SelectExpressionItem e = (SelectExpressionItem) f;
                if (e.getExpression() instanceof Function) {
                    Expression x = new Column(null, e.getExpression().toString());
                    row[index] = eval.eval(x);
                } else {
                    row[index] = eval.eval(e.getExpression());
                }
            } catch (SQLException e1) {
                System.out.println("error in ProjectIterator");
            }
            index++;
        }
        return row;
    }


    @Override
    public Table getTable() {
        return table;
    }

}
