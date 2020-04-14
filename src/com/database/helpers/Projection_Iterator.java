package com.database.helpers;

import com.database.RAtree.RA_Tree;
import com.database.sql.Evaluator;
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
import java.util.Iterator;
import java.util.List;

public class Projection_Iterator implements DB_Iterator {

    final DB_Iterator op;
    final HashMap<String, Integer> schema;
    final boolean allColumns;
    ArrayList<SelectItem> to_keep;
    Object[] row;

    public Projection_Iterator(RA_Tree op, List<SelectItem> p, boolean allColumns) {
        this.op = op.get_iterator();
        this.row = new Object[p.size()];
        this.to_keep = (ArrayList<SelectItem>) p;
        this.schema = op.getSchema();
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
        if (temp == null) return null;
        if (allColumns) return temp;
        ArrayList<SelectItem> list = new ArrayList<>();
        for (int i = 0, toProjectSize = to_keep.size(); i < toProjectSize; i++) {
            SelectItem f;
            f = to_keep.get(i);
            if (f instanceof AllTableColumns) {
                AllTableColumns a = (AllTableColumns) f;
                Table tab = a.getTable();
                for (Iterator<String> iterator = this.schema.keySet().iterator(); iterator.hasNext(); ) {
                    String j;
                    j = iterator.next();
                    SelectExpressionItem expItem;
                    expItem = new SelectExpressionItem();
                    j = j.substring(j.indexOf(".") + 1);
                    expItem.setAlias(j);
                    expItem.setExpression(new Column(tab, j));
                    list.add(expItem);
                }
            } else {
                list.add(f);
            }
        }
        to_keep = list;
        row = new Object[to_keep.size()];
        for (int i = 0; i < to_keep.size(); i++) {
            SelectItem f = to_keep.get(i);
            try {
                SelectExpressionItem e;
                e = (SelectExpressionItem) f;
                if (e.getExpression() instanceof Function) {
                    Expression x;
                    x = new Column(null, e.getExpression().toString());
                    row[index] = eval.eval(x);
                } else
                    row[index] = eval.eval(e.getExpression());
            } catch (SQLException e1) {
                e1.printStackTrace();
                System.out.println("error in ProjectIterator");
            }
            index++;
        }
        return row;
    }


    @Override
    public Table getTable() {
        return null;
    }

}
