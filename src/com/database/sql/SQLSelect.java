package com.database.sql;


import com.database.Execute;
import com.database.Global;
import com.database.helpers.HelperImp;
import com.database.helpers.ScanHelper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SQLSelect {
    private Select sql;

    public SQLSelect(Select stmt) {
        this.sql = stmt;
    }

    public static void populateAliases(SelectBody body) {


        ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) ((PlainSelect) body).getSelectItems();
        Global.alias = new HashMap<String, Expression>();
        if (((PlainSelect) body).getSelectItems().get(0) instanceof AllColumns)
            return;
        for (SelectItem a : selectItems) {
            SelectExpressionItem s = (SelectExpressionItem) a;
            String alias = s.getAlias();
            if (alias == null) {
                s.setAlias(s.getExpression().toString());
                Global.alias.put(s.getAlias(), s.getExpression());
            } else if (alias != null)
                Global.alias.put(s.getAlias(), s.getExpression());
        }
    }

    public static ArrayList<Object> getParameters(PlainSelect body) {
        //list of parameters in the sequence - From Item, Condition Item,  Select Items, Joins, GroupByColumnReference, Having, allColumns,Limit
        Table t = null;
        ArrayList<Object> parameters = new ArrayList<Object>();
        if (body.getFromItem() instanceof Table) {
            t = (Table) body.getFromItem();
            if (t.getAlias() != null) {
                Global.tableAlias.put(t.getAlias(), t);
            }
            parameters.add(Global.dataDir.toString() + File.separator + t.getName() + ".dat");
            parameters.add(t.getName());
            parameters.add(body.getWhere());
            parameters.add(new ArrayList<SelectExpressionItem>(Arrays.asList((SelectExpressionItem[]) (body).getSelectItems().toArray())));
            parameters.add(body.getJoins());
            parameters.add(body.getGroupByColumnReferences());
            parameters.add(body.getHaving());

            if (body.getSelectItems().get(0) instanceof AllColumns)
                parameters.add(true);
            else
                parameters.add(false);

            parameters.add(body.getLimit());
            return parameters;
        }
        return parameters;

    }

    public static HelperImp getOperator(PlainSelect body) {
        //todo:work on subselect
        Table t = null;
        HelperImp op = null;
        boolean allCol = false;
        if (body.getFromItem() instanceof SubSelect) {
            SQLSelect.populateAliases(body);
            t = (Table) body.getFromItem();
            checkTableAlias(t);
            allCol = body.getSelectItems().get(0) instanceof AllColumns;
            String tableFile = Global.dataDir.toString() + File.separator + t.getName() + ".dat";


            HelperImp readOp = new ScanHelper(new File(tableFile), t);
            op = Execute.executeSelect(readOp,
                    t,
                    body.getWhere(),
                    body.getSelectItems(),
                    (ArrayList<Join>) body.getJoins(),
                    (ArrayList<Column>) body.getGroupByColumnReferences(),
                    body.getHaving(),
                    allCol,
                    body.getLimit());
            return op;
        } else {
            SQLSelect.populateAliases(body);
            t = (Table) body.getFromItem();
            checkTableAlias(t);
            allCol = body.getSelectItems().get(0) instanceof AllColumns;
            String tableFile = Global.dataDir.toString() + File.separator + t.getName() + ".dat";


            HelperImp readOp = new ScanHelper(new File(tableFile), t);
            op = Execute.executeSelect(readOp,
                    t,
                    body.getWhere(),
                    body.getSelectItems(),
                    (ArrayList<Join>) body.getJoins(),
                    (ArrayList<Column>) body.getGroupByColumnReferences(),
                    body.getHaving(),
                    allCol,
                    body.getLimit());
            return op;
        }
    }

    public static void checkTableAlias(Table t) {
        if (t.getAlias() == null) {
            t.setAlias(t.getName());
        }
        Global.tableAlias.put(t.getAlias(), t);

        if (!Global.tables.containsKey(t.getAlias())) {
            HashMap<String, Integer> tempSchema = Global.tables.get(t.getName());
            HashMap<String, Integer> newSchema = new HashMap<String, Integer>();
            for (String key : tempSchema.keySet()) {
                String[] temp = key.split("\\.");
                newSchema.put(t.getAlias() + "." + temp[1], tempSchema.get(key));
            }
            Global.tables.put(t.getAlias(), newSchema);
        }
    }

    public static void createSchema(ArrayList<SelectExpressionItem> selectItems, Table t) {
        HashMap<String, Integer> schema = new HashMap<String, Integer>();
        for (int i = 0; i < selectItems.size(); i++) {
            schema.put(selectItems.get(i).getExpression().toString(), i);
        }
        Global.tables.put(t.getAlias(), schema);
    }

    public String getResult() throws SQLException, IOException {
        SelectBody body = sql.getSelectBody();

        if (body instanceof PlainSelect) {
            HelperImp oper = getOperator((PlainSelect) body);
            Execute.print(oper);
        } else if (body instanceof Union) {
            //todo union
            List<PlainSelect> plainSelects = ((Union) body).getPlainSelects();
        }
        return "";
    }
}
