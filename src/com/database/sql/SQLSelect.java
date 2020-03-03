package com.database.sql;


import com.database.Execute;
import com.database.Global;
import com.database.helpers.DistinctItrator;
import com.database.helpers.ItratorImp;
import com.database.helpers.ScanItrator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class SQLSelect {
    private Select sql;

    public SQLSelect(Select stmt) {
        this.sql = stmt;
    }

    public static void populateAliases(SelectBody body) {


        ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) ((PlainSelect) body).getSelectItems();
        Global.alias = new HashMap<String, Expression>();
        for (SelectItem a : selectItems) {
            if ((a instanceof AllTableColumns) || (a instanceof AllColumns))
                return;
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

    public static ItratorImp getOperator(PlainSelect body) {
        Table t = null;
        ItratorImp op = null;
        boolean allCol = false;
        int i;
        i = 0;
        ArrayList<Table> joins = new ArrayList<Table>();
        ArrayList<String> tablenames = new ArrayList<>();
        tablenames.add(body.getFromItem().getAlias());
        Expression expressionjoin = null;
        if (body.getJoins() != null) {
            for (Join join : body.getJoins()) {
                if (join.getOnExpression() != null) {
                    expressionjoin = join.getOnExpression();
                }
                Table tx = (Table) join.getRightItem();
                if (tablenames.contains(tx.getName())) {
                    if (tx.getAlias() == null) {
                        tx.setAlias(String.format("%d", i));
                    }
                }
                checkTableAlias(tx);
                joins.add(tx);
                i += 1;
            }
        }
        Expression where;
        if (body.getFromItem() instanceof SubSelect) {
            t = new Table();
            if (body.getFromItem().getAlias() == null) {
                t.setName("SubQuery");
                t.setAlias("SubQuery");
            } else {
                t.setName(body.getFromItem().getAlias());
                t.setAlias(body.getFromItem().getAlias());
            }

            createSchema(((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getSelectItems(), t, ((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getFromItem());
            op = getOperator((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
            op = Execute.executeSelect(op,
                    t,
                    body.getWhere(),
                    expressionjoin,
                    body.getSelectItems(),
                    joins,
                    (ArrayList<Column>) body.getGroupByColumnReferences(),
                    body.getHaving(),
                    allCol,
                    body.getLimit());
            return op;
        } else {
            SQLSelect.populateAliases(body);
            t = (Table) body.getFromItem();
            checkTableAlias(t);
            allCol = ((body.getSelectItems().get(0) instanceof AllColumns));
            ArrayList<SelectItem> list = new ArrayList<>();
            String tableFile = Global.dataDir.toString() + File.separator + t.getName() + ".dat";
            ItratorImp readOp = new ScanItrator(new File(tableFile), t);
            op = Execute.executeSelect(readOp,
                    t,
                    body.getWhere(),
                    expressionjoin,
                    body.getSelectItems(),
                    joins,
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
            LinkedHashMap<String, Integer> tempSchema = Global.tables.get(t.getName());
            LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<String, Integer>();
            for (String key : tempSchema.keySet()) {
                String[] temp = key.split("\\.");
                newSchema.put(t.getAlias() + "." + temp[1], tempSchema.get(key));
            }
            Global.tables.put(t.getAlias(), newSchema);
        }
    }

    public static void createSchema(List<SelectItem> selectItems, Table t, FromItem fromItem) {
        LinkedHashMap<String, Integer> schema = new LinkedHashMap<String, Integer>();
        if ((selectItems.get(0) instanceof AllColumns) || (selectItems.get(0) instanceof AllTableColumns)) {
            Table table = (Table) fromItem;
            schema = (Global.tables.get(table.getName()));
        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                SelectExpressionItem abc = (SelectExpressionItem) selectItems.get(i);
                schema.put(abc.getExpression().toString(), i);
            }
        }
        Global.tables.put(t.getAlias(), schema);
    }

    public String getResult() throws SQLException, IOException {
        SelectBody body = sql.getSelectBody();

        if (body instanceof PlainSelect) {
            ItratorImp oper = getOperator((PlainSelect) body);
            Execute.dump(oper);
        } else if (body instanceof Union) {
            List<PlainSelect> plainSelects = ((Union) body).getPlainSelects();
            ItratorImp current = getOperator(plainSelects.get(0));
            current = new DistinctItrator(current);
            for (PlainSelect i : plainSelects.subList(1, plainSelects.size())) {
                current = Execute.executeUnion(current, new DistinctItrator(getOperator(i)));
            }
            Execute.dump(current);
        }
        return "";
    }
}
