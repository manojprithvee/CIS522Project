package com.database.sql;


import com.database.Execute;
import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Scan_Iterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class SQL_Select {
    private final Select sql;

    public SQL_Select(Select stmt) {
        this.sql = stmt;
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

    public static DB_Iterator getIterator(PlainSelect body) throws SQLException {
        Table t;
        DB_Iterator op;
        boolean allCol;
        int i;
        i = 0;
        ArrayList<Table> joins = new ArrayList<>();
        ArrayList<String> tablenames = new ArrayList<>();
        tablenames.add(((Table) body.getFromItem()).getName());
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
                    tablenames.add(tx.getName());
                }
                managetablerenaming(tx);
                joins.add(tx);
                i += 1;
            }
        }
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
            op = getIterator((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
            op = Execute.select_tree(op,
                    body.getWhere(), expressionjoin, body.getSelectItems(), t,
                    false, joins, body.getOrderByElements(), body.getLimit(), body.getGroupByColumnReferences(), body.getHaving()
            );
        } else {
            SQL_Select.manage_renaming(body);
            t = (Table) body.getFromItem();
            managetablerenaming(t);
            allCol = ((body.getSelectItems().get(0) instanceof AllColumns));
            String tableFile = Shared_Variables.table_location.toString() + File.separator + t.getName().toLowerCase() + ".dat";
            DB_Iterator readOp = new Scan_Iterator(new File(tableFile), t);
            op = Execute.select_tree(readOp,
                    body.getWhere(), expressionjoin, body.getSelectItems(), t,
                    allCol, joins, body.getOrderByElements(), body.getLimit(), body.getGroupByColumnReferences(), body.getHaving()
            );
        }
        return op;
    }

    public static void managetablerenaming(Table t) {
        if (t.getAlias() == null) {
            t.setAlias(t.getName());
        }

        if (!Shared_Variables.list_tables.containsKey(t.getAlias())) {
            LinkedHashMap<String, Integer> tempSchema = Shared_Variables.list_tables.get(t.getName());
            LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
            for (String key : tempSchema.keySet()) {
                String[] temp = key.split("\\.");
                newSchema.put(t.getAlias() + "." + temp[1], tempSchema.get(key));
            }
            Shared_Variables.list_tables.put(t.getAlias(), newSchema);
        }
    }

    public static void createSchema(List<SelectItem> selectItems, Table t, FromItem fromItem) {
        LinkedHashMap<String, Integer> schema = new LinkedHashMap<>();
        if ((selectItems.get(0) instanceof AllColumns) || (selectItems.get(0) instanceof AllTableColumns)) {
            Table table = (Table) fromItem;
            schema = (Shared_Variables.list_tables.get(table.getName()));
        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                SelectExpressionItem abc = (SelectExpressionItem) selectItems.get(i);
                if (abc.getAlias() != null) {
                    schema.put(abc.getAlias(), i);
                } else {
                    schema.put(abc.getExpression().toString(), i);
                }
            }
        }
        Shared_Variables.list_tables.put(t.getAlias(), schema);
    }

    public void getResult() throws SQLException {
        SelectBody body = sql.getSelectBody();
        DB_Iterator current = null;
        if (body instanceof PlainSelect) {
            current = getIterator((PlainSelect) body);
        } else if (body instanceof Union) {
            List<PlainSelect> plainSelects = ((Union) body).getPlainSelects();
            current = getIterator(plainSelects.get(0));
            for (PlainSelect i : plainSelects.subList(1, plainSelects.size())) {
                current = Execute.union_tree(current, getIterator(i));
            }
        }
        Execute.print(current);
    }
}
