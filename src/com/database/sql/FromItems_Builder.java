package com.database.sql;

import com.database.RAtree.RA_Tree;
import com.database.RAtree.Scan_Node;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.LinkedHashMap;

public class FromItems_Builder implements FromItemVisitor {
    FromItem fromItem;
    RA_Tree current;
    boolean flag;
    private LinkedHashMap<String, Integer> schema;

    public FromItems_Builder(FromItem fromItems) {
        this.fromItem = fromItems;
        this.flag = false;
        fromItems.accept(this);
    }

    public FromItems_Builder(FromItem fromItems, boolean flag) {
        this.fromItem = fromItems;
        this.flag = flag;
        fromItems.accept(this);
    }

    public RA_Tree getCurrent() {
        return current;
    }

    public LinkedHashMap<String, Integer> getSchema() {
        return schema;
    }

    @Override
    public void visit(SubSelect subSelect) {

        Build_Tree Build_Tree = new Build_Tree(subSelect.getSelectBody());
        current = Build_Tree.getRoot();
        if (subSelect.getAlias() != null) {
            LinkedHashMap<String, Integer> sschema = new LinkedHashMap<>();
            for (var i : Build_Tree.getSchema().keySet()) {
                sschema.put(subSelect.getAlias() + "." + i.split("\\.")[1], Build_Tree.getSchema().get(i));
            }
            current.setSchema(sschema);
        }
    }

    @Override
    public void visit(Table table) {
        current = new Scan_Node(table, flag);
    }


    @Override
    public void visit(SubJoin subJoin) {
        current = new FromItems_Builder(subJoin.getJoin().getRightItem()).getCurrent();
    }

}
