package com.database.RAtree;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromItems_Builder implements FromItemVisitor {
    FromItem fromItem;
    RA_Tree current;

    public FromItems_Builder(FromItem fromItems) {
        this.fromItem = fromItems;
        fromItems.accept(this);
    }

    public RA_Tree getCurrent() {
        return current;
    }

    @Override
    public void visit(Table table) {
        current = new TableNode(null, table);
    }

    @Override
    public void visit(SubSelect subSelect) {
        Build_Tree Build_Tree = new Build_Tree(subSelect.getSelectBody());
        current = Build_Tree.getRoot();
    }

    @Override
    public void visit(SubJoin subJoin) {
        current = new FromItems_Builder(subJoin.getJoin().getRightItem()).getCurrent();
    }

}
