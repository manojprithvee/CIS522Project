package com.database.RAtree;

import net.sf.jsqlparser.expression.Expression;

public class SelectNode extends RA_Tree {
    public SelectNode(RA_Tree projectTree, Expression where) {
        super(projectTree);
    }
}
