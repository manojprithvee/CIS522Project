package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Selection_Iterator;
import net.sf.jsqlparser.expression.Expression;

public class SelectNode extends RA_Tree {
    private final Expression where;

    public SelectNode(Expression where) {
        super();
        this.where = where;
    }

    public DB_Iterator get_iterator() {
        return new Selection_Iterator(left.get_iterator(), where);
    }

}
