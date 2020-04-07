package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Selection_Iterator;
import net.sf.jsqlparser.expression.Expression;

public class Select_Node extends RA_Tree {
    private final Expression where;

    public Select_Node(Expression where) {
        super();
        this.where = where;
    }

    public DB_Iterator get_iterator() {
        return new Selection_Iterator(
                left.get_iterator(),
                where);
    }

}
