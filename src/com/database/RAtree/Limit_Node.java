package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Limit_Iterator;
import net.sf.jsqlparser.statement.select.Limit;

public class Limit_Node extends RA_Tree {
    private final Limit limit;

    public Limit_Node(RA_Tree left, Limit limit) {
        this.left = left;
        this.limit = limit;
        left.setParent(this);
        schema = left.getSchema();
    }

    public Limit getLimit() {
        return limit;
    }

    public DB_Iterator get_iterator() {
        return new Limit_Iterator(left.get_iterator(), limit);
    }

    @Override
    public String toString() {
        return "lim";
    }
}
