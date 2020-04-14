package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Limit_Iterator;
import net.sf.jsqlparser.statement.select.Limit;

public class Limit_Node extends RA_Tree {
    private final Limit limit;

    public Limit_Node(RA_Tree left, Limit limit) {
        super();
        this.left = left;
        this.limit = limit;
        schema = left.getSchema();
    }

    public DB_Iterator get_iterator() {
        return new Limit_Iterator(left.get_iterator(), limit);
    }

    @Override
    public String toString() {
        return "Limit_Node{" +
                "limit=" + limit +
                '}';
    }
}
