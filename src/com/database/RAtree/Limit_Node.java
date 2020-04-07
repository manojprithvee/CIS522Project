package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Limit_Iterator;
import net.sf.jsqlparser.statement.select.Limit;

public class Limit_Node extends RA_Tree {
    private final Limit limit;

    public Limit_Node(Limit limit) {
        super();
        this.limit = limit;
    }

    public DB_Iterator get_iterator() {
        return new Limit_Iterator(left.get_iterator(), limit);
    }
}
