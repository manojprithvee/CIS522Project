package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Distinct_Iterator;

public class Distinct_Node extends RA_Tree {
    public Distinct_Node(RA_Tree left) {
        super();
        this.left = left;
        schema = left.getSchema();
    }

    public DB_Iterator get_iterator() {
        return new Distinct_Iterator(left.get_iterator());
    }

    @Override
    public String toString() {
        return "Distinct_Node()";
    }
}
