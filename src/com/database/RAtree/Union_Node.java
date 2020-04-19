package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Distinct_Iterator;
import com.database.helpers.Union_Iterator;

public class Union_Node extends RA_Tree {

    public Union_Node(RA_Tree left, RA_Tree right) {
        this.left = left;
        this.right = right;
        left.setParent(this);
        right.setParent(this);
        schema = left.getSchema();
    }

    public DB_Iterator get_iterator() {
        return new Distinct_Iterator(new Union_Iterator(
                left.get_iterator(),
                right.get_iterator()
        ));
    }

    @Override
    public String toString() {
        return "U";
    }
}
