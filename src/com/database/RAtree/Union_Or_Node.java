package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Union_Iterator;

public class Union_Or_Node extends RA_Tree {

    public Union_Or_Node(RA_Tree left, RA_Tree right) {
        this.left = left;
        this.right = right;
        left.setParent(this);
        right.setParent(this);
        schema = left.getSchema();
    }

    public DB_Iterator get_iterator() {
        return new Union_Iterator(
                left.get_iterator(),
                right.get_iterator()
        );
    }
}
