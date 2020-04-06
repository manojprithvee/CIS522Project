package com.database.RAtree;

import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;

public class CartesianNode extends RA_Tree {
    public CartesianNode(RA_Tree o, RA_Tree left, RA_Tree right) {
        super(o);
    }

    public DB_Iterator get_iterator() {
        return new Cross_Product_Iterator(left.get_iterator(),
                right.get_iterator(), right.get_iterator().getTable(), left.get_iterator().getTable());
    }

}
