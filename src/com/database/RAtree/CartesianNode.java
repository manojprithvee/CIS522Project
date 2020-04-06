package com.database.RAtree;

import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;
import net.sf.jsqlparser.schema.Table;

public class CartesianNode extends RA_Tree {
    public CartesianNode(RA_Tree left, RA_Tree right, Table lefttable, Table righttable) {
        this.left = left;
        this.right = right;
    }

    public DB_Iterator get_iterator() {
        return new Cross_Product_Iterator(
                left.get_iterator(),
                right.get_iterator(),
                right.get_iterator().getTable(),
                left.get_iterator().getTable());
    }

}
