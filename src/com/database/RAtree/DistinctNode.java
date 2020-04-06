package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Distinct_Iterator;

public class DistinctNode extends RA_Tree {
    public DistinctNode() {
        super();
    }

    public DB_Iterator get_iterator() {
        return new Distinct_Iterator(left.get_iterator());
    }
}
