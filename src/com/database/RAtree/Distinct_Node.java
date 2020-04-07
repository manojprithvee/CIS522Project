package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Distinct_Iterator;

public class Distinct_Node extends RA_Tree {
    public Distinct_Node() {
        super();
        schema = Shared_Variables.current_schema;
    }

    public DB_Iterator get_iterator() {
        return new Distinct_Iterator(left.get_iterator());
    }

    @Override
    public String toString() {
        return "Distinct_Node()";
    }
}
