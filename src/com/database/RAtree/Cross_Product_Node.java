package com.database.RAtree;

import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

public class Cross_Product_Node extends RA_Tree {

    public Cross_Product_Node(RA_Tree left, RA_Tree right) {
        this.left = left;
        this.right = right;
        left.setParent(this);
        right.setParent(this);
        schema = create_new_schema(left.getSchema(), right.getSchema());
    }

    LinkedHashMap<String, Integer> create_new_schema(LinkedHashMap<String, Integer> leftSchema, LinkedHashMap<String, Integer> rightSchema) {
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        LinkedHashMap<String, Integer> oldschema = leftSchema;
        int sizes = 0;
        Set<Integer> a = new HashSet<>();
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
            a.add(oldschema.get(col) + sizes);
        }
        sizes = a.size();
        oldschema = rightSchema;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        return newSchema;
    }

    public DB_Iterator get_iterator() {
        return new Cross_Product_Iterator(
                left,
                right);
    }

    @Override
    public String toString() {
        return "x";
    }
}
