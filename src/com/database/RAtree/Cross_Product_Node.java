package com.database.RAtree;

import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;

import java.util.LinkedHashMap;

public class Cross_Product_Node extends RA_Tree {

    public Cross_Product_Node(RA_Tree left, RA_Tree right) {
        this.left = left;
        this.right = right;
        left.setParent(this);
        right.setParent(this);
        schema = create_new_schema(right.getSchema(), left.getSchema());
    }

    LinkedHashMap<String, Integer> create_new_schema(LinkedHashMap<String, Integer> leftSchema, LinkedHashMap<String, Integer> rightSchema) {
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        LinkedHashMap<String, Integer> oldschema = leftSchema;
        int sizes = 0;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        sizes = newSchema.size();
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
        return "Cross_Product_Node{" +
                "lefttable=" + left.getSchema() +
                ", righttable=" + right.getSchema() +
                '}';
    }
}
