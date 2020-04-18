package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Grace_Join_Iterator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.LinkedHashMap;


public class Join_Node extends RA_Tree {
//    private final Table table;
    private final int size;
    DB_Iterator itrator;
    BinaryExpression expression;

    public Join_Node(RA_Tree left, RA_Tree right, BinaryExpression expression) {
        this.left = left;
        this.right = right;
        this.expression = expression;
        left.setParent(this);
        right.setParent(this);
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        size = newSchema.size();
        this.expression = expression;
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
        String c1 = ((Column) expression.getLeftExpression()).getWholeColumnName();
        String c2 = ((Column) expression.getRightExpression()).getWholeColumnName();
        Integer index = left.getSchema().get(c1);
        int leftIndex;
        int rightIndex;
        if (index == null) {
            leftIndex = left.getSchema().get(c2);
            rightIndex = right.getSchema().get(c1);

        } else {
            leftIndex = index;
            rightIndex = right.getSchema().get(c2);
        }
        itrator = new Grace_Join_Iterator(left, right, leftIndex, rightIndex);
        return itrator;
    }

    @Override
    public String toString() {
        return "Join_Node{" +
                "lefttable=" + left.getSchema() +
                ", righttable=" + right.getSchema() +
                '}';
    }
}