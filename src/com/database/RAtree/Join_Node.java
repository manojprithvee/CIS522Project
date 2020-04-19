package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Grace_Join_Iterator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;


public class Join_Node extends RA_Tree {
    BinaryExpression expression;

    public Join_Node(RA_Tree left, RA_Tree right, BinaryExpression expression) {
        this.left = left;
        this.right = right;
        this.expression = expression;
        left.setParent(this);
        right.setParent(this);
        this.expression = expression;
        schema = create_new_schema(left.getSchema(), right.getSchema());
    }

    public BinaryExpression getExpression() {
        return expression;
    }

    public void setExpression(BinaryExpression expression) {
        this.expression = expression;
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
        return new Grace_Join_Iterator(left, right, leftIndex, rightIndex);
    }

    @Override
    public String toString() {
        return "⋈ " + expression;
    }
}