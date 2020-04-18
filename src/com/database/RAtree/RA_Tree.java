package com.database.RAtree;

import com.database.helpers.DB_Iterator;

import java.util.LinkedHashMap;

public abstract class RA_Tree implements Cloneable {
    RA_Tree left;
    RA_Tree right;
    RA_Tree parent;
    LinkedHashMap<String, Integer> schema;

    public RA_Tree() {

    }

    public RA_Tree getParent() {
        return parent;
    }

    public void setParent(RA_Tree parent) {
        this.parent = parent;
    }

    public RA_Tree getLeft() {
        return left;
    }

    public void setLeft(RA_Tree left) {
        this.left = left;
    }

    public abstract DB_Iterator get_iterator();

    public RA_Tree getRight() {
        return right;
    }

    public void setRight(RA_Tree right) {
        this.right = right;
    }

    public LinkedHashMap<String, Integer> getSchema() {
        return schema;
    }

    public void setSchema(LinkedHashMap<String, Integer> schema) {
        this.schema = schema;
    }
}
