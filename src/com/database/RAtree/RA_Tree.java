package com.database.RAtree;

import com.database.helpers.DB_Iterator;

public abstract class RA_Tree {
    RA_Tree left;
    RA_Tree right;
    RA_Tree parent;

    public RA_Tree() {

    }

    public static RA_Tree push(RA_Tree tree, DB_Iterator oper) {
        return null;
    }

    public static RA_Tree pop(RA_Tree tree, DB_Iterator oper) {
        return null;
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
}
