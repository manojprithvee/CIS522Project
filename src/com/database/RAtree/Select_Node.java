package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Selection_Iterator;
import net.sf.jsqlparser.expression.Expression;

public class Select_Node extends RA_Tree {
    public Expression where;

    public Expression getWhere() {
        return where;
    }

    public void setWhere(Expression where) {
        this.where = where;
    }

    public Select_Node(RA_Tree left, Expression where) {
        this.left = left;
        left.setParent(this);
        this.where = where;
        schema = left.getSchema();
    }

    public Select_Node(RA_Tree left, Expression where, boolean optimize) {
        super();
        this.left = left;
        this.where = where;
        schema = left.getSchema();
    }


    public DB_Iterator get_iterator() {
        return new Selection_Iterator(
                left.get_iterator(),
                where, schema);
    }

    @Override
    public String toString() {
        return "σ " + where;
    }
}
