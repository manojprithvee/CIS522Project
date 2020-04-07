package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;
import net.sf.jsqlparser.schema.Table;

public class Cross_Product_Node extends RA_Tree {
    private final Table lefttable, righttable;

    public Cross_Product_Node(RA_Tree left, RA_Tree right, Table lefttable, Table righttable) {
        this.left = left;
        this.right = right;
        this.lefttable = lefttable;
        this.righttable = righttable;
        if (this.lefttable.getAlias() == null) {
            this.lefttable.setAlias(this.lefttable.getName());
        }
        if (this.righttable.getAlias() == null) {
            this.righttable.setAlias(this.righttable.getName());
        }
        schema = Shared_Variables.current_schema;
    }

    public DB_Iterator get_iterator() {

        return new Cross_Product_Iterator(
                left.get_iterator(),
                right.get_iterator(),
                righttable,
                lefttable);
    }

    @Override
    public String toString() {
        return "Cross_Product_Node{" +
                "lefttable=" + lefttable +
                ", righttable=" + righttable +
                '}';
    }
}
