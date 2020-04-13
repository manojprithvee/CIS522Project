package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Selection_Iterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class Select_Node extends RA_Tree {
    private final Table table;
    public Expression where;

    public Select_Node(Expression where, Table table) {
        super();
        schema = Shared_Variables.current_schema;
        this.where = where;
        this.table = table;
    }

    public DB_Iterator get_iterator() {
        return new Selection_Iterator(
                left.get_iterator(),
                where, table);
    }

    @Override
    public String toString() {
        return "Select_Node{" +
                "where=" + where +
                '}';
    }
}
