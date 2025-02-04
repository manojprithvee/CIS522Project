package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Order_By_Iterator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

public class Order_By_Node extends RA_Tree {
    private final List<OrderByElement> orderByElements;
    private final Table table;

    public Order_By_Node(RA_Tree left, List<OrderByElement> orderByElements, Table t) {
        this.left = left;
        left.setParent(this);
        this.orderByElements = orderByElements;
        schema = left.getSchema();
        table = t;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public Table getTable() {
        return table;
    }

    public DB_Iterator get_iterator() {
        return new Order_By_Iterator(left, orderByElements);
    }

    @Override
    public String toString() {
        return 'τ' + " " + orderByElements;
    }
}
