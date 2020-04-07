package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Order_By_Iterator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

public class Order_By_Node extends RA_Tree {
    private final List<OrderByElement> orderByElements;
    private final Table table;

    public Order_By_Node(List<OrderByElement> orderByElements, Table t) {
        super();
        this.orderByElements = orderByElements;
        schema = Shared_Variables.current_schema;
        table = t;
    }

    public DB_Iterator get_iterator() {
        return new Order_By_Iterator(left.get_iterator(), orderByElements);
    }

    @Override
    public String toString() {
        return "Order_By_Node{" +
                "orderByElements=" + orderByElements +
                ", table=" + table +
                '}';
    }
}
