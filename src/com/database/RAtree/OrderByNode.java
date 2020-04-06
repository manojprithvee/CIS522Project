package com.database.RAtree;

import com.database.helpers.DB_Iterator;
import com.database.helpers.Order_By_Iterator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

public class OrderByNode extends RA_Tree {
    private final List<OrderByElement> orderByElements;
    private final Table table;

    public OrderByNode(List<OrderByElement> orderByElements, Table t) {
        super();
        this.orderByElements = orderByElements;
        table = t;
    }

    public DB_Iterator get_iterator() {
        return new Order_By_Iterator(left.get_iterator(), orderByElements);
    }
}
