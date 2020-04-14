package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Scan_Iterator;
import net.sf.jsqlparser.schema.Table;

import java.io.File;

public class Scan_Node extends RA_Tree {
    private final Table table;
    boolean flag;

    public Scan_Node(Table table, boolean flag) {
        super();
        this.table = table;
        this.flag = flag;
        schema = Shared_Variables.list_tables.get(table.getName().toUpperCase());
    }


    public Table getTable() {
        return table;
    }

    @Override
    public Scan_Iterator get_iterator() {
        String tableFile = Shared_Variables.table_location.toString() + File.separator + table.getName().toLowerCase() + ".dat";
        return new Scan_Iterator(new File(tableFile), table, flag, schema);
    }

    @Override
    public String toString() {
        return "Scan_Node{" +
                "table=" + table +
                '}';
    }
}
