package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Scan_Iterator;
import net.sf.jsqlparser.schema.Table;

import java.io.File;

public class Scan_Node extends RA_Tree {
    private final Table table;

    public Scan_Node(Table table) {
        super();
        this.table = table;
    }

    @Override
    public DB_Iterator get_iterator() {
        String tableFile = Shared_Variables.table_location.toString() + File.separator + table.getName().toLowerCase() + ".dat";

        return new Scan_Iterator(new File(tableFile), table);
    }
}
