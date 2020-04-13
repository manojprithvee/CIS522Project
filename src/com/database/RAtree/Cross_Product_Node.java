package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Cross_Product_Node extends RA_Tree {
    private final Table righttable;
    private final Table lefttable;
    private final Table table;
    private final int size;
    Cross_Product_Iterator iterator;

    public Cross_Product_Node(RA_Tree left, RA_Tree right) {
        this.left = left;
        this.right = right;
        this.righttable = left.get_iterator().getTable();
        this.lefttable = right.get_iterator().getTable();
        if (righttable.getAlias() == null) righttable.setAlias(righttable.getName());
        if (lefttable.getAlias() == null) lefttable.setAlias(lefttable.getName());
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = String.valueOf(Shared_Variables.table);
        Shared_Variables.table += 1;
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = create_new_schema(newSchema, lefttable, righttable, dataType);
        Shared_Variables.list_tables.put(newTableName, newSchema);
        Shared_Variables.schema_store.put(newTableName, dataType);
        size = newSchema.size();
        Shared_Variables.current_schema = (LinkedHashMap<String, Integer>) newSchema.clone();
        schema = Shared_Variables.current_schema;
    }

    ArrayList<String> create_new_schema(HashMap<String, Integer> newSchema, Table lefttable, Table righttable, ArrayList<String> dataType) {
        LinkedHashMap<String, Integer> oldschema = Shared_Variables.list_tables.get(lefttable.getAlias());
        dataType.addAll(Shared_Variables.schema_store.get(lefttable.getName()));
        int sizes = 0;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        sizes = newSchema.size();
        oldschema = Shared_Variables.list_tables.get(righttable.getAlias());
//        System.out.println(oldschema);
        dataType.addAll(Shared_Variables.schema_store.get(righttable.getName()));
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        return dataType;
    }

    public DB_Iterator get_iterator() {
        iterator = new Cross_Product_Iterator(
                left,
                right);
//        schema = Shared_Variables.current_schema;
        return iterator;
    }

    @Override
    public String toString() {
        return "Cross_Product_Node{" +
                "lefttable=" + left.get_iterator().getTable().getName() +
                ", righttable=" + right.get_iterator().getTable().getName() +
                '}';
    }
}
