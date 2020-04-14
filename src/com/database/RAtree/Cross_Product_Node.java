package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Cross_Product_Iterator;
import com.database.helpers.DB_Iterator;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Cross_Product_Node extends RA_Tree {
    Cross_Product_Iterator iterator;

    public Cross_Product_Node(RA_Tree left, RA_Tree right) {
        this.left = left;
        this.right = right;
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = String.valueOf(Shared_Variables.table);
        Shared_Variables.table += 1;
        Table table = new Table(newTableName, newTableName);
        table.setAlias(newTableName);
        create_new_schema(newSchema, right.getSchema(), left.getSchema(), dataType);
        Shared_Variables.list_tables.put(newTableName, newSchema);
        Shared_Variables.schema_store.put(newTableName, dataType);
        int size = newSchema.size();
        schema = newSchema;
    }

    ArrayList<String> create_new_schema(HashMap<String, Integer> newSchema, LinkedHashMap<String, Integer> leftSchema, LinkedHashMap<String, Integer> rightSchema, ArrayList<String> dataType) {
        LinkedHashMap<String, Integer> oldschema = leftSchema;
        int sizes = 0;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        sizes = newSchema.size();
        oldschema = rightSchema;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        return dataType;
    }

    public DB_Iterator get_iterator() {
        iterator = new Cross_Product_Iterator(
                left,
                right);
        return iterator;
    }

    @Override
    public String toString() {
        return "Cross_Product_Node{" +
                "lefttable=" + left.getSchema() +
                ", righttable=" + right.getSchema() +
                '}';
    }
}
