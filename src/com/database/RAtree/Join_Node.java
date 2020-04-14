package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.Grace_Join_Iterator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;


public class Join_Node extends RA_Tree {
    private final Table table;
    private final int size;
    Grace_Join_Iterator itrator;
    Expression expression;

    public Join_Node(RA_Tree left, RA_Tree right, Expression expression) {
        this.left = left;
        this.right = right;
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = String.valueOf(Shared_Variables.table);
        Shared_Variables.table += 1;
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = create_new_schema(newSchema, left.getSchema(), right.getSchema(), dataType);
        Shared_Variables.list_tables.put(newTableName, newSchema);
        Shared_Variables.schema_store.put(newTableName, dataType);
        size = newSchema.size();
        this.expression = expression;
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

    public Grace_Join_Iterator get_iterator() {
        itrator = new Grace_Join_Iterator(left, right, (BinaryExpression) expression);
        schema = Shared_Variables.current_schema;
        return itrator;
    }

    @Override
    public String toString() {
        return "Join_Node{" +
                "lefttable=" + left.getSchema() +
                ", righttable=" + right.getSchema() +
                '}';
    }
}