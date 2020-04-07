package com.database.helpers;

import com.database.Shared_Variables;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Cross_Product_Iterator implements DB_Iterator {

    Table table;
    private int size;
    private Table righttable, lefttable;
    DB_Iterator leftIterator, rightIterator;
    private Object[] temp1;

    public Cross_Product_Iterator(DB_Iterator leftIterator, DB_Iterator rightIterator, Table righttable,
                                  Table lefttable) {
        this.righttable = lefttable;
        this.lefttable = righttable;
        this.leftIterator = rightIterator;
        this.rightIterator = leftIterator;
        main(lefttable, righttable);
    }

    public Cross_Product_Iterator(DB_Iterator oper, Table righttable, Table lefttable) {
        leftIterator = oper;
        this.righttable = righttable;
        this.lefttable = lefttable;
        String dataFileName = righttable.getName() + ".dat";
        dataFileName = Shared_Variables.table_location.toString() + File.separator + dataFileName.toLowerCase();
        try {
            rightIterator = new Scan_Iterator(new File(dataFileName), righttable, true);
        } catch (NullPointerException e) {
            System.out.println("Null pointer exception in JoinOperator()");
        }
        main(lefttable, righttable);

    }

    private void main(Table lefttable, Table righttable) {
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = lefttable.getAlias() +
                "," +
                righttable.getAlias();
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = create_new_schema(newSchema, lefttable, righttable, dataType);
        Shared_Variables.list_tables.put(newTableName, newSchema);
        Shared_Variables.schema_store.put(newTableName, dataType);
        temp1 = leftIterator.next();
        size = newSchema.size();
        Shared_Variables.current_schema = (LinkedHashMap<String, Integer>) newSchema.clone();
    }

    @Override
    public void reset() {
        leftIterator.reset();
        rightIterator.reset();
        temp1 = leftIterator.next();
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

    @Override
    public Object[] next() {
        Object[] temp2 = rightIterator.next();
        if (temp2 == null) {
            temp1 = leftIterator.next();

            if (temp1 == null) {

                return null;
            }
            rightIterator.reset();
            temp2 = rightIterator.next();
        }
        return create_row(temp1, temp2);
    }


    public Object[] create_row(Object[] left, Object[] right) {
        Object[] new_row = new Object[size];
        int index = 0;
        if (left == null || right == null) return null;
        for (Object o : left) {
            new_row[index] = o;
            index++;
        }
        for (Object o : right) {
            new_row[index] = o;
            index++;
        }
        return new_row;
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}