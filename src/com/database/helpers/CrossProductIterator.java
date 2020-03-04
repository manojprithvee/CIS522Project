package com.database.helpers;

import com.database.Global;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class CrossProductIterator implements DB_Iterator {

    final ArrayList<DB_Iterator> readOps;
    final Table table;
    private final int size;
    private Object[] temp1;

    public CrossProductIterator(DB_Iterator oper, Table joinTables,
                                Table table) {
        readOps = new ArrayList<>();
        readOps.add(oper);

        String dataFileName = joinTables.getName() + ".dat";
        dataFileName = Global.table_location.toString() + File.separator + dataFileName.toLowerCase();
        try {
            readOps.add(new ScanIterator(new File(dataFileName), joinTables));
        } catch (NullPointerException e) {
            System.out.println("Null pointer exception in JoinOperator()");
        }
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        int size1 = Global.list_tables.get(table.getAlias()).size();
        int size2 = Global.list_tables.get(joinTables.getAlias()).size();
        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = table.getAlias() + "," + joinTables.getAlias();
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = updateSchema(newSchema, 0, table, dataType);
        dataType = updateSchema(newSchema, size1, joinTables, dataType);
        Global.list_tables.put(newTableName, newSchema);
        Global.schema_store.put(newTableName, dataType);
        temp1 = readOps.get(0).next();

        size = size1 + size2;
    }

    @Override
    public void reset() {
        readOps.get(0).reset();
        readOps.get(1).reset();
    }


    ArrayList<String> updateSchema(HashMap<String, Integer> newSchema,
                                   int size, Table table, ArrayList<String> dataType) {

        HashMap<String, Integer> tempSchema = Global.list_tables.get(table.getAlias());
        dataType.addAll(Global.schema_store.get(table.getName()));
        for (String col : tempSchema.keySet()) {
            newSchema.put(col, tempSchema.get(col) + size);
        }
        return dataType;
    }

    @Override
    public Object[] next() {
        Object[] temp2 = readOps.get(1).next();
        if (temp2 == null) {
            temp1 = readOps.get(0).next();
            if (temp1 == null)
                return null;
            readOps.get(1).reset();
            temp2 = readOps.get(1).next();
        }
        return createTuple(temp1, temp2);

    }


    public Object[] createTuple(Object[] toReturn1, Object[] toReturn2) {
        Object[] toReturn = new Object[size];
        int index = 0;
        for (Object o : toReturn1) {
            toReturn[index] = o;
            index++;
        }

        for (Object o : toReturn2) {
            toReturn[index] = o;
            index++;
        }
        return toReturn;
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}