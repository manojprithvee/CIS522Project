package com.database.helpers;

import com.database.Global;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class CrossProductItrator implements ItratorImp {

    private final int size;
    private int size2, size1;
    ArrayList<Object[]> allTuples;
    int counter;
    ArrayList<ItratorImp> readOps;
    ArrayList<Table> joinTables;
    Table table;
    private Object[] temp1, temp2;

    public CrossProductItrator(ItratorImp oper, Table joinTables,
                               Table table) {
        counter = 0;
        readOps = new ArrayList<ItratorImp>();
        this.joinTables = new ArrayList<Table>();
        allTuples = new ArrayList<Object[]>();
        readOps.add(oper);
        this.joinTables.add(table);
        this.joinTables.add(joinTables);

        String dataFileName = joinTables.getName() + ".dat";
        dataFileName = Global.dataDir.toString() + File.separator + dataFileName;
        try {
            readOps.add(new ScanItrator(new File(dataFileName), joinTables));
        } catch (NullPointerException e) {
            System.out.println("Null pointer exception in JoinOperator()");
        }
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<String, Integer>();
        LinkedHashMap<String, Integer> tempSchema = new LinkedHashMap<String, Integer>();
        size1 = Global.tables.get(this.joinTables.get(0).getAlias()).size();
        size2 = Global.tables.get(this.joinTables.get(1).getAlias()).size();
        ArrayList<String> dataType = new ArrayList<String>();
        String newTableName = this.joinTables.get(0).getAlias() + "," + this.joinTables.get(1).getAlias();
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = updateSchema(newSchema, tempSchema, 0, 0, dataType);
        dataType = updateSchema(newSchema, tempSchema, size1, 1, dataType);
        Global.tables.put(newTableName, newSchema);
        Global.tableSchema.put(newTableName, dataType);
        temp1 = readOps.get(0).next();

        size = size1 + size2;
    }

    @Override
    public void reset() {
        counter = 0;
    }


    ArrayList<String> updateSchema(HashMap<String, Integer> newSchema, HashMap<String, Integer> tempSchema,
                                   int size, int index, ArrayList<String> dataType) {

        tempSchema = Global.tables.get(joinTables.get(index).getAlias());
        dataType.addAll(Global.tableSchema.get(joinTables.get(index).getName()));
        for (String col : tempSchema.keySet()) {
            newSchema.put(col, tempSchema.get(col) + size);
        }
        return dataType;
    }
    @Override
    public Object[] next() {
        temp2 = readOps.get(1).next();
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
        for (int i = 0; i < toReturn1.length; i++) {
            toReturn[index] = toReturn1[i];
            index++;
        }

        for (int i = 0; i < toReturn2.length; i++) {
            toReturn[index] = toReturn2[i];
            index++;
        }
        return toReturn;
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}