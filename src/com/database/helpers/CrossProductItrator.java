package com.database.helpers;

import com.database.Global;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class CrossProductItrator implements ItratorImp {

    ArrayList<Object[]> allTuples;
    int counter;
    ArrayList<ItratorImp> readOps;
    ArrayList<Table> joinTables;
    Table table;

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

        getData();
    }

    @Override
    public void reset() {
        counter = 0;
    }

    @Override
    public Object[] next() {
        Object[] temp = null;
        if (counter < allTuples.size()) {
            temp = allTuples.get(counter);
            counter++;
        }
        return temp;
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

    void getData() {
        HashMap<String, Integer> newSchema = new HashMap<String, Integer>();
        HashMap<String, Integer> tempSchema = new HashMap<String, Integer>();
        Object[] temp1 = readOps.get(0).next();
        Object[] temp2 = readOps.get(1).next();

        int size1 = Global.tables.get(joinTables.get(0).getAlias()).size();
        int size2 = Global.tables.get(joinTables.get(1).getAlias()).size();

        ArrayList<String> dataType = new ArrayList<String>();
        String newTableName = joinTables.get(0).getAlias() + "," + joinTables.get(1).getAlias();
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = updateSchema(newSchema, tempSchema, 0, 0, dataType);
        dataType = updateSchema(newSchema, tempSchema, size1, 1, dataType);
        Global.tables.put(newTableName, newSchema);
        Global.tableSchema.put(newTableName, dataType);

        int size = size1 + size2;
        while (temp1 != null) {
            Object[] toReturn1 = new Object[size1];
            for (int i = 0; i < size1; i++) {
                toReturn1[i] = temp1[i];
            }
            while (temp2 != null) {
                Object[] toReturn2 = new Object[size2];
                for (int j = 0; j < size2; j++) {
                    toReturn2[j] = temp2[j];
                }
                if (readOps.size() == 2) {
                    Object[] x = createTuple(toReturn1, toReturn2, size);
                    allTuples.add(x);
                }
                temp2 = readOps.get(1).next();
            }
            readOps.get(1).reset();
            temp2 = readOps.get(1).next();
            temp1 = readOps.get(0).next();
        }
    }


    public Object[] createTuple(Object[] toReturn1, Object[] toReturn2, int size) {
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