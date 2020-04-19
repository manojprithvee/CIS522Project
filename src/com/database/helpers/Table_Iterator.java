package com.database.helpers;

import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Table_Iterator implements DB_Iterator {
    final Table table;
    private final boolean full;
    private final LinkedHashMap<String, Integer> newschema;
    private final HashMap<Integer, Integer> index;
    File file;
    BufferedReader br = null;
    Iterator scan = null;
    LinkedHashMap<String, Integer> schema;
    private List<CSVRecord> data;

    public Table_Iterator(File f, Table table, boolean full, LinkedHashMap<String, Integer> schema) {
        this.file = f;
        this.table = table;
        this.full = full;
        this.schema = Shared_Variables.list_tables.get(table.getName().toUpperCase());
        this.newschema = schema;
        this.index = new HashMap<>();
        for (String name : newschema.keySet())
            index.put(this.schema.get(name), newschema.get(name));
        Shared_Variables.current_schema = schema;
        reset();
    }

    public Table_Iterator(File f, String table, LinkedHashMap<String, Integer> schema) {
        this.file = f;
        this.full = false;
        this.table = new Table(table);
        this.schema = schema;
        reset();
        index = new HashMap<>();
        int count = 0;
        for (String name : schema.keySet()) {
            index.put(count, count);
            count++;
        }
        newschema = null;
    }

    @Override
    public void reset() {
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (IOException e) {
            System.out.println("error");
        }
    }

    @Override
    public Object[] next() {

        String raw_line = null;
        try {
            raw_line = br.readLine();
            if (raw_line == null)
                return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] line = raw_line.split("\\|");
        if (line == null)
            return null;
        Object[] row;
        ArrayList<String> dataType;

        dataType = Shared_Variables.schema_store.get(table.getName().toUpperCase());
        row = new Object[index.size()];

        for (int i = 0; i < schema.size(); i++) {
            Integer id = index.get(i);
            if (id == null) continue;
            switch (dataType.get(i).toUpperCase()) {
                case "INT":
                    row[id] = new LongValue(line[i]);
                    break;
                case "DECIMAL":
                case "DOUBLE":
                    row[id] = new DoubleValue(line[i]);
                    break;
                case "DATE":
                    row[id] = new DateValue(line[i]);
                    break;
                default:
                    row[id] = new StringValue(line[i]);
                    break;
            }
        }
        return row;
    }


    @Override
    public Table getTable() {
        return table;
    }


}
