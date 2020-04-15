package com.database.helpers;

import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Scan_Iterator implements DB_Iterator {
    final Table table;
    private final boolean full;
    private final LinkedHashMap<String, Integer> newschema;
    private final HashMap<Integer, Integer> index;
    File file;
    BufferedReader br = null;
    Iterator scan = null;
    LinkedHashMap<String, Integer> schema;
    private List<CSVRecord> data;

    public Scan_Iterator(File f, Table table, boolean full, LinkedHashMap<String, Integer> schema) {
        this.file = f;
        this.table = table;
        this.full = full;
        this.schema = Shared_Variables.list_tables.get(table.getName().toUpperCase());
        this.newschema = schema;
        this.index = new HashMap<Integer, Integer>();
        for (String name : newschema.keySet())
            index.put(this.schema.get(name), newschema.get(name));
        Shared_Variables.current_schema = schema;
        reset();
    }

    @Override
    public void reset() {
        try {
            br = new BufferedReader(new FileReader(file));
            CSVParser parser = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter('|'));
            if (full) {
                if (data == null) data = parser.getRecords();
                scan = data.iterator();
            } else {
                parser = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter('|'));
                scan = parser.iterator();
            }
        } catch (IOException e) {
            System.out.println("error");
        }
    }

    @Override
    public Object[] next() {

        if (!scan.hasNext())
            return null;
        CSVRecord line = (CSVRecord) scan.next();
//        System.out.println(line);
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
                    row[id] = new LongValue(line.get(i));
                    break;
                case "DECIMAL":
                case "DOUBLE":
                    row[id] = new DoubleValue(line.get(i));
                    break;
                case "DATE":
                    row[id] = new DateValue(line.get(i));
                    break;
                default:
                    row[id] = new StringValue(line.get(i));
                    break;
            }
        }
//        System.out.println(Arrays.deepToString(row));
        return row;

    }


    @Override
    public Table getTable() {
        return table;
    }


}
