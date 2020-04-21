package com.database.helpers;

import com.database.NewBufferedReader;
import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

public class Scan_Iterator implements DB_Iterator {
    final Table table;
    private final LinkedHashMap<String, Integer> newschema;
    private final HashMap<Integer, Integer> index;
    private static String REGEX = "\\|";
    File file;
    NewBufferedReader br = null;
    LinkedHashMap<String, Integer> schema;
    private Pattern pattern;

    public Scan_Iterator(File f, Table table, boolean full, LinkedHashMap<String, Integer> schema) {
        this.file = f;
        this.table = table;
        this.schema = Shared_Variables.list_tables.get(table.getName().toUpperCase());
        this.newschema = schema;
        System.out.println(f);
        System.out.println(table);
        System.out.println(schema);
        this.index = new HashMap<>();
        for (String name : newschema.keySet())
            index.put(this.schema.get(name), newschema.get(name));
        reset();
    }

    public Scan_Iterator(File f, String table, LinkedHashMap<String, Integer> schema) {
        this.file = f;
        this.table = new Table(table);
        this.schema = schema;
        System.out.println(f);
        System.out.println(table);
        System.out.println(schema);
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
            br = new NewBufferedReader(new FileReader(file), 5242880);

//             scnr = new Scanner(file);
        } catch (IOException e) {
            System.out.println("error");
        }
        pattern = Pattern.compile(REGEX);
    }

    // optimized version of String split
    public String[] split(String string, char ch) {
        int off = 0;
        int next = 0;
        ArrayList<String> list = new ArrayList<>();
        while ((next = string.indexOf(ch, off)) != -1) {
            list.add(string.substring(off, next));
            off = next + 1;
        }
        if (off == 0)
            return new String[]{string};
        int resultSize = list.size();
        String[] result = new String[resultSize];
        return list.toArray(result);
    }

    @Override
    public Object[] next() {
        String raw_line = null;
        try {
            raw_line = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (raw_line == null)
            return null;

        String[] line = split(raw_line, '|');

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
