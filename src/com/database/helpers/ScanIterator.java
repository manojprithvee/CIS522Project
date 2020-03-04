package com.database.helpers;

import com.database.Global;
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
import java.util.ArrayList;
import java.util.Iterator;


public class ScanIterator implements DB_Iterator {
    final Table table;
    File file;
    BufferedReader br = null;
    Iterator scan = null;

    public ScanIterator(File f, Table table) {
        this.file = f;
        this.table = table;
        reset();
    }

    @Override
    public void reset() {
        try {
            br = new BufferedReader(new FileReader(file));
            scan = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter('|')).iterator();
        } catch (IOException e) {
            System.out.println("error");
        }
    }

    @Override
    public Object[] next() {

        if (!scan.hasNext())
            return null;
        CSVRecord line = (CSVRecord) scan.next();

        if (line == null)
            return null;
        Object[] row = new Object[line.size()];
        ArrayList<String> dataType = Global.schema_store.get(table.getName().toUpperCase());
        for (int i = 0; i < line.size(); i++) {
            switch (dataType.get(i).toUpperCase()) {
                case "INT":
                    row[i] = new LongValue(line.get(i));
                    break;
                case "DECIMAL":
                case "DOUBLE":
                    row[i] = new DoubleValue(line.get(i));
                    break;
                case "DATE":
                    row[i] = new DateValue(line.get(i));
                    break;
                case "CHAR":
                case "STRING":
                case "VARCHAR":
                    row[i] = new StringValue(line.get(i));
                    break;
                default: {
                    if (dataType.get(i).contains("CHAR")) {
                        row[i] = new StringValue(line.get(i));
                    }
                }
            }
        }
        return row;

    }

    @Override
    public Table getTable() {
        return table;
    }


}
