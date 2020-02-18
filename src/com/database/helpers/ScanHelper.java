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


public class ScanHelper implements HelperImp {
    File file = null;
    Iterator<CSVRecord> scan = null;
    BufferedReader br = null;
    Table table;

    public ScanHelper(File f, Table table) {
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
            System.out.println("ScanOperator1");
        }
    }

    @Override
    public Object[] read() {

        if (!scan.hasNext())
            return null;
        CSVRecord line = scan.next();

        if (line == null)
            return null;
        Object[] tuple = new Object[line.size()];
        ArrayList<String> dataType = Global.tableSchema.get(table.getName().toUpperCase());
        for (int i = 0; i < line.size(); i++) {
            switch (dataType.get(i).toUpperCase()) {
                case "INT":
                    tuple[i] = new LongValue(line.get(i));
                    break;
                case "DECIMAL":
                case "DOUBLE":
                    tuple[i] = new DoubleValue(line.get(i));
                    break;
                case "DATE":
                    tuple[i] = new DateValue(line.get(i));
                    break;
                case "CHAR":
                case "STRING":
                case "VARCHAR":
                    tuple[i] = new StringValue(line.get(i));
                    break;
                default: {
                    if (dataType.get(i).contains("CHAR")) {
                        tuple[i] = new StringValue(line.get(i));
                    }
                }
            }
        }
        return tuple;

    }

    @Override
    public Table getTable() {
        return table;
    }


}
