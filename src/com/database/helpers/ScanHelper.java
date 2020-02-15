package com.database.helpers;

import com.database.Global;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class ScanHelper implements HelperImp {
    File file = null;
    BufferedReader scan = null;
    Table table;

    public ScanHelper(File f, Table table) {
        this.file = f;
        this.table = table;
        reset();
    }

    @Override
    public void reset() {
        try {
            scan = new BufferedReader(new FileReader(file));
        } catch (IOException e) {
            System.out.println("ScanOperator1");
        }
    }

    @Override
    public Object[] read() {
        if (scan == null)
            return null;
        String line = "";

        try {
            line = scan.readLine();
            if (line == null)
                return null;
            String[] cols = line.split("\\|");
            Object[] tuple = new Object[cols.length];
            ArrayList<String> dataType = Global.tableSchema.get(table.getName().toUpperCase());
            for (int i = 0; i < cols.length; i++) {
                switch (dataType.get(i)) {
                    case "int":
                    case "INT":
                        tuple[i] = new LongValue(cols[i]);
                        break;
                    case "decimal":
                    case "DECIMAL":
                    case "DOUBLE":
                        tuple[i] = new DoubleValue(cols[i]);
                        break;
                    case "date":
                    case "DATE":
                        tuple[i] = new DateValue(" " + cols[i] + " ");
                        break;
                    case "char":
                    case "CHAR":
                    case "string":
                    case "STRING":
                    case "varchar":
                    case "VARCHAR":
                        tuple[i] = new StringValue(" " + cols[i] + " ");
                        break;
                    default: {
                        if (dataType.get(i).contains("CHAR") || dataType.get(i).contains("char")) {
                            tuple[i] = new StringValue(" " + cols[i] + " ");
                        }
                    }
                }
            }
            return tuple;
        } catch (IOException e) {
            System.out.println("IOException in ReadOperator.readOneTuple()");
        }
        return null;

    }

    @Override
    public Table getTable() {
        return table;
    }


}
