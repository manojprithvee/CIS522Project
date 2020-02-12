package com.database.storage;

import java.io.File;
import java.util.Arrays;

public class Table {
    public Columns[] columns;
    public Rows[] rows;
    protected String name;

    public Table(String tableName, Columns[] attrs) {
        name = tableName;
        columns = new Columns[]{};
        if (attrs != null) {
            for (Columns attr : attrs) {
                addColumns(attr);
            }
        }
    }

    @Override
    public String toString() {
        String sql = "CREATE TABLE \"" + name + "\" (";
        int i = 0;
        for (Columns field : columns) {
            if (i > 0) {
                sql += ", ";
            }
            sql += field.toString();
            ++i;
        }
        sql += ")";
        return sql;
    }

    public boolean addColumns(Columns f) {
        columns = Arrays.copyOf(columns, columns.length + 1); //create new array from old array and allocate one more element
        columns[columns.length - 1] = f;
        return true;
    }

    public Columns[] getColumns() {
        Columns[] r = new Columns[columns.length];
        int i = 0;
        for (Columns a : columns) {
            r[i++] = a;
        }
        return r;
    }

    public boolean saveTable() {
        File file = new File("data/");
        file.mkdirs();
        return true;
    }

}
