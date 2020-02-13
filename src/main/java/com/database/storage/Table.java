package com.database.storage;


import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


public class Table {
    protected Columns[] columns = new Columns[]{};
    protected java.lang.String name;

    public Table(CreateTable sql) throws SQLException {
        File data = new File(java.lang.String.format("data/%s.dat", sql.getTable()));
        if (data.exists()) {
            throw new SQLException(java.lang.String.format("Table %s Already Exists", sql.getTable()));
        } else {
            try {
                data.createNewFile();
                name = sql.getTable().toString();
                List<ColumnDefinition> cols = sql.getColumnDefinitions();
                if (!cols.isEmpty()) {
                    for (ColumnDefinition column : sql.getColumnDefinitions()) {
                        java.lang.String sqlType = column.getColDataType().getDataType().toUpperCase();
                        if (!Type.exists(sqlType)) {
                            throw new SQLException("Unknown SQL type '" + sqlType + "'");
                        }
                        addColumns(new Columns(column.getColumnName(), sqlType));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Reader input = new StringReader("CREATE TABLE R (A int, B date, C string)");
        CCJSqlParser parser = new CCJSqlParser(input);
        try {
            Statement stmt = parser.Statement();
            System.out.println(new Table((CreateTable) stmt));
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    @Override
    public java.lang.String toString() {
        java.lang.String sql = "CREATE TABLE \"" + name + "\" (";
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

    public boolean saveTable() throws SQLException, IOException {
        // write object to file
        //todo: need to work on it to make table serializable
//        FileOutputStream fos = new FileOutputStream("Table.ser");
//        ObjectOutputStream oos = new ObjectOutputStream(this);
//        oos.writeObject(this);
//        oos.close();
//
//        // read object from file
//        FileInputStream fis = new FileInputStream("mybean.ser");
//        ObjectInputStream ois = new ObjectInputStream(fis);
//        MyBean result = (MyBean) ois.readObject();
//        ois.close();
        return true;
    }

}
