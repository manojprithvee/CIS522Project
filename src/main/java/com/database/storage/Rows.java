package com.database.storage;

public class Rows {
    public int dataID = -1;
    private Object[] columns;

    public Rows(int dataID, int fields) {
        this(fields);
        this.dataID = dataID;
    }

    public Rows(int fields) {
        columns = new Object[fields];
    }

    public void set(int position, int value) {
        columns[position] = value;
    }

    public void set(int position, float value) {
        columns[position] = value;
    }

    public void set(int position, String value) {
        columns[position] = value;
    }

    public void set(int position, Object value) {
        columns[position] = value;
    }

    public Object get(int position) {
        return columns[position];
    }

    public int size() {
        return columns.length;
    }
}
