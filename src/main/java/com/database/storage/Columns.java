package com.database.storage;

public class Columns {
    public String name;
    public String type;
    public Object data;

    public Columns(String name, String type, String data) throws Exception {
        this.name = name;
        this.type = type.toLowerCase();
        this.data = Type.getClassForType(this.type, data);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return this.name + " " + this.type;
    }

//    public static void main(String[] args) throws Exception {
//        Columns column = new Columns("title","float","3452");
//        System.out.println(column.data);
//    }
}
