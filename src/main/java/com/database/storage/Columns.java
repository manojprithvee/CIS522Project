package com.database.storage;

public class Columns {
    public String name;
    public String type;

    public Columns(String name, String type) {
        this.name = name;
        this.type = type.toUpperCase();
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
