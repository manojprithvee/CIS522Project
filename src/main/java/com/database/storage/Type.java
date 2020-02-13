package com.database.storage;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Type {
    public static Serializable getClassForType(String type, String value) throws Exception {
        switch (type.toUpperCase()) {
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return value;
            case "INT":
            case "LONG":
                return Long.parseLong(value);
            case "FLOAT":
                return Float.parseFloat(value);
            case "DOUBLE":
                return Double.parseDouble(value);
            case "DATE":
                Date date = new SimpleDateFormat("yyyy-MM-dd").parse(value);
                return date;
            case "TIMESTAMP":
                return new Timestamp(Long.parseLong(value));
            default:
                return null;
        }
    }

    public static boolean exists(String type) {
        List<String> arr = Arrays.asList("STRING", "VARCHAR", "CHAR", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP");
        return arr.contains(type);
    }
}
