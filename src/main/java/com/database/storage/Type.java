package com.database.storage;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Type {
    public static Serializable getClassForType(String type, String value) throws Exception {
        switch (type.toLowerCase()) {
            case "string":
                return value;
            case "int":
                return Integer.parseInt(value);
            case "float":
                return Float.parseFloat(value);
            case "double":
                return Double.parseDouble(value);
            case "Date":
                Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(value);
            case "long":
                return Long.parseLong(value);
            case "TimeStamp":
                return new Timestamp(Long.parseLong(value));
            default:
                throw new Exception("invalid Datatype " + type);
        }
    }
}
