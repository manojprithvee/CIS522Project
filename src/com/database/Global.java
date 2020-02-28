package com.database;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Global {
    public static File dataDir = new File("data/");
    public static HashMap<String, LinkedHashMap<String, Integer>> tables = new HashMap<>();
    public static HashMap<String, ArrayList<String>> tableSchema = new HashMap<>();
    public static HashMap<String, Expression> alias = new HashMap<>();
    public static HashMap<String, Table> tableAlias = new HashMap<>();
}
