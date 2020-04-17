package com.database;

import com.database.helpers.Scan_Iterator;
import net.sf.jsqlparser.expression.Expression;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

public class Shared_Variables {
    public static final HashMap<String, ArrayList<String>> schema_store = new HashMap<>();
    public static final HashMap<String, LinkedHashMap<String, Integer>> list_tables = new HashMap<>();
    public static LinkedHashMap<String, Integer> current_schema = new LinkedHashMap<String, Integer>();
    public static LinkedHashMap<String, Scan_Iterator> Sub_Select = new LinkedHashMap<String, Scan_Iterator>();
    public static Set<String> column_used;
    public static int table = 0;
    public static File table_location = new File("data/");
    public static HashMap<String, Expression> rename = new HashMap<>();
}
