package com.database.RAtree;

import com.database.Shared_Variables;
import com.database.helpers.DB_Iterator;
import com.database.helpers.Scan_Iterator;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;

public class Scan_Node extends RA_Tree {
    private final Table table;
    private final String tableFile;
    boolean flag;
    private long size;

    public Scan_Node(Table table, boolean flag) {
        this.table = table;
        this.flag = flag;
        LinkedHashMap<String, Integer> schemas = Shared_Variables.list_tables.get(table.getWholeTableName().toUpperCase());
        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
        int count = 0;
        for (String s : Shared_Variables.column_used) {
            if (schemas.containsKey(s)) {
                newSchema.put(s, count);
                count++;
            }
        }
        tableFile = Shared_Variables.table_location.toString() + File.separator + table.getName().toLowerCase() + ".dat";
        size = 0L;
        try {
            size = Files.size(Paths.get(tableFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        schema = newSchema;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public String getTableFile() {
        return tableFile;
    }

    public long getSize() {
        return size;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public DB_Iterator get_iterator() {
        return new Scan_Iterator(new File(tableFile), table, flag, schema);
    }

    @Override
    public String toString() {
        return table.getName();
    }
}
