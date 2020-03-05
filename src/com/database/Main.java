package com.database;

import com.database.sql.SQLCreateTable;
import com.database.sql.SQLSelect;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length > 1) {
            File file = new File(args[0]);
            String str;
            try {
                Global.table_location = new File(args[1]);
                FileInputStream fis = new FileInputStream(file);
                byte[] data = new byte[(int) file.length()];
                int read = fis.read(data);
                fis.close();
                str = new String(data, StandardCharsets.UTF_8);
                String[] arrOfStr = str.split(";");
                for (String s : arrOfStr) {
                    if (!s.strip().equals("")) {
                        Reader input = new StringReader(s.toUpperCase());
                        CCJSqlParser parser = new CCJSqlParser(input);
                        try {
                            Statement stmt = parser.Statement();
                            if (stmt instanceof Select) {
                                try {
                                    new SQLSelect((Select) stmt).getResult();
                                } catch (Exception e) {
                                    System.out.println("SQL syntax error"); //$NON-NLS-1$
                                    e.printStackTrace();
                                }
                                System.out.println("=");
                            } else if (stmt instanceof CreateTable) {
                                new SQLCreateTable((CreateTable) stmt).getResult();
                            } else {
                                throw new ParseException("Only SELECT and CREATE TABLE statement is valid"); //$NON-NLS-1$
                            }
                        } catch (Exception e) {
                            System.out.println("SQL syntax error"); //$NON-NLS-1$
                            e.printStackTrace();
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            Global.table_location = new File(args[1]);

        } else {

            Scanner sc = new Scanner(System.in);
            String s;
            while (!(s = sc.nextLine()).isEmpty()) {
                Reader input = new StringReader(s.toUpperCase());
                CCJSqlParser parser = new CCJSqlParser(input);

                try {
                    Statement stmt = parser.Statement();
                    if (stmt instanceof Select) {
                        new SQLSelect((Select) stmt).getResult();
                        System.out.println("=");
                    } else if (stmt instanceof CreateTable) {
                        new SQLCreateTable((CreateTable) stmt).getResult();
                    } else {
                        throw new ParseException("Only SELECT and CREATE TABLE statement is valid"); //$NON-NLS-1$
                    }
                } catch (Exception e) {
                    System.out.println("SQL syntax error"); //$NON-NLS-1$
                    e.printStackTrace();
                }

            }
        }

    }
}
