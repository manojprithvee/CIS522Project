package com.database;

import com.database.sql.Sql_Parse;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length > 1) {
            File file = new File(args[0]);
            String str;
            try {
                Shared_Variables.table_location = new File(args[1]);
                FileInputStream fis = new FileInputStream(file);
                byte[] data = new byte[(int) file.length()];
                fis.read(data);
                fis.close();
                str = new String(data, StandardCharsets.UTF_8);
                String[] arrOfStr = str.split(";");
                for (String s : arrOfStr) {
                    if (!s.strip().equals("")) {
                        Reader input = new StringReader(s);
                        CCJSqlParser parser = new CCJSqlParser(input);
                        try {
//                            long startTime = System.currentTimeMillis();
                            Statement stmt = parser.Statement();
                            stmt.accept(new Sql_Parse());
//                            long stopTime = System.currentTimeMillis();
//                            System.out.println(stopTime - startTime);
                        } catch (Exception e) {
                            System.out.println("SQL syntax error");
                            e.printStackTrace();
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            Shared_Variables.table_location = new File(args[1]);

        } else {

            Scanner sc = new Scanner(System.in);
            String s;
            while (!(s = sc.nextLine()).isEmpty()) {
                Reader input = new StringReader(s.toUpperCase());
                CCJSqlParser parser = new CCJSqlParser(input);

                try {
                    Statement stmt = parser.Statement();
                    stmt.accept(new Sql_Parse());
                } catch (Exception e) {
                    System.out.println("SQL syntax error");
                    e.printStackTrace();
                }

            }
        }
        new File(Shared_Variables.table_location.toString() + File.separator + "temp.dat").delete();
    }
}
