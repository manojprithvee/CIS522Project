package com.database;

import com.database.sql.SQLCreateTable;
import com.database.sql.SQLSelect;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.Scanner;

public class Parser {
    public static void main(String[] args) {
        if (args.length > 1) {
            Global.dataDir = new File(args[1]);
        }
        Scanner sc = new Scanner(System.in);
        String s;

        //TODO: convert GET DATA FROM QUERY.TXT
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
