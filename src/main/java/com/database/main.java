package com.database;
import com.database.sql.SQLCreateTable;
import com.database.sql.SQLSelect;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Union;
import org.apache.commons.csv.CSVParser;
import net.sf.jsqlparser.parser.CCJSqlParser;


import java.io.Reader;
import java.io.StringReader;
import java.util.Scanner;

public class main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String s;
        while(!(s = sc.nextLine()).isEmpty()) {
            Reader input = new StringReader(s);
            CCJSqlParser parser = new CCJSqlParser(input);
            String result;
            try {
                Statement stmt = parser.Statement();
                if (stmt instanceof Select) {
                    result = new SQLSelect((Select) stmt).getResult();
                }
                else if (stmt instanceof CreateTable){
                    result = new SQLCreateTable((CreateTable) stmt).getResult();
                }
                else {
                    throw new ParseException("Only SELECT statement is valid"); //$NON-NLS-1$
                }
                System.out.println(result);
            }
            catch (Exception e) {
                System.out.println("SQL syntax error"); //$NON-NLS-1$
            }

        }

    }
}
