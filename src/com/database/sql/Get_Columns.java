package com.database.sql;

import com.database.Shared_Variables;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

public class Get_Columns implements SelectVisitor, SelectItemVisitor, FromItemVisitor, ExpressionVisitor {
    private static final String WILDCARD = "*";
    Set<String> columns = new HashSet<String>(), tables = new HashSet<String>(), wildcards = new HashSet<String>();
    private Map<String, String> aliases = new HashMap<String, String>();

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {
        if (function.getParameters() != null) {
            if (function.getParameters().getExpressions() != null) {
                for (Object expression : function.getParameters().getExpressions())
                    ((Expression) expression).accept(this);
            }
        }
    }

    @Override
    public void visit(InverseExpression inverseExpression) {

    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }

    @Override
    public void visit(LongValue longValue) {

    }

    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(BooleanValue booleanValue) {

    }

    @Override
    public void visit(StringValue stringValue) {

    }

    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getLeftExpression().accept(this);
        addition.getRightExpression().accept(this);
    }

    @Override
    public void visit(Division division) {
        division.getRightExpression().accept(this);
        division.getRightExpression().accept(this);
        division.getRightExpression().accept(this);
        division.getRightExpression().accept(this);
        division.getRightExpression().accept(this);
        division.getRightExpression().accept(this);
        division.getRightExpression().accept(this);
        division.getLeftExpression().accept(this);
        division.getRightExpression().accept(this);
    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        multiplication.getRightExpression().accept(this);
    }

    @Override
    public void visit(Subtraction subtraction) {
        subtraction.getLeftExpression().accept(this);
        subtraction.getRightExpression().accept(this);
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        orExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(Between between) {

    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        greaterThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        greaterThanEquals.getRightExpression().accept(this);
    }

    @Override
    public void visit(InExpression inExpression) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {
        likeExpression.getLeftExpression().accept(this);
        likeExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        notEqualsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(Column column) {
        if (wildcards.contains(WILDCARD)) return;

        if (!column.getTable().toString().equals("null")) {
            String tableName = column.getTable().getWholeTableName().toLowerCase();
            if (aliases.containsKey(tableName)) {
                column.getTable().setName(aliases.get(tableName));
            }
        }

        for (String table : tables) {
//            System.out.println(Shared_Variables.list_tables.get(table.toUpperCase()));
            String temp = get(Shared_Variables.list_tables.get(table.toUpperCase()), column);
            if (!temp.equals("")) {
                column.setTable(new Table(null, table));
                columns.add(temp);
                break;
            }
        }
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
        if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
        if (caseExpression.getWhenClauses() != null) {
            for (Object object : caseExpression.getWhenClauses()) {
                WhenClause when = (WhenClause) object;
                when.accept(this);
            }
        }
    }

    @Override
    public void visit(WhenClause whenClause) {
        if (whenClause.getThenExpression() != null) whenClause.getThenExpression().accept(this);
        if (whenClause.getWhenExpression() != null) whenClause.getWhenExpression().accept(this);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {
        concat.getLeftExpression().accept(this);
        concat.getRightExpression().accept(this);
    }

    @Override
    public void visit(Matches matches) {
        matches.getLeftExpression().accept(this);
        matches.getRightExpression().accept(this);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        bitwiseAnd.getLeftExpression().accept(this);
        bitwiseAnd.getRightExpression().accept(this);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        bitwiseOr.getLeftExpression().accept(this);
        bitwiseOr.getRightExpression().accept(this);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        bitwiseXor.getLeftExpression().accept(this);
        bitwiseXor.getRightExpression().accept(this);
    }

    @Override
    public void visit(Table table) {
        String name = table.getWholeTableName().toUpperCase();
        if (table.getAlias() != null) aliases.put(table.getAlias().toUpperCase(), name);
        tables.add(name);
    }

    @Override
    public void visit(SubSelect subSelect) {
        Get_Columns extractor = new Get_Columns();
        subSelect.getSelectBody().accept(extractor);
        columns.addAll(extractor.columns);
    }

    @Override
    public void visit(SubJoin subJoin) {
        if (subJoin.getJoin() != null) {
            Join join = subJoin.getJoin();
            if (join.getOnExpression() != null) join.getOnExpression().accept(this);
            if (join.getRightItem() != null) join.getRightItem().accept(this);
        }
        if (subJoin.getLeft() != null) subJoin.getLeft().accept(this);
    }

    @Override
    public void visit(AllColumns allColumns) {
        wildcards.add(WILDCARD);
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        String name = allTableColumns.getTable().getWholeTableName().toLowerCase();
        if (aliases.containsKey(name)) name = aliases.get(name);
        wildcards.add(name);
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        // FROM
        plainSelect.getFromItem().accept(this);

        // JOIN
        if (plainSelect.getJoins() != null) {
            for (Object object : plainSelect.getJoins()) {
                Join join = (Join) object;
                if (join.getOnExpression() != null) join.getOnExpression().accept(this);
                if (join.getRightItem() != null) join.getRightItem().accept(this);
            }
        }

        // SELECT
        if (plainSelect.getSelectItems() != null) {
            for (Object object : plainSelect.getSelectItems()) {
                SelectItem item = (SelectItem) object;
                item.accept(this);
            }
        }

        // WHERE
        if (plainSelect.getWhere() != null) plainSelect.getWhere().accept(this);

        // GROUP BY
        if (plainSelect.getGroupByColumnReferences() != null) {
            for (Object object : plainSelect.getGroupByColumnReferences()) {
                Expression expression = (Expression) object;
                expression.accept(this);
            }
        }

        // HAVING
        if (plainSelect.getHaving() != null) plainSelect.getHaving().accept(this);

        // ORDER BY
        if (plainSelect.getOrderByElements() != null) {
            for (Object object : plainSelect.getOrderByElements()) {
                OrderByElement element = (OrderByElement) object;
                element.getExpression().accept(this);
            }
        }

        // Handle wildcards
        for (String table : tables) {
            if (wildcards.contains(WILDCARD) || wildcards.contains(table)) {
                for (String column : Shared_Variables.list_tables.get(table.toUpperCase()).keySet())
                    columns.add(column.toUpperCase());
            }
        }

    }

    @Override
    public void visit(Union union) {
        Get_Columns extractor = new Get_Columns();
        union.accept(extractor);
        columns.addAll(extractor.columns);
    }

    public String get(LinkedHashMap structure, Column main_column) {
        String table;
        String id = "";
        if ((main_column.getTable() != null) && (main_column.getTable().getName() != null)) {
            table = main_column.getTable().getName();
            if (!structure.containsKey(table + "." + main_column.getColumnName()))
                id = columnchange(structure, id, main_column.getTable() + "." + main_column.getColumnName());
            else id = table + "." + main_column.getColumnName();
        } else if (!Shared_Variables.rename.containsKey(main_column.getColumnName()))
            id = columnchange(structure, id, main_column.getColumnName());
        else if (structure.containsKey(main_column.getColumnName()))
            id = main_column.getColumnName();
        else if (structure.containsKey(Shared_Variables.rename.get(main_column.getColumnName()).toString()))
            id = Shared_Variables.rename.get(main_column.getColumnName()).toString();
        else id = columnchange(structure, id, main_column.getColumnName());
        return id;
    }

    public String columnchange(LinkedHashMap structure, String id, String columnName) {
        for (Iterator<String> iterator = structure.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(columnName)) id = column;
        }
        return id;
    }
}
