package com.github.kspatola.mapper;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.util.IOUtils;

import com.github.kspatola.exception.InvalidCellValueException;
import com.github.kspatola.mapper.rules.ColumnConstraint;
import com.github.kspatola.mapper.rules.ColumnType;

/**
*
* @author Kelvin Spátola
*/
public abstract class Mapper {
    protected final Map<String, ColumnType> params = new LinkedHashMap<>();
    protected final Map<String, ColumnConstraint> constraints = new LinkedHashMap<>();
    protected StringBuilder insertStatement;
    protected String tableName;
    protected int columnCount;
    protected boolean checkErrors;
    
    static protected Connection conn;
    protected PreparedStatement statement;
    
    static protected int BATCH_SIZE = 10_000;

    // CONSTRUCTOR
    public Mapper(Connection connection, boolean checkErrors) throws SQLException {
        Mapper.conn = connection;
        conn.setAutoCommit(false);
        
        checkErrors(checkErrors);
        IOUtils.setByteArrayMaxOverride(Integer.MAX_VALUE - 8);
    }

    public final Mapper mapTable(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public final Mapper column(String param, ColumnType type) {
        return column(param, type, ColumnConstraint.NOT_NULL);
    }

    public final Mapper column(String param, ColumnType type, ColumnConstraint constraint) {
        if (param == null) {
            throw new NullPointerException("Column name cannot be null");
        }
        if (param.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be blank");
        }
        if (params.containsKey(param)) {
            throw new IllegalStateException("Duplicate column name: " + param);
        }
        if (constraint == ColumnConstraint.PRIMARY_KEY && constraints.containsValue(constraint)) {
            throw new IllegalStateException("Cannot assign two primary keys");
        }
        params.put(param, type);
        constraints.put(param, constraint);
        columnCount++;
        return this;
    }

    public final void buildStatement() throws SQLException {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalStateException("Missing a table reference. Call method insertInto(String tableName)");
        }
        if (params.isEmpty()) {
            throw new IllegalStateException("You need to set the columns.");
        }

        createTableIfNotExists();

        insertStatement = new StringBuilder("INSERT INTO ")
                .append(tableName)
                .append(" ")
                .append(placeholders(params.keySet()))
                .append(" VALUES ")
                .append(placeholders(null));
        
        statement = conn.prepareStatement(insertStatement.toString());
        statement.setFetchSize(Integer.MIN_VALUE);
    }

    private void createTableIfNotExists() throws SQLException {
        List<String> columnNames = params.keySet().stream().collect(Collectors.toList());

        boolean isDefaultPK = false;
        if (!constraints.values().contains(ColumnConstraint.PRIMARY_KEY)) {
            columnNames.add(0, "id");
            params.put("id", ColumnType.INT);
            constraints.put("id", ColumnConstraint.PRIMARY_KEY);
            isDefaultPK = true;
            System.out.println("Creating a default 'id' column for PK");
        }

        StringBuilder createTableStatement = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
        String pk = null;
        boolean first = true;
        
        for (String column : columnNames) {
            String type = params.get(column).getValue();
            String constraint = constraints.get(column).getValue();

            if (constraints.get(column) == ColumnConstraint.PRIMARY_KEY) {
                pk = column;
                if (isDefaultPK)
                    constraint += " AUTO_INCREMENT";
            }

            if (first) {
                createTableStatement.append("`").append(column).append("` ").append(type).append(constraint);
                first = false;
            } else {
                createTableStatement.append(", `").append(column).append("` ").append(type).append(constraint);
            }

        }
        createTableStatement.append(", PRIMARY KEY (`").append(pk).append("`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;");
        
        conn.createStatement().execute(createTableStatement.toString());
        params.remove("id");
    }
    
    private StringBuilder placeholders(Set<String> columnNames) {
        StringBuilder result = new StringBuilder("(");
        
        if (columnNames == null) {
            for (int i = 0; i < columnCount - 1; i++) {
                result.append("?, ");
            }
            return result.append("?)");
        }
        
        Iterator<String> iterator = columnNames.iterator();
        int index = columnCount;
        while (index > 1) {
            String name = iterator.next();
            result.append(name).append(", ");
            index--;
        }
        result.append(iterator.next()).append(")");
        return result;
    }

    public final String getSqlStatement() {
        if (statement == null) {
            throw new IllegalStateException("You need to build the statement first. Call method buildStatement().");
        }
        return insertStatement.toString();
    }
    
    public abstract void readFile(File file) throws InvalidCellValueException, IOException, SQLException;

    public final void checkErrors(boolean checkErrors) {
        this.checkErrors = checkErrors;
    }

    static class CellErrorChecker {
        static final Map<ColumnType, Pattern> map = new HashMap<>();
        static {
            map.put(ColumnType.BOOLEAN, Pattern.compile("(?i)true|false"));
            map.put(ColumnType.DATE, Pattern.compile("\\d{4}-\\d{2}-\\d{2}"));
            map.put(ColumnType.DOUBLE, Pattern.compile("-?\\d+(\\.\\d+)?"));
            map.put(ColumnType.INT, Pattern.compile("-?\\d+"));
            map.put(ColumnType.TIME, Pattern.compile("(?:\\d{2}|\\d):\\d{2}:\\d{2}(?:\\sAM|\\sPM)?"));
        }

        static String getInvalidCellValue(Cell cell, ColumnType type) {
            if (type == ColumnType.VARCHAR) {
                return null;
            }
            String strValue = formatCellValue(cell, type);
            return map.get(type).matcher(strValue).matches() ? null : strValue;
        }

        static String formatCellValue(Cell cell, ColumnType type) {
            try {
                return switch (type) {
                case BOOLEAN -> cell.getBooleanCellValue() ? "TRUE" : "FALSE";
                case DOUBLE -> String.valueOf(cell.getNumericCellValue());
                case INT -> String.valueOf((int) cell.getNumericCellValue());
                case VARCHAR, TIME -> cell.getStringCellValue();
                case DATE -> cell.getLocalDateTimeCellValue().toLocalDate().toString();
                };
            } catch (Exception any) {
                return cell.getStringCellValue();
            }
        }
    }
}
