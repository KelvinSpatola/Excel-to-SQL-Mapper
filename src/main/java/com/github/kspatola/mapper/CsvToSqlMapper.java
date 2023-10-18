package com.github.kspatola.mapper;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author Kelvin Spátola
 */
public class CsvToSqlMapper extends Mapper {

    // CONSTRUCTOR
    public CsvToSqlMapper(Connection connection) throws SQLException {
        super(connection, true);
    }

    // CONSTRUCTOR
    public CsvToSqlMapper(Connection connection, boolean checkErrors) throws SQLException {
        super(connection, checkErrors);
    }

    public void readFile(File file) {
        if (statement == null) {
            throw new IllegalStateException("You need to build the statement first. Call method buildStatement().");
        }
        
        
    }

    public void setValues(String[] row, PreparedStatement statement) throws NumberFormatException, SQLException {
        setValues(row, statement, null);
    }
    
    public void setValues(String[] row, PreparedStatement statement, Set<Integer> skippableColumns)
            throws NumberFormatException, SQLException {
        Iterator<String> columnSetIterator = params.keySet().iterator();

        int columnIndex = 1;
        for (int i = 0; i < row.length; i++) {
            if (skippableColumns != null && skippableColumns.contains(i)) {
                continue;
            }

            switch (params.get(columnSetIterator.next())) {
            case BOOLEAN -> statement.setBoolean(columnIndex, Boolean.parseBoolean(row[i]));
            case DOUBLE -> statement.setDouble(columnIndex, Double.parseDouble(row[i]));
            case INT -> statement.setInt(columnIndex, Integer.parseInt(row[i]));
            case VARCHAR, TIME -> statement.setString(columnIndex, row[i]);
            case DATE -> statement.setObject(columnIndex, LocalDate.parse(row[i], DateTimeFormatter.ISO_LOCAL_DATE));
            }
            columnIndex++;
        }
    }

}