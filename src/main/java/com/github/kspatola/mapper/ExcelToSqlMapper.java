package com.github.kspatola.mapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.github.kspatola.exception.InvalidCellValueException;
import com.github.kspatola.mapper.rules.ColumnConstraint;
import com.github.kspatola.mapper.rules.ColumnType;
import com.github.pjfanning.xlsx.StreamingReader;

/**
 *
 * @author Kelvin Spátola
 */
public class ExcelToSqlMapper extends Mapper {

    // CONSTRUCTOR
    public ExcelToSqlMapper(Connection connection) throws SQLException {
        super(connection, true);
    }
    
    public ExcelToSqlMapper(Connection connection, boolean checkErrors) throws SQLException {
        super(connection, checkErrors);
    }

    @Override
    public void readFile(File file) throws InvalidCellValueException, IOException, SQLException {
        if (statement == null) {
            throw new IllegalStateException("You need to build the statement first. Call method buildStatement().");
        }
        
        FileInputStream inputStream = new FileInputStream(file);
        Workbook workbook = StreamingReader.builder().rowCacheSize(500).bufferSize(16384).open(inputStream);
        Iterator<Sheet> sheetItr = workbook.sheetIterator();
        int sheetCount = workbook.getNumberOfSheets();
        int sheetIndex = 1;
        
        
        while (sheetItr.hasNext()) {
            Sheet sheet = sheetItr.next();
            Iterator<Row> rowItr = sheet.iterator();
            int lastRow = sheet.getLastRowNum();
            int rowIndex = 1; // 1 - header
            var row = rowItr.next(); // skip the header row

            while (rowItr.hasNext() && !isRowEmpty(row)) {
                
                setValues(rowItr.next());
                statement.addBatch();

                rowIndex++;
                if (rowIndex % BATCH_SIZE == 0) {
                    statement.executeBatch();
                }
                System.out.println("Sheet: " + sheetIndex + "/" + sheetCount + " | row: " + rowIndex + " - "
                        + (rowIndex * 100) / lastRow + "%");
                    
            }
            inputStream.close();
            statement.executeBatch();
            conn.commit();
            sheetIndex++;
//            break;
        }
        // execute the remaining queries
        statement.executeBatch();
        conn.commit();
        System.out.println();
    }
    
    private void setValues(Row row) throws InvalidCellValueException, SQLException {
        Objects.requireNonNull(row, "Row must not be null");

        Iterator<String> columnSetItr = params.keySet().iterator();

        for (int i = 0; i < columnCount; i++) {
            String columnName = columnSetItr.next();
            int columnNum = i + 1;

            Cell cell = row.getCell(i);

            if (cell == null) {
                if (constraints.get(columnName) == ColumnConstraint.NOT_NULL) {
                    throw new NullPointerException("You have a null value in your Excel file at line "
                            + (row.getRowNum() + 1) + ", column " + columnNum);
                }
                statement.setNull(columnNum, java.sql.Types.NULL);
                continue;
            }

            ColumnType columnType = params.get(columnName);

            if (checkErrors) {
                String error = CellErrorChecker.getInvalidCellValue(cell, columnType);

                if (error != null) {
                    if (error.isEmpty() && constraints.get(columnName) == ColumnConstraint.NULLABLE) {
                        statement.setNull(columnNum, java.sql.Types.NULL);
                        continue;
                    }
                    
                    String message = "Incorrect value '" + error + "' for column '" + columnName
                            + "'. Excel location: sheet '" + cell.getSheet().getSheetName() + "', row "
                            + (row.getRowNum() + 1) + ", column " + columnNum;

                    throw new InvalidCellValueException(message, error);
                }
            }

            switch (columnType) {
            case BOOLEAN -> statement.setBoolean(columnNum, cell.getBooleanCellValue());
            case DOUBLE -> statement.setDouble(columnNum, cell.getNumericCellValue());
            case INT -> statement.setInt(columnNum, (int) cell.getNumericCellValue());
            case VARCHAR, TIME -> statement.setString(columnNum, cell.getStringCellValue());
            case DATE -> statement.setObject(columnNum, cell.getLocalDateTimeCellValue().toLocalDate());
            }

        }
    }

    private boolean isRowEmpty(Row row) {
        if (row == null || row.getCell(0) == null) {
            return true;
        }
        return false;
    }
}