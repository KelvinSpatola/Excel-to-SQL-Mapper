package eu.aird.gta.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.poi.ss.usermodel.Row;

public class ExcelToSqlMapper {
	static private final Map<String, ColumnType> params = new LinkedHashMap<>();
	private StringBuilder sqlStatement;
	private String tableName;
	private int columnCount;

	public enum ColumnType {
		BOOLEAN, VARCHAR, INT, DOUBLE, DATE, TIME
	}

	// CONSTRUCTOR
	public ExcelToSqlMapper() {
	}

	public ExcelToSqlMapper insertInto(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public ExcelToSqlMapper column(String param, ColumnType type) {
		params.put(param, type);
		columnCount++;
		return this;
	}

	public void buildStatement() {
		if (tableName == null || tableName.isBlank()) {
			throw new IllegalStateException("Missing a table reference. Call method insertInto(String tableName)");
		}
		if (params.isEmpty()) {
			throw new IllegalStateException("You need to set the columns.");
		}
		var columnNames = params.keySet();
		var first = true;

		sqlStatement = new StringBuilder("INSERT INTO ").append(tableName + " (");
		for (var name : columnNames) {
			if (first) {
				first = false;
				sqlStatement.append(name);
			} else {
				sqlStatement.append(", " + name);
			}
		}
		sqlStatement.append(") VALUES (?").append(new String(new char[columnCount - 1]).replace("\0", ", ?"))
				.append(")");
	}

	public String getSqlStatement() {
		return Objects
				.requireNonNull(sqlStatement, "You need to build an sql statement first. Call method buildStatement().")
				.toString();
	}

	public void setValues(Row row, PreparedStatement statement) throws SQLException {
		Objects.requireNonNull(row, "Row must not be null");

		var columnSetItr = params.keySet().iterator();

		for (int i = 0; i < columnCount; i++) {
			var cell = row.getCell(i);
			var columnType = params.get(columnSetItr.next());
			var columnNum = i + 1;
			
			switch (columnType) {
			case BOOLEAN -> statement.setBoolean(columnNum, Optional.ofNullable(cell.getBooleanCellValue()).orElseGet(null));
			case DATE -> {
				java.util.Date date = cell.getDateCellValue();
				if (date == null) {
					statement.setDate(columnNum, null);
					break;
				}
				statement.setDate(columnNum, new java.sql.Date(date.getTime()));
			}
			case DOUBLE -> statement.setDouble(columnNum, Optional.ofNullable(cell.getNumericCellValue()).orElseGet(null));
			case INT -> statement.setInt(columnNum, Optional.ofNullable((int) cell.getNumericCellValue()).orElseGet(null));
			case VARCHAR, TIME -> statement.setString(columnNum, Optional.ofNullable(cell.getStringCellValue()).orElseGet(null));
			}
		}
	}

}
