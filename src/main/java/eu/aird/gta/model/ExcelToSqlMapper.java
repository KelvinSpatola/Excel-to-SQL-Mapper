package eu.aird.gta.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Row;

public class ExcelToSqlMapper {
	static private final Map<String, ColumnType> params = new LinkedHashMap<>();
	static private Connection conn;
	private StringBuilder insertStatement;
	private String tableName;
	private int columnCount;

	public enum ColumnType {
		BOOLEAN, VARCHAR, INT, DOUBLE, DATE, TIME, PRIMARY_KEY
	}

	// CONSTRUCTOR
	public ExcelToSqlMapper(Connection connection) {
		ExcelToSqlMapper.conn = connection;
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

	public void buildStatement() throws SQLException {
		if (tableName == null || tableName.isBlank()) {
			throw new IllegalStateException("Missing a table reference. Call method insertInto(String tableName)");
		}
		if (params.isEmpty()) {
			throw new IllegalStateException("You need to set the columns.");
		}
		
		createTableIfNotExists();
		
		insertStatement = new StringBuilder("INSERT INTO ").append(tableName + " (");
		var columnNames = params.keySet();
		var first = true;		
	
		for (var name : columnNames) {
			if (first) {
				first = false;
				insertStatement.append(name);
			} else {
				insertStatement.append(", " + name);
			}
		}
		insertStatement.append(") VALUES (?").append(new String(new char[columnCount - 1]).replace("\0", ", ?"))
				.append(")");
	}
	
	private void createTableIfNotExists() throws SQLException {
		var columnNames = params.keySet().stream().collect(Collectors.toList());
		var createTableStatement = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName + " (");
		
		if (!params.values().contains(ColumnType.PRIMARY_KEY)) {
			columnNames.add(0, "id");
			params.put("id", ColumnType.PRIMARY_KEY);
			System.out.println("Creating id column for PK");
		}
		
		var first = true;
		String pk = null;
		for (var column : columnNames) {
			var suffix = switch (params.get(column)) {
			case PRIMARY_KEY -> {
				pk = column;
				yield "BIGINT NOT NULL AUTO_INCREMENT";
			}
			case BOOLEAN -> "BOOLEAN NOT NULL";
			case DATE -> "DATE NOT NULL";
			case DOUBLE -> "DOUBLE NOT NULL";
			case INT -> "INT NOT NULL";
			case TIME -> "TIME NOT NULL";
			case VARCHAR -> "VARCHAR(100) NOT NULL";
			};

			if (first) {
				first = false;
				createTableStatement.append("`" + column + "` ").append(suffix);
			} else {
				createTableStatement.append(", `" + column + "` ").append(suffix);
			}
		}
		createTableStatement.append(", PRIMARY KEY (`" + pk + "`)")
				.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;");

		conn.createStatement().execute(createTableStatement.toString());
		params.remove("id");
	}

	public String getSqlStatement() {
		return Objects
				.requireNonNull(insertStatement, "You need to build an sql statement first. Call method buildStatement().")
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
			case PRIMARY_KEY, INT -> statement.setInt(columnNum, Optional.ofNullable((int) cell.getNumericCellValue()).orElseGet(null));
			case VARCHAR, TIME -> statement.setString(columnNum, Optional.ofNullable(cell.getStringCellValue()).orElseGet(null));
			}
		}
	}

}