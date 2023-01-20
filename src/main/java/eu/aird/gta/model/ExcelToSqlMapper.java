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

/**
*
* @author Kelvin Spátola
*/
public class ExcelToSqlMapper {
	private final Map<String, ColumnType> params = new LinkedHashMap<>();
	private final Map<String, ColumnConstraint> constraints = new LinkedHashMap<>();
	private StringBuilder insertStatement;
	private String tableName;
	private int columnCount;
	static private Connection conn;

	public enum ColumnType {
		BOOLEAN("BOOLEAN"), VARCHAR("VARCHAR(100)"), INT("INT"), DOUBLE("DOUBLE"), DATE("DATE"), TIME("TIME");

		private final String value;

		ColumnType(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	}

	public enum ColumnConstraint {
		NOT_NULL("NOT NULL"), NULLABLE(""), PRIMARY_KEY("NOT NULL"), UNIQUE("UNIQUE"); // DEFAULT, CHECK

		private final String value;

		ColumnConstraint(String value) {
			this.value = value;
		}

		private String getValue() {
			return " " + value;
		}
	}

	// CONSTRUCTOR
	public ExcelToSqlMapper(Connection connection) {
		ExcelToSqlMapper.conn = connection;
	}

	public ExcelToSqlMapper mapTable(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public ExcelToSqlMapper column(String param, ColumnType type) {
		return column(param, type, ColumnConstraint.NOT_NULL);
	}

	public ExcelToSqlMapper column(String param, ColumnType type, ColumnConstraint constraint) {
		if (param.isBlank()) {
			throw new IllegalArgumentException("Column name cannot be blank");
		}
		if (params.containsKey(param)) {
			throw new IllegalArgumentException("Duplicate column name: " + param);
		}
		if (constraint == ColumnConstraint.PRIMARY_KEY && constraints.containsValue(constraint)) {
			throw new IllegalArgumentException("Cannot assign two primary keys");
		}
		params.put(param, type);
		constraints.put(param, constraint);
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
		var createTableStatement = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName + " (");
		var columnNames = params.keySet().stream().collect(Collectors.toList());
		
		var isDefaultPK = false;
		if (!constraints.values().contains(ColumnConstraint.PRIMARY_KEY)) {
			columnNames.add(0, "id");
			params.put("id", ColumnType.INT);
			constraints.put("id", ColumnConstraint.PRIMARY_KEY);
			isDefaultPK =  true;
			System.out.println("Creating a default 'id' column for PK");
		}

		var first = true;
		String pk = null;

		for (var column : columnNames) {
			var type = params.get(column).getValue();
			var constraint = constraints.get(column).getValue();

			if (constraints.get(column) == ColumnConstraint.PRIMARY_KEY) {
				pk = column;
				if (isDefaultPK)					
					constraint += " AUTO_INCREMENT";
			}

			if (first) {
				first = false;
				createTableStatement.append("`" + column + "` ").append(type).append(constraint);
			} else {
				createTableStatement.append(", `" + column + "` ").append(type).append(constraint);
			}

		}
		createTableStatement.append(", PRIMARY KEY (`" + pk + "`)")
				.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;");

		conn.createStatement().execute(createTableStatement.toString());
		params.remove("id");
	}

	public String getSqlStatement() {
		return Objects.requireNonNull(insertStatement,
				"You need to build the statement first. Call method buildStatement().").toString();
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
			case DOUBLE -> statement.setDouble(columnNum, Optional.ofNullable(cell.getNumericCellValue()).orElseGet(null));
			case INT -> statement.setInt(columnNum, Optional.ofNullable((int) cell.getNumericCellValue()).orElseGet(null));
			case VARCHAR, TIME -> statement.setString(columnNum, Optional.ofNullable(cell.getStringCellValue()).orElseGet(null));
			case DATE -> {
				java.util.Date date = cell.getDateCellValue();
				if (date == null) {
					statement.setDate(columnNum, null);
					break;
				}
				statement.setDate(columnNum, new java.sql.Date(date.getTime()));
			}
			}
		}
	}

}