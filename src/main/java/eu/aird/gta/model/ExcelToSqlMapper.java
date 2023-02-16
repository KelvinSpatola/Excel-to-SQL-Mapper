package eu.aird.gta.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Cell;
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
	private boolean checkErrors;
	static private Connection conn;

	// CONSTRUCTOR
	public ExcelToSqlMapper(Connection connection) {
		this(connection, true);
	}
	
	// CONSTRUCTOR
	public ExcelToSqlMapper(Connection connection, boolean checkErrors) {
		ExcelToSqlMapper.conn = connection;
		checkErrors(checkErrors);
	}

	public ExcelToSqlMapper mapTable(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public ExcelToSqlMapper column(String param, ColumnType type) {
		return column(param, type, ColumnConstraint.NOT_NULL);
	}

	public ExcelToSqlMapper column(String param, ColumnType type, ColumnConstraint constraint) {		
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

	public void setValues(Row row, PreparedStatement statement) throws InvalidCellValueException, SQLException {
		Objects.requireNonNull(row, "Row must not be null");

		var columnSetItr = params.keySet().iterator();

		for (int i = 0; i < columnCount; i++) {
			var columnName = columnSetItr.next();
			var columnNum = i + 1;
			
			var cell = row.getCell(i);
			
			if (cell == null) {
				if (constraints.get(columnName) == ColumnConstraint.NOT_NULL) {
					throw new NullPointerException("You have a null value in your Excel file at line " + (row.getRowNum() + 1) + ", column " + columnNum);
				}				
				statement.setNull(columnNum, java.sql.Types.NULL);
				continue;
			}
			
			var columnType = params.get(columnName);
			
			if (checkErrors) {
				var error = CellErrorChecker.getInvalidCellValue(cell, columnType);		
				
				if (error != null) {
					var message = "Incorrect value '" + error + "' for column '" + columnName 
							+ "'. Excel location: sheet '" + cell.getSheet().getSheetName() + "', row " + (row.getRowNum() + 1) + ", column " + columnNum;
					
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
	
	public void setValues(String[] row, PreparedStatement statement) throws NumberFormatException, SQLException {
		setValues(row, statement, null);
	}
		
	public void setValues(String[] row, PreparedStatement statement, Set<Integer> skippableColumns) throws NumberFormatException, SQLException {
		var columnSetIterator = params.keySet().iterator();
		
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
	
	public void checkErrors(boolean checkErrors) {
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
			var strValue = formatCellValue(cell, type);
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