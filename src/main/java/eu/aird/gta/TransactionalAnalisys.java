package eu.aird.gta;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaError;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.util.IOUtils;

import com.monitorjbl.xlsx.StreamingReader;

import eu.aird.gta.model.ExcelToSqlMapper;
import eu.aird.gta.model.ExcelToSqlMapper.ColumnConstraint;
import eu.aird.gta.model.ExcelToSqlMapper.ColumnType;
import eu.aird.gta.util.GTAProperties;

/**
*
* @author Kelvin Spátola
*/
public class TransactionalAnalisys {
	private static final GTAProperties props = GTAProperties.getInstance();
	private static int BATCH_SIZE = 10;

	public static Connection getConnection() throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "admin123");
		// mysql database
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/transactional_mapper_test?useSSL=false&createDatabaseIfNotExist=true", connectionProps);
		return (conn);
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		IOUtils.setByteArrayMaxOverride(Integer.MAX_VALUE - 8);

		File[] allFiles = new File(props.get("data.product-hierarchy")).listFiles();
//		File[] allFiles = new File(props.get("data.transactional-data")).listFiles();
//		File[] allFiles = new File(props.get("data.ways-of-payment")).listFiles();

		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);

			ExcelToSqlMapper mapper = new ExcelToSqlMapper(conn);
//			mapper.mapTable("transactions")
//					.column("ticket", ColumnType.VARCHAR)
//					.column("store", ColumnType.INT)
//					.column("store_desc", ColumnType.VARCHAR)
//					.column("date", ColumnType.DATE)
//					.column("time", ColumnType.VARCHAR)
//					.column("sku", ColumnType.INT)
//					.column("sku_desc", ColumnType.VARCHAR)
//					.column("value", ColumnType.DOUBLE)
//					.column("quantity", ColumnType.DOUBLE)
//					.column("total_value", ColumnType.DOUBLE)
//					.buildStatement();
			mapper.mapTable("product")
					.column("sku", ColumnType.INT, ColumnConstraint.PRIMARY_KEY)
					.column("sku_desc", ColumnType.VARCHAR)
					.column("sub_cat", ColumnType.VARCHAR)
					.column("cat", ColumnType.VARCHAR)
					.column("macro", ColumnType.VARCHAR)
					.buildStatement();
			
			PreparedStatement statement = conn.prepareStatement(mapper.getSqlStatement());
			statement.setFetchSize(Integer.MIN_VALUE);
			
			for (var file : allFiles) {
				if (file.isHidden()) {
					continue; // Skip temporary files 
				}
				System.out.println("**********************************************************");
				System.out.println("Reading file: " + file.getName());
				System.out.println("**********************************************************");

				var inputStream = new FileInputStream(file);
				var workbook = StreamingReader.builder().rowCacheSize(500).bufferSize(16384).open(inputStream);
				var sheetItr = workbook.sheetIterator();
				var sheetCount = workbook.getNumberOfSheets();
				var sheetIndex = 1;

				while (sheetItr.hasNext()) {
					var sheet = sheetItr.next();
					var rowItr = sheet.iterator();
					var lastRow = sheet.getLastRowNum();
					var rowIndex = 1; // 1 - header
					var row = rowItr.next(); // skip the header row

					while (rowItr.hasNext() && !isRowEmpty(row)) {
						row = rowItr.next();
						mapper.setValues(row, statement);
						statement.addBatch();

						rowIndex++;
						if (rowIndex % BATCH_SIZE == 0) {
							statement.executeBatch();
						}
						System.out.println("Sheet: " + sheetIndex + "/" + sheetCount + " | row: " + rowIndex + " - "
								+ (rowIndex * 100) / lastRow + "%");
//						printRow(row);
					}
					inputStream.close();
					statement.executeBatch();
					conn.commit();
					sheetIndex++;
				}
				// execute the remaining queries
				statement.executeBatch();
				conn.commit();
				System.out.println();
			}
			printExecutionTime(start);

		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Quiting...");
		System.exit(0);
	}

	private static boolean isRowEmpty(Row row) {
		if (row == null || row.getCell(0) == null) {
			return true;
		}
		return false;
	}

	private static void printRow(Row row) {
		StringBuilder sb = new StringBuilder("Row: ");
		sb.append(row.getRowNum() + 1); // plus 1 (header)
		for (var cell : row) {
			switch (cell.getCellType()) {
			case BOOLEAN -> sb.append(" | " + (cell.getBooleanCellValue() ? "TRUE" : "FALSE"));
			case NUMERIC -> {
				if (DateUtil.isCellDateFormatted(cell)) {
					sb.append(" | " + cell.getDateCellValue());
				} else {
					sb.append(" | " + cell.getNumericCellValue());
				}
			}
			case STRING -> sb.append(" | " + cell.getRichStringCellValue().getString());
			case BLANK -> sb.append(" | BLANK");
			case ERROR -> sb.append(" | " + FormulaError.forInt(cell.getErrorCellValue()).getString());
			case FORMULA -> sb.append(" | " + cell.getCellFormula());
			case _NONE -> sb.append(" | " + cell.getDateCellValue());
			default -> throw new IllegalArgumentException("Unexpected value: " + cell.getCellType());
			}
		}
		System.out.println(sb.toString());
	}

	private static void printExecutionTime(long startTime) {
		long end = System.currentTimeMillis();
		long time = (end - startTime) / 1000;
		long ss = time % 60;
		long mm = (time / 60) % 60;
		long hh = time / 3600;
		System.out.printf("\nIMPORT DONE in %02d.%02d.%02d ms\n", hh, mm, ss);
	}

}