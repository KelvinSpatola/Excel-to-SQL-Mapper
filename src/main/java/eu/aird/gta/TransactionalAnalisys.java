package eu.aird.gta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;

import com.monitorjbl.xlsx.StreamingReader;

import eu.aird.gta.model.Product;
import eu.aird.gta.model.Transaction;
import eu.aird.gta.util.GTAProperties;

public class TransactionalAnalisys {
	private static final GTAProperties props = GTAProperties.getInstance();
	private static int BATCH_SIZE = 350;

	public static Connection getConnection() throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "admin123");
		// mysql database
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/at_makani?useSSL=false&createDatabaseIfNotExist=true", connectionProps);
		return (conn);
	}

	public static void main(String[] args) {
		IOUtils.setByteArrayMaxOverride(Integer.MAX_VALUE - 8);

//		loadProductStructureFile();
		loadTransactionalFiles();

		System.out.println("Quiting...");
		System.exit(0);
	}

	public static void loadTransactionalFiles() {
		long start = System.currentTimeMillis();

//		File[] storeFiles = new File(props.get("data.ways-of-payment")).listFiles();
		File[] transactionalFiles = new File(props.get("data.stores")).listFiles();

		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);

			var sql = "INSERT INTO transactions"
					+ " (ticket, store, store_desc, date, time, sku, sku_desc, value, quantity, total_value)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setFetchSize(Integer.MIN_VALUE);

			for (var file : transactionalFiles) {
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
					rowItr.next(); // skip the header row
					
					Transaction transaction;
					while (rowItr.hasNext()) {
						var row = rowItr.next();
						if (row.getCell(0) == null) {
							System.err.println("ERROR IN ROW " + rowIndex + ", cell 0!");
							break;
						}

						transaction = new Transaction(row);
						transaction.setStatements(statement);

						statement.addBatch();
						rowIndex++;
						if (rowIndex % BATCH_SIZE == 0) {
							statement.executeBatch();
						}
						System.out.println("Sheet: " + sheetIndex + "/" + sheetCount + " | row: " + rowIndex + " - " + (rowIndex * 100) / lastRow + "%");
					}
					inputStream.close();
					statement.executeBatch();
					conn.commit();
					sheetIndex++;
				}

				// execute the remaining queries
				statement.executeBatch();
				conn.commit();
			}

			printExecutionTime(start);

		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}

	public static void loadProductStructureFile() {
		long start = System.currentTimeMillis();

		String productsPath = props.get("data.product-structure");
		System.out.println("path: " + productsPath);
		File[] productFiles = new File(productsPath).listFiles();
		int totalproductFiles = productFiles.length;

		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);

			String sql = "INSERT INTO product "
					+ "(sku, sku_desc, sub_cat, cat, macro, pack_size, activation_date, disactivation_date) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setFetchSize(Integer.MIN_VALUE);
			InputStream inputStream;
			int productFileCount = 0;

			for (File productFile : productFiles) {
				System.out.println("**********************************************************");
				System.out.println("Reading file: " + productFile.getName());
				System.out.println("**********************************************************");

				inputStream = new FileInputStream(productFile);
				Workbook workbook = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputStream);

				Iterator<Row> rowIterator = workbook.getSheetAt(0).iterator();
				rowIterator.next(); // skip the header row

				productFileCount++;
				int rowsCount = 1;
				int totalRows = workbook.getSheetAt(0).getLastRowNum();
				Product product;

				while (rowIterator.hasNext()) {
					Row nextRow = rowIterator.next();
					if (nextRow.getCell(0) == null) {
						System.err.println("ERROR IN ROW " + rowsCount + " cell 0!");
						break;
					}

					product = new Product(nextRow);
					product.setStatements(statement);

					statement.addBatch();
					rowsCount++;
					if (rowsCount % BATCH_SIZE == 0) {
						statement.executeBatch();
					}
					System.out.println("File: " + productFileCount + "/" + totalproductFiles + " - "
							+ productFile.getName() + " - " + (rowsCount * 100) / totalRows + "% - row: " + rowsCount);
				}

				// execute the remaining queries
				statement.executeBatch();
				conn.commit();
			}

			printExecutionTime(start);

		} catch (IOException ex1) {
			System.out.println("Error reading file");
			ex1.printStackTrace();
		} catch (SQLException ex2) {
			System.out.println("Database error");
			ex2.printStackTrace();
		}
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
