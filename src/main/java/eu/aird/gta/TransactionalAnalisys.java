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
import java.util.Scanner;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;

import com.monitorjbl.xlsx.StreamingReader;

import eu.aird.gta.model.Product;
import eu.aird.gta.model.TransactionCard;
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
				"jdbc:mysql://localhost/at_oncost?useSSL=false&createDatabaseIfNotExist=true", connectionProps);
		return (conn);
	}

	public static void main(String[] args) {
		IOUtils.setByteArrayMaxOverride(Integer.MAX_VALUE - 8);

		boolean quit = false;

		while (!quit) {
			System.out.println(" ******************* GALP TRANSACTIONAL ANALYSIS ******************* ");
			System.out.println(" --- MENU --- ");
			System.out.println("1. Load transactional files");
			System.out.println("2. Load product structure file");
			System.out.println("0. Exit");
			System.out.print("\nChoose option: ");
			Scanner sc = new Scanner(System.in);
			int menuOption = sc.nextInt();

			switch (menuOption) {
			case 0:
				quit = true;
				sc.close();
				break;
			case 1:
				loadTransactionDetailsFile();
				break;
			case 2:
//				loadProductStructureFile();
				break;
			}
		}
		System.out.println("Quiting...");
		System.exit(0);
	}

	private static void loadTransactionDetailsFile() {
		long start = System.currentTimeMillis();
		
		final int TOTAL_SHEETS = 1;
		int workbookSheet = 0;
		
		String storesPath = props.get("data.stores");
//		String storesPath = props.get("data.ways-of-payment");
		File[] storeFiles = new File(storesPath).listFiles();
		
		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);

//			final String sql = "INSERT INTO payment_details"
//					+ " (date, amount, card_type, card_number)"
//					+ " VALUES (?, ?, ?, ?)";
//			final String sql = "INSERT INTO transaction_details"
//					+ " (ticket, date, amount, card_type)"
//					+ " VALUES (?, ?, ?, ?)";
//			final String sql = "INSERT INTO transactions"
//					+ " (ticket, store, date, sku, value, quantity, unit, payment_type)"
//					+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
			final String sql = "INSERT INTO transaction_card"
					+ " (transaction_ticket, card_number)"
					+ " VALUES (?, ?)";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setFetchSize(Integer.MIN_VALUE);
			InputStream inputStream;

			while (workbookSheet < TOTAL_SHEETS) {
				for (File storeFile : storeFiles) {
					System.out.println("**********************************************************");
					System.out.println("Reading file: " + storeFile.getName());
					System.out.println("**********************************************************");
					
					inputStream = new FileInputStream(storeFile);
					Workbook workbook = StreamingReader.builder().rowCacheSize(500).bufferSize(16384).open(inputStream);
					
					Iterator<Row> rowIterator = workbook.getSheetAt(workbookSheet).iterator();
					rowIterator.next(); // skip the header row
					
					int rowsCount = 1; // 1 - header
					int totalRows = workbook.getSheetAt(workbookSheet).getLastRowNum();
					TransactionCard transaction;
					
//					if (storeFile.getName().contains("Sulebhikhat payment details anonino - mod")) {
//					if (storeFile.getName().contains("Sulebhikhat-2151 Transaction Details - mod")) {
//					if (storeFile.getName().contains("Sulebhikhat-2151 Transaction Details Article wise - mod")) {
					if (storeFile.getName().contains("Sulebhikhat  - etapa_1 - mod")) {
						while (rowIterator.hasNext()) {
							Row nextRow = rowIterator.next();
							if (nextRow.getCell(0) == null)
								break;
							
							transaction = new TransactionCard(nextRow);
							transaction.setStatements(statement);
							
							statement.addBatch();
							rowsCount++;
							if (rowsCount % BATCH_SIZE == 0) {
								statement.executeBatch();
							}
							System.out.println((rowsCount * 100) / totalRows + "% - row: " + rowsCount);
						}
					}
					inputStream.close();
					statement.executeBatch();
					conn.commit();
				}
				
				// execute the remaining queries
				statement.executeBatch();
				conn.commit();
				
				// let's run the next sheet
				workbookSheet++;
			}

			printExecutionTime(start);

		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void loadProductStructureFile() {
		long start = System.currentTimeMillis();
		
		String productsPath = props.get("data.product-structure");
		System.out.println("path: " + productsPath);
		File[] productFiles = new File(productsPath).listFiles();
		int totalproductFiles = productFiles.length;

		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);
			
			String sql = "INSERT INTO product "
					+ "(sku, sku_desc, sub_cat, cat, macro) "
					+ "VALUES (?, ?, ?, ?, ?)";
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
						System.out.println("ERROR IN ROW " + rowsCount + " cell 0!");
						break;
					}
					
					product = new Product(nextRow);
					product.setStatements(statement);
					
					statement.addBatch();
					rowsCount++;
					if (rowsCount % BATCH_SIZE == 0) {
						statement.executeBatch();
					}
					System.out.println("File: " + productFileCount + "/" + totalproductFiles +  " - " + productFile.getName() + " - "
							+ (rowsCount * 100) / totalRows + "% - row: " + rowsCount);
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
