package eu.aird.gta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import com.monitorjbl.xlsx.StreamingReader;

import eu.aird.gta.model.Product;
import eu.aird.gta.model.Transaction;
import eu.aird.gta.util.GTAProperties;

public class GTA {
	private static final GTAProperties props = GTAProperties.getInstance();
	private static int BATCH_SIZE = 350;
//	List<String> header = List.of("Nº Ticket", "Posição", "Desc Posição", "Data", "Hora", "Nº Caixa",
//			"Nome Caixa", "Meio Pag", "Desc Meio Pag", "Material", "Desc Material", "Categoria",
//			"Desc Categoria", "Unid", "Valor");

	public static Connection getConnection() throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "admin123");
		// mysql database
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/at_galp?useSSL=false&createDatabaseIfNotExist=true", connectionProps);
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
			System.out.println("3. Return non-existing transactional products");
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
				loadTransactionsFile();
				break;
			case 2:
//				loadProductStructureFile();
				break;
			case 3:
				returnNonExistingTransactionalProducts();
				break;
			case 4:
				String storesPath = props.get("data.product-structure")
						+ "/PRODUCT STRUCTURE FOR TRANSACTIONAL ANALYSIS V2.xlsx";
				File f = new File(storesPath);
				System.out.println(f);
				System.out.println(f.exists());

				break;
			}
		}
		System.out.println("Quiting...");
		System.exit(0);
	}

	private static void loadTransactionsFile() {
		try (Connection conn = getConnection()) {

			long start = System.currentTimeMillis();

			String storesPath = props.get("data.stores");
			File[] storeFiles = new File(storesPath).listFiles();
			int totalStores = storeFiles.length;
			int storesCount = 0;

			conn.setAutoCommit(false);

			String sql = "INSERT INTO transaction "
					+ "(ticket_id, position, position_desc, date, time, cashdesk, cashdesk_name, payment, payment_desc, "
					+ "material, material_desc, cat, cat_desc, quantity, value) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setFetchSize(Integer.MIN_VALUE);
			InputStream inputStream;

			for (File storeFile : storeFiles) {
				System.out.println("**********************************************************");
				System.out.println("Reading file: " + storeFile.getName());
				System.out.println("**********************************************************");
				
				inputStream = new FileInputStream(storeFile);
				Workbook workbook = StreamingReader.builder().rowCacheSize(500).bufferSize(16384).open(inputStream);

				Iterator<Row> rowIterator = workbook.getSheetAt(1).iterator();
				rowIterator.next(); // skip the header row
				
				storesCount++;
				int rowsCount = 1; // 1 - header
				int totalRows = workbook.getSheetAt(1).getLastRowNum();
				Transaction transaction;

				if (storeFile.getName().contains("Gare do Oriente") || storeFile.getName().contains("Telheiras")) {
					while (rowIterator.hasNext()) {
						Row nextRow = rowIterator.next();
						if (nextRow.getCell(0) == null)
							break;

						transaction = new Transaction(nextRow, true);
						transaction.setStatements(statement);

						statement.addBatch();
						rowsCount++;
						if (rowsCount % BATCH_SIZE == 0) {
							statement.executeBatch();
						}
						System.out.println(storesCount + "/" + totalStores + " - " + storeFile.getName() + " - "
								+ (rowsCount * 100) / totalRows + "% - row: " + rowsCount);
					}
				} else {
					while (rowIterator.hasNext()) {
						Row nextRow = rowIterator.next();
						if (nextRow.getCell(0) == null)
							break;

						transaction = new Transaction(nextRow, false);
						transaction.setStatements(statement);

						statement.addBatch();
						rowsCount++;
						if (rowsCount % BATCH_SIZE == 0) {
							statement.executeBatch();
						}
						System.out.println(storesCount + "/" + totalStores + " - " + storeFile.getName() + " - "
								+ (rowsCount * 100) / totalRows + "% - row: " + rowsCount);
					}
				}
				inputStream.close();
				statement.executeBatch();
				conn.commit();
			}

			// execute the remaining queries
			statement.executeBatch();
			conn.commit();

			long end = System.currentTimeMillis();
			System.out.printf("\nIMPORT DONE in %d ms\n", (end - start));

		} catch (IOException ex1) {
			System.out.println("Error reading file");
			ex1.printStackTrace();
		} catch (SQLException ex2) {
			System.out.println("Database error");
			ex2.printStackTrace();
		}
	}

	private static void loadProductStructureFile() {
		String productFile = props.get("data.product-structure")
				+ "/PRODUCT STRUCTURE FOR TRANSACTIONAL ANALYSIS V2.xlsx";

		try (Connection conn = getConnection(); InputStream inputStream = new FileInputStream(new File(productFile));) {
			long start = System.currentTimeMillis();
			conn.setAutoCommit(false);

			Workbook workbook = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputStream);

			String sql = "INSERT INTO product "
					+ "(material, material_desc, sub_class, sub_class_desc, class, class_desc, cat, cat_desc, macro, shopping_mission) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setFetchSize(Integer.MIN_VALUE);

			Iterator<Row> rowIterator = workbook.getSheetAt(0).iterator();
			rowIterator.next(); // skip the header row
			int count = 0;
			int totalRows = workbook.getSheetAt(0).getLastRowNum();
			Product product;

			while (rowIterator.hasNext()) {
				Row nextRow = rowIterator.next();
				if (nextRow.getCell(0) == null)
					break;

				product = new Product(nextRow);
				product.setStatements(statement);

				statement.addBatch();
				count++;
				if (count % BATCH_SIZE == 0) {
					statement.executeBatch();
				}
				System.out.println((count * 100) / totalRows + "% - row: " + count);
			}

			// execute the remaining queries
			statement.executeBatch();
			conn.commit();

			long end = System.currentTimeMillis();
			System.out.printf("\nIMPORT DONE in %d ms\n", (end - start));

		} catch (IOException ex1) {
			System.out.println("Error reading file");
			ex1.printStackTrace();
		} catch (SQLException ex2) {
			System.out.println("Database error");
			ex2.printStackTrace();
		}
	}

	private static void returnNonExistingTransactionalProducts() {
		String outputPath = props.get("data.output") + "/Cross-analysis by material description 2.xlsx";

		try (Connection conn = getConnection();) {
			long start = System.currentTimeMillis();

			// PRODUCT STRUCTURE

			String prod_query = "SELECT * FROM product";
			PreparedStatement statement = conn.prepareStatement(prod_query);

			ResultSet rs = statement.executeQuery(prod_query);
			Map<String, Product> productStructure = new HashMap<>();

			System.out.println("Fetching product structure data...");
			while (rs.next()) {
				String productMaterialCode = rs.getString("material");
				productStructure.put(productMaterialCode, new Product(rs));
			}
			System.out.println("size: " + productStructure.size());

//			Map<String, SKUCounter> crossResult = new HashMap<>();
			Map<String, SKUCounter2> crossResult2 = new HashMap<>();
			
			try (SXSSFWorkbook workbook = new SXSSFWorkbook(); FileOutputStream outputStream = new FileOutputStream(outputPath)) {
				SXSSFSheet sheet = workbook.createSheet("01.01.2019 a 31.12.2019");

				String[] storePositions = { "11019", "11020", "11034", "11104", "11169", "11302", "11374", "11500",
						"11682", "14226", "11558", "11401" };

				// TRANSACTIONS

				Set<Transaction> transactions;

				String[] header = { "Material", "Desc Material", "Ocorrências" }; // the header
				int rowCount = 0, columnCount = 0;
				Row headerRow = sheet.createRow(rowCount++);
				for (String title : header) {
					headerRow.createCell(columnCount++).setCellValue(title);
				}

				for (String storeID : storePositions) {
					String trans_query = "SELECT * FROM transaction WHERE position = '" + storeID + "' ORDER BY position, date, time, ticket_id ASC";

					statement = conn.prepareStatement(trans_query);
					rs = statement.executeQuery(trans_query);
					transactions = new HashSet<>();

					System.out.print("\nFetching transactions data for store: " + storeID + " ... ");
					while (rs.next()) {
						transactions.add(new Transaction(rs));
					}
					System.out.println("size: " + transactions.size());


					// CROSS-ANALYSIS
					
					
					System.out.print("Crossing ... ");
					int resultSize = 0;
					for (Transaction trans : transactions) {
						final String material = new String(trans.getMaterial().trim());
						
						if (productStructure.get(material) != null) {
							if (!crossResult2.containsKey(material)) {
								crossResult2.put(material, new SKUCounter2(trans.getMaterialDesc()));
							} else {
								crossResult2.get(material).count++;
							}
							resultSize++;
						}
					}
					System.out.println("result size: " + resultSize);
					
					System.out.print("Analysing ... ");
					for (String material : crossResult2.keySet()) {
						String matDesc = crossResult2.get(material).materialDesc;
						int count = crossResult2.get(material).count;

						Row row = sheet.createRow(rowCount++);
						row.createCell(0).setCellValue(material);
						row.createCell(1).setCellValue(matDesc);
						row.createCell(2).setCellValue(count);
					}
//					
//					System.out.print("Crossing ... ");
//					int resultSize = 0;
//					for (Transaction trans : transactions) {
//						String material = trans.getMaterial();
//						
//						if (!productStructure.containsKey(material)) {
//							String materialDesc = trans.getMaterialDesc();
//							
//							if (!crossResult.containsKey(material)) {
//								crossResult.put(material, new SKUCounter(trans.getMaterialDesc()));
//							} else {
//								var map = crossResult.get(material).map;
//								
//								if (map.containsKey(materialDesc)) {
//									int num = map.get(materialDesc) + 1;
//									map.replace(materialDesc, num);
//								} else {
//									map.put(materialDesc, 1);
//								}
//							}
//							resultSize++;
//						}
//					}
//					System.out.println("result size: " + resultSize);
//					
//					System.out.print("Analysing ... ");
//					for (String material : crossResult.keySet()) {
//						for (String materialDesc : crossResult.get(material).map.keySet()) {
//							
//							int count = crossResult.get(material).map.get(materialDesc);
//							
//							Row row = sheet.createRow(rowCount++);
//							row.createCell(0).setCellValue(material);
//							row.createCell(1).setCellValue(materialDesc);
//							row.createCell(2).setCellValue(count);
//						}
//						
//					}
					
					
					
//					sheet.flushRows();
					
//					// CROSS-ANALYSIS
//					System.out.print("Analysing ... ");
//					int resultSize = 0;
//					for (Transaction trans : transactions) {
//						if (!productStructure.containsKey(trans.getMaterial())) {
//							Row row = sheet.createRow(rowCount++);
//							row.createCell(0).setCellValue(trans.getMaterial());
//							row.createCell(1).setCellValue(trans.getMaterialDesc());
//							resultSize++;
//						}
//					}
//					System.out.println("result size: " + resultSize);
////					sheet.flushRows();
				}
				System.out.println("writting to output file...");
				workbook.write(outputStream);
			}
			
			long end = System.currentTimeMillis();
			System.out.printf("\nIMPORT DONE in %d ms\n", (end - start));

		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}
	
	static class SKUCounter {
		public Map<String, Integer> map = new HashMap<>();
//		public String materialDesc;
//		public int count;
		public SKUCounter(String materialDesc) {
			map.put(materialDesc, 1);
//			this.materialDesc = materialDesc;
//			this.count++;
		}
	}
	
	static class SKUCounter2 {
		public String materialDesc;
		public int count;
		public SKUCounter2(String materialDesc) {
			this.materialDesc = materialDesc;
			this.count++;
		}
	}

//	private static String getColumnName(Sheet sheet, Cell cell) {
//		return sheet.getRow(sheet.getFirstRowNum()).getCell(cell.getColumnIndex()).getStringCellValue();
//	}

}
