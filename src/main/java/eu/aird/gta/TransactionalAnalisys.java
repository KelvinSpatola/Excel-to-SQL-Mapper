package eu.aird.gta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.util.IOUtils;

import com.monitorjbl.xlsx.StreamingReader;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import eu.aird.gta.model.ExcelToSqlMapper;
import eu.aird.gta.model.ExcelToSqlMapper.ColumnConstraint;
import eu.aird.gta.model.ExcelToSqlMapper.ColumnType;
import eu.aird.gta.model.InvalidCellValueException;
import eu.aird.gta.util.GTAProperties;

/**
 *
 * @author Kelvin Spátola
 */
public class TransactionalAnalisys {
    private static final GTAProperties props = GTAProperties.getInstance();
    private static int BATCH_SIZE = 10000;

    public static Connection getConnection() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("user", "root");
        connectionProps.setProperty("password", "admin123");
        return DriverManager.getConnection(
                "jdbc:mysql://localhost/at_test?useSSL=false&createDatabaseIfNotExist=true",
                connectionProps);
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        readFromExcel();
//		readFromCSV();

        System.out.println("Quiting...");
        printExecutionTime(start);
        System.exit(0); 
    }

    public static void readFromExcel() {
        IOUtils.setByteArrayMaxOverride(Integer.MAX_VALUE - 8);

        File[] allFiles = new File(props.get("data.product-hierarchy")).listFiles();
//		File[] allFiles = new File(props.get("data.transactional-data")).listFiles();
//		File[] allFiles = new File(props.get("data.ways-of-payment")).listFiles();

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            ExcelToSqlMapper mapper = new ExcelToSqlMapper(conn, true);

//			mapper.mapTable("transactions2")
//        			.column("store_code", ColumnType.VARCHAR)
//        			.column("store_desc", ColumnType.VARCHAR)
//					.column("date", ColumnType.DATE)
//					.column("time", ColumnType.TIME)
//					.column("ticket", ColumnType.VARCHAR)
//					.column("sku", ColumnType.INT)
//					.column("sku_desc", ColumnType.VARCHAR)
//					.column("uom", ColumnType.VARCHAR)
//					.column("quantity", ColumnType.DOUBLE)
//					.column("unit_value", ColumnType.DOUBLE)
//					.column("total_value", ColumnType.DOUBLE)
////					.column("payment_type", ColumnType.VARCHAR)
//					.buildStatement();

//			mapper.mapTable("store11930")
//        			.column("ticket", ColumnType.INT)
//					.column("store", ColumnType.INT)
//					.column("date", ColumnType.DATE)
//					.column("time", ColumnType.TIME)
//					.column("cashdesk", ColumnType.INT)
//					.column("payment", ColumnType.VARCHAR)
//					.column("sku", ColumnType.VARCHAR)
//					.column("quantity", ColumnType.DOUBLE)
//					.column("value", ColumnType.DOUBLE)
//					.buildStatement();
            
//            mapper.mapTable("product")
//                    .column("sku", ColumnType.INT, ColumnConstraint.PRIMARY_KEY)
//                    .column("sku_desc", ColumnType.VARCHAR)
//                    .column("sub_cat", ColumnType.VARCHAR)
//                    .column("cat", ColumnType.VARCHAR)
//                    .column("macro", ColumnType.VARCHAR)
//                    .column("pack_size", ColumnType.VARCHAR)
//                    .column("activation_date", ColumnType.DATE)
//                    .column("deactivation_date", ColumnType.DATE, ColumnConstraint.NULLABLE)
//                    .buildStatement();
            
            // AUCHAN
//            mapper.mapTable("pos2")
//                    .column("pos_store", ColumnType.INT)
//                    .column("cashdesk_id", ColumnType.INT)
//                    .column("cashdesk_group", ColumnType.VARCHAR)
//                    .column("pos_type_generic", ColumnType.VARCHAR)
//                    .column("pos_type_specific", ColumnType.VARCHAR)
//                    .buildStatement();
            
            mapper.mapTable("product")
                    .column("universe_id", ColumnType.VARCHAR)
                    .column("universe_desc", ColumnType.VARCHAR)
                    .column("market_id", ColumnType.VARCHAR)
                    .column("market_desc", ColumnType.VARCHAR)
                    .column("cat_id", ColumnType.INT, ColumnConstraint.UNIQUE)
                    .column("cat_desc", ColumnType.VARCHAR)
                    .buildStatement();

            PreparedStatement statement = conn.prepareStatement(mapper.getSqlStatement());
            statement.setFetchSize(Integer.MIN_VALUE);

            for (var file : allFiles) {
                if (file.isHidden()) {
                    continue; // Skip temporary files
                }
//                if (!file.getName().contains("Auchan_unlisted_cats_Respostas Auchan_mod")) {
//                    System.out.println("Skipping file: " + file.getName());
//                    continue; // Skip temporary files
//                }
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
                        
                        try {
                            mapper.setValues(row, statement);
                            statement.addBatch();
    
                            rowIndex++;
                            if (rowIndex % BATCH_SIZE == 0) {
                                statement.executeBatch();
                            }
                            System.out.println("Sheet: " + sheetIndex + "/" + sheetCount + " | row: " + rowIndex + " - "
                                    + (rowIndex * 100) / lastRow + "%");
    //                      printRow(row);
                            
                        } catch (InvalidCellValueException e) {
                            e.printStackTrace();
                            System.exit(0);
                        }
                    }
                    inputStream.close();
                    statement.executeBatch();
                    conn.commit();
                    sheetIndex++;
                    break;
                }
                // execute the remaining queries
                statement.executeBatch();
                conn.commit();
                System.out.println();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readFromCSV() {
        List<File> allFiles = Stream.of(new File(props.get("data.transactional-data")).listFiles())
                .sorted((f1, f2) -> f1.getName().compareTo(f2.getName())).collect(Collectors.toList());

        final int allFilesCount = allFiles.size();

//        final List<String> headers = List.of("Nº Ticket", "Posição", "Data", "Hora", "Nº Caixa", "Meio Pag", "Material",
//                "Unid", "Valor");

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            ExcelToSqlMapper mapper = new ExcelToSqlMapper(conn);
            mapper.mapTable("trans2151")
                    .column("store", ColumnType.INT)
                    .column("ticket", ColumnType.INT)
                    .column("date", ColumnType.DATE)
//                    .column("time", ColumnType.TIME)
//                    .column("cashdesk", ColumnType.INT)
                    .column("sku", ColumnType.INT)
                    .column("quantity", ColumnType.DOUBLE)
                    .column("payment", ColumnType.VARCHAR)
                    .column("sku", ColumnType.VARCHAR)
                    .column("value", ColumnType.DOUBLE).buildStatement();

            PreparedStatement statement = conn.prepareStatement(mapper.getSqlStatement());
            statement.setFetchSize(Integer.MIN_VALUE);

            long totalRows = 0;
            int errorCount = 0;

            int currentFileIndex = 1;

            for (var file : allFiles) {
                if (file.isHidden()) {
                    continue; // Skip temporary files
                }
                if (!file.getName().contains("2151 data")) {
                    System.out.println("Skipping file: " + file.getName());
                    continue; // Skip temporary files
                }

                var filename = file.getName();

                if (!file.getName().contains(".csv")) {
                    System.out.println("Skipping file: " + filename);
                    continue; // Skip temporary files
                }
                System.out.println("**********************************************************");
                System.out.println("Reading file: " + filename);
                System.out.println("**********************************************************");

                CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();

                CSVReader reader = new CSVReaderBuilder(new FileReader(file))
//					    .withSkipLines(1)
                        .withCSVParser(parser).build();

                var allLines = reader.readAll();
                var lastRow = allLines.size();
                var rowIndex = 1; // 1 - header

                totalRows += lastRow;
                System.out.println("lastRow: " + lastRow);

//                var skippableColumns = new HashSet<Integer>();
//                var fileHeader = allLines.get(0);
//                int dateIndex = -1;
//                int timeIndex = -1;
//                int quantityIndex = -1;
//                int valueIndex = -1;

//                for (int i = 0; i < fileHeader.length; i++) {
//                    var headerName = fileHeader[i];
//
//                    if (!headers.contains(headerName)) {
//                        skippableColumns.add(i);
//                        System.out.println("SKIPPING COLUMN: " + headerName);
//                    }
//                    if (headerName.equals("Data")) {
//                        dateIndex = i;
//                    } else if (headerName.equals("Hora")) {
//                        timeIndex = i;
//                    } else if (headerName.equals("Unid")) {
//                        quantityIndex = i;
//                    } else if (headerName.equals("Valor")) {
//                        valueIndex = i;
//                    }
//                }

//                System.out.println("dateIndex: " + dateIndex + " - timeIndex: " + timeIndex + " - quantityIndex: "
//                        + quantityIndex + " - valueIndex: " + valueIndex);

//                boolean isHeader = true;

                for (String[] data : allLines) {
//                    if (isHeader) {
//                        isHeader = false;
//                        continue;
//                    }

//                    if (data[dateIndex].equals("#") || data[timeIndex].equals("#")) {
//                        errorCount++;
//                        rowIndex++;
//                        continue;
//                    }
//                    data[dateIndex] = data[dateIndex].substring(0, 10);
//                    data[quantityIndex] = data[quantityIndex].replace(',', '.');
//                    data[valueIndex] = data[valueIndex].replace(',', '.');

//                    mapper.setValues(data, statement, skippableColumns);
                    mapper.setValues(data, statement);
                    statement.addBatch();

                    rowIndex++;
                    if (rowIndex % BATCH_SIZE == 0) {
                        statement.executeBatch();
                    }
                    System.out.println("File " + currentFileIndex + "/" + allFilesCount + " : " + filename + " -> row: "
                            + rowIndex + " - " + (rowIndex * 100) / lastRow + "%");
                }
                // execute the remaining queries
                statement.executeBatch();
                conn.commit();

                reader.close();
                currentFileIndex++;
                System.out.println();
            }
            System.out.println("TOTAL NR OF ROWS: " + totalRows);
            System.out.println("ERROR COUNT: " + errorCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean isRowEmpty(Row row) {
        if (row == null || row.getCell(0) == null) {
            return true;
        }
        return false;
    }

    private static void printExecutionTime(long startTime) {
        long end = System.currentTimeMillis();
        long time = (end - startTime) / 1000;
        long ss = time % 60;
        long mm = (time / 60) % 60;
        long hh = time / 3600;
        System.out.printf("\nIMPORT DONE in %02d.%02d.%02d\n", hh, mm, ss);
    }

}