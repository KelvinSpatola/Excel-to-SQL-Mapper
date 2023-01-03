package eu.aird.gta.model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class Transaction {
	private String ticket;
	private Integer store;
	private String storeDesc;
	private Date date;
	private String time;
	private Integer sku;
	private String skuDesc;
	private Double value;
	private Double quantity;
	private Double totalValue;

	public Transaction(Row row) {
		this.ticket = row.getCell(0).getStringCellValue();
		this.store = (int) row.getCell(1).getNumericCellValue();
		this.storeDesc = row.getCell(2).getStringCellValue();
		this.date = new Date(row.getCell(3).getDateCellValue().getTime());
		this.time = row.getCell(4).getStringCellValue();
		this.sku = (int) row.getCell(5).getNumericCellValue();
		this.skuDesc = row.getCell(6).getStringCellValue();
		this.value = row.getCell(7).getNumericCellValue();
		this.quantity = row.getCell(8).getNumericCellValue();
		this.totalValue = row.getCell(9).getNumericCellValue();
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setString(1, ticket);
		statement.setInt(2, store);
		statement.setString(3, storeDesc);
		statement.setDate(4, date);
		statement.setString(5, time);
		statement.setInt(6, sku);
		statement.setString(7, skuDesc);
		statement.setDouble(8, value);
		statement.setDouble(9, quantity);
		statement.setDouble(10, totalValue);
	}
	
}
