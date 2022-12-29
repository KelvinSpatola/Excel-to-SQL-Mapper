package eu.aird.gta.model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class TransactionWise {
	private String ticket;
	private String store = "Sulebhikhat";
	private Date date;
	private Integer sku;
	private Double value;
	private Double quantity;
	private String unit;
	private String paymentType;

	public TransactionWise(Row row) {
		this.ticket = row.getCell(0).getStringCellValue();
		this.date = new java.sql.Date(row.getCell(1).getDateCellValue().getTime());
		this.sku = (int) row.getCell(2).getNumericCellValue();
		this.value = row.getCell(3).getNumericCellValue();
		this.unit = row.getCell(4).getStringCellValue();
		this.quantity = row.getCell(5).getNumericCellValue();
		this.paymentType = row.getCell(6).getStringCellValue();
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setString(1, ticket);
		statement.setString(2, store);
		statement.setDate(3, date);
		statement.setInt(4, sku);
		statement.setDouble(5, value);
		statement.setDouble(6, quantity);
		statement.setString(7, unit);
		statement.setString(8, paymentType);
	}
}
