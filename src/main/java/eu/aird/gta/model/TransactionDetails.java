package eu.aird.gta.model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class TransactionDetails {
	private String ticket;
	private Date date;
	private Double amount;
	private String cardType;

	public TransactionDetails(Row row) {
		this.ticket = row.getCell(0).getStringCellValue();
		this.date = new java.sql.Date(row.getCell(1).getDateCellValue().getTime());
		this.amount = row.getCell(2).getNumericCellValue();
		this.cardType = row.getCell(3).getStringCellValue();
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setString(1, ticket);
		statement.setDate(2, date);
		statement.setDouble(3, amount);
		statement.setString(4, cardType);
	}
}
