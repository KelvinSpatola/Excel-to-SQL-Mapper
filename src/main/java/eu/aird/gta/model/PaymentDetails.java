package eu.aird.gta.model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class PaymentDetails {
	private Date date;
	private Double amount;
	private String cardType;
	private Integer cardNumber;

	public PaymentDetails(Row row) {
		this.date = new java.sql.Date(row.getCell(0).getDateCellValue().getTime());
		this.amount = row.getCell(1).getNumericCellValue();
		this.cardType = row.getCell(2).getStringCellValue();
		this.cardNumber = (int) row.getCell(3).getNumericCellValue();
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setDate(1, date);
		statement.setDouble(2, amount);
		statement.setString(3, cardType);
		statement.setInt(4, cardNumber);
	}
}
