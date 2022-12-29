package eu.aird.gta.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class TransactionCard {
	private String ticket;
	private Integer cardNumber;

	public TransactionCard(Row row) {
		this.ticket = row.getCell(0).getStringCellValue();
		this.cardNumber = (int) row.getCell(1).getNumericCellValue();
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setString(1, ticket);
		statement.setInt(2, cardNumber);
	}
}
