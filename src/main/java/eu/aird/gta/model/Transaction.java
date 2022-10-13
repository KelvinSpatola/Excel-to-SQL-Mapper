package eu.aird.gta.model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Transaction {
	private String ticketId;
	private String position;
	private String positionDesc;
	private Date date;
	private String time;
//	private Time time;
	private String cashdesk;
	private String cashdeskName;
	private String wayOfPayment;
	private String wayOfPaymentDesc;
	private String material;
	private String materialDesc;
	private String cat;
	private String catDesc;
	private String quantity;
	private String value;

	public Transaction(Row row, boolean isFocusStore) {
		this.ticketId = row.getCell(0).getStringCellValue();
		this.position = row.getCell(1).getStringCellValue();
		this.positionDesc = row.getCell(2).getStringCellValue();
		this.date = new java.sql.Date(row.getCell(3).getDateCellValue().getTime());
		this.time = row.getCell(4) == null ? null : row.getCell(4).getStringCellValue();
//		this.time = new java.sql.Time(row.getCell(4).getDateCellValue().getTime());
		this.cashdesk = row.getCell(5).getStringCellValue();
		this.cashdeskName = row.getCell(6) == null ? null : row.getCell(6).getStringCellValue();
		this.wayOfPayment = row.getCell(7).getStringCellValue();
		this.wayOfPaymentDesc = row.getCell(8).getStringCellValue();
		this.material = row.getCell(9).getStringCellValue();
		this.materialDesc = row.getCell(10).getStringCellValue();
		if (isFocusStore) {
			this.cat = row.getCell(11).getStringCellValue();
			this.catDesc = row.getCell(12).getStringCellValue();
			this.quantity = row.getCell(13).getStringCellValue();
			this.value = row.getCell(14).getStringCellValue();
		} else {
			this.quantity = row.getCell(11).getStringCellValue();
			this.value = row.getCell(12).getStringCellValue();
		}
	}

	public Transaction(ResultSet rs) throws SQLException {
		this.ticketId = rs.getString("ticket_id");
		this.position = rs.getString("position");
		this.positionDesc = rs.getString("position_desc");
		this.date = rs.getDate("date");
		this.time = rs.getString("time");
//		this.time = rs.getTime("time");
		this.cashdesk = rs.getString("cashdesk");
		this.cashdeskName = rs.getString("cashdesk_name");
		this.wayOfPayment = rs.getString("payment");
		this.wayOfPaymentDesc = rs.getString("payment_desc");
		this.material = rs.getString("material");
		this.materialDesc = rs.getString("material_desc");
		this.cat = rs.getString("cat");
		this.catDesc = rs.getString("cat_desc");
		this.quantity = rs.getString("quantity");
		this.value = rs.getString("value");
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setString(1, ticketId);
		statement.setString(2, position);
		statement.setString(3, positionDesc);
		statement.setDate(4, date);
		statement.setString(5, time);
//		statement.setTime(5, time);
		statement.setString(6, cashdesk);
		statement.setString(7, cashdeskName);
		statement.setString(8, wayOfPayment);
		statement.setString(9, wayOfPaymentDesc);
		statement.setString(10, material);
		statement.setString(11, materialDesc);
		statement.setString(12, cat);
		statement.setString(13, catDesc);
		statement.setString(14, quantity);
		statement.setString(15, value);
	}

	public void populateRow(Row row) {
		var wb = (XSSFWorkbook) row.getSheet().getWorkbook();
		var createHelper = wb.getCreationHelper();
		var dateStyle = wb.createCellStyle();
		dateStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd/MM/yyyy"));
		var timeStyle = wb.createCellStyle();
		timeStyle.setDataFormat(createHelper.createDataFormat().getFormat("h:mm:ss"));

		row.createCell(0).setCellValue(ticketId);
		row.createCell(1).setCellValue(position);
		row.createCell(2).setCellValue(positionDesc);
		Cell dateCell = row.createCell(3);
		dateCell.setCellValue(date);
		dateCell.setCellStyle(dateStyle);
		Cell timeCell = row.createCell(4);
		timeCell.setCellValue(time);
//		timeCell.setCellStyle(timeStyle);
		row.createCell(5).setCellValue(cashdesk);
		row.createCell(6).setCellValue(cashdeskName);
		row.createCell(7).setCellValue(wayOfPayment);
		row.createCell(8).setCellValue(wayOfPaymentDesc);
		row.createCell(9).setCellValue(material);
		row.createCell(10).setCellValue(materialDesc);
		row.createCell(11).setCellValue(cat);
		row.createCell(12).setCellValue(catDesc);
		row.createCell(13).setCellValue(quantity);
		row.createCell(14).setCellValue(value);
	}
	
	public String getMaterial() {
		return this.material;
	}
	
	public String getMaterialDesc() {
		return this.materialDesc;
	}
}
