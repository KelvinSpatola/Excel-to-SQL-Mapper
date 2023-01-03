package eu.aird.gta.model;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import org.apache.poi.ss.usermodel.Row;

public class Product {
	private Integer sku;
	private String skuDesc;
	private String subCat;
	private String cat;
	private String macro;
	private String packSize;
	private Date activationDate;
	private Date disactivationDate;
	
	public Product(Row row) {
		this.sku = (int) row.getCell(0).getNumericCellValue();
		this.skuDesc = row.getCell(1).getStringCellValue();
		this.subCat = row.getCell(2).getStringCellValue();
		this.cat = row.getCell(3).getStringCellValue();
		this.macro = row.getCell(4).getStringCellValue();
		this.packSize = row.getCell(5).getStringCellValue();
		this.activationDate = new Date(row.getCell(6).getDateCellValue().getTime());
		Optional.ofNullable(row.getCell(7).getDateCellValue()).ifPresent(date -> disactivationDate = new Date(date.getTime()));
	}

	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setInt(1, sku);
		statement.setString(2, skuDesc);
		statement.setString(3, subCat);
		statement.setString(4, cat);
		statement.setString(5, macro);
		statement.setString(6, packSize);
		statement.setDate(7, activationDate);
		statement.setDate(8, disactivationDate);
	}

}
