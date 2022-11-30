package eu.aird.gta.model;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class Product {
	private Integer sku;
	private String skuDesc;
	private String subCat;
	private String cat;
	private String macro;
	
	public Product(Row row) {
		this.sku = (int) row.getCell(0).getNumericCellValue();
		this.skuDesc = row.getCell(1).getStringCellValue();
		this.subCat = row.getCell(2).getStringCellValue();
		this.cat = row.getCell(3).getStringCellValue();
		this.macro = row.getCell(4).getStringCellValue();
	}
	
	public Product(ResultSet rs) throws SQLException {
		this.sku = rs.getInt("sku");
		this.skuDesc = rs.getString("sku_desc");
		this.subCat = rs.getString("sub_cat");
		this.cat = rs.getString("cat");
		this.macro = rs.getString("macro");
	}
	
	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setInt(1, sku);
		statement.setString(2, skuDesc);
		statement.setString(3, subCat);
		statement.setString(4, cat);
		statement.setString(5, macro);
	}

}
