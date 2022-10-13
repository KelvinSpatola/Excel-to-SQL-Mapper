package eu.aird.gta.model;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Row;

public class Product {
	private String material;
	private String materialDesc;
	private String subClass;
	private String subClassDesc;
	private String prodClass;
	private String prodClassDesc;
	private String cat;
	private String catDesc;
	private String macro;
	private String sm;
	
	public Product(Row row) {
		this.material = row.getCell(0) == null ? null : row.getCell(0).getStringCellValue();
		this.materialDesc = row.getCell(1).getStringCellValue();
		this.subClass = row.getCell(2).getStringCellValue();
		this.subClassDesc = row.getCell(3).getStringCellValue();
		this.prodClass = row.getCell(4).getStringCellValue();
		this.prodClassDesc = row.getCell(5).getStringCellValue();
		this.cat = row.getCell(6).getStringCellValue();
		this.catDesc = row.getCell(7).getStringCellValue();
		this.macro = row.getCell(8).getStringCellValue();
		this.sm = row.getCell(9).getStringCellValue();
	}
	
	public Product(ResultSet rs) throws SQLException {
		this.material = rs.getString("material");
		this.materialDesc = rs.getString("material_desc");
		this.subClass = rs.getString("sub_class");
		this.subClassDesc = rs.getString("sub_class_desc");
		this.prodClass = rs.getString("class");
		this.prodClassDesc = rs.getString("class_desc");
		this.cat = rs.getString("cat");
		this.catDesc = rs.getString("cat_desc");
		this.macro = rs.getString("macro");
		this.sm = rs.getString("shopping_mission");
	}
	
	public void setStatements(PreparedStatement statement) throws SQLException {
		statement.setString(1, material);
		statement.setString(2, materialDesc);
		statement.setString(3, subClass);
		statement.setString(4, subClassDesc);
		statement.setString(5, prodClass);
		statement.setString(6, prodClassDesc);
		statement.setString(7, cat);
		statement.setString(8, catDesc);
		statement.setString(9, macro);
		statement.setString(10, sm);
	}

}
