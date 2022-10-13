package eu.aird.gta.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class GTAProperties {
	static private GTAProperties instance;
	static private Properties properties;
	static private String propertyFileName = "gta.properties";

	private GTAProperties() {
		try (InputStream is = getClass().getClassLoader().getResourceAsStream(propertyFileName)) {
			properties = new Properties();
			properties.load(is);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	static public GTAProperties getInstance() {
		if (instance == null) {
			instance = new GTAProperties();
		}
		return instance;
	}
	
	public String get(String propertyName) {
		return properties.getProperty(propertyName);
	}
}
