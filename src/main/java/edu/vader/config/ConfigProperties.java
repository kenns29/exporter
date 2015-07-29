package edu.vader.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigProperties {
	public String dataScienceToolkitBaseUrl = null;
	public String coordinate2politicsUrl = null;
	public ConfigProperties() throws IOException{
		this("config.properties");
	}
	public ConfigProperties(String propFileName) throws IOException{
		getPropValues(propFileName);
	}
	public void getPropValues() throws IOException{
		getPropValues("config.properties");
	}
	@SuppressWarnings("unused")
	public void getPropValues(String propFileName) throws IOException{
		Properties prop = new Properties();
		File file = new File(propFileName);
		FileInputStream inputStream= new FileInputStream(file);
		//InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		
		if(inputStream != null){
			prop.load(inputStream);
		}
		else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
		
		this.dataScienceToolkitBaseUrl = prop.getProperty("dataScienceToolkitBaseUrl");
		this.coordinate2politicsUrl = prop.getProperty("coordinate2politicsUrl");
		
	}
	
}
