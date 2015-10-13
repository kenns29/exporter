package edu.vader.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;

import edu.vader.util.MongoUtils;

public class ConfigProperties {
	// Task settings for export to PostgreSQL
	public boolean exportPostgreSqlTask = false;
	public int exportPostgreSqlTaskCatID = 0;
	public String exportPostgreSqlTaskBBFile = null;
	public String postgresqlDataUrl = null;
	public String postgresqlUser = null;
	
	// Task settings for export to MongoDB web docs url queue
	public boolean exportUrlsTask = false;
	public int exportUrlsTaskCatID = 0;

	// General settings
	public String inputDataHost = null;
	public int inputDataPort = 0;
	public String inputDataDB = null;
	public String inputDataColl = null;
	
	public String dataScienceToolkitBaseUrl = null;
	public String coordinate2politicsUrl = null;
	
	public String nerProgramBaseUrl = null;
	public String safestObjectIdUrl = null;
	
	public boolean useObjectIdLimit = false;
	public ObjectId startObjectId = null;
	public ObjectId endObjectId = null;
	public boolean stopAtEnd = false;
	public int simpleRestletServerPort = 0;
	
	public int insertionReportInterval = 0;
	public int documentReportInterval = 0;
	
	public boolean deleteOldData = false;
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
		
		this.exportPostgreSqlTask = Boolean.parseBoolean(prop.getProperty("exportPostgreSqlTask"));
		this.exportPostgreSqlTaskCatID = Integer.valueOf(prop.getProperty("exportPostgreSqlTaskCatID"));
		this.exportPostgreSqlTaskBBFile = prop.getProperty("exportPostgreSqlTaskBBFile");
		this.postgresqlDataUrl = prop.getProperty("postgresqlDataUrl");
		this.postgresqlUser = prop.getProperty("postgresqlUser");

		this.exportUrlsTask = Boolean.parseBoolean(prop.getProperty("exportUrlsTask"));
		this.exportUrlsTaskCatID = Integer.valueOf(prop.getProperty("exportUrlsTaskCatID"));
		
		this.dataScienceToolkitBaseUrl = prop.getProperty("dataScienceToolkitBaseUrl");
		this.coordinate2politicsUrl = prop.getProperty("coordinate2politicsUrl");
				
		this.inputDataHost = prop.getProperty("inputDataHost");
		this.inputDataPort = Integer.valueOf(prop.getProperty("inputDataPort"));
		this.inputDataDB = prop.getProperty("inputDataDB");
		this.inputDataColl = prop.getProperty("inputDataColl");
		
		this.nerProgramBaseUrl = prop.getProperty("nerProgramBaseUrl");
		this.safestObjectIdUrl = prop.getProperty("safestObjectIdUrl");
		
		this.useObjectIdLimit = Boolean.parseBoolean(prop.getProperty("useObjectIdLimit"));
		String startObjectIdStr = prop.getProperty("startObjectId");
		String endObjectIdStr = prop.getProperty("endObjectId");
		if(!startObjectIdStr.equals("none")){
			startObjectId = new ObjectId(startObjectIdStr);
		}
		if(!endObjectIdStr.equals("none")){
			endObjectId = new ObjectId(endObjectIdStr);
		}
		
		this.stopAtEnd = Boolean.parseBoolean(prop.getProperty("stopAtEnd"));
		this.simpleRestletServerPort = Integer.valueOf(prop.getProperty("simpleRestletServerPort"));
		
		this.insertionReportInterval = Integer.valueOf(prop.getProperty("insertionReportInterval"));
		this.documentReportInterval = Integer.valueOf(prop.getProperty("documentReportInterval"));
		
		this.deleteOldData = Boolean.parseBoolean(prop.getProperty("deleteOldData"));				
	}
	
	public void initStartEnd(MongoCollection<Document> coll){
		if(startObjectId == null){
			startObjectId = MongoUtils.minObjectId(coll);
		}
		if(endObjectId == null){
			endObjectId = MongoUtils.maxObjectId(coll);
		}
	}
	
}
