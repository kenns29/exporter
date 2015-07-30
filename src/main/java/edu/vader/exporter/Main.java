package edu.vader.exporter;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import edu.vader.communicate.Communicate;
import edu.vader.config.ConfigProperties;
import edu.vader.geo.GeoBoundingBox;
import edu.vader.geo.GeoHandler;
import edu.vader.util.DBUtils;
import edu.vader.util.TimeUtils;

public class Main{
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static ConfigProperties configProperties = null;
	public static Properties props = new Properties();
	public static Connection conn = null;
	public static MongoClient mongoClient = null;
	public static MongoDatabase mongoDatabase = null; 
	public static MongoCollection<Document> mongoColl = null; 
	public static GeoHandler geoHandler = new GeoHandler();
	public static GeoBoundingBox geoBoundingBox = null; 
	static{
		try {
			configProperties = new ConfigProperties("config.properties");
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.error("did not successfully load the config file.", e);
		}
		props.setProperty("user", Main.configProperties.postgresqlUser);
		try {
			conn = DriverManager.getConnection(Main.configProperties.postgresqlDataUrl, props);
		} catch (SQLException e) {
			HIGH_PRIORITY_LOGGER.error("could not connect to postgres.", e);
		}
		
		mongoClient = new MongoClient(Main.configProperties.inputDataHost, Main.configProperties.inputDataPort);
		mongoDatabase = mongoClient.getDatabase(Main.configProperties.inputDataDB);
		mongoColl = mongoDatabase.getCollection(Main.configProperties.inputDataColl);
		Properties logProps = new Properties();
		InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
		try {
			logProps.load(inputStream);
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.error("Unable to load log config file.", e);
		}
		PropertyConfigurator.configure(logProps);
		
		try {
			geoBoundingBox = new GeoBoundingBox("boundingBox.txt");
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.error("can not load the bounding box", e);
		}
		
		configProperties.initStartEnd(mongoColl);
	}
	
	public static void main(String args[]) throws SQLException{
		DBUtils.deleteAll();
		Convert convert = new Convert();
		Communicate communicate = new Communicate();
		
		ObjectId startObjectId = Main.configProperties.startObjectId;
		ObjectId endObjectId = Main.configProperties.endObjectId;
		startObjectId = TimeUtils.decrementObjectId(startObjectId);
		ObjectId safestObjectId = null;
		while(safestObjectId == null){	
			try {
				safestObjectId = communicate.getSafestIdFromNer();
			} catch (IOException e) {
				HIGH_PRIORITY_LOGGER.error("can not get the safest id from ner", e);
			}
		}
		
		while(true){
			try {
				safestObjectId = communicate.getSafestIdFromNer();
			} catch (IOException e) {
				HIGH_PRIORITY_LOGGER.error("can not get the safest id from ner", e);
				continue;
			}
			if(startObjectId.compareTo(safestObjectId) != 0){
				convert.convertMongoToSql(startObjectId, safestObjectId);
			}
			startObjectId = safestObjectId;
		}
	}
}