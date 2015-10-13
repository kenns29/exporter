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
import edu.vader.config.ConfigProperties;
import edu.vader.geo.GeoBoundingBox;
import edu.vader.geo.GeoHandler;
import edu.vader.simpleRestletServer.EmbeddedServerComponent;
import edu.vader.util.DBUtils;
import edu.vader.version.VersionControl;


public class Main{
	public static final int VERSION_MAJOR = 1;
	public static final int VERSION_MINOR = 1;
	public static final int BUILD_ID = 0;
	
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static VersionControl versionControl =
			new VersionControl(VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID,
				"07-30-2015");
	public static ConfigProperties configProperties = null;
	public static Properties props = new Properties();
	public static Connection conn = null;
	public static MongoClient mongoClient = null;
	public static MongoDatabase mongoDatabase = null; 
	public static MongoCollection<Document> mongoColl = null; 
	public static GeoHandler geoHandler = new GeoHandler();
	public static GeoBoundingBox exportSqlGeoBoundingBox = null;
	
	public static ObjectId currentObejctId = null;
	public static ObjectId currentSafestObjectId = null;
	public static int documentCount = 0;
	
	public static long mainStartTime = 0;
	public static long preStartTime = 0;
	public static int minuteDocCount = 0;
	public static int lastMinuteDocCount = 0;
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
		
		if (configProperties.exportPostgreSqlTask) {
			try {
				exportSqlGeoBoundingBox = new GeoBoundingBox(configProperties.exportPostgreSqlTaskBBFile);
			} catch (IOException e) {
				HIGH_PRIORITY_LOGGER.error("can not load the bounding box", e);
			}
		}
		
		configProperties.initStartEnd(mongoColl);
	}
	
	public static void main(String args[]) throws Exception{
		if(Main.configProperties.deleteOldData){
			DBUtils.deleteAll();
		}
		EmbeddedServerComponent server = new EmbeddedServerComponent(Main.configProperties.simpleRestletServerPort);
		server.start();
		Run run = new Run();
		if(Main.configProperties.stopAtEnd){
			run.processWithStartEndObjectId();
		}
		else{
			run.processWithSafestObjectId();
		}
	}
}