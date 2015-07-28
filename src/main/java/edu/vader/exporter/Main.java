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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import edu.vader.geo.GeoBoundingBox;
import edu.vader.geo.GeoHandler;
import edu.vader.util.DBUtils;

public class Main{
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	
	public static String dataUrl = "jdbc:postgresql://fsdb1.dtn.asu.edu:5432/temp_twitter";
	public static Properties props = new Properties();
	public static Connection conn = null;
	public static MongoClient mongoClient = new MongoClient("fsdb2.dtn.asu.edu");
	public static MongoDatabase mongoDatabase = mongoClient.getDatabase("tweettracker");
	public static MongoCollection<Document> mongoColl = mongoDatabase.getCollection("tweets");
	public static GeoHandler geoHandler = new GeoHandler();
	public static GeoBoundingBox geoBoundingBox = null; 
	static{
		props.setProperty("user", "postgres");
		try {
			conn = DriverManager.getConnection(dataUrl, props);
		} catch (SQLException e) {
			HIGH_PRIORITY_LOGGER.error("could not connect to postgres.", e);
		}
		
		Properties logProps = new Properties();
		InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
		try {
			logProps.load(inputStream);
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.error("Unable to load config file.", e);
		}
		PropertyConfigurator.configure(logProps);
		
		try {
			geoBoundingBox = new GeoBoundingBox("boundingBox.txt");
		} catch (IOException e) {
			HIGH_PRIORITY_LOGGER.error("can not load the bounding box", e);
		}
	}
	
	public static void main(String args[]) throws SQLException{	
		DBUtils.deleteAll();
		Convert convert = new Convert();
		convert.convertMongoToSql();
	}
}