package edu.vader.exporter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import edu.vader.util.DBUtils;

public class Main{
	public static String dataUrl = "jdbc:postgresql://fsdb1.dtn.asu.edu:5432/temp_twitter";
	public static Properties props = new Properties();
	public static Connection conn = null;
	public static MongoClient mongoClient = new MongoClient("fsdb2.dtn.asu.edu");
	public static MongoDatabase mongoDatabase = mongoClient.getDatabase("tweettracker");
	public static MongoCollection<Document> mongoColl = mongoDatabase.getCollection("tweets");
	
	static{
		props.setProperty("user", "postgres");
		try {
			conn = DriverManager.getConnection(dataUrl, props);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) throws SQLException{	
		DBUtils.deleteAll();
		Convert convert = new Convert();
		convert.convertMongoToSql();
	}
}