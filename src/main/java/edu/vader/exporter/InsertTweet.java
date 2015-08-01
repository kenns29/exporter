package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class InsertTweet {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	public static int tweetCount = 0;
	public long tid = 0;
	public DateTime date = null;
	public String tweet = null;
	public String place = null;
	public String language = null;
	public int retweet_count = 0;
	double longitude = 0.0;
	double latitude = 0.0;
	public long time_numeric = 0;
	public long uid = 0;
	public String original_geo_field = null;
	
	ObjectId oid = null;
	public InsertTweet(ObjectId oid, long tid, DateTime date, String tweet, String place, String language, int retweet_count, long uid, double longitude, double latitude, String original_geo_field){
		this.tid = tid;
		this.date = date;
		this.tweet = escape + tweet + escape;
		this.place = escape + place + escape;
		this.language = escape + language + escape;
		this.retweet_count = retweet_count;
		this.uid = uid;
		this.longitude = longitude;
		this.latitude = latitude;
		this.original_geo_field = escape + original_geo_field + escape;
		
		this.oid = oid;
	}
	
	public InsertTweet(ObjectId oid, long tid, long timestamp, String tweet, String place, String language, int retweet_count, long uid, double longitude, double latitude, String original_geo_field){
		DateTime date = new DateTime(timestamp);
		
		this.tid = tid;
		this.date = date;
		this.tweet = escape + tweet + escape;
		this.place = escape + place + escape;
		this.language = escape + language + escape;
		this.retweet_count = retweet_count;
		this.uid = uid;
		this.longitude = longitude;
		this.latitude = latitude;
		this.original_geo_field = escape + original_geo_field + escape;
	
		this.oid = oid;
	}
	
	private int insert() throws SQLException{
		String timestamp = "TIMESTAMP '" + convertDateTimeToTimeString(this.date) + "'";
		String prep = "INSERT into tweet (tid, date, tweet, place, language, retweet_count, uid, longitude, latitude, original_geo_field) VALUES (" 
				+ this.tid + "," 
				+ timestamp + "," 
				+ this.tweet + "," 
				+ ((this.place != null) ? this.place : "NULL") + "," 
				+ this.language + "," 
				+ this.retweet_count + "," 
				+ this.uid + ","
				+ ((this.longitude != Convert.INVALID_COORDINATE_DOUBLE) ? this.longitude : "NULL") + ","
				+ ((this.latitude != Convert.INVALID_COORDINATE_DOUBLE) ?  this.latitude : "NULL")+ ","
				+ ((this.original_geo_field != null) ? this.original_geo_field : "NULL") + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		return rowsUpdated;
	}
	
	private int update() throws Exception{
		String timestamp = "TIMESTAMP '" + convertDateTimeToTimeString(this.date) + "'";
		String prep = "UPDATE tweet SET"
				+ " date = " + timestamp + ","
				+ " tweet = " + this.tweet + ","
				+ " place = " + ((this.place != null) ? this.place : "NULL") + ","
				+ " language = " + this.language + ","
				+ " retweet_count = " + this.retweet_count + ","
				+ " uid = " + this.uid + ","
				+ " longitude = " + ((this.longitude != Convert.INVALID_COORDINATE_DOUBLE) ? this.longitude : "NULL") + ","
				+ " latitude = " + ((this.latitude != Convert.INVALID_COORDINATE_DOUBLE) ?  this.latitude : "NULL")+ ","
				+ " original_geo_field = " + ((this.original_geo_field != null) ? this.original_geo_field : "NULL") 
				+ " WHERE tid = " + this.tid;
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		return rowsUpdated;
	}
	
	private int insertOrReplace(){
		while(true){
			int rowsUpdated = 0;
			try {
				rowsUpdated = update();
			} catch (Exception e) {
				LOGGER.error("did not successfully update " + tid, e);
			}
			if(rowsUpdated > 0){
				return rowsUpdated;
			}
			else {
				
				try {
					rowsUpdated = insert();
					return rowsUpdated;
				} catch (Exception e) {
					LOGGER.error("did not succesfully insert " + tid + ", retry updating again. Object id is " + this.oid, e);
				}
			}
		}
		
	}
	
	public int insertOrReplaceWithReport(){
		int rowsUpdated = insertOrReplace();
		tweetCount += rowsUpdated;
		if(tweetCount % Main.configProperties.insertionReportInterval == 0){
			LOGGER.info(tweetCount + " rows inserted into tweets");
		}
		return rowsUpdated;
		
	}
	private String convertDateTimeToTimeString(DateTime date){
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z").withZone(DateTimeZone.UTC);
		return fmt.print(date);
	}
}
