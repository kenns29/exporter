package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class InsertTweet {
	private static final String escape = "$verySpecialToken$";
	
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
	public InsertTweet(long tid, DateTime date, String tweet, String place, String language, int retweet_count, long uid, double longitude, double latitude, String original_geo_field){
		this.tid = tid;
		this.date = date;
		this.tweet = escape + tweet + escape;
		this.place = place;
		this.language = language;
		this.retweet_count = retweet_count;
		this.uid = uid;
		this.longitude = longitude;
		this.latitude = latitude;
		this.original_geo_field = original_geo_field;
	}
	
	public InsertTweet(long tid, long timestamp, String tweet, String place, String language, int retweet_count, long uid, double longitude, double latitude, String original_geo_field){
		DateTime date = new DateTime(timestamp);
		
		this.tid = tid;
		this.date = date;
		this.tweet = escape + tweet + escape;
		this.place = place;
		this.language = language;
		this.retweet_count = retweet_count;
		this.uid = uid;
		this.longitude = longitude;
		this.latitude = latitude;
		this.original_geo_field = original_geo_field;
	}
	
	public void insert() throws SQLException{
		String timestamp = "TIMESTAMP '" + convertDateTimeToTimeString(this.date) + "'";
		String prep = "INSERT into tweet (tid, date, tweet, place, language, retweet_count, uid, longitude, latitude, original_geo_field) VALUES (" 
				+ this.tid + "," 
				+ timestamp + "," 
				+ this.tweet + "," 
				+ this.place + "," 
				+ this.language + "," 
				+ this.retweet_count + "," 
				+ this.uid + ","
				+ ((this.longitude != Convert.INVALID_COORDINATE_DOUBLE) ? "NULL" : this.longitude) + ","
				+ ((this.latitude != Convert.INVALID_COORDINATE_DOUBLE) ? "NULL" : this.latitude)+ ","
				+ ((this.original_geo_field != null) ? "NULL" : this.original_geo_field) + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows inserted into tweets");
	}
	
	private String convertDateTimeToTimeString(DateTime date){
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z").withZone(DateTimeZone.UTC);
		return fmt.print(date);
	}
}
