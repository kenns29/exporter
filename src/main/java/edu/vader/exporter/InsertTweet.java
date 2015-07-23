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
	public int retweet = 0;
	public String geo = null;
	public long time_numeric = 0;
	public long uid = 0;
	
	public InsertTweet(long tid, DateTime date, String tweet, String place, String language, int retweet, String geo, long time_numeric, long uid){
		this.tid = tid;
		this.date = date;
		this.tweet = tweet;
		this.place = place;
		this.language = language;
		this.retweet = retweet;
		this.geo = geo;
		this.time_numeric = time_numeric;
		this.uid = uid;
	}
	
	public InsertTweet(long tid, long timestamp, String tweet, String place, String language, int retweet, String geo, long time_numeric, long uid){
		DateTime date = new DateTime(timestamp);
		
		this.tid = tid;
		this.date = date;
		this.tweet = escape + tweet + escape;
		this.place = place;
		this.language = language;
		this.retweet = retweet;
		this.geo = geo;
		this.time_numeric = time_numeric;
		this.uid = uid;
	}
	
	public void insert() throws SQLException{
		String timestamp = "TIMESTAMP '" + convertDateTimeToTimeString(this.date) + "'";
		String prep = "INSERT into tweet (tid, date, tweet, uid) VALUES (" + this.tid + "," + timestamp + "," + this.tweet + "," + this.uid + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows inserted into tweets");
	}
	
	private String convertDateTimeToTimeString(DateTime date){
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z").withZone(DateTimeZone.UTC);
		return fmt.print(date);
	}
}
