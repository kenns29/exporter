package edu.vader.bulkInsert;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.vader.exporter.Main;

public class BulkInsertTweet {
	private PreparedStatement preparedStatement = null;
	
	public BulkInsertTweet() throws SQLException{
		this.preparedStatement = Main.conn.prepareStatement("INSERT INTO tweets"
				+ " (tweet_id, date, text, uid, longitude, latitude, time_numeric, name, screen_name)"
				+ " VALUES (?,?,?,?,?,?,?,?,?,?)");
	}
	
	public void insertToBatch(long tweet_id, 
			Date date, 
			String text, 
			int uid, 
			double longitude, 
			double latitude, 
			long time_numeric, 
			String name, 
			String screen_name) throws Exception{
		this.preparedStatement.setLong(1, tweet_id);
		this.preparedStatement.setDate(2, date);
		this.preparedStatement.setString(3, text);
		this.preparedStatement.setInt(4, uid);
		this.preparedStatement.setDouble(5, longitude);
		this.preparedStatement.setDouble(6, latitude);;
		this.preparedStatement.setLong(7, time_numeric);
		this.preparedStatement.setString(8, name);
		this.preparedStatement.setString(9, screen_name);
		this.preparedStatement.addBatch();
	}
	
	public void reset() throws Exception{
		this.preparedStatement.clearBatch();
	}
	
	public void insertToSql() throws Exception{
		this.preparedStatement.executeBatch();
		Main.conn.commit();
	}
	
	
}
