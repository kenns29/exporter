package edu.vader.bulkInsert;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.vader.exporter.Main;

public class BulkInsertTweetLocationRelation {
	private PreparedStatement deletePreparedStatement = null;
	private PreparedStatement insertPreparedStatement = null;
		
	public BulkInsertTweetLocationRelation() throws SQLException{
		this.insertPreparedStatement = Main.conn.prepareStatement("INSERT INTO tweet_location_relation "
				+ " (tid, type, name, code)"
				+ " VALUES (?,?,?,?)");
		
		this.deletePreparedStatement = Main.conn.prepareStatement("DELETE FROM tweet_location_relation "
				+ " WHERE tid = ?");
	}
	public void insertToDeleteBatch(long tid) throws SQLException{
		this.deletePreparedStatement.setLong(1, tid);
	}
	public void insertToInsertBatch(long tid, String type, String name, String code) throws SQLException{
		this.insertPreparedStatement.setLong(1, tid);
		this.insertPreparedStatement.setString(2, type);
		this.insertPreparedStatement.setString(3, name);
		this.insertPreparedStatement.setString(4, code);
	}
	
	public void insertToSql() throws Exception{
		this.insertPreparedStatement.executeBatch();
		this.deletePreparedStatement.executeBatch();
		Main.conn.commit();
	}	
	public void reset() throws Exception{
		this.deletePreparedStatement.clearBatch();
		this.insertPreparedStatement.clearBatch();
	
	}
}
