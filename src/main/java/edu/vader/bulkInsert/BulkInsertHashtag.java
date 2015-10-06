package edu.vader.bulkInsert;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.vader.exporter.Main;

public class BulkInsertHashtag {
	private PreparedStatement deletePreparedStatement = null;
	private PreparedStatement insertPreparedStatement = null;
	
	public BulkInsertHashtag() throws SQLException{
		this.insertPreparedStatement = Main.conn.prepareStatement("INSERT INTO hashtag"
				+ " (tid, hashtag)"
				+ " VALUES (?,?)");
		
		this.deletePreparedStatement = Main.conn.prepareStatement("DELETE FROM hashtag "
				+ " WHERE tid = ?");
	}
	
	public void insertToDeleteBatch(long tid) throws SQLException{
		this.deletePreparedStatement.setLong(1, tid);
	}
	public void insertToInsertBatch(long tid, String hashtag) throws SQLException{
		this.insertPreparedStatement.setLong(1, tid);
		this.insertPreparedStatement.setString(2, hashtag);
	}
	public void insertToSql() throws Exception{
		this.insertPreparedStatement.executeBatch();
		this.deletePreparedStatement.executeBatch();
		Main.conn.commit();
	}	
	public void reset() throws Exception{
		this.insertPreparedStatement.clearBatch();
		this.deletePreparedStatement.clearBatch();
	}

}
