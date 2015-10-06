package edu.vader.bulkInsert;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.vader.exporter.Main;

public class BulkInsertURL {
	private PreparedStatement deletePreparedStatement = null;
	private PreparedStatement insertPreparedStatement = null;
	
	public BulkInsertURL() throws SQLException{
		this.insertPreparedStatement = Main.conn.prepareStatement("INSERT INTO url"
				+ " (tid, url)"
				+ " VALUES (?,?)");
		
		this.deletePreparedStatement = Main.conn.prepareStatement("DELETE FROM url "
				+ " WHERE tid = ?");
	}
	
	public void insertToDeleteBatch(long tid) throws SQLException{
		this.deletePreparedStatement.setLong(1, tid);
	}
	public void insertToInsertBatch(long tid, String url) throws SQLException{
		this.insertPreparedStatement.setLong(1, tid);
		this.insertPreparedStatement.setString(2, url);
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
