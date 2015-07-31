package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class InsertLink {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	
	public static int linkCount = 0;
	private String url = null;
	private long tid = 0;
	private ObjectId oid = null;
	public InsertLink(ObjectId oid, String url, long tid){
		this.url = escape + url + escape;
		this.tid = tid;
		this.oid = oid;
	}
	
	private int insert() throws SQLException{
		String prep = "INSERT into link (url, tid) VALUES (" 
				+ this.url + "," 
				+ this.tid + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		return rowsUpdated;
	}
	
	public int deleteAllWithTid() throws Exception{
		String prep = "DELETE from link"
				+ " WHERE tid = " + this.tid;
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		return rowsUpdated;
	}
	
	public int insertWithReport(){
		boolean retryFlag = false;
		int rowsUpdated = 0;
		int errorCount = 0;
		int errorCountLimit = 5;
		do{
			try{
				rowsUpdated = insert();
				retryFlag = false;
			}
			catch(Exception e){
				retryFlag = true;
				++errorCount;
				if(errorCount > errorCountLimit){
					retryFlag = false;
					HIGH_PRIORITY_LOGGER.error("did not successfully update the Link for tid " + this.tid + ", the Object Id is " + this.oid, e);
				}
			}
		}
		while(retryFlag);
		linkCount += rowsUpdated;
		if(linkCount % Main.configProperties.insertionReportInterval == 0){
			LOGGER.info(linkCount + " rows inserted into tweets");
		}
		return rowsUpdated;
		
	}
	
}
