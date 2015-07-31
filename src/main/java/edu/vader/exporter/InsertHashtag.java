package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class InsertHashtag {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	public static int hashtagCount = 0;
	private long tid = 0;
	private String hashtag = null;
	private ObjectId oid = null;
	public InsertHashtag(ObjectId oid, long tid, String hashtag){
		this.tid = tid;
		this.hashtag = escape + hashtag + escape;
		this.oid = oid;
	}
	
	public int insert() throws SQLException{
		String prep = "INSERT into hashtag (tid, hashtag) VALUES (" 
				+ this.tid + "," + this.hashtag + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		
		return rowsUpdated;
	}
	
	public int deleteAllWithTid() throws Exception{
		String prep = "DELETE from hashtag"
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
					HIGH_PRIORITY_LOGGER.error("did not successfully update the hastag for tid " + this.tid + ", the Object Id is " + this.oid, e);
				}
			}
		}
		while(retryFlag);
		hashtagCount += rowsUpdated;
		if(hashtagCount % Main.configProperties.insertionReportInterval == 0){
			LOGGER.info(hashtagCount + " rows inserted into hashtag");
		}
		return rowsUpdated;
		
	}
}
