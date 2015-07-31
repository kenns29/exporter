package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class InsertRetweet {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	public static int retweetCount = 0;
	private long tid = 0;
	private String screen_name_from = null;
	private String screen_name_to = null;
	private ObjectId oid = null;
	public InsertRetweet(ObjectId oid, long tid, String screen_name_from, String screen_name_to){
		this.tid = tid;
		this.screen_name_from = escape + screen_name_from + escape;
		this.screen_name_to = escape + screen_name_to + escape;
		this.oid = oid;
	}
	
	private int insert() throws Exception{
		String prep = "INSERT into retweet (tid, screen_name_from, screen_name_to) VALUES (" 
				+ this.tid + "," 
				+ this.screen_name_from + ","
				+ this.screen_name_to + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		
		return rowsUpdated;
	}
	public int deleteAllWithTid() throws Exception{
		String prep = "DELETE from retweet"
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
					HIGH_PRIORITY_LOGGER.error("did not successfully update the Retweet for tid " + this.tid + ", the Object Id is " + this.oid, e);
				}
			}
		}
		while(retryFlag);
		retweetCount += rowsUpdated;
		if(retweetCount % Main.configProperties.insertionReportInterval == 0){
			LOGGER.info(retweetCount + " rows inserted into tweets");
		}
		return rowsUpdated;
	}
}
