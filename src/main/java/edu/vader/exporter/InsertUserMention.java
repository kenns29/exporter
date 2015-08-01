package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class InsertUserMention {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static int userMentionCount = 0;
	private static final String escape = "$verySpecialToken$";
	private long uid = 0;
	private String screen_name = null;
	private String name = null;
	private long tid = 0;
	private ObjectId oid = null;
	public InsertUserMention(ObjectId oid, long tid){
		this.oid = oid;
		this.tid = tid;
	}
	public InsertUserMention(ObjectId oid, long uid, String screen_name, String name, long tid){
		setUserMention(oid, uid, screen_name, name, tid);
	}
	
	private int insert() throws Exception{
		String prep = "INSERT into user_mention (uid, screen_name, name, tid) VALUES (" 
				+ this.uid + ","
				+ this.screen_name + "," 
				+ this.name + "," 
				+ this.tid + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		
		return rowsUpdated;
	}
	
	public void setUserMention(ObjectId oid, long uid, String screen_name, String name, long tid){
		this.uid = uid;
		this.screen_name = escape + screen_name + escape;
		this.name = escape + name + escape;
		this.tid = tid;
		this.oid = oid;
	}
	
	private int deleteAllWithTid() throws Exception{
		String prep = "DELETE from user_mention"
				+ " WHERE tid = " + this.tid;
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		return rowsUpdated;
	}
	
	public int deleteAllWithTidWithReport(){
		boolean retryFlag = false;
		int rowsUpdated = 0;
		int errorCount = 0;
		int errorCountLimit = 5;
		do{
			try{
				rowsUpdated = deleteAllWithTid();
				retryFlag = false;
			}
			catch(Exception e){
				retryFlag = true;
				++errorCount;
				if(errorCount > errorCountLimit){
					retryFlag = false;
					HIGH_PRIORITY_LOGGER.error("did not successfully delete the User_Mention for tid " + this.tid + ", the Object Id is " + this.oid, e);
				}
			}
		}
		while(retryFlag);
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
					HIGH_PRIORITY_LOGGER.error("did not successfully update the User_Mention for tid " + this.tid + ", the Object Id is " + this.oid, e);
				}
			}
		}
		while(retryFlag);
		userMentionCount += rowsUpdated;
		if(userMentionCount % Main.configProperties.insertionReportInterval == 0){
			LOGGER.info(userMentionCount + " rows inserted into user_mentions");
		}
		return rowsUpdated;
	}
}
