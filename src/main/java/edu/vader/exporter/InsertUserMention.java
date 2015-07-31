package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class InsertUserMention {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static int userMentionCount = 0;
	private static final String escape = "$verySpecialToken$";
	public long uid = 0;
	public String screen_name = null;
	public String name = null;
	public long tid = 0;
	
	public InsertUserMention(long uid, String screen_name, String name, long tid){
		this.uid = uid;
		this.screen_name = escape + screen_name + escape;
		this.name = escape + name + escape;
		this.tid = tid;
	}
	
	public void insert() throws SQLException{
		String prep = "INSERT into user_mention (uid, screen_name, name, tid) VALUES (" 
				+ this.uid + ","
				+ this.screen_name + "," 
				+ this.name + "," 
				+ this.tid + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		userMentionCount += rowsUpdated;
		if(userMentionCount % Main.configProperties.insertionReportInterval == 0){
			LOGGER.info(userMentionCount + " rows inserted into user_mentions");
		}
	}
}
