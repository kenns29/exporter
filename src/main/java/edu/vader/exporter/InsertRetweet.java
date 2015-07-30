package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class InsertRetweet {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	public static int retweetCount = 0;
	public long tid = 0;
	public String screen_name_from = null;
	public String screen_name_to = null;
	
	public InsertRetweet(long tid, String screen_name_from, String screen_name_to){
		this.tid = tid;
		this.screen_name_from = escape + screen_name_from + escape;
		this.screen_name_to = escape + screen_name_to + escape;
	}
	
	public void insert() throws SQLException{
		String prep = "INSERT into retweet (tid, screen_name_from, screen_name_to) VALUES (" 
				+ this.tid + "," 
				+ this.screen_name_from + ","
				+ this.screen_name_to + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		retweetCount += rowsUpdated;
		if(retweetCount % Main.REPORT_INTERVAL == 0){
			LOGGER.info(retweetCount + " rows inserted into tweets");
		}
	}
}
