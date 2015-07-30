package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class InsertHashtag {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	public static int hashtagCount = 0;
	public long tid = 0;
	public String hashtag = null;
	
	public InsertHashtag(long tid, String hashtag){
		this.tid = tid;
		this.hashtag = escape + hashtag + escape;
	}
	
	public void insert() throws SQLException{
		String prep = "INSERT into hashtag (tid, hashtag) VALUES (" 
				+ this.tid + "," + this.hashtag + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		hashtagCount += rowsUpdated;
		if(hashtagCount % Main.REPORT_INTERVAL == 0){
			LOGGER.info(hashtagCount + " rows inserted into hashtag");
		}
	}
}
