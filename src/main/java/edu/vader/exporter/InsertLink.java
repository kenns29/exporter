package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class InsertLink {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private static final String escape = "$verySpecialToken$";
	
	public static int linkCount = 0;
	public String url = null;
	public long tid = 0;
	
	public InsertLink(String url, long tid){
		this.url = escape + url + escape;
		this.tid = tid;
	}
	
	public void insert() throws SQLException{
		String prep = "INSERT into link (url, tid) VALUES (" 
				+ this.url + "," 
				+ this.tid + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		linkCount += rowsUpdated;
		if(linkCount % Main.REPORT_INTERVAL == 0){
			LOGGER.info(linkCount + " rows inserted into tweets");
		}
	}
}
