package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertRetweet {
	private static final String escape = "$verySpecialToken$";
	
	public long tid = 0;
	public String screen_name_from = null;
	public String screen_name_to = null;
	
	public InsertRetweet(long tid, String screen_name_from, String screen_name_to){
		this.tid = tid;
		this.screen_name_from = escape + screen_name_from + escape;
		this.screen_name_to = escape + screen_name_to + escape;
	}
	
	public void insert() throws SQLException{
		String prep = "INSERT into tweet (tid, screen_name_from, screen_name_to) VALUES (" 
				+ this.tid + "," 
				+ this.screen_name_from + ","
				+ this.screen_name_to + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows inserted into tweets");
	}
}
