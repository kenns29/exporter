package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertLink {
	private static final String escape = "$verySpecialToken$";
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
		System.out.println(rowsUpdated + " rows inserted into tweets");
	}
}
