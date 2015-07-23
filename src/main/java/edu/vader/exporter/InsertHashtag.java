package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertHashtag {
	private static final String escape = "$verySpecialToken$";
	
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
		System.out.println(rowsUpdated + " rows inserted into hashtag");
	}
}
