package edu.vader.exporter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertUserMention {
	private static final String escape = "$verySpecialToken$";
	public long uid = 0;
	public String screen_name = null;
	public String name = null;
	public long tid = 0;
	public long tweet_uid = 0;
	
	public InsertUserMention(long uid, String screen_name, String name, long tid, long tweet_uid){
		this.uid = uid;
		this.screen_name = escape + screen_name + escape;
		this.name = escape + name + escape;
		this.tid = tid;
		this.tweet_uid = tweet_uid;
	}
	
	public void insert() throws SQLException{
		String prep = "INSERT into user_mention (uid, screen_name, name, tid) VALUES (" 
				+ this.tid + "," + this.screen_name + "," 
				+ this.name + "," + this.tid + ")";
		PreparedStatement st = Main.conn.prepareStatement(prep);
		int rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows inserted into user_mentions");
	}
}
