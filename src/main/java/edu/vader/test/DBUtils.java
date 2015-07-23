package edu.vader.test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.vader.exporter.Main;

public class DBUtils {
	public static void deleteAll() throws SQLException{
		PreparedStatement st = Main.conn.prepareStatement("DELETE FROM retweet");
		int rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows updated in retweet.");
		
		st = Main.conn.prepareStatement("DELETE FROM tweet");
		rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows updated in tweet.");
		
		st = Main.conn.prepareStatement("DELETE FROM user_mention");
		rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows updated in user_mention.");
		
		st = Main.conn.prepareStatement("DELETE FROM link");
		rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows updated in link.");
		
		st = Main.conn.prepareStatement("DELETE FROM hashtag");
		rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows updated in hashtag.");
	}
}
