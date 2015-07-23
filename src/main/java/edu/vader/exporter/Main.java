package edu.vader.exporter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Main{
	public static String dataUrl = "jdbc:postgresql://fsdb1.dtn.asu.edu:5432/fsdb";
	public static Properties props = new Properties();
	public static Connection conn = null;
	static{
		props.setProperty("user", "webserver");
		try {
			conn = DriverManager.getConnection(dataUrl, props);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public static void main(String args[]) throws SQLException{	
		PreparedStatement st = conn.prepareStatement("DELETE FROM retweet");
		int rowsUpdated = st.executeUpdate();
		System.out.println(rowsUpdated + " rows updated.");
		
		
	}
}