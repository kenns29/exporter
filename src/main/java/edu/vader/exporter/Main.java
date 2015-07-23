package edu.vader.exporter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class Main{
	public static String dataUrl = "jdbc:postgresql://fsdb1.dtn.asu.edu:5432/temp_twitter";
	public static Properties props = new Properties();
	public static Connection conn = null;
	static{
		props.setProperty("user", "postgres");
		try {
			conn = DriverManager.getConnection(dataUrl, props);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) throws SQLException{	
		
		
		
	}
}