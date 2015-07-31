package edu.vader.geocode;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;

import edu.vader.communicate.Connection;
import edu.vader.exporter.Main;

public class Geocoding {
	public String reverseLookUp(Coordinate coordinate) throws Exception{
		JsonArray jsonArray = getJsonArrayFromCoordinate(coordinate);
		
		return null;
	}
	
	public JsonArray getJsonArrayFromCoordinate(Coordinate coordinate) throws Exception{
		JsonArray jsonArray = null;
		Connection connection = new Connection(Main.configProperties.dataScienceToolkitBaseUrl + "/"
				+ Main.configProperties.coordinate2politicsUrl + "/" 
				+ coordinateToUrlParam(coordinate));
		
		String response = connection.getResponse();
		
		if(response != null){
			jsonArray = new JsonParser().parse(response).getAsJsonArray();
		}
		return jsonArray;
	}
	private String coordinateToUrlParam(Coordinate coordinate){
		return coordinate.y + "%2c" + coordinate.x;
	}
}
