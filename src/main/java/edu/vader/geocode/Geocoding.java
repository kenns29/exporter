package edu.vader.geocode;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;

import edu.vader.communicate.Connection;
import edu.vader.exporter.Main;

public class Geocoding {
	public String reverseLookUp(Coordinate coordinate) throws Exception{
		JsonArray jsonArray = getJsonArrayFromCoordinate(coordinate);
		return getPlaceName(jsonArray);
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
	
	public String getPlaceName(JsonArray jsonArray){
		if(jsonArray != null && jsonArray.size() > 0){
			JsonObject firstObject = jsonArray.get(0).getAsJsonObject();
			JsonElement politicsElement = firstObject.get("politics");
			if(politicsElement != null && !politicsElement.isJsonNull()){
				JsonArray politicsArray = politicsElement.getAsJsonArray();
				if(politicsArray.size() > 0){
					JsonObject politicObject = politicsArray.get(0).getAsJsonObject();
					for(int i = 1; i < politicsArray.size(); i++){
						JsonObject politicObject1 = politicsArray.get(i).getAsJsonObject();
						if(compareLocationObject(politicObject, politicObject1) < 0){
							politicObject = politicObject1;
						}
					}
					return politicObject.get("name").getAsString();
				}
				
			}
		}
		
		return null;
	}
	
	private int compareLocationObject(JsonObject obj1, JsonObject obj2){
		String type1 = obj1.get("type").getAsString();
		String type2 = obj2.get("type").getAsString();
		return compareLocationType(type1, type2);
	}
	private int compareLocationType(String type1, String type2){
		//0 : equal
		//<0: type1 < type2
		//>0 : type1 > type2
		if(type1.matches("^admin[\\d*]") && !type2.matches("^admin[\\d*]")){
			return 1;
		}
		else if(!type1.matches("^admin[\\d*]") && type2.matches("^admin[\\d*]")){
			return -1;
		}
		else if(!type1.matches("^admin[\\d*]") && !type2.matches("^admin[\\d*]")){
			return type1.compareTo(type2);
		}
		else{
			String numStr1 = type1.replaceAll("[^0-9]", "");
			String numStr2 = type2.replaceAll("[^0-9]", "");
			int num1 = Integer.valueOf(numStr1).intValue();
			int num2 = Integer.valueOf(numStr2).intValue();
			return num1 - num2;
		}
	}
	private String coordinateToUrlParam(Coordinate coordinate){
		return coordinate.y + "%2c" + coordinate.x;
	}
}
