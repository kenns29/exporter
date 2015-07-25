package edu.vader.geo;

import java.io.IOException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Polygon;

import edu.vader.util.Utils;

public class GeoBoundingBox {
	public JsonObject readGeojsonFromFile(String name) throws IOException{
		String jsonStr = Utils.readFile(name);
		return new JsonParser().parse(jsonStr).getAsJsonObject();
	}
	
	public Polygon getBoundingBoxPolygon(JsonObject jsonObject){
		JsonArray jsonArray = jsonObject.get("features").getAsJsonArray();
		JsonObject featureObj = jsonArray.get(0).getAsJsonObject();
		
	}
}
