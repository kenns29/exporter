package edu.vader.geo;

import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import edu.vader.util.Utils;

public class GeoBoundingBox {
	private static GeometryFactory geomFac = new GeometryFactory();
	private Polygon boundingBox = null;
	
	/**
	 * Create a boundary object from a GeoJSON Feature Collection
	 * JSON file.  See http://geojson.org/geojson-spec.html
	 * @param fileName File name of GeoJSON file
	 * @throws IOException
	 */
	public GeoBoundingBox(String fileName) throws IOException{
		this.setBoundingBox(getBoundingBoxPolygonFromFile(fileName));
	}
	
	public static JsonObject readGeojsonFromFile(String name) throws IOException{
		String jsonStr = Utils.readFile(name);
		return new JsonParser().parse(jsonStr).getAsJsonObject();
	}
	
	public static Polygon getBoundingBoxPolygon(JsonObject jsonObject){
		JsonArray jsonArray = jsonObject.get("features").getAsJsonArray();
		JsonObject featureObj = jsonArray.get(0).getAsJsonObject();
		JsonObject geometry = featureObj.get("geometry").getAsJsonObject();
		JsonArray jsonCoordinates = geometry.get("coordinates").getAsJsonArray();
		JsonArray jsonPoly = jsonCoordinates.get(0).getAsJsonArray();
		ArrayList<Coordinate> coordinateArray = new ArrayList<Coordinate>();
		for(int i = 0; i < jsonPoly.size(); i++){
			JsonArray jsonCoord = jsonPoly.get(i).getAsJsonArray();
			coordinateArray.add(new Coordinate(jsonCoord.get(0).getAsDouble(), jsonCoord.get(1).getAsDouble()));
		}
		Coordinate[] coordinates = new Coordinate[coordinateArray.size()];
		coordinates = coordinateArray.toArray(coordinates);
		return geomFac.createPolygon(coordinates);
	}
	
	public static Polygon getBoundingBoxPolygonFromFile(String name) throws IOException{
		JsonObject jsonObj = readGeojsonFromFile(name);
		return getBoundingBoxPolygon(jsonObj);
	}
	
	public Polygon getBoundingBox() {
		return boundingBox;
	}
	
	private void setBoundingBox(Polygon boundingBox) {
		this.boundingBox = boundingBox;
	}
}
