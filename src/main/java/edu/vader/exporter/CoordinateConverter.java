package edu.vader.exporter;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class CoordinateConverter {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private GeometryFactory geomFac = new GeometryFactory();
	private static class GeoField{
		public static final String COORDINATE = "coordinate";
		public static final String PLACE = "place";
		public static final String USER_PROFILE = "user_profile";
		public static final String TEXT = "text";
	}
	private static final double INVALID_DOUBLE = 0;
	private Document doc = null;
	private Coordinate coordinate = null;
	private String original_geo_field = null;
	private boolean isWithinBoundingBox = false;
	public CoordinateConverter(Document doc){
		this.doc = doc;
		this.setCoordinate(this.getCoordinateFromDoc(this.doc));
		this.setWithinBoundingBox(this.hasIntersection(this.coordinate, Main.geoBoundingBox.getBoundingBox()));
	}
	
	public boolean hasIntersection(Coordinate coordinate, Geometry boundingBox){
		Point point = geomFac.createPoint(coordinate);
		if(point.intersects(boundingBox)){
			return true;
		}
		else{
			return false;
		}
	}
	public Coordinate getCoordinateFromDoc(Document doc){
		Document mongoCoord = (Document) doc.get("coordinates");
		if(mongoCoord != null){
			Coordinate coordinate = convertMongoCoordinateToCoordinate(mongoCoord);
			Point point = geomFac.createPoint(coordinate);
			if(point.intersects(Main.geoBoundingBox.getBoundingBox())){
				setOriginal_geo_field(GeoField.COORDINATE);
				return coordinate;
			}
		}
		
		Document place = (Document) doc.get("place");
		if(place != null){
			Document placeBoundingBox = (Document) place.get("bounding_box");
			if(placeBoundingBox != null){
				Coordinate coordinate = convertMongoPlaceBoundingBoxToCoordinate(placeBoundingBox);
				Point point = geomFac.createPoint(coordinate);
				if(point.intersects(Main.geoBoundingBox.getBoundingBox())){
					setOriginal_geo_field(GeoField.PLACE);
					return coordinate; 
				}
			}
		}
		@SuppressWarnings("unchecked")
		ArrayList<Document> ner = (ArrayList<Document>) doc.get("ner");
		if(ner != null && ner.size() > 0){
			return getCoordinateFromNer(ner);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public Coordinate convertMongoCoordinateToCoordinate(Document mongoCoord){
		ArrayList<Object> mongoCoordArray = (ArrayList<Object>) mongoCoord.get("coordinates");
		double lng = getDoubleFromCoordinatesItem(mongoCoordArray.get(0));
		double lat = getDoubleFromCoordinatesItem(mongoCoordArray.get(1));
		return new Coordinate(lng, lat);
	}
	
	@SuppressWarnings("unchecked")
	public Coordinate convertMongoPlaceBoundingBoxToCoordinate(Document mongoBoundingBox){
		ArrayList<ArrayList<ArrayList<Object>>> mongoCoords = (ArrayList<ArrayList<ArrayList<Object>>>) mongoBoundingBox.get("coordinates");
		ArrayList<ArrayList<Object>> mongoPoly = mongoCoords.get(0);
		double lngSum = 0;
		double latSum = 0;
		for(int i = 0; i < mongoPoly.size(); i++){
			ArrayList<Object> mongoCoord = mongoPoly.get(i);
			lngSum += getDoubleFromCoordinatesItem(mongoCoord.get(0));
			latSum += getDoubleFromCoordinatesItem(mongoCoord.get(1));
		}
		
		return new Coordinate(lngSum / mongoPoly.size(), latSum / mongoPoly.size());
	}
	
	public Coordinate getCoordinateFromNer(ArrayList<Document> ner){
		for(int i = 0; i < ner.size(); i++){
			Document nerDoc = ner.get(i);
			Document geonameObj = (Document) nerDoc.get("geoname");
			if(geonameObj != null){
				String from = nerDoc.getString("from");
				if(from.equals("user.location")){	
					Coordinate coordinate = convertGeonameToCoordinate(geonameObj);
					Point point = geomFac.createPoint(coordinate);
					if(point.intersects(Main.geoBoundingBox.getBoundingBox())){
						this.setOriginal_geo_field(GeoField.USER_PROFILE);
						return coordinate;
					}
				}
			}
		}
		
		for(int i = 0; i < ner.size(); i++){
			Document nerDoc = ner.get(i);
			Document geonameObj = (Document) nerDoc.get("geoname");
			if(geonameObj != null){
				Coordinate coordinate = convertGeonameToCoordinate(geonameObj);
				Point point = geomFac.createPoint(coordinate);
				if(point.intersects(Main.geoBoundingBox.getBoundingBox())){
					this.setOriginal_geo_field(GeoField.TEXT);
					return coordinate;
				}
			}
		}
		
		return null;
	}
	
	public Coordinate convertGeonameToCoordinate(Document geonameObj){
		Document coord = (Document) geonameObj.get("coord");
		return new Coordinate(coord.getDouble("lng"), coord.getDouble("lat"));
	}
	
	private double getDoubleFromCoordinatesItem(Object coordItem){
		if(coordItem instanceof Double){
			return ((Double)coordItem).doubleValue();
		}
		else if(coordItem instanceof Integer){
			return ((Integer) coordItem).doubleValue();
		}
		else{
			return INVALID_DOUBLE;
		}
	}

	public Document getDoc() {
		return doc;
	}

	public void setDoc(Document doc) {
		this.doc = doc;
	}

	public void setCoordinate(Coordinate coordinate) {
		this.coordinate = coordinate;
	}

	public Coordinate getCoordinate() {
		return coordinate;
	}

	public String getOriginal_geo_field() {
		return original_geo_field;
	}

	public void setOriginal_geo_field(String original_geo_field) {
		this.original_geo_field = original_geo_field;
	}

	public boolean isWithinBoundingBox() {
		return isWithinBoundingBox;
	}

	public void setWithinBoundingBox(boolean isWithinBoundingBox) {
		this.isWithinBoundingBox = isWithinBoundingBox;
	}
	

}
