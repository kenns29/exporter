package edu.vader.exporter;

import java.util.ArrayList;

import org.bson.Document;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateConverter {
	private static final double INVALID_DOUBLE = 0;
	private Document doc = null;
	private Coordinate coordinate = null;
	public CoordinateConverter(Document doc){
		this.doc = doc;
		this.setCoordinate(this.getCoordinateFromDoc(this.doc));
	}
	
	public Coordinate getCoordinateFromDoc(Document doc){
		Document mongoCoord = (Document) doc.get("coordinates");
		if(mongoCoord != null){
			return convertMongoCoordinateToCoordinate(mongoCoord);
		}
		
		Document place = (Document) doc.get("place");
		if(place != null){
			Document placeBoundingBox = (Document) place.get("bounding_box");
			if(placeBoundingBox != null){
				return convertMongoPlaceBoundingBoxToCoordinate(placeBoundingBox);
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
					return convertGeonameToCoordinate(geonameObj);
				}
			}
		}
		
		for(int i = 0; i < ner.size(); i++){
			Document nerDoc = ner.get(i);
			Document geonameObj = (Document) nerDoc.get("geoname");
			if(geonameObj != null){
				return convertGeonameToCoordinate(geonameObj);
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
	

}
