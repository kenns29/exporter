package edu.vader.geo;

import java.util.ArrayList;

import org.bson.Document;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class GeoHandler {
	private GeometryFactory geomFac = new GeometryFactory();
	@SuppressWarnings("unchecked")
	public Point convertMongoGeojsonPointToPoint(Document locationCollectionObj){
		ArrayList<Double> coordinate = (ArrayList<Double>) locationCollectionObj.get("coordinates");
		return geomFac.createPoint(new Coordinate(coordinate.get(0), coordinate.get(1)));
	}
	
	@SuppressWarnings("unchecked")
	public Polygon convertMongoGeojsonPolygonToPolygon(Document locationCollectionObj){
		ArrayList<ArrayList<ArrayList<Double>>> mongoPolys = (ArrayList<ArrayList<ArrayList<Double>>>) locationCollectionObj.get("coordinates");
		ArrayList<ArrayList<Double>> mongoPoly = mongoPolys.get(0);
		Coordinate coordinates[] = new Coordinate[5];
		for(int i = 0; i < mongoPoly.size(); i++){
			coordinates[i] = new Coordinate(mongoPoly.get(i).get(0), mongoPoly.get(i).get(1));
		}
		return geomFac.createPolygon(coordinates);
	}
		
	public boolean hasIntersection(Document doc, Geometry boundingBox){
		@SuppressWarnings("unchecked")
		ArrayList<Document> mongoGeometries = (ArrayList<Document>) doc.get("geometries");
		ArrayList<Point> points = new ArrayList<Point>();
		ArrayList<Polygon> polys = new ArrayList<Polygon>();
		
		for(int i = 0; i < mongoGeometries.size(); i++){
			Document mongoGeometry = mongoGeometries.get(i);
			String type = mongoGeometry.getString("type");
			if(type.equals("Point")){
				points.add(convertMongoGeojsonPointToPoint(mongoGeometry));
			}
			else{
				polys.add(convertMongoGeojsonPolygonToPolygon(mongoGeometry));
			}
		}
		
		Point[] p = new Point[points.size()];
		p = points.toArray(p);
		
		MultiPoint multiPoints = new MultiPoint(p, geomFac);
		if(multiPoints.intersects(boundingBox)){
			return true;
		}
		
		if(polys.size() > 0){
			Geometry multiPolys = polys.get(0);
			for(int i = 1; i < polys.size(); i++){
				multiPolys = multiPolys.union(polys.get(i));
			}
			if(multiPolys.intersects(boundingBox)){
				return true;
			}
		}
		return false;
	}
	
}
