package edu.vader.exporter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

public class Convert {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static final double INVALID_COORDINATE_DOUBLE = -1000;
	@SuppressWarnings("unchecked")
	public void convertMongoToSql() throws SQLException{
		Document query = new Document();
		
		FindIterable<Document> iterable = Main.mongoColl.find().sort(new Document("_id", 1)).limit(10000);
		MongoCursor<Document> mongoCursor = iterable.iterator();
		
		try{
			while(mongoCursor.hasNext()){
				Document doc = mongoCursor.next();
				Document locationCollection = (Document) doc.get("locationCollection");
				
				if(locationCollection != null){
					if(Main.geoHandler.hasIntersection(locationCollection, Main.geoBoundingBox.getBoundingBox())){
						//System.out.println(doc.toJson());
						convertOneDoc(doc);
					}
				}
			}
		}
		finally{
			mongoCursor.close();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void convertOneDoc(Document doc) throws SQLException{
		long id = doc.getLong("id");
		String text = doc.getString("text");
		Document user = (Document) doc.get("user");
		long uid = 0;
		Object uidObj = user.get("id");
		if(uidObj instanceof Long){
			uid = ((Long)uidObj).longValue();
		}
		else{
			uid = ((Integer)uidObj).longValue();
		}
		
		long timestamp = doc.getLong("timestamp");
		String language = doc.getString("lang");
		int retweet_count = doc.getInteger("retweet_count", 0);
		CoordinateConverter coordinateConverter = new CoordinateConverter(doc);
		Coordinate coordinate = coordinateConverter.getCoordinate();
		
		double lng = Convert.INVALID_COORDINATE_DOUBLE;
		double lat = Convert.INVALID_COORDINATE_DOUBLE;
		if(coordinate != null){
			lng = coordinate.x;
			lat = coordinate.y;
		}	
		
		InsertTweet insertTweet = new InsertTweet(id, timestamp, text, " ", language, retweet_count, uid, lng, lat, coordinateConverter.getOriginal_geo_field());
		insertTweet.insert();
		
		Document entities = (Document) doc.get("entities");
		if(entities != null){
			ArrayList<Document> user_mentions = (ArrayList<Document>) entities.get("user_mentions");
			if(user_mentions != null){
				for(int i = 0; i < user_mentions.size(); i++){
					Document m = user_mentions.get(i);
					//InsertUserMention(long uid, String screen_name, String name, long tid, long tweet_uid)
					Object mUidObj = m.get("id");
					long mUid = -1;
					if(mUidObj instanceof Long){
						mUid = ((Long)mUidObj).longValue();
					}
					else if(mUidObj instanceof Integer){
						mUid = ((Integer)mUidObj).longValue();
					}
					String mScreen_name = m.getString("screen_name");
					String mName = m.getString("name");
					InsertUserMention insertUserMention = new InsertUserMention(mUid, mScreen_name, mName, id);
					insertUserMention.insert();
				}
			}
			
			ArrayList<Document> hashtags = (ArrayList<Document>) entities.get("hashtags");
			if(hashtags != null){
				for(int i = 0; i < hashtags.size(); i++){
					Document tag = hashtags.get(i);
					String tagText = tag.getString("text");
					InsertHashtag insertHashtag = new InsertHashtag(id, tagText);
					insertHashtag.insert();
				}
			}
		}
	}
}
