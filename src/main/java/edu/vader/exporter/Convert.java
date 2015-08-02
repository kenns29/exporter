package edu.vader.exporter;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.vividsolutions.jts.geom.Coordinate;

import edu.vader.geocode.Geocoding;

public class Convert {
	private class HasNextTime{
		public long time = 0;
	}
	private static final Logger DEBUG_LOGGER = Logger.getLogger("debugLog");
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static final double INVALID_COORDINATE_DOUBLE = -1000;
	public void convertMongoToSql() throws Exception{
		if(Main.configProperties.catID >= 0){
			Document query = new Document("cat", Main.configProperties.catID);
			this.convertMongoToSql(query);
		}
		else{
			Document query = new Document();
			this.convertMongoToSql(query);
		}
		
	}
	
	public void convertMongoToSql(ObjectId startObjectId, ObjectId endObjectId) throws Exception{
		Document query = new Document("_id", 
				new Document("$gt", startObjectId)
				.append("$lte", endObjectId));
		this.convertMongoToSql(query);
	}
	
	public void convertMongoToSql(Document query) throws Exception{
		DEBUG_LOGGER.info("query = " + query.toJson());
		FindIterable<Document> iterable = Main.mongoColl.find(query).sort(new Document("_id", 1));
		MongoCursor<Document> mongoCursor = iterable.iterator();
		
		try{
			HasNextTime hasNextTime = new HasNextTime();
			while(hasNext(mongoCursor, hasNextTime)){
				
				long startTime = System.currentTimeMillis();
				long getDocStartTime = System.currentTimeMillis();
				Document doc = mongoCursor.next();
				long getDocEndTime = System.currentTimeMillis();
				long docGetTime = getDocEndTime - getDocStartTime;
				long convertTime = 0;
				
				if(hasNextTime.time > 0){
					if(hasNextTime.time > 0){
						DEBUG_LOGGER.info("hasNext time = " + hasNextTime.time + " milliseconds. doc = " + doc.toJson());
					}
				}
				try{
					long convertStartTime = System.currentTimeMillis();
					convertOneDocWithFilter(doc);
					long convertEndTime = System.currentTimeMillis();
					convertTime = convertEndTime - convertStartTime;
					Main.currentObejctId = doc.getObjectId("_id");
					++Main.documentCount;
					++Main.minuteDocCount;
					if(Main.documentCount % Main.configProperties.documentReportInterval == 0){
						LOGGER.info(Main.documentCount + " documents have been processed, current object id is " + Main.currentObejctId);
					}
					long currentTime = System.currentTimeMillis();
					if(currentTime - Main.preStartTime >= 60000){
						Main.lastMinuteDocCount = Main.minuteDocCount;
						Main.minuteDocCount = 0;
						Main.preStartTime = currentTime;
					}
				}
				catch(Exception e){
					HIGH_PRIORITY_LOGGER.error("Document " + doc.getObjectId("_id") + " caused an error.", e);
				}
				
				long endTime = System.currentTimeMillis();
				long elapsedTime = endTime - startTime;
				if(elapsedTime > 0){
					DEBUG_LOGGER.info("one loop time = " + elapsedTime 
							+ ", convert time = " + convertTime + " milliseconds. " 
							+  "doc get time = " + docGetTime + " milliseconds.");
				}
			}
		}
		finally{
			mongoCursor.close();
		}
	}
	
	private boolean hasNext(MongoCursor<Document> mongoCursor, HasNextTime hasNextTime){
		long hasNextStartTime = System.currentTimeMillis();
		boolean has = mongoCursor.hasNext();
		long hasNextEndTime = System.currentTimeMillis();
		hasNextTime.time = hasNextEndTime - hasNextStartTime;
		return has;
	}
	private void convertOneDocWithFilter(Document doc) throws Exception{
		if(Main.configProperties.catID > 0){
			int cat = doc.getInteger("cat", -1);
			if(cat == Main.configProperties.catID){
				CoordinateConverter coordinateConverter = new CoordinateConverter(doc);
				if(coordinateConverter.isWithinBoundingBox()){
					convertOneDoc(doc, coordinateConverter);
				}
			}
		}
		else{
			CoordinateConverter coordinateConverter = new CoordinateConverter(doc);
			if(coordinateConverter.isWithinBoundingBox()){
				convertOneDoc(doc, coordinateConverter);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void convertOneDoc(Document doc, CoordinateConverter coordinateConverter){
		Coordinate coordinate = coordinateConverter.getCoordinate();
		ObjectId oid = doc.getObjectId("_id");
		long id = doc.getLong("id");
		String text = doc.getString("text");
		Document user = (Document) doc.get("user");
		long uid = 0;
		int cat = doc.getInteger("cat", -1);
		if(user != null){
			Object uidObj = user.get("id");
			if(uidObj instanceof Long){
				uid = ((Long)uidObj).longValue();
			}
			else{
				uid = ((Integer)uidObj).longValue();
			}
		}
		long timestamp = doc.getLong("timestamp");
		String language = doc.getString("lang");
		int retweet_count = doc.getInteger("retweet_count", 0);
		
		double lng = Convert.INVALID_COORDINATE_DOUBLE;
		double lat = Convert.INVALID_COORDINATE_DOUBLE;
		String place = null;
		if(coordinate != null){
			lng = coordinate.x;
			lat = coordinate.y;
			Geocoding geocoding = new Geocoding();
			try {
				place = geocoding.reverseLookUp(coordinate);
			} catch (Exception e) {
				HIGH_PRIORITY_LOGGER.error("Geocoding did not complete successfually, document with the error is tweet " + id + ". and the object id is " + doc.getObjectId("_id"));
			}
		}
		InsertTweet insertTweet = new InsertTweet(oid, id, timestamp, text, place , language, retweet_count, uid, lng, lat, coordinateConverter.getOriginal_geo_field(), cat);
		insertTweet.insertOrReplaceWithReport();
		InsertUserMention insertUserMention = new InsertUserMention(oid, id);
		InsertHashtag insertHashtag = new InsertHashtag(oid, id);
		InsertLink insertLink = new InsertLink(oid, id);
		InsertRetweet insertRetweet = new InsertRetweet(oid, id);
		
		insertUserMention.deleteAllWithTidWithReport();
		insertHashtag.deleteAllWithTidWithReport();
		insertLink.deleteAllWithTidWithReport();
		insertRetweet.deleteAllWithTidWithReport();
		
		Document entities = (Document) doc.get("entities");
		if(entities != null){
			//Insert User Mentions
			ArrayList<Document> user_mentions = (ArrayList<Document>) entities.get("user_mentions");
			if(user_mentions != null){
				for(int i = 0; i < user_mentions.size(); i++){
					Document m = user_mentions.get(i);
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
					insertUserMention.setUserMention(oid, mUid, mScreen_name, mName, id);
					insertUserMention.insertWithReport();
				}
			}
			//Insert Hashtags
			ArrayList<Document> hashtags = (ArrayList<Document>) entities.get("hashtags");
			if(hashtags != null){
				for(int i = 0; i < hashtags.size(); i++){
					Document tag = hashtags.get(i);
					String tagText = tag.getString("text");
					insertHashtag.setInsertHashtag(oid, id, tagText);
					insertHashtag.insertWithReport();
				}
			}
			
			//Insert Links
			ArrayList<Document> urls = (ArrayList<Document>) entities.get("urls");
			if(urls != null){
				for(int i = 0; i < urls.size(); i++){
					Document url = urls.get(i);
					String shortUrl = url.getString("url");
					insertLink.setInsertLink(oid, shortUrl, id);
					insertLink.insertWithReport();
				}
			}
		}
		
		//Insert retweet
		Document retweeted_status = (Document) doc.get("retweeted_status");
		if(retweeted_status != null){
			String screen_name_from = "";
			String screen_name_to = "";
			Document retweeted_user = (Document) retweeted_status.get("user");
			if(retweeted_user != null){
				screen_name_from = retweeted_user.getString("screen_name");
			}
			if(user != null){
				screen_name_to = user.getString("screen_name");
			}
			
			insertRetweet.setRetweet(oid, id, screen_name_from, screen_name_to);
			insertRetweet.insertWithReport();
		}
	}
}
