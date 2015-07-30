package edu.vader.exporter;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.vividsolutions.jts.geom.Coordinate;

public class Convert {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public static final double INVALID_COORDINATE_DOUBLE = -1000;
	public void convertMongoToSql() throws SQLException{
		Document query = new Document();
		this.convertMongoToSql(query);
	}
	
	public void convertMongoToSql(Document query) throws SQLException{	
		FindIterable<Document> iterable = Main.mongoColl.find(query).sort(new Document("_id", -1)).limit(10000);
		MongoCursor<Document> mongoCursor = iterable.iterator();
		
		try{
			while(mongoCursor.hasNext()){
				Document doc = mongoCursor.next();
				convertOneDoc(doc);
			}
		}
		finally{
			mongoCursor.close();
		}
	}
	
	public void convertMongoToSql(ObjectId startObjectId, ObjectId endObjectId) throws SQLException{
		Document query = new Document("_id", 
				new Document("$gt", startObjectId)
				.append("$lte", endObjectId));
		this.convertMongoToSql(query);
	}
	
	@SuppressWarnings("unchecked")
	private void convertOneDoc(Document doc) throws SQLException{
		CoordinateConverter coordinateConverter = new CoordinateConverter(doc);
		if(coordinateConverter.isWithinBoundingBox()){
			Coordinate coordinate = coordinateConverter.getCoordinate();
			long id = doc.getLong("id");
			String text = doc.getString("text");
			Document user = (Document) doc.get("user");
			long uid = 0;
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
			if(coordinate != null){
				lng = coordinate.x;
				lat = coordinate.y;
			}
			InsertTweet insertTweet = new InsertTweet(id, timestamp, text, " ", language, retweet_count, uid, lng, lat, coordinateConverter.getOriginal_geo_field());
			insertTweet.insert();
			
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
						InsertUserMention insertUserMention = new InsertUserMention(mUid, mScreen_name, mName, id);
						insertUserMention.insert();
					}
				}
				//Insert Hashtags
				ArrayList<Document> hashtags = (ArrayList<Document>) entities.get("hashtags");
				if(hashtags != null){
					for(int i = 0; i < hashtags.size(); i++){
						Document tag = hashtags.get(i);
						String tagText = tag.getString("text");
						InsertHashtag insertHashtag = new InsertHashtag(id, tagText);
						insertHashtag.insert();
					}
				}
				
				//Insert Links
				ArrayList<Document> urls = (ArrayList<Document>) entities.get("urls");
				if(urls != null){
					for(int i = 0; i < urls.size(); i++){
						Document url = urls.get(i);
						String shortUrl = url.getString("url");
						InsertLink insertLink = new InsertLink(shortUrl, id);
						insertLink.insert();
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
				
				InsertRetweet insertRetweet = new InsertRetweet(id, screen_name_from, screen_name_to);
				insertRetweet.insert();
			}
		}
	}
}
