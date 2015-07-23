package edu.vader.exporter;

import java.sql.SQLException;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

public class Convert {
	@SuppressWarnings("unchecked")
	public void convertMongoToSql() throws SQLException{
		Document query = new Document();
		
		FindIterable<Document> iterable = Main.mongoColl.find().limit(10);
		MongoCursor<Document> mongoCursor = iterable.iterator();
		
		try{
			while(mongoCursor.hasNext()){
				Document doc = mongoCursor.next();
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
				InsertTweet insertTweet = new InsertTweet(id, timestamp, text, " ", " ", 0, " ", 0, uid);
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
							InsertUserMention insertUserMention = new InsertUserMention(mUid, mScreen_name, mName, id, uid);
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
		finally{
			mongoCursor.close();
		}
	}
}
