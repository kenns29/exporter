package edu.vader.util;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import edu.vader.exporter.Main;

public class MongoUtils {
	public static ObjectId minObjectId(MongoCollection<Document> coll){
		FindIterable<Document> iterable = Main.mongoColl.find().sort(new Document("_id", 1)).limit(1);
		MongoCursor<Document> cursor = iterable.iterator();
		try{
			if(cursor.hasNext()){
				Document doc = cursor.next();
				return doc.getObjectId("_id");
			}
		}
		finally{
			cursor.close();
		}
		return null;	
	}
	
	public static ObjectId maxObjectId(MongoCollection<Document> coll){
		FindIterable<Document> iterable = Main.mongoColl.find().sort(new Document("_id", -1)).limit(1);
		MongoCursor<Document> cursor = iterable.iterator();
		try{
			if(cursor.hasNext()){
				Document doc = cursor.next();
				return doc.getObjectId("_id");
			}
		}
		finally{
			cursor.close();
		}
		return null;
	}
	
	public static int getDocumentCount(MongoCollection<Document> coll, ObjectId startObjectId, ObjectId endObjectId){
		if(startObjectId != null && endObjectId != null){
			Document query = new Document("_id", 
					new Document("$gt", startObjectId)
						.append("$lte", endObjectId));
			return (int) coll.count(query);
		}
		else{
			return (int) coll.count();
		}
		
	}
	public static int getTotalDocumentCount(MongoCollection<Document> coll, ObjectId startObjectId){
		if(startObjectId != null){
			Document query = new Document("_id", 
					new Document("$gt", startObjectId));
			
			return (int) coll.count(query);
		}
		else{
			return (int) coll.count();
		}
	}
	public static int getTotalDocumentCountWithStopAtEnd(MongoCollection<Document> coll){
		return getDocumentCount(coll, Main.currentObejctId, Main.configProperties.endObjectId);
	}
}
