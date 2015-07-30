package edu.vader.communicate;

import java.io.IOException;

import org.bson.types.ObjectId;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Communicate {

	public Communicate(){}
	
	public ObjectId getSafestIdFromNer() throws IOException{
		Connection connection = new Connection();
		String response = connection.getResponse();
		connection.disconnect();
		
		JsonObject jsonObject = new JsonParser().parse(response).getAsJsonObject();
		JsonElement objId = jsonObject.get("safestObjectId");
		if(!objId.isJsonNull()){
			String objIdStr = objId.getAsString();
			return new ObjectId(objIdStr);
		}
		return null;
	}
}
