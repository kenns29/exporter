package edu.vader.communicate;

import org.bson.types.ObjectId;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.vader.exporter.Main;

public class Communicate {

	public Communicate(){}
	
	public ObjectId getSafestIdFromNer() throws Exception{
		String connStr = Main.configProperties.nerProgramBaseUrl + "/" 
				+ Main.configProperties.safestObjectIdUrl;
		System.out.println("connStr = " + connStr);
		Connection connection = new Connection(connStr);
		String response = connection.getResponse();
		connection.disconnect();
		if(response != null){
			JsonObject jsonObject = new JsonParser().parse(response).getAsJsonObject();
			System.out.println("jsonObject = " + jsonObject.toString());
			JsonElement objId = jsonObject.get("safestObjectId");
			if(!objId.isJsonNull()){
				String objIdStr = objId.getAsString();
				return new ObjectId(objIdStr);
			}
		}
		return null;
	}
}
