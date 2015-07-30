package edu.vader.simpleRestletServer.resources;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import edu.vader.exporter.Main;

public class Status extends ServerResource{
	@Get ("html")
    public String represent(){
		String response = "";
		//DecimalFormat df = new DecimalFormat("#.00");
		response += "<p> Current Object ID is " + Main.currentObejctId + " </p>";
	    return response;
    }
}
