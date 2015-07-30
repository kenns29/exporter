package edu.vader.simpleRestletServer.resources;

import java.text.DecimalFormat;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import edu.vader.exporter.InsertTweet;
import edu.vader.exporter.Main;
import edu.vader.util.MongoUtils;
import edu.vader.util.TimeUtils;

public class Progress extends ServerResource{
	@Get ("html")
    public String represent(){
		int remainingDocumentCount = 0;
		if(Main.configProperties.stopAtEnd){
			remainingDocumentCount = MongoUtils.getTotalDocumentCountWithStopAtEnd(Main.mongoColl);
		}
		else{
			remainingDocumentCount = MongoUtils.getTotalDocumentCount(Main.mongoColl, Main.currentObejctId);
		}
		
		long currentTime = System.currentTimeMillis();
		long elapsedTime = (Main.mainStartTime > 0) ? currentTime - Main.mainStartTime : 0;
		double elapsedTimeInMinute = (double)elapsedTime / TimeUtils.minutesMillis;
		
		int documentProcessed = Main.documentCount;
		double percentComplete = (documentProcessed + remainingDocumentCount > 0) ? (double)documentProcessed / (documentProcessed + remainingDocumentCount) : 0;
		int averageDocsPerMinute = (int) (documentProcessed / elapsedTimeInMinute);
		int lastMinuteCount = Main.lastMinuteDocCount;
		double estimatedTimeToFinish = (averageDocsPerMinute > 0) ? (double)remainingDocumentCount / averageDocsPerMinute : 0;
		int numTweetsInserted = InsertTweet.tweetCount;
		
		String response = "";
		DecimalFormat df = new DecimalFormat("#.00");
		String overallTable = "<table border=\"1\" style=\"border:1px solid black;width:100%\">";
		
		overallTable += "<tr>"
					  + "<td> Doument Processed </td>"
				      + "<td>" + documentProcessed + "</td>"
				      + "</tr>";
		
		overallTable += "<tr>"
				  + "<td> Number of Tweets Inserted </td>"
			      + "<td>" + numTweetsInserted + "</td>"
			      + "</tr>";
		
		overallTable += "<tr>"
				  + "<td> Percent of Tweets Inserted </td>"
			      + "<td>" + df.format((double)numTweetsInserted / documentProcessed * 100)  + "%</td>"
			      + "</tr>";
		overallTable += "<tr>"
				  + "<td> Percent Completed </td>"
			      + "<td>" + df.format((double)percentComplete * 100)  + "%</td>"
			      + "</tr>";
		overallTable += "<tr>"
				  + "<td> Number of Documents Processed in the last minute </td>"
			      + "<td>" + lastMinuteCount + "</td>"
			      + "</tr>";
		
		overallTable += "<tr>"
				  + "<td> Average Number of Document Processed per minute</td>"
			      + "<td>" + averageDocsPerMinute + "</td>"
			      + "</tr>";
		
		overallTable += "<tr>"
				  + "<td> Estimated Time to Finish (minutes) </td>"
			      + "<td>" + df.format(estimatedTimeToFinish) + "</td>"
			      + "</tr>";
		
		overallTable += "<tr>"
				  + "<td>Elapsed Time (minutes) </td>"
			      + "<td>" + df.format(elapsedTimeInMinute) + "</td>"
			      + "</tr>";
		overallTable += "</table>";
		response += overallTable;
	    return response;
    }
}

