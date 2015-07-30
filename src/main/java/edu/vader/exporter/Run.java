package edu.vader.exporter;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import edu.vader.communicate.Communicate;
import edu.vader.util.TimeUtils;

public class Run {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	private Convert convert = new Convert();
	private Communicate communicate = new Communicate();
	public Run(){
		
	}
	public void convertWithSafestObjectId(){
		ObjectId startObjectId = Main.configProperties.startObjectId;
		startObjectId = TimeUtils.decrementObjectId(startObjectId);
		ObjectId safestObjectId = null;
		while(true){
			try {
				safestObjectId = communicate.getSafestIdFromNer();
			} catch (IOException e) {
				HIGH_PRIORITY_LOGGER.error("can not get the safest id from ner", e);
				try {
					Thread.sleep(300000);
				} catch (InterruptedException e1) {
					HIGH_PRIORITY_LOGGER.error("Interrupted while sleep.", e1);
				}
				continue;
			}
			LOGGER.info("Safest Object Id is " + safestObjectId);
			if(safestObjectId == null){
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					HIGH_PRIORITY_LOGGER.error("Interrupted while sleep.", e);
				}
				continue;
			}
			if(startObjectId.compareTo(safestObjectId) != 0){
				LOGGER.info("Running the convertion for " + startObjectId + " to " + safestObjectId);
				try {
					convert.convertMongoToSql(startObjectId, safestObjectId);
				} catch (Exception e) {
					HIGH_PRIORITY_LOGGER.fatal("the range " + startObjectId + " to " + safestObjectId + " was not fully processed. Due to error ", e);
				}
				LOGGER.info("Finished the convertion for " + startObjectId + " to " + safestObjectId);
			}
			else{
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					HIGH_PRIORITY_LOGGER.error("Interrupted while sleep.", e);
				}
			}
			startObjectId = safestObjectId;
		}
	}
	public void convertWithStartEndObjectId(){
		ObjectId startObjectId = Main.configProperties.startObjectId;
		ObjectId endObjectId = Main.configProperties.endObjectId;
		try {
			convert.convertMongoToSql(startObjectId, endObjectId);
		} catch (Exception e) {
			HIGH_PRIORITY_LOGGER.fatal("the range " + startObjectId + " to " + endObjectId + " was not fully processed. Due to error ", e);
		}
	}
}
