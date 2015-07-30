package edu.vader.exporter;

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
		boolean firstStart = true;
		ObjectId startObjectId = Main.configProperties.startObjectId;
		startObjectId = TimeUtils.decrementObjectId(startObjectId);
		Main.currentSafestObjectId = null;
		while(true){
			try {
				Main.currentSafestObjectId = communicate.getSafestIdFromNer();
			} catch (Exception e) {
				LOGGER.error("can not get the safest id from ner, waiting to retrive it again", e);
				try {
					Thread.sleep(300000);
				} catch (InterruptedException e1) {
					HIGH_PRIORITY_LOGGER.error("Interrupted while sleep.", e1);
				}
				continue;
			}
			LOGGER.info("Safest Object Id is " + Main.currentSafestObjectId);
			if(Main.currentSafestObjectId == null){
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					HIGH_PRIORITY_LOGGER.error("Interrupted while sleep.", e);
				}
				continue;
			}
			if(startObjectId.compareTo(Main.currentSafestObjectId) != 0){
				if(firstStart){
					Main.mainStartTime = System.currentTimeMillis();
					Main.preStartTime = System.currentTimeMillis();
				}
				firstStart = false;
				LOGGER.info("Running the convertion for " + startObjectId + " to " + Main.currentSafestObjectId);
				try {
					convert.convertMongoToSql(startObjectId, Main.currentSafestObjectId);
				} catch (Exception e) {
					HIGH_PRIORITY_LOGGER.fatal("the range " + startObjectId + " to " + Main.currentSafestObjectId + " was not fully processed. Due to error ", e);
				}
				LOGGER.info("Finished the convertion for " + startObjectId + " to " + Main.currentSafestObjectId);
			}
			else{
				try {
					LOGGER.info("reached the end of the current safest id, waiting");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					HIGH_PRIORITY_LOGGER.error("Interrupted while sleep.", e);
				}
			}
			startObjectId = Main.currentSafestObjectId;
		}
	}
	public void convertWithStartEndObjectId(){
		ObjectId startObjectId = Main.configProperties.startObjectId;
		ObjectId endObjectId = Main.configProperties.endObjectId;
		Main.mainStartTime = System.currentTimeMillis();
		Main.preStartTime = System.currentTimeMillis();
		try {
			convert.convertMongoToSql(startObjectId, endObjectId);
		} catch (Exception e) {
			HIGH_PRIORITY_LOGGER.fatal("the range " + startObjectId + " to " + endObjectId + " was not fully processed. Due to error ", e);
		}
	}
}
