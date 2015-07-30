package edu.vader.communicate;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import edu.vader.exporter.Main;
import edu.vader.util.Utils;

public class Connection {
	private static final Logger LOGGER = Logger.getLogger("reportsLog");
	private static Logger HIGH_PRIORITY_LOGGER = Logger.getLogger("highPriorityLog");
	public URL url = null;
	public HttpURLConnection httpURLConnection = null;

	public Connection() {
		try {
			url = new URL(Main.configProperties.nerProgramBaseUrl + Main.configProperties.safestObjectIdUrl);
			try {
				httpURLConnection = (HttpURLConnection) url.openConnection();
				httpURLConnection.setRequestMethod("GET");
				httpURLConnection.setRequestProperty("Content-Type", "application/json");
				httpURLConnection.setRequestProperty("Content-language", "en-US");
				
			} catch (IOException e) {
				HIGH_PRIORITY_LOGGER.error("Can not connect to ner program connection", e);
			}
		} catch (MalformedURLException e) {
			HIGH_PRIORITY_LOGGER.error("Ner program url is not properly formed.", e);
		}
	}
	
	public String getResponse() throws IOException{
		String response = "{}";
		if(this.httpURLConnection != null){
			InputStream is = this.httpURLConnection.getInputStream();
			response = Utils.readFromInputStreamByLine(is);
		}
		return response;
	}
	
	public void disconnect(){
		if(this.httpURLConnection != null){
			this.httpURLConnection.disconnect();
		}
	}
}
