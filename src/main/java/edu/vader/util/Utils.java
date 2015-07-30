package edu.vader.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;

public class Utils {
	public static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	} 
	
	public static String readFile(String path) throws IOException{
		return readFile(path, StandardCharsets.UTF_8);
	}
	
	public static String readFromInputStreamByLine(InputStream is) throws IOException{
		BufferedReader rd = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();
		String line = null;
		while((line = rd.readLine()) != null){
			sb.append(line);
		}
		rd.close();
		return sb.toString();
	}
	
	public static String readFromInputStream(InputStream is) throws IOException{
		StringWriter sw = new StringWriter();
		IOUtils.copy(is, sw, "UTF-8");
		return sw.toString();
	}
}
