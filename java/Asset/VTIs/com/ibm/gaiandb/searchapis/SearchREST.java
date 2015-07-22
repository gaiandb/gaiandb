/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.searchapis;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Vector;

import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.Logger;

public class SearchREST {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "SearchREST", 25 );
	
//	public static void main(String[] args) {
//		
//		try {
//			if (args.length < 4) {
//				printUsage();
//				System.exit(-1);
//			}
//			
//			// program arguments
//			String hostname = args[0];
//			String port = args[1];
//			String index = args[2];
//			String query = args[3];
//			
//			String localXsl = null;
//			if (args.length == 5) {				
//				localXsl = args[4];
//			}
//			
//			// print arguments
//			System.out.println("Hostname:           " + hostname);
//			System.out.println("Port:               " + port);
//			System.out.println("Index:              " + index);
//			System.out.println("Query:              " + query);
//			
//			if (localXsl != null) {
//				System.out.println("Local XSL File:     " + localXsl);
//			}
//			
//			System.out.println("");
//			
//			// construct url
//			String urlCtx = "/api/search";
//			urlCtx += "?query=" + query;
//			urlCtx += "&index=" + index;
//			
//			URL url = new URL("http", hostname, Integer.valueOf(port).intValue(), urlCtx);
//	        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//	        connection.setDoInput(true);
//	        connection.setDoOutput(true);
//	        connection.setRequestMethod("GET");
//	        
//	        // print response code
//	        System.out.println("Response Code:      " + connection.getResponseCode());
//	        
//	        // is there an error?
//	        Boolean error = Boolean.valueOf(connection.getHeaderField("hasError"));
//	        System.out.println("API Error Occurred: " + error);
//	        
//	        System.out.println("");
//	        
//	        // input stream
//	        InputStream is = connection.getInputStream();
//	        
//	        // print error
//	  
//	        if (localXsl == "" || localXsl == null) {
///*	        	BufferedReader d = new BufferedReader(new InputStreamReader(is));
//	            String str = null;
//	            while (null != ((str = d.readLine()))) {	            	
//	                System.out.println(str);
//	            }
//*/	            
////				Create a dom parser and get the docment parameters returned from query.	        
//	            DomParser dom = new DomParser();
//	            dom.parseXmlFile(is);
//	            dom.parseDocument();
//	            dom.printData();
////	            d.close ();
//	        } else { // no error
//	        	
//	        	byte[] data = streamToByteArray(is);
//	        		        	
//	        	// print Atom feed	        	
//	            TransformerFactory tFactory = TransformerFactory.newInstance();
//	            Transformer transformer = tFactory.newTransformer();
//	            System.out.println("ATOM Feed:");
//	            transformer.transform(new StreamSource(new ByteArrayInputStream(data)), new StreamResult(System.out));
//	        	
//	            System.out.println("");
//	            System.out.println("");
//	        	
//	        	// print XSL results
//	        	StreamSource xsl = new StreamSource(new File(localXsl).toURL().toString());
//	            Templates xslTemplate = tFactory.newTemplates(xsl);
//	            transformer = xslTemplate.newTransformer();
//	            System.out.println("Processed Results:");
//	            transformer.transform(new StreamSource(new ByteArrayInputStream(data)), new StreamResult(System.out));	               
//	        }
//	        
//		} catch (Exception e) {
//			System.out.println("Exception executing search.  Exception: " + e);
//			e.printStackTrace();
//			System.exit(-1);
//		}
//		
//		System.exit(0);
//	}
	public static void printTest(){
		logger.logInfo("In Search");
	}
	
	// The Vector rows argument contains DataValueDescriptor[] elements, each of which is a derby database row.
	public static void retrieveDocumentReferences(Vector<DataValueDescriptor[]> rows,
			String hostname, String port, String index, String query) {
		
		try {
			String localXsl = null;
			
			if ( null == index ) index = "Default";
			
			// print arguments
			logger.logInfo("Hostname:           " + hostname);
			logger.logInfo("Port:               " + port);
			logger.logInfo("Index:              " + index);
			logger.logInfo("Query:              " + query);
			
			if (localXsl != null) {
				logger.logInfo("Local XSL File:     " + localXsl);
			}
			
			logger.logInfo("");
			
			// construct url
			String urlCtx = "/api/search";
			urlCtx += "?query=" + query;
			urlCtx += "&index=" + index;
			
			URL url = new URL("http", hostname, Integer.valueOf(port).intValue(), urlCtx);
	        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	        connection.setDoInput(true);
	        connection.setDoOutput(true);
	        connection.setRequestMethod("GET");
	        
	        // print response code
	        logger.logInfo("Response Code:      " + connection.getResponseCode());
	        
	        // is there an error?
	        Boolean error = Boolean.valueOf(connection.getHeaderField("hasError"));
	        logger.logInfo("API Error Occurred: " + error);
	        
	        logger.logInfo("");
	        
	        // input stream
	        InputStream is = connection.getInputStream();
	        
	        // print error
	  
	        if (localXsl == "" || localXsl == null) {
/*	        	BufferedReader d = new BufferedReader(new InputStreamReader(is));
	            String str = null;
	            while (null != ((str = d.readLine()))) {	            	
	                logger.logInfo(str);
	            }
*/	            
//				Create a dom parser and get the docment parameters returned from query.	        
	            DomParser dom = new DomParser();
	            dom.parseXmlFile(is);
	            dom.parseAndStoreDocument( rows );
	            dom.parseDocument();
	            dom.printData();
//	            d.close ();
	        } else { // no error
	        	
	        	byte[] data = streamToByteArray(is);
	        		        	
	        	// print Atom feed	        	
	            TransformerFactory tFactory = TransformerFactory.newInstance();
	            Transformer transformer = tFactory.newTransformer();
	            logger.logInfo("ATOM Feed:");
	            transformer.transform(new StreamSource(new ByteArrayInputStream(data)), new StreamResult(System.out));
	        	
	            logger.logInfo("");
	            logger.logInfo("");
	        	
	        	// print XSL results
                StreamSource xsl = new StreamSource(new File(localXsl).toURI().toURL().toString());
	            Templates xslTemplate = tFactory.newTemplates(xsl);
	            transformer = xslTemplate.newTransformer();
	            logger.logInfo("Processed Results:");
	            transformer.transform(new StreamSource(new ByteArrayInputStream(data)), new StreamResult(System.out));	               
	        }
	        
		} catch (Exception e) {
			logger.logInfo("Exception executing search.  Exception: " + e);
			e.printStackTrace();
//			System.exit(-1);
		}
		
//		System.exit(0);
	}
	
	protected static void printUsage() {
		logger.logInfo("Search <hostname> <port> <index> <query> [Local XSL File]");
	}
	
	protected static byte[] streamToByteArray(InputStream stream) throws IOException {
	    
	    // Handle null
	    if(stream == null) {
	       return null;
	    }
	    
	    BufferedInputStream bufStream = new BufferedInputStream(stream);
	    
	    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
	    
	    // Read in 4K chunks - this could be increased to improve performance, but
	    // 4K seems to be a generally accepted size to use.
	    byte[] buffer = new byte[4096];
	    int len = 0;
	    while((len = bufStream.read(buffer, 0, 4096)) > 0) {
	       outStream.write(buffer, 0, len);
	    }
	    
	    bufStream.close();
	    outStream.close();
	    
	    return outStream.toByteArray();  
	}
}
