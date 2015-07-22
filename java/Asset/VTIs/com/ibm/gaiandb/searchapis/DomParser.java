/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.searchapis;

/**
 * @author gabent
 *
*/
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLInteger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ibm.gaiandb.Logger;

public class DomParser {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "DomParser", 30 );

	//No generics
	List<Entries> myEntries;
	Document dom;


	public DomParser(){
		//create a list to hold the Entries
		 myEntries = new ArrayList<Entries>();
	}

//	public void runExample() {
//		File fileName = new File("c:\\TestXML.xml");
//		FileInputStream is;
//		try {
//			is = new FileInputStream(fileName);
//			//parse the xml file and get the dom object
//			parseXmlFile(is);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//
//		
//		//get each employee element and create a Employee object
//		parseDocument();
//		
//		//Iterate through the list and print the data
//		printData();
//		
//	}
//	
//	
	public void parseXmlFile(InputStream is){
		//get the factory
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		
		try {
			
			//Using factory get an instance of document builder
			DocumentBuilder db = dbf.newDocumentBuilder();
			
			//parse using builder to get DOM representation of the XML file
//			dom = db.parse("c:\\TestXML.xml");
			dom = db.parse(is);
			

		}catch(ParserConfigurationException pce) {
			pce.printStackTrace();
		}catch(SAXException se) {
			se.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	
	public void parseDocument(){
		//get the root elememt
		Element docEle = dom.getDocumentElement();
		
		//get a nodelist of <entry> elements
		NodeList nl = docEle.getElementsByTagName("entry");
		if(nl != null && nl.getLength() > 0) {
			for(int i = 0 ; i < nl.getLength();i++) {
				
				//get the entry element
				Element el = (Element)nl.item(i);
				
				//get the entry object
				Entries e = getEntry(el);
				
				//add it to list
				myEntries.add(e);
			}
		}
	}

//	public void parseAndStoreDocument(Connection conn){
//		//get the root elememt
//		Element docEle = dom.getDocumentElement();
//		
//		//get a nodelist of <entry> elements
//		NodeList nl = docEle.getElementsByTagName("entry");
//		try {
//			PreparedStatement pstmt = conn.prepareStatement("Insert into documents(dnum) values(?)");
//			PreparedStatement clearTable = conn.prepareStatement("Delete from documents");
//		System.out.println("clear documents table");
//			clearTable.executeUpdate();
//			System.out.println("documents table cleared");		
//			if(nl != null && nl.getLength() > 0) {
//				for(int i = 0 ; i < nl.getLength();i++) {
//				
//	//				get the entry element
//					Element el = (Element)nl.item(i);
//					String documentPath = getTextValue(el,"id");
//					//documentPath = (String) documentPath.subSequence(0,documentPath.length()-1);
//					System.out.println("Document ID: = " + documentPath);
//					
//					int id = documentPath.hashCode();
//					System.out.println("Document HashID: = " +id);
//					String Test = "file://localhost/C:/temp/crawlme/Germany_sends_jets_to_Afghanistan.txt";
//					System.out.println("Document Test HashID: = " + Test.hashCode());
//					String updated = getTextValue(el,"updated");
//		System.out.println("inserting doc_id: "+id);
//					pstmt.setInt(1,id);
//					pstmt.executeUpdate();
//				}
//			}	
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//	}
	
	public void parseAndStoreDocument(Vector<DataValueDescriptor[]> rows) throws UnsupportedEncodingException{
		//get the root elememt
		Element docEle = dom.getDocumentElement();
		
		//get a nodelist of <entry> elements
		NodeList nl = docEle.getElementsByTagName("entry");
		
		logger.logInfo("Building document result rows vector, with Doc ID hash values based on encoded URIs");
		int idx = 0;
		if(nl != null && nl.getLength() > 0) {
			for (; idx < nl.getLength(); idx++) {
			
//				get the entry element
				Element el = (Element)nl.item(idx);
				String uri = getTextValue(el,"id");
				
				//documentPath = (String) documentPath.subSequence(0,documentPath.length()-1);
								
           		// DRV 09/12/10 - Removing:
           		// 1) decoding of URIs and hence, 2) option to hash on the encoded or decoded URIs
//				String documentPath = URLDecoder.decode( uri, Charset.defaultCharset().name() );
//				int id = hashDecodedPaths ? documentPath.hashCode() : uri.hashCode();
				int id = uri.hashCode();
				
//				String Test = "file://localhost/C:/temp/crawlme/Germany_sends_jets_to_Afghanistan.txt";
//				logger.logInfo("Document Test HashID: = " + Test.hashCode());
//				String updated = getTextValue(el,"updated");
				
   				logger.logDetail("Adding row with docHashID: " + id + ", docURI: " + uri);
				rows.add( new DataValueDescriptor[] { new SQLInteger(id), new SQLChar(uri) } );
			}
		}
		logger.logInfo("Number of rows added: " + idx);
	}

	/**
	 * Take an entry element and read the values in, create
	 * a entry object and return it
	 * @param entryEl
	 * @return
	 */
	private Entries getEntry(Element entryEl) {
		
		//for each <entry> element get text  values of 
		//id and updated
		String id = getTextValue(entryEl,"id");
		String updated = getTextValue(entryEl,"updated");

		
		//Create a new Entry with the value read from the xml nodes
		Entries e = new Entries(id,updated);
		
		return e;
	}


	/**
	 * I take a xml element and the tag name, look for the tag and get
	 * the text content 
 
	 * @param ele
	 * @param tagName
	 * @return
	 */
	private String getTextValue(Element ele, String tagName) {
		String textVal = null;
		NodeList nl = ele.getElementsByTagName(tagName);
		if(nl != null && nl.getLength() > 0) {
			Element el = (Element)nl.item(0);
			textVal = el.getFirstChild().getNodeValue();
		}

		return textVal;
	}
//
//	
//	/**
//	 * Calls getTextValue and returns a int value
//	 * @param ele
//	 * @param tagName
//	 * @return
//	 */
//	private int getIntValue(Element ele, String tagName) {
//		//in production application you would catch the exception
//		return Integer.parseInt(getTextValue(ele,tagName));
//	}
//	
	/**
	 * Iterate through the list and print the 
	 * content to console
	 */
	public void printData(){
		
		logger.logInfo("No documents '" + myEntries.size() + "'.");
		
		Iterator<Entries> it = myEntries.iterator();
		while(it.hasNext()) {
			logger.logInfo(it.next().toString());
		}
	}
//
//	
//	public static void main(String[] args){
//		//create an instance
//		DomParser dpe = new DomParser();
//		
//		//call run example
//		dpe.runExample();
//	}

}

