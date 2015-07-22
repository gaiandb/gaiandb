/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.searchapis;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Vector;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SLikeResultsParser extends DefaultHandler {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";

	private byte[] results;
    private String value;
    
    private ArrayList<SLikeHeadObject> headObjects = new ArrayList<SLikeHeadObject>();
	private SLikeHeadObject headObject;
    
	public SLikeResultsParser(byte[] results) {
		this.results = results;
	}

/*
 * The results will be of the format of
 * 
 * <?xml version="1.0" encoding="utf-8"?>
 * <Entity>
 * 	  <LWType>
 *		 <head>james luke</head>
 *       <headType>uima.tt.Person</headType>
 *    </LWType>
 *    <LWType>
 *       <head>alan</head>
 *       <headType>uima.tt.Place</headType>
 *    </LWType>
 * </Entity>
 * 
 */
	public void parseResults() {
		
		// Convert the results into a string
		try {
			String strResults = new String(this.results, "UTF8");

			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setValidating(false);
			SAXParser parser = spf.newSAXParser();

			StringReader reader = new StringReader(strResults);
			InputSource source = new InputSource(reader);
			
	        parser.parse(source, this);
		
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			if(qName.equals("LWType")){
				// Create a new instance of HeadObject
				headObject = new SLikeHeadObject();
			}

	}

	public void characters(char[] ch, int start, int length) throws SAXException {
		value = new String(ch,start,length);
	}
	
	
	public void endElement(String uri, String localName, String qName) throws SAXException {

		if(qName.equals("LWType")) {
			//add it to the list
			headObjects.add(headObject);
		}
		else if (qName.equalsIgnoreCase("head")) {
			headObject.setHead(value);
		}
		else if (qName.equalsIgnoreCase("headType")) {
			headObject.setHeadType(value);
		}
	}
	
	public Vector<DataValueDescriptor[]> getParsedResults(){
		Vector<DataValueDescriptor[]> rows = new Vector<DataValueDescriptor[]>();
		for(SLikeHeadObject ho: headObjects){
//			rows.add( new DataValueDescriptor[] { new SQLChar(ho.getHead()), new SQLChar(ho.getHeadType()) } );
			rows.add( new DataValueDescriptor[] { new SQLChar(ho.getHeadType()) } );
		}
		return rows;
	}
	
	public void close(){
		if(headObjects != null){
			headObjects.clear();
			headObjects.trimToSize();
			headObjects = null;
		}
	}
	
}
