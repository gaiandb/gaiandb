/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.db2j.GenericWS;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.webservices.scanner.GaianHandler;
import com.ibm.juno.core.ObjectList;
import com.ibm.juno.core.ObjectMap;
import com.ibm.juno.core.json.JsonParser;
import com.ibm.juno.core.parser.ParseException;

/**
 * 
 * The purpose of this class is to provide a JSON scanner, which can parse JSON 
 * files thanks to a GaianHandler handler.
 * 
 * @author remi - IBM Hursley
 *
 */
public class JsonScanner implements Runnable {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";

	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	
	/** 
	 * The name of the json objects which depth is on the root of the 
	 * input stream to scan. 
	 */
	private static final String ROOT_NAME = "root";
	
	
	// -------------------------------------------------------------------------- Dynamic	

	private GenericWS caller;
	private InputStream inputStream;
	private DefaultHandler handler;
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/** Creates a JsonScanner object. */
	public JsonScanner(GenericWS caller, InputStream inputStream) {//GenericWS caller, 
		this.caller = caller;
		this.inputStream = inputStream;
		this.handler = new GaianHandler(this.caller);
	}
	
	/** Creates a JsonScanner object. */
	public JsonScanner(GenericWS caller, InputStream inputStream, DefaultHandler handler) {
		this.caller = caller;
		this.inputStream = inputStream;
		this.handler = handler;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	@Override
	public void run() {
		try {
			this.start(this.handler);
		} catch (IOException ioe) {
			if (this.caller != null)
				this.caller.logException(
						GDBMessages.DSWRAPPER_GENERICWS_LOST_CONNECTION, 
						"An IO exception occurred while reading the data sent by the web service.\n" +
						ioe.getMessage(), 
						ioe);
		} catch (Exception e) {
			if (this.caller != null)
				this.caller.logException(
						GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
						e.getMessage(), 
						e);
		} catch (NoClassDefFoundError e) {
			if (this.caller != null)
				this.caller.logException(
						// Probably means that the JUNO jar can't be found
						GDBMessages.DSWRAPPER_GENERICWS_PARSER_ERROR, 
						e.getMessage(), 
						e);		}
		finally {
			if (this.caller != null) {
				this.caller.confirmSendingOfLastRecord();
			}
		}
	}
	

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Starts the scan of the given stream.
	 * 
	 * @param inputStream
	 * 			The stream to read, containing a json file's content.
	 * @param handler
	 * 			The handler managing the scan of the Json file.
	 * 
	 * @throws IOException if problems occur while reading the stream.
	 * @throws NullPointerException if the input stream is null.
	 * @throws ParseException if the content of the stream is not is the json format.
	 */
	private void start(DefaultHandler handler) throws IOException, ParseException, NullPointerException {
				
		if (this.inputStream == null) {
			throw new NullPointerException("Trying to parse a null data.");
		}
		
		// Generates JsonObject
		Reader jsonReader = new InputStreamReader(this.inputStream);
		Object o = JsonParser.DEFAULT.parse(jsonReader, Object.class);
		
		// Goes through the object
		this.handleObject(o, null, handler);
		
	}
	
	/**
	 * Depending on the type of the object given, lets the handler know
	 * about the behaviour to have. 
	 *  
	 * @param jsonObject
	 * 			the object parsed by the scanner. It can be a ObjectMap,
	 * a ObectList or a String, Long, Integer, Double, Float or Boolean.
	 * For any other type, the method doesn't call the handler.
	 * @param objectName
	 * 			The name of the current object. If this name is null, it
	 * set up with the value "root".
	 * @param handler
	 * 			The GaianHandler to be called.
	 */
	private void handleObject(Object jsonObject, String objectName, DefaultHandler handler) {
		
		// Algorithm:
		// The Object can basically be 3 kind of objects:
		// - ObjectMap
		// - String, Long, Integer, Double, Float or Boolean
		// - ObjectList
		// 
		// 	If the object is an Object Map, 
		//		the method will scan all the children. 
		// 			If the child scanned is a String, Long, Integer, Double, Float or Boolean
		//				Their name, value will define an attribute of the current object
		//		the method will scan all the other children... 
		//			...and apply the method recursively
		// 	If the object is an Object List, 
		//		the method will scan all the other children... 
		//			...and apply the method recursively
		
		// ------------------------------------------------------------
		//  Beginning of document
		if (objectName == null || objectName.isEmpty()) {
			try {
				handler.startDocument();
			} catch (SAXException e) {
				try {
					handler.error(new SAXParseException(e.getMessage(), null));
				} catch (SAXException e1) { }
			}
			objectName = ROOT_NAME;
		}
		
		// ------------------------------------------------------------
		// 	If the object is an Object Map, 
		if (jsonObject instanceof ObjectMap) {
			JsonAttributes attributes = new JsonAttributes();
			
			// --- Gets the details of the current object ---
			// scans all the children and generates an attribute if the child is 
			// neither an ObjectList, nor an ObjectMap (but still a known type)
			Set<String> kids = ((ObjectMap)jsonObject).keySet();
			Set<String> filteredKids = new HashSet<String>(kids);
			for (String kid : kids) {
				Object kidsValue = ((ObjectMap)jsonObject).get(kid);
				if ((kidsValue instanceof String) 
						|| (kidsValue instanceof Long) || (kidsValue instanceof Integer) 
						|| (kidsValue instanceof Double) || (kidsValue instanceof Float)
						|| (kidsValue instanceof Boolean)) {
					attributes.addAttribute(kid, kidsValue);
					filteredKids.remove(kid);
				}
			}
			try {
				handler.startElement(null, null, objectName, attributes);
			} catch (SAXException e) {
				try {
					handler.error(new SAXParseException(e.getMessage(), null));
				} catch (SAXException e1) { }
			}
			
			// --- Go through the kids of the current object ---
			// all the kids which are still in the Set are ObjectList or ObjectMap
			for (String kid : filteredKids) {
				// recursive call
				Object kidsValue = ((ObjectMap)jsonObject).get(kid);
				this.handleObject(kidsValue, kid, handler);
			}
			
			// --- Closes the current object ---
			try {
				handler.endElement(null, null, objectName);
			} catch (SAXException e) {
				try {
					handler.error(new SAXParseException(e.getMessage(), null));
				} catch (SAXException e1) { }
			}
		}

		// ------------------------------------------------------------
		// 	If the object is an Object Map, 
		else if (jsonObject instanceof ObjectList) {
			ObjectList ol = (ObjectList)jsonObject;
			for (Object kid : ol) {
				this.handleObject(kid, objectName, handler);
			}
		}
		
		// ------------------------------------------------------------
		// If the object is a String, Long, Integer, Double, Float or Boolean,
		// just print the content
		else if ((jsonObject instanceof String) 
				|| (jsonObject instanceof Long) || (jsonObject instanceof Integer) 
				|| (jsonObject instanceof Double) || (jsonObject instanceof Float)
				|| (jsonObject instanceof Boolean)) {
			char  [] objectValue = jsonObject.toString().toCharArray();
			try {
				handler.startElement(null, null, objectName, null);
				handler.characters(objectValue, 0, objectValue.length);
				handler.endElement(null, null, objectName);
			} catch (SAXException e) {
				try {
					handler.error(new SAXParseException(e.getMessage(), null));
				} catch (SAXException e1) { }
			}
		}

		// ------------------------------------------------------------
		//  End of document 
		if (objectName.equals(ROOT_NAME)) {
			try {
				handler.endDocument();
			} catch (SAXException e) {
				try {
					handler.error(new SAXParseException(e.getMessage(), null));
				} catch (SAXException e1) { }
			}
		}
	}
	
	
}
