/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner.sax;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;
//import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.db2j.GenericWS;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.webservices.scanner.GaianHandler;

/**
 * The purpose of this class is to define a Sax scanner, parsing a XML document,
 * And called by a {@link GenericWS} object.
 * 
 * @author remi - IBM Hursley
 */
public class SaxScanner implements Runnable {

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
	// -------------------------------------------------------------------------- Dynamic
	
	/** The GenericWS object calling this runnable. */
	private GenericWS caller;

	/** The stream which is to be scanned by the object. */
	private InputStream inputStream;
	
	private DefaultHandler handler;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/** Crates a SaxScanner oject. */
	public SaxScanner(GenericWS caller, InputStream inputStream) {
		this.caller = caller;
		this.inputStream = inputStream;
		this.handler = new GaianHandler(this.caller);
	}
	public SaxScanner(GenericWS caller, InputStream inputStream, DefaultHandler handler) {
		this.caller = caller;
		this.inputStream = inputStream;
		this.handler = handler;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * Starts the scan of a xml file, which coordinates are defined in
	 * the property file read by the {@link com.ibm.db2j.GenericWS} object 
	 * calling the current scanner.
	 */
	@Override
	public void run() {
		
		try {
			
			// Initialises the SaxParser tools
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			
//			// Define the handler which has to be used for the parsing
//			// This handler has been specified especially for an GenericWS object. 
//			GaianHandler genericWsHandler = new GaianHandler(this.caller);
			
			// Scan the file - resultCommand.getFilteredResult()
			saxParser.parse(this.inputStream, this.handler);
//			saxParser.parse(this.inputStream, genericWsHandler);
				
			this.inputStream.close();
			
		} catch (ParserConfigurationException pce) {
			if (this.caller != null) {
				this.caller.logException(
						GDBMessages.DSWRAPPER_GENERICWS_PARSER_ERROR, 
						"An exception occurred while creating the Sax parser. Contact IBM " +
						"to bring out this exception.",
						pce);
			}
		} catch (SAXException saxe) {
			if (this.caller != null) {
				this.caller.logException(
						GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
						"An exception occurred while reading the data sent by the web service.\n" +
						"Check the received data are in XML format.",
						saxe);
			}
		} catch (IOException ioe) {
			if (this.caller != null) {
				this.caller.logException(
						GDBMessages.DSWRAPPER_GENERICWS_LOST_CONNECTION, 
						"An IO exception occurred while reading the data sent by the web service.\n" +
						ioe.getMessage(), 
						ioe);
			}
		} 
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
	
}
