/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.data;

//import java.io.IOException;
import java.io.InputStream;
//import java.net.MalformedURLException;
import java.util.ArrayList;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.webservices.patternmatcher.MatcherManager;
import com.ibm.gaiandb.webservices.patternmatcher.TagMatcher;
//import com.ibm.gaiandb.webservices.scanner.FormatSpecifierInputStream;
import com.ibm.gaiandb.webservices.scanner.WsDataFormat;
import com.ibm.gaiandb.webservices.scanner.json.JsonScanner;
import com.ibm.gaiandb.webservices.scanner.sax.SaxScanner;
//import com.ibm.gaiandb.webservices.ws.RestWS;
//import com.ibm.gaiandb.webservices.ws.WebService;

/**
 * <p>
 * The purpose of this class is to read a stream containing either XML or JSON data 
 * and infer the logical table - associated to the stream - which has to be requested
 * by GAIAN.
 * <p>
 * The logical table structure is returned by the method getLogcalTableColumns() which
 * returns the name of each column.
 * 
 * @author remi - IBM Hursley
 *
 */
public class LogicalTableGenerator {

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
	
	private Logger logger = new Logger("LogicalTableGenerator for GenericWS", 0);
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	/** The name of the found columns. */
	private String[] columnNames = null;
	
	/** The MatcherManager generated from reading the stream. */
	private MatcherManager matcherManager = null;

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * <p>
	 * Will scan a stream in order to:											<br/>
	 * - define what the column names of the logical table designing the 
	 * received data are														<br/>
	 * - define the TagMatchers allowing GenericWS to display the data into a VTI.
	 * <p>
	 */
	public LogicalTableGenerator(InputStream is, WsDataFormat format) {
		if (format != null && format != WsDataFormat.UNKNOWN_FORMAT) {
			// Defines handler
			LogicalTableGeneratorHandler handler = new LogicalTableGeneratorHandler();
			
			// Defines scanner requirements
			Runnable scanner;
			Thread scannerLauncher;
			if (format == WsDataFormat.JSON) {
				// Scans file
				scanner = new JsonScanner(null, is, handler);
			}
			else { // if (format == WsDataFormat.XML) {
				// Scans file
				scanner = new SaxScanner(null, is, handler);
			}
			
			// Start scan, and thus, init the handler
			scannerLauncher = new Thread(scanner,"webservicesScannerLauncher");
			scannerLauncher.start();
			
			try {
				// waits the scan is done
				handler.getSemaphoreMatchingComplete().acquire();
				
				ArrayList<TagMatcher> matchers;
				// defines attributes
				if (format == WsDataFormat.JSON) {
					matchers = handler.getAttributeMatchers();
					this.columnNames = handler.getColumnNamesAttributeMatching();
				}
				else { // if (format == WsDataFormat.XML) {
					matchers = handler.getValueMatchers();
					this.columnNames = handler.getColumnNamesValueMatching();
				}
				
				// Generates the matcher
				this.matcherManager = new MatcherManager(matchers.size());
				this.matcherManager.setMatchers(matchers);
				
//				this.objectToReturnName = handler.getObjectToReturnName();
				
			} catch (InterruptedException e) {
				logger.logException(
						GDBMessages.DSWRAPPER_GENERICWS_THREAD_SYNCHRONIZATION, 
						"The application has been interrupted while a logical " +
						"table was being created for the VTI GenericWS", 
						e);
			}
		}
		else if (format != null) {
			logger.logWarning(
					GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
					"When generating a logical table for the VTI GenericWS, the data " +
					"cannot be parsed either in XML, or in JSON. ");
		}
		else { // if (format == null) {
			logger.logWarning(
					GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
					"When generating a logical table for the VTI GenericWS, there is " +
					"no data to be read for generating it.");
		}
	}
	
	/**
	 * Returns the name of the columns found in the web service return value.
	 * @return the name of the columns found in the web service return value.
	 */
	public String[] getLTColumnNames() {
		return this.columnNames;
	}
	
	/**
	 * Returns the MatcherManager generated during he construction of the object.
	 * @return the MatcherManager generated during he construction of the object.
	 */
	public MatcherManager getGeneratedMatcherManager() {
		return this.matcherManager;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	
}
