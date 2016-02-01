/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.db2j;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.utils.Pair;
import com.ibm.gaiandb.webservices.XmlElement;
import com.ibm.gaiandb.webservices.caching.CachableInputStream;
import com.ibm.gaiandb.webservices.caching.StringCacher;
import com.ibm.gaiandb.webservices.parser.NonParsableStringException;
import com.ibm.gaiandb.webservices.parser.properties.GenericWsPropertiesParser;
import com.ibm.gaiandb.webservices.patternmatcher.AttributeMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.ErrorMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.MatcherManager;
import com.ibm.gaiandb.webservices.patternmatcher.TagMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.TagPattern;
import com.ibm.gaiandb.webservices.patternmatcher.ValueMatcher;
import com.ibm.gaiandb.webservices.scanner.FormatSpecifierInputStream;
import com.ibm.gaiandb.webservices.scanner.IntoXmlInputStream;
import com.ibm.gaiandb.webservices.scanner.WsDataFormat;
import com.ibm.gaiandb.webservices.scanner.json.JsonScanner;
import com.ibm.gaiandb.webservices.scanner.sax.HTMLFilterInputStream;
import com.ibm.gaiandb.webservices.scanner.sax.SaxScanner;
import com.ibm.gaiandb.webservices.tools.Inserter;
import com.ibm.gaiandb.webservices.ws.PostRestWS;
import com.ibm.gaiandb.webservices.ws.RestWS;
import com.ibm.gaiandb.webservices.ws.SoapWS;
import com.ibm.gaiandb.webservices.ws.WebService;

/**
 * <p>
 * The purpose of this class is to import XML files of loading a GenericWS VTI. 
 * It reads the gaian_config.properties file and extract informations for each 
 * one of the columns of the VTI. each column has its own definition in the 
 * config file.
 * <p>
 * The XML files can be imported either from aweb server using REST web services
 * or a local XML file.
 * <p>
 * An extension will be done for importing Json files, and another one for using
 * SOAP web services.
 * 
 * @author remi - IBM Hursley
 *
 */
public class GenericWS extends AbstractVTI {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	
	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";
	
	
	// ------- Properties -------
	
	/** 
	 * <p>
	 * Property <b>ELT_CONTENT_TO_RMV</b> to read in gaian_config.properties.
	 * It defines the tags and their content which have to be ignored when
	 * scanning an inputStream containing a HTML file.
	 * <p>
	 * Is to be used with the method getVTIProperty(String propertyName)
	 * <p>
	 * Property name: 'GenericWs.ELT_CONTENT_TO_RMV'
	 */
	public static final String PROP_ELEMENT_CONTENTS_TO_REMOVE = "ELT_CONTENT_TO_RMV";
	
	/** 
	 * <p>
	 * Property <b>ELT_TO_RMV</b> to read in gaian_config.properties.
	 * It defines the tags and their content which have to be ignored when
	 * scanning an inputStream containing a HTML file.
	 * <p>
	 * Is to be used with the method getVTIProperty(String propertyName)
	 * <p>
	 * Property name: 'GenericWs.ELT_TO_RMV'
	 */
	public static final String PROP_ELEMENTS_TO_REMOVE = "ELT_TO_RMV";
	
	/** 
	 * <p>
	 * Property to read in gaian_config.properties. It defines the url the file 
	 * has to be downloaded from. 
	 * <p>
	 * Property name: 'GenericWs.url' 
	 */
	public static final String PROP_URL = "url";
	
	/** 
	 * <p>
	 * Property to read in gaian_config.properties. It defines the name
	 * of the tag which (when is closed) means the current record of data 
	 * has to be loaded in the VTI. 
	 * <p>
	 * Property name: 'sendWhenClosing' */
	public static final String PROP_SEND_WHEN_CLOSING = "sendWhenClosing";
	
	/** 
	 * <p>
	 * Property to read in gaian_config.properties. It defines the prefix of
	 * the column property definition. The name of the full property (for the 
	 * column having the index '2' in the VTI, will be: 
	 * <b>C2</b>. The indexes of the columns start at 1. 
	 * <p>
	 * Property name: 'genericWs.CX', X being an integer.
	 */
	public static final String PROP_COLUMN_TAG_PREFIX = "C";

	/** 
	 * <p>
	 * Property to read in gaian_config.properties. It defines the suffix of
	 * the column property definition. The name of the full property (for the 
	 * column having the index '2' in the VTI, will be: 
	 * <b>C2.XML_LOCATE_EXPRESSION</b>. The indexes of the columns start at 1. 
	 * <p>
	 * Property name: 'genericWs.C#.XML_LOCATE_EXPRESSION', # being an integer.
	 */
	public static final String PROP_COLUMN_TAG_SUFFIX = ".XML_LOCATE_EXPRESSION";
	
	/**
	 * <p>
	 * Property to read in gaian_config.properties. It defines whether the HTML 
	 * filter will be applied on the stream or not. 
	 * <p>
	 * Property name: 'GenericWs.applyHtmlFilter'
	 * <p>
	 * Values can be: TRUE or FALSE
	 */
	public static final String PROP_APPLY_HTML_FILTER = "applyHtmlFilter";
	
	/**
	 * <p>
	 * Property to read in gaian_config.properties. It defines which type 
	 * of web service will be used. 
	 * <p>
	 * Property name: 'GenericWs.wstype'
	 * <p>
	 * Values can be: REST (by default) / SOAP / LOCAL
	 */
	public static final String PROP_WS_TYPE = "wstype";
	
	/** Possible value for the property PROP_WS_TYPE. */
	public static final String PROP_WS_TYPE_VALUE_SOAP = "SOAP";
	
	/** Possible value for the property PROP_WS_TYPE. */
	public static final String PROP_WS_TYPE_VALUE_REST = "REST";
	
	/** Possible value for the property PROP_WS_TYPE. */
	public static final String PROP_WS_TYPE_VALUE_LOCAL_FILE = "LOCAL";
	
	/**
	 * <p>
	 * Property to read in gaian_config.properties. It defines the data  
	 * to send when using a REST web service of type POST. If this value
	 * is null or empty, the web service will be considered as a REST
	 * web service of type GET. 
	 * <p>
	 * Property name: 'GenericWs.wstype'
	 * <p>
	 * Values can be: either the argument to send, or the name of a file 
	 * containing the values to send.
	 */
	public static final String PROP_POST_DATA = "POST_DATA";
	
	/**
	 * <p>
	 * Property to read in gaian_config.properties. It defines whether the 
	 * received data will be "converted" into XML or not. If set a true, the
	 * received data will start with &#60;xml&#62; and end with &#60;/xml&#62;.
	 * <p>
	 * Property name: 'GenericWs.convertToXml'
	 * <p>
	 * Values can be: TRUE / FALSE (default value).
	 */
	public static final String PROP_APPLY_XML_CONVERTOR = "convertToXml";

	/**
	 * <p>
	 * Property to read in gaian_config.properties. It defines the data  
	 * format received as an answer from the web service.
	 * <p>
	 * Property name: 'GenericWs.wstype'
	 * <p>
	 * Values can be: JSON / XML / AUTO (default value).
	 */
	public static final String PROP_DATA_FORMAT = "DATA_FORMAT";
	
	/** Possible value for the property PROP_DATA_FORMAT. */
	public static final String PROP_DATA_FORMAT_VALUE_JSON = "JSON";
	
	/** Possible value for the property PROP_DATA_FORMAT. */
	public static final String PROP_DATA_FORMAT_VALUE_XML = "XML";
	
	/** Possible value for the property PROP_DATA_FORMAT. */
	public static final String PROP_DATA_FORMAT_VALUE_AUTO = "AUTO";
	
	/**
	 * <p>
	 * Property to read in gaian_config.properties. It defines the url
	 * to the WSDL in the case of a SOAP web service. This option is 
	 * optional. 
	 * <p>
	 * Property name: 'GenericWs.wsdl'
	 */
	public static final String PROP_WSDL = "wsdl";
	
	/** Default value for the cache. */
	public static final int PROP_CACHE_EXPIRES_DEFAULT_VALUE = 0;
	
	public static final String PROP_SCHEMA = AbstractVTI.PROP_SCHEMA;
	
	
	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	
	private static final String CLASS = GenericWS.class.getSimpleName();//"GenericWS";
	
	private static final int NB_CACHED_RECORDS = 10;
	private static Map<String, StringCacher> cachedStreams 
						= new CachedHashMap<String, StringCacher>(NB_CACHED_RECORDS);
	
	private static final Logger logger = new Logger( CLASS, 20 );
	
	
	// ------- Managing the list of records -------
	
	/** The maximum size of theArrayBlockingQueue used for the records. */
	private static final int RECORD_CAPACITY = 10;
	
	/**
	 * Value which will be inserted in the record for indicating that 
	 * the last record has been sent. 
	 */
	private static final String[] POISON_PILL = {};
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	// ------- Properties -------
	
	/** 
	 * <p>
	 * Represents the value of the tag representing the main object of the properties
	 * looked in the xml file.
	 * <p>
	 * For the following xml content:										<br/> 
	 * &#60;person&#62;														<br/>&nbsp;&nbsp;&nbsp;
	 * 		&#60;name&#62;  Remi  &#60;/name&#62;							<br/>&nbsp;&nbsp;&nbsp;
	 * 		&#60;address&#62;												<br/>&nbsp;&nbsp;&nbsp;
	 * 																		&nbsp;&nbsp;&nbsp;
	 * 				&#60;city&#62;  Southampton  &#60;/city&#62;			<br/>
	 * 		&#60;/address#62;												<br/>
	 * &#60;/person&#62;
	 * <p>
	 * If the VTI to fill needs the name and the city, the tagForSendingData will 
	 * be the value "person" since both are an information for this tag. Each time 
	 * the scanner will meet an end tag which name is tagForSendingData, it will
	 * send the records currently saved. 
	 * <p>
	 * Found in the gaian_properties.config file. 
	 * <p>
	 * Property: 'genericWs.sendWhenClosing'
	 */
	 // FYI: In HTML &#60; = '<' and &#62; = '>'
	private String tagForSendingData;
	
	/** 
	 * <p>
	 * Defines the properties for the different columns of the VTI. 
	 * <p>
	 * Loaded by reading the config file. 
	 */
	private MatcherManager columnsPropertiesManager;
	

	// ------- Managing the list of records -------
	
	/**
	 * Each String[] represents a line which is going to be inserted in the VTI. 
	 * The arrayLit represent all the set to be inserted.
	 */
	private ArrayBlockingQueue<String[]> recordsQ = 
							new ArrayBlockingQueue<String[]>(RECORD_CAPACITY);
	
	/** The number of records received when sending the request. */
	private int nbRecords = 0;

	/** 
	 * The qualifiers of the VTI.
	 * <p>
	 * <a href="https://builds.apache.org/job/Derby-trunk/lastSuccessfulBuild/artifact/trunk/
	 *javadoc/engine/org/apache/derby/iapi/store/access/Qualifier.html">Doc for Qualifiers</a>
	 */
	private Qualifier[][] qualifiers = null;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS
	
	/** 
	 * <p>
	 * The different web services which can be used.
	 * <p> 
	 * <b>Currently, only REST is supported.</b>
	 */
	private enum WS_FAMILY { REST, SOAP, LOCAL};

//	public enum ReadFormat { XML, JSON };
	
	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS
	
	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * Creates a GenericWS object.
	 * 
	 * @param constructor
	 * 			Argument given to the parameter ARGS in the gaian config file. <br/>
	 * i.e. if the VTI is defined by LT_NAME_DS0_VTI=com.ibm.db2j.GenericWS (for 
	 * the table LT_NAME) in the config file, the constructor will be the value 
	 * given to the parameter LT_NAME_DS0_ARGS. ex: LT_NAME_DS0_ARGS=name,100
	 */
	public GenericWS(String constructor, String prefix) throws Exception {
		super(constructor, prefix);
	}
	public GenericWS(String constructor) throws Exception { 
		super(constructor); 
	}
	public GenericWS() throws Exception { super(null, null); }
	
	
	// -------------------------------------------------------------------------- Private
	

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * Returns true if the name of the tag given in parameter is the same
	 * than the one defining the flag requiring to send the records to the 
	 * VTI. False otherwise. 
	 * 
	 * @param tagName
	 * 			The tag name which could match the tag defining the
	 * flag requiring to send the records to the VTI.  
	 * 
	 * @return true if the name of the tag given in parameter is the same
	 * than the one defining the flag requiring to send the records to the 
	 * VTI. False otherwise. 
	 */
//	public boolean isTagForSendingData(String tagName) {
//		return this.tagForSendingData.equals(tagName);
//	}
	
	/**
	 * Returns the MatcerManager matching the properties of the VTI's columns.
	 * @return the MatcerManager matching the properties of the VTI's columns.
	 */
	public MatcherManager getColumnsPropertiesManager() {
		return this.columnsPropertiesManager;
	}
	
	/**
	 * Saves the current record into the list of records and reinitialises 
	 * the current record.
	 * @throws InterruptedException 
	 * 			If thread is interrupted when waiting for writing a record in the 
	 * ArrayBlockingQueue.
	 */
	public void saveCurrentRecord() {
		
		// --- Checks that the record is not empty
		boolean recordHasValue = false;
		String recordCells[] = this.columnsPropertiesManager.getResult();
		for (String cell : recordCells){
			if (cell != null) {
				recordHasValue = true;
				break;
			}
		}

		logger.logDetail("Got new record: " + Arrays.asList(recordCells));
		
		// --- Pastes the record in the list of records to write in the VTI 
		try {
			if (recordCells != null && recordHasValue) {
				// Write in the ArrayBlockingQueue
				this.recordsQ.put(recordCells);
				this.nbRecords++;
				
				// Reinitialise the current record
				this.columnsPropertiesManager.reinitializeResults();
			}
		} catch (InterruptedException ie) {
			logger.logException(GDBMessages.DSWRAPPER_GENERICWS_KILLED_PROCESS, 
					"A process of GaianDB has been killed and the application might " +
					"enter in dead lock.", ie);
		}
	}
	
	/**
	 * Informs the object that the last record has been sent.
	 */
	public void confirmSendingOfLastRecord() {
		try {
			this.recordsQ.put(POISON_PILL);
		} catch (InterruptedException e) {
			logger.logException(GDBMessages.DSWRAPPER_GENERICWS_KILLED_PROCESS, 
					"A process of GaianDB has been killed and the application might " +
					"enter in dead lock.", e);
		}
	}
	
	/** 
	 * Logs the exception in the GenericWS logger. 
	 * @param errorCode
	 * 			Exception's name.
	 * @param message
	 * 			Exception's message.
	 * @param e
	 * 			Exception to log.
	 */
	public void logException(String errorCode,
			String message, Throwable e) {
		synchronized (GenericWS.logger) {
			GenericWS.logger.logException(errorCode, message, e);
		}
	}
	
	/**
	 * Reads the properties in the config file and send the web service
	 * request to get the result and scan it.
	 * 
	 * @return True if the scan is completed, false if an exception occurs 
	 * during the scan.
	 */
	@Override 
	public boolean executeAsFastPath() 
			throws StandardException, SQLException {  
		
		// Reads properties for generating the MatcherManager
		this.loadMatcherManager();
		
		// Scans xml file returned by request and stores the results in  
		InputStream is = this.getData();
		
		logger.logInfo("Starting data scan");
		
		if (is != null){
			this.startScan(is);
		}
		else {
			logger.logWarning(
					GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_URL, 
					"The property " + CLASS + "." + this.getPrefix() +
						"." + PROP_URL + " starts a web service not returning " +
						"any results.");
			
			this.confirmSendingOfLastRecord();
		}
		
		return true; 
	}

	/**
	 * Saves the qualifiers into an object's attribute.
	 * 
	 * @param vtie
	 * 			VTI?
	 * @param qual
	 * 			the qualifiers filtering the query.
	 */
	@Override
	public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual)
			throws SQLException { 
				this.qualifiers  = qual;
	}

	/**
	 * Sets the next VTI's row to be displayed.
	 * 
	 * @param rowToFill
	 * 			The row to set up.
	 * 
	 * @return IFastPath.SCAN_COMPLETED if a row is returned, IFastPath.SCAN_COMPLETED
	 * in case of exception, or if there is no row to display. 
	 */
	@Override
	public int nextRow(DataValueDescriptor[] rowToFill) throws StandardException,
			SQLException { 
		
		// Gets the first row
		try {
			
			logger.logDetail("Getting nextRow()");
			
			boolean isFilledDvd = false;
			
			// Fill the DVD with the last record until the record passes the qualifiers
			while (!isFilledDvd) {
				
				String [] firstLine = (String[])this.recordsQ.take();
				
				// if records are still being found
				if (!isPoisonPill(firstLine)) {
					int iDVD = 0; // Index going through the DaaValueDescriptor arg0
					
					// And insert it into the DataValueDescriptor
					for (String cell : firstLine) {
						try {
							rowToFill[iDVD].setValue(cell);
						} catch (StandardException se) {
							logger.logException( 
									GDBMessages.DSWRAPPER_GENERICWS_WRONG_VALUES_FORMAT_IN_FILE, 
									"The value " + cell + " scanned in the read file cannot be " +
									"conveted in the right format " + rowToFill[iDVD].getTypeName(), se);
						}
						iDVD++;
					}
					
					if(this.qualifiers == null || RowsFilter.testQualifiers( rowToFill, qualifiers )) {
						// The current record fits
						return IFastPath.GOT_ROW;
					}
					// else: the DVD does not passes the tests of the qualifiers
					// so does not return IFastPath.GOT_ROW
					
				}
				// The last record has been found
				else {
					return IFastPath.SCAN_COMPLETED;
				}
			}
			
		} catch (InterruptedException ie) {
			logger.logException( 
					GDBMessages.DSWRAPPER_GENERICWS_THREAD_SYNCHRONIZATION, 
					"The application has been interrupted while waiting for reading a record", 
					ie);
		}
		return IFastPath.SCAN_COMPLETED;
	}

	/**
	 * Returns the number of records returned when receiving the answer of the 
	 * Web Service.
	 * 
	 * @return the number of records returned when receiving the answer of the 
	 * Web Service.
	 */
	@Override
	public int getRowCount() throws Exception { return this.nbRecords; }

	@Override public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) 
			throws SQLException { 
		return 0; }
	@Override public double getEstimatedRowCount(VTIEnvironment arg0) 
			throws SQLException {  
		return 0; }
	@Override public boolean supportsMultipleInstantiations(VTIEnvironment arg0)
			throws SQLException {  
		return false; }
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * <p>
	 * TODO - synchronisation on cache
	 * <p>
	 * Returns either the data after accessing a web service, either the 
	 * cached data if the return value of the web service has been cached.
	 * 
	 * @return  either the data after accessing a web service, either the 
	 * cached data if the return value of the web service has been cached.
	 */
	private InputStream getData() {
		try {
			// -------------------------------------------------------------
			// Get te properties defining the key of the cached values 
			String wsUrl = this.getVTIPropertyWithReplacements(PROP_URL);
			String postData = this.getVTIPropertyNullable(PROP_POST_DATA);
			int expringTime = this.getPositiveIntegerVTIProperty(PROP_CACHE_EXPIRES);
			
			String urlKeyForCach = new String(wsUrl);
			
			if (postData != null) {
				urlKeyForCach += postData;
			}

			// ######################################################################
			//
			// Note - web service access + creation of a cache value for distinct 
			// URLs is not currently synchronized - i.e. restricted to single-thread 
			// processing.
			//
			// ######################################################################
			//
			// Solution would be to use a concurrent set which would contain the URLs 
			// of web services which are currently being accessed to populate the 
			// cache - Threads accessing a same URL would compete to PUT() their URL 
			// into this set, which would allow them to either 1) retrieve their value
			// from the cache or 2) start accessing the web service and populating the
			// cache - After either of these, the thread would remove their URL from
			// the set..
			// 
			// Using a concurrent map object and the "take()" operation would not work
			// because we couldn't differentiate between whether a thread was currently
			// working to populate the cache or not (i.e. the take() would erroneously
			// block for the first thread accessing a particular URL)
			//
			// ######################################################################
			
			synchronized( urlKeyForCach.intern() ) {
				
				// -------------------------------------------------------------
				// Get the cached values
				StringCacher cacher = cachedStreams.get(urlKeyForCach);
				
				// -------------------------------------------------------------
				// Return stream depending on if it has been cached or not 
				if (expringTime > 0 && cacher != null && !cacher.hasExpired()) {
					// has been cached
//					cacher.resetExpiring();
					return new ByteArrayInputStream(cacher.getCachedData().getBytes());
				}
				// If the value is null, it means that the stream hasn't been cached
				else {
					return this.sendCommand(wsUrl, postData);
				}	
			}
			
		} catch (Exception e) {
			logger.logException(
						GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_URL, 
						"Either the url given in the property  " + CLASS + "." + this.getPrefix() +
						"." + PROP_URL + " cannot start any web services, or the parameters " +
						"replacement within this url failed to generate a valid url.",
						e);
		}
		return null;
	}
	
	/**
	 * Sends the command given, using the web service architecture defined in
	 * the attribute this.ws. 
	 * 
	 * @return The result of the command. Usually the content of a file.
	 * 
	 */
//	 * @throws IOException 
//	 * 			If issue occurs during the connection or if the URL is 
//	 * invalid (it must start with the protocol used for sending the request)
//	 * ex: <b>http://</b>www.ibm.com .
	private InputStream sendCommand(String wsUrl, String postData) {
		
		WebService webService;
//		 = null; // Declared here for being handled if an exception occurs.
		
		try {
			// -------------------------------------------------------------
			// --- Reads the url for the web services
//			wsUrl = this.getVTIPropertyWithReplacements(PROP_URL);
			Inserter urlInserter = new Inserter();
			wsUrl = urlInserter.qualifiersIntoUrl(wsUrl, qualifiers, this.grsmd);

			// -------------------------------------------------------------
			// --- Reads the type of web Service
			String wsTypeStr = this.getVTIPropertyNullable(PROP_WS_TYPE);
			WS_FAMILY wsType;
			if (wsTypeStr != null && wsTypeStr.equalsIgnoreCase(PROP_WS_TYPE_VALUE_SOAP)) {
				wsType = WS_FAMILY.SOAP;
			}
			else if (wsTypeStr != null && wsTypeStr.equalsIgnoreCase(PROP_WS_TYPE_VALUE_LOCAL_FILE)) {
				wsType = WS_FAMILY.LOCAL;
			}
			else{
				if (wsTypeStr == null || wsTypeStr.isEmpty()) {
					logger.logInfo("The property " +  
							CLASS + "." + 
							this.getPrefix() + "." +
							 PROP_WS_TYPE + 
							" has not been given. The VTI uses a REST " +
							"web service by default");
				}
				else if (!wsTypeStr.equalsIgnoreCase(PROP_WS_TYPE_VALUE_REST)){
					logger.logWarning(GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE, 
							"The property " + CLASS + "." + this.getPrefix() +
							 "." + PROP_WS_TYPE + " has not been correctly given. The value " +
							"should be either " + PROP_WS_TYPE_VALUE_REST + " or " + 
							PROP_WS_TYPE_VALUE_SOAP + ". The VTI uses a REST web service by default.");
				}
				wsType = WS_FAMILY.REST;
			}

			// -------------------------------------------------------------
			// --- Loads inputStream
			switch (wsType) {
				case SOAP:
					String soapPostData = this.parseRestPostProperty(postData);

					// raise exception by default
					if (soapPostData == null || soapPostData.isEmpty()) {
						logger.logWarning(GDBMessages.DSWRAPPER_GENERICWS_MISSING_PROPERTY, 
								"The property " + CLASS + "." + this.getPrefix() +
								 "." + PROP_POST_DATA + " is missing. the url has been " +
								 		"sent as a REST/GET Web service.");
						webService = new RestWS(wsUrl);
					}
					// and WS SOAP if specified
					else {
						String wsdl = this.getVTIPropertyNullable(PROP_WSDL);
						
						if (wsdl == null || wsdl.isEmpty()) {
							webService = new SoapWS(wsUrl, soapPostData);
						}
						else {
							webService = new SoapWS(wsUrl, wsdl, soapPostData);
						}
					}
					break;
					
				case LOCAL:
					webService = new RestWS(wsUrl);
					break;
					
				case REST:
				default:
					String restPostData = this.parseRestPostProperty(postData);
					
					// apply Web service REST GET by default
					if (restPostData == null || restPostData.isEmpty()) {
						webService = new RestWS(wsUrl);
					}
					// and WS REST POST if specified
					else {
						webService = new PostRestWS(wsUrl, restPostData);
					}
			}

			webService.openConnection();
			
			InputStream is = webService.getInputStream(); 

			// -------------------------------------------------------------
			// --- Applies filter if needed
			
			// This filter removes some flags from the HTML contained in the stream
			boolean applyHtmlFilter = getBooleanProperty(PROP_APPLY_HTML_FILTER);
			
			if (applyHtmlFilter) {
				is = new HTMLFilterInputStream(is);
				is = this.filterInputStream(is);
			}
			
			// This filter adds the flags <xml> at the beginning of the stream 
			// and </xml> at the end
			boolean convertToXml = getBooleanProperty(PROP_APPLY_XML_CONVERTOR);
			if (convertToXml) {
				is = new IntoXmlInputStream(is);
			}
			
//			is = new DisplayInputStream(is); // For tests and debug

			// -------------------------------------------------------------
			// Caching
			int caching = this.getPositiveIntegerVTIProperty(PROP_CACHE_EXPIRES);
			if (caching > 0) {
				is = new CachableInputStream(is, caching);
				StringCacher cacher = ((CachableInputStream)is).getCacher();
				
				String key = new String(wsUrl);
				if (postData != null && !postData.isEmpty()) {
					key += postData;
				}
				GenericWS.cachedStreams.put(key, cacher);
			}
			
			return is;
			
		} catch (FileNotFoundException fnfe) { // CAUTION FileNotFoundException extends IOException
			// L587: This exception should never be raised since the method 
			// parseRestPostProperty() checks if the file exists before opening 
			// it. 
			logger.logException(
					GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_FILE_NOT_FOUND, 
						"The file " + wsUrl + " given in the property " + 
						CLASS + "." + this.getPrefix() + "." + PROP_URL + 
						" does not exist or cannot be opened.",
					fnfe);
			return null;
		} catch (MalformedURLException mue) { // CAUTION MalformedURLException extends IOException
			logger.logException(
					GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_URL, 
					"The url given in the property " + CLASS + "." + this.getPrefix() +
						 "." + PROP_URL + " does not give any accesses to any servers.",
					 mue);
			return null;
		} catch (IOException ioe) { // CAUTION IOException extends Exception
			String serverName = "[not defined]";
			if (wsUrl != null) {
				serverName = wsUrl;
			}
			logger.logException(
					GDBMessages.DSWRAPPER_GENERICWS_LOST_CONNECTION, 
					"An error occured on the connection to the server [" + serverName + 
						"], given in the property " + CLASS + "." + this.getPrefix() +
						"." + PROP_URL,
					ioe);
			return null;
		} catch (Exception e) {
			logger.logException(
					GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_URL, 
					"Either the url given in the property  " + CLASS + "." + this.getPrefix() +
						"." + PROP_URL + " cannot start any web services, or the parameters " +
						"replacement within this url failed to generate a valid url.",
					e);
			return null;
		}
	}
	
	/**
	 * Override the method in order not to set any default value to the expiration time 
	 * for the cache. Therefore, if the property is not given, the default value is 
	 * GenericWS.PROP_CACHE_EXPIRES_DEFAULT_VALUE.
	 */
	@Override
	public Hashtable<String, String> getDefaultVTIProperties() {

        // NOTE THIS METHOD IS MOST LIKELY OVERRIDEN - OR SHOULD BE...
        // All default values for abstract VTI properties are defined here
        if (null == defaultVTIProperties)
            defaultVTIProperties = new Hashtable<String, String>() {
                private static final long serialVersionUID = 1L;

                {
                    // put(getPrefix() + "." + PROP_CACHE_EXPIRES, DEFAULT_EXPIRY_SECONDS);
                    put(PROP_CACHE_EXPIRES, "" + PROP_CACHE_EXPIRES_DEFAULT_VALUE); 
                }
            };

        return defaultVTIProperties;
	}
	
	/**
	 * Reads the properties from the config file which are needed to generate 
	 * the MatcherManager of the current object.
	 * 
	 * @throws NumberFormatException
	 * 				When the properties which are supposed to be converted into 
	 * 				numbers have a non-convertible format.
	 * 
	 * @throws Exception 
	 * 				If errors appear during the properties fetching.
	 */
	private void loadMatcherManager() {

		// -------------------------------------------------------------
		// Reads the main tag property 
		this.tagForSendingData = this.getVTIPropertyNullable(PROP_SEND_WHEN_CLOSING);
		
		
		// -------------------------------------------------------------
		// Creates a manager for managing the properties read for the columns.  
		try {

			// ----- Reads the column properties
			int nbColumns = this.getMetaData().getColumnCount();
			
			// ----- Checks if the property presenting the pattern of the object to return is given..
			if (this.tagForSendingData != null && !this.tagForSendingData.isEmpty()) {
				GenericWsPropertiesParser parser = new GenericWsPropertiesParser(this.tagForSendingData);
				try {
					ArrayList<TagPattern> patternObjectToReturn = parser.parseTags();
					
					// --- Checks the pattern given is looking for a 'value' element 
					// (element between <myTag> and </myTag> in XML)...
					if (parser.getRequestType() == XmlElement.VALUE) {
						this.columnsPropertiesManager = new MatcherManager(nbColumns, patternObjectToReturn);
					}
					
					// --- ... Otherwise, it is automatically defined
					else {
						logger.logWarning(GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
								"the value of the property " + 
								CLASS + "." + this.getPrefix() + "." + PROP_SEND_WHEN_CLOSING +
								" cannot be parsed into a value matcher.\n" +
								"The value of this property will be automatically defined by " +
								"defining the common root of the tags defined by all the properties " +
								CLASS + "." + this.getPrefix() + "." + 
										PROP_COLUMN_TAG_PREFIX + "#" + PROP_COLUMN_TAG_SUFFIX);
						this.columnsPropertiesManager = new MatcherManager(nbColumns);
					}
					
				} catch (NonParsableStringException e) {
					logger.logWarning(GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
							"the value of the property " + 
							CLASS + "." + this.getPrefix() + "." + PROP_SEND_WHEN_CLOSING +
							" cannot be parsed into a element matcher.\n" +
							"The value of this property will be automatically defined by " +
							"defining the common root of the tags defined by all the properties " +
							CLASS + "." + this.getPrefix() + "." + 
									PROP_COLUMN_TAG_PREFIX + "#" + PROP_COLUMN_TAG_SUFFIX);
					this.columnsPropertiesManager = new MatcherManager(nbColumns);
				}
			}
			// ----- ... Otherwise, calculate it automatically
			else {
				logger.logInfo("The value of the property " +
						CLASS + "." + this.getPrefix() + "." + PROP_SEND_WHEN_CLOSING +
						"will be automatically defined by defining the common root of " +
						"the tags defined by all the properties " +
						CLASS + "." + this.getPrefix() + "." + 
								PROP_COLUMN_TAG_PREFIX + "#" + PROP_COLUMN_TAG_SUFFIX);
				this.columnsPropertiesManager = new MatcherManager(nbColumns);
			}
			
			
			// -------------------------------------------------------------
			// Defines the pattern for each element of the MacherManager previously defined
			for (int i = 1; i <= nbColumns; i++) {
				// gets the property GenericWS.C#.XML_LOCATE_EXPRESSION
				// # being a number between 1 and this.nbColumns
				String propCol = this.getVTIPropertyNullable(
						PROP_COLUMN_TAG_PREFIX + i + PROP_COLUMN_TAG_SUFFIX);
				
				// the stamp matcher which will be added to the MatcherManager
				TagMatcher matcher = null;
					
				if (propCol != null && !propCol.isEmpty()) {
					// Gets and parses the property for the column
					GenericWsPropertiesParser parser = new GenericWsPropertiesParser(propCol);
					ArrayList<TagPattern> tagsDefiningPropety = null;
					try {
						tagsDefiningPropety = parser.parseTags();
					} catch (NonParsableStringException e) {
						logger.logException(e.getType(), 
								"In the definition of the property " 
								+ CLASS + "." 
									+ this.getPrefix() + "." 
									+ PROP_COLUMN_TAG_PREFIX + i + PROP_COLUMN_TAG_SUFFIX 
									+ "=" + propCol + "\n" 
									+ e.getMessage(), 
								e);
					}
					
					// Checks which kind of TagMatcher it has to create
					XmlElement kindOfParsedProperty = parser.getRequestType();
					
					// Creates the right matcher - with a null value if none can be defined
					switch (kindOfParsedProperty) {
						case VALUE:
							matcher = new ValueMatcher(tagsDefiningPropety);
							break;
						case TAG_ATTIBUTE:
							try {
								Pair<String, Integer> results = 
										(Pair<String, Integer>)parser.getRequestQualifiers();
								matcher = new AttributeMatcher(
													tagsDefiningPropety,
													results.getFirst(),
													results.getSecond().intValue());
							}
							catch (Exception e) {
								logger.logException(
										GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
										"In the definition of the property " 
										+ CLASS + "." 
											+ this.getPrefix() + "." 
											+ PROP_COLUMN_TAG_PREFIX + i + PROP_COLUMN_TAG_SUFFIX 
											+ "=" + propCol + "\n" 
											+ "The object matching the defined sequence of tags does not provide " 
											+ "both, the name of the attriute to find and the depth of the tag " 
											+ "containing this attribute.", 
										e);
								matcher = new ErrorMatcher();
							}
							break;
						case ERROR_TAG:
						case UNDEFINED:
							matcher = new ErrorMatcher();
							break;
						default:
							// we keep the matcher = null
								
					}
				}
				else {
					// if a column number is missing in the property file
					logger.logWarning("GenericWS error", 
							"Illegal GenericWS Type defined for " + 
							super.getPrefix()+
							", column " + PROP_COLUMN_TAG_PREFIX + i +
							" is not defined.");
				}
				
				// Saves the matcher - even if null
				this.columnsPropertiesManager.addMatcher(matcher);
			}
			
			if (this.columnsPropertiesManager.getPatternSequence() == null) {
				this.columnsPropertiesManager.defineCommonRoot();
			}
		
		} catch (SQLException sqle) {
			logger.logException(GDBMessages.DSWRAPPER_METADATA_RESOLVE_ERROR, 
					sqle.getMessage(), 
					sqle);
		}
	}
	
	/**
	 * Starts a thread scanning the stream given as a parameter. 
	 * Will define the format of the received data (XML / JSON).
	 * 
	 * @param is
	 * 			InputStream which as to be scanned.
	 * 
	 */
	private void startScan(InputStream is) {
		
		//-------------------------------------------------------
		// Define the format of the received data
		String format = this.getVTIPropertyNullable(PROP_DATA_FORMAT);
		WsDataFormat readData = WsDataFormat.UNKNOWN_FORMAT;
		
		if (format != null && format.equalsIgnoreCase(PROP_DATA_FORMAT_VALUE_JSON)) {
			readData = WsDataFormat.JSON;
		}
		else if (format != null && format.equalsIgnoreCase(PROP_DATA_FORMAT_VALUE_XML)) {
			readData = WsDataFormat.XML;
		}
		else {
			is = new FormatSpecifierInputStream(is);
			
			readData = ((FormatSpecifierInputStream)is).defineFormat();
		}

//		//-------------------------------------------------------
//		// Checks the juno package is installed
//		if (readData == WsDataFormat.JSON) {
//			try { 
//				Object.class.getClass().getClassLoader().loadClass(
//						com.ibm.juno.core.json.JsonParser.class.getName()
//				);
//			} catch ( Throwable e ) { 
//				logger.logInfo("The Juno package is not installed. GenericWS cannot parse JSON:\n" + e); 
//				readData = WsDataFormat.UNKNOWN_FORMAT;
//			}
//		}
		
		//-------------------------------------------------------
		// Reads the data result depending on its format
		// -- JSON file received
		if (readData == WsDataFormat.JSON) {
			
			// Scans file
			Runnable scanner = new JsonScanner(this, is);
			
//			((JsonScanner)scanner).run(); // either this line or the two next ones
			Thread scannerLauncher = new Thread(scanner, "ScannerLauncher");
			scannerLauncher.start();
			
		}
		// -- XML file received
		else if (readData == WsDataFormat.XML) {
				
			// Scans file
			Runnable scanner = new SaxScanner(this, is);
			
//			((SaxScanner)scanner).run(); // either this line OR the two next ones
			Thread scannerLauncher = new Thread(scanner, "Scanner");
			scannerLauncher.start();
			
		}
		// -- unknown received data format
		else { // if (readData == WsDataFormat.UNKNOWN_FORMAT)
			
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					// The input stream was not opened
				}
				finally {
					String url = this.getVTIPropertyNullable(PROP_URL);
					
					if (url == null) {
						url = "which GenericWS prefix is " + this.getPrefix();
					}
					
					logger.logWarning(GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
							"The data received from the web service " + url + 
							" seems to have a format which is neither " + PROP_DATA_FORMAT_VALUE_JSON +
							" nor " + PROP_DATA_FORMAT_VALUE_XML + ".\n" +
							"No data will be returned.");
					this.confirmSendingOfLastRecord();
				}
			}
			
		}
		
	}

	/**
	 * Checks if the array given as a parameter is the poison pill, 
	 * indicating there is no data to read anymore.
	 * 
	 * @param firstLine
	 * 			array which has to be compared to the poison pill.
	 * 
	 * @return true if firstLine is the poison pill, false otherwise.
	 */
	private boolean isPoisonPill(String[] firstLine) {
//		if (firstLine == null) {
//			return false;
//		}
//		
//		if (firstLine.length != POISON_PILL.length) {
//			return false;
//		}
//		
//		for (int i = 0; i < firstLine.length; i++) {
//			if (firstLine[i] != POISON_PILL[i]) {
//				return false;
//			}
//		}
//		
//		return true;
		return firstLine == POISON_PILL;
	}
	
	/**
	 * Checks if the inputStream is an instance of HTMLFilterInputStream, and 
	 * if so sets the tags to be ignored in this stream with the values found 
	 * in the config file. 
	 * <p>
	 * The tags which have to be removed from the filter (only the tags having 
	 * the names given in the file) are given in the config file with the property 
	 * GenericWS.mySource.ELT_TO_RMV=&#60;nameTag1&#62;,&#60;nameTag2&#62;
	 * file. 
	 * <p>
	 * The tags which have to be removed from the filter with all their content 
	 * (start element + value + internal tags + end element) are given in the 
	 * config file with the property 
	 * GenericWS.mySource.ELT_CONTENT_TO_RMV=&#60;nameTag1&#62;,&#60;nameTag2&#62;
	 * 
	 * @param inputStream 
	 * 			The inputStream to filter.
	 * 
	 * @return The inputStream on which the tag read in the config file are being 
	 * removed.
	 */
	 // FYI: In HTML &#60; = '<' and &#62; = '>'
	private InputStream filterInputStream(InputStream inputStream) {
		
		if (inputStream instanceof HTMLFilterInputStream) {
			String[] tagContents2Rmv = this.getTagsToRemove(
											GenericWS.PROP_ELEMENT_CONTENTS_TO_REMOVE);
			((HTMLFilterInputStream)inputStream).setTagContentsToRemove(tagContents2Rmv);
			
			String[] tags2Rmv = this.getTagsToRemove(
											GenericWS.PROP_ELEMENTS_TO_REMOVE);
			((HTMLFilterInputStream)inputStream).setTagsToRemove(tags2Rmv);
		}
//		InputSource is = new InputSource(new InputStreamReader(inputStream,"UTF-8"));
//		is.setEncoding("UTF-8");
		
		return inputStream;
	}
	
	/**
	 * <p>
	 * Parses a list  of tags to remove (read from the config file) into an
	 * array of String.
	 * <p>
	 * The String "&#60;nameTag1&#62;,&#60;nameTag2&#62;" will be parsed 
	 * into the array ["nameTag1", "nameTag2"].
	 * 
	 * @param property
	 * 			The propety's content to parse.
	 * 
	 * @return An array containing the different Tag's names which will have
	 * to be removed from a HtmlFilterInputStream object later on. 
	 */
	 // FYI: In HTML &#60; = '<' and &#62; = '>'
	private String[] getTagsToRemove(String property) {
		String propsLine = this.getVTIPropertyNullable(property);
		
		// if nothing to read, returns empty array
		if (propsLine == null || propsLine.isEmpty()) {
			return new String[0];
		}
		
		// The different values are separated by ','
		String[] props = propsLine.split("\\s*,\\s*");
		
		// Generates tag names from the values "<tagName>" 
		// and store them into an arrayList 
		ArrayList<String> alProps = new ArrayList<String>();
		for (String prop : props) {
			Pattern p = Pattern.compile("^\\s*<\\s*([\\w-]*)\\s*>\\s*");
			Matcher m = p.matcher(prop);
			if (m.find()) {
				alProps.add(m.group(1));
			}
		}
		
		// Parses the Arraylist into an array (method .toArray() is not compiling)
		props = (String[])alProps.toArray(new String[0]);
		
		return props;
	}
	
	/**
	 * Checks if the property's content is the name of an existing file. If so
	 * returns the content of the file. Otherwise, returns the string given as 
	 * a parameter.
	 * 
	 * @param property
	 * 			The property's content to parse.
	 * 
	 * @return the content of the file if property is the name of an existing
	 * file. Returns the string given as a parameter otherwise.
	 * 
	 * @throws FileNotFoundException  if problems occur while reading the file.
	 */
	private String parseRestPostProperty(String property) throws FileNotFoundException {
		if (property == null || property.isEmpty()) return null;
		if ((new File(property)).exists()) {
			StringBuilder text = new StringBuilder();
		    
			String NL = System.getProperty("line.separator");
		    Scanner scanner = new Scanner(new FileInputStream(property));
		    while (scanner.hasNextLine()){
		    	text.append(scanner.nextLine() + NL);		
		    }
	    	scanner.close();
	    	property = text.toString();
		}
		return property;
	}
	
	/**
	 * Returns true if the value of the property, given as a parameter, 
	 * is "TRUE". Returns false otherwise. If the read value is neither
	 * "TRUE" nor "FALSE", a GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE
	 * message will be logged and false will be returned.
	 * @param property
	 * 			Name of the property to read and translate into a boolean.
	 * @return true if the value of the property, given as a parameter, 
	 * is "TRUE". Returns false otherwise.
	 */
	private boolean getBooleanProperty(String property) {
		try {
			boolean ret = false;
			String prop = getVTIPropertyNullable(property);
			
			if (prop != null && !prop.isEmpty()) {
				ret = Boolean.parseBoolean(prop);
			}
			return ret;
		}
		catch (Exception e) {
			logger.logWarning(GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE, 
					"The property " + CLASS + "." + this.getPrefix() +
					 "." + property + " has not been correctly given. The value " +
					"should be either TRUE or FALSE. The value is FALSE " +
					"by default");
			return false;
		}
	}
	
	/**
	 * Returns the integer value of the property given as a parameter. 
	 * Returns -1 if the value cannot be parsed into an integer.
	 * If the parameter optional is set at false and and that the value 
	 * cannot be parsed into an integer, prints a 
	 * GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE
	 * message into the log file.
	 * @param property
	 * 			Name of the property to read and parse into a integer.
	 * @return the integer value of the property given as a parameter. 
	 * Returns -1 if the value cannot be parsed into an integer.
	 */
	private int getPositiveIntegerVTIProperty(String property) {
		String prop = null;
		try {
			int ret = -1;
			prop = getVTIPropertyNullable(property);
			if (prop != null && !prop.isEmpty()) {
				ret = Integer.parseInt(prop);
			}
			return ret;
		}
		catch (Exception e) {
			logger.logException(GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE, 
					"The property " + CLASS + "." + this.getPrefix() +
					 "." + property + " has not been correctly given. The value " +
					"should be parsable into a boolean. The value is 0 " +
					"by default",
					e);
			return -1;
		}
	}
	
}
