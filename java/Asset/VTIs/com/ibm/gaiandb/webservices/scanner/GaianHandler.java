/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner;

import java.io.IOException;
import java.util.ArrayList;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.db2j.GenericWS;
//import com.ibm.gaiandb.diags.GDBMessages;

import static com.ibm.gaiandb.webservices.patternmatcher.MatcherManager.GOT_RESULT;

/**
 * <p>
 * The purpose of this class is to define a handler which will be used for scanning
 * and parsing XML documents when using a sax parser.
 * 
 * <p>
 * It can only be used by an GenericWS object.
 *
 * <p>
 * See {@link com.ibm.db2j.GenericWS}.
 * 
 * @author remi - IBM Hursley
 */
public class GaianHandler extends DefaultHandler {

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
	
	/** When the depth of a tag is not defined yet. */
	private static final int DEPTH_NOT_DEFINED = -1;
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	/** The GenericWS object calling the SaxParser that the current object handles. */
	private GenericWS caller;
	
	/** The depth (tag talking) the scanner is currently scanning. */
	private int depth = DEPTH_NOT_DEFINED;
	
	/** The sequence of tags the object has visited. */
	private ArrayList<Tag> visitedTags = new ArrayList<Tag>();
	private int rootPosition = -1;
	private boolean parsingIsOver;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
		
	/**
	 * Creates a new GaianHandler object.
	 * @param caller
	 * 			The GenericWS object the current object will have to work for.
	 */
	public GaianHandler(GenericWS caller) {
		this.caller = caller;
		this.parsingIsOver = false;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	public boolean hasFinishedParsing() {
		return this.parsingIsOver;
	}
	
	/**
	 * @Override
	 * <p>
	 * Overriding the DefaultHandler.startElement(...) method.
	 * <p>
	 * When finds a start tag in the xml file, warns the caller of the saxParser
	 * that it gets in a XML element.
	 */
	@Override
	public void startElement(String uri, String localName,String qName, 
            Attributes attributes) throws SAXException {
		this.depth++;
		// Saves the current position of the scanner
		int indexPosition = 0;
		if (this.depth > 0) {
			this.visitedTags.get(this.depth - 1).addChild();
			indexPosition = this.visitedTags.get(this.depth - 1).getLastChildIndex();
		}
		else {
			this.rootPosition++;
			indexPosition = this.rootPosition;
		}
		
		Tag tag = new Tag(qName, attributes, indexPosition);
		this.visitedTags.add(tag);
		this.caller.getColumnsPropertiesManager().getIn(tag);
	}
			
	/**
	 * @Override
	 * <p>
	 * Overriding the DefaultHandler.endElement(...) method.
	 * <p>
	 * When finds an end tag in the xml file, warns the caller of the saxParser
	 * that it gets out of a XML element. And checks if the caller has to save the 
	 * record which is currently saved. 
	 */
	@Override
	public void endElement(String uri, String localName,
		String qName) throws SAXException {

		// -------------- call matcher manager --------------
		String sentData = this.caller.getColumnsPropertiesManager().getOut(qName);
		this.depth--;
		
		// -------------- saves record --------------
		// Finally checks if the current record has to be committed in the
		// records array...
//		if (this.caller.isTagForSendingData(qName)) {
		if (sentData != null
				&& sentData.equals(GOT_RESULT)) { //MatcherManager.GOT_RESULT
			
			this.caller.saveCurrentRecord();
			
		}
		
		this.visitedTags.remove(this.visitedTags.size()-1);
	}
 
	/**
	 * @Override
	 * <p>
	 * Overriding the DefaultHandler.characters(...) method.
	 * <p>
	 * When finds the value of a tag in the xml file, warns the caller of
	 * the saxParser that it found the content of a XML element.
	 */
	@Override
	public void characters(char ch[], int start, int length) 
												throws SAXException {
		//Gets the value to insert in the VTI
		String value = new String(ch, start, length);
		
		this.caller.getColumnsPropertiesManager().getValue(value);
		
	}
	
	


	@Override
	public void endPrefixMapping(String prefix) throws SAXException {
		// Auto-generated method stub
		super.endPrefixMapping(prefix);
	}


	@Override
	public void error(SAXParseException e) throws SAXException {
//		this.caller.logException(
//				GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
//				"A fatal error occurred during the reading of the data sent by the web service.\n" +
//				"Check the data are in the right format (" + GenericWS.PROP_DATA_FORMAT_VALUE_JSON +
//				" or " + GenericWS.PROP_DATA_FORMAT_VALUE_XML + ").",
//				e);
//		this.caller.confirmSendingOfLastRecord();
	}


	@Override
	public void fatalError(SAXParseException e) throws SAXException {
//		this.caller.logException(
//				GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
//				"A fatal error occurred while reading the data sent by the web service.\n" +
//				"Check the data are in the right format (" + GenericWS.PROP_DATA_FORMAT_VALUE_JSON +
//				" or " + GenericWS.PROP_DATA_FORMAT_VALUE_XML + ").",
//				e);
//		this.caller.confirmSendingOfLastRecord();
	}


	@Override
	public void processingInstruction(String target, String data)
			throws SAXException {
		// Auto-generated method stub
		super.processingInstruction(target, data);
	}


	@Override
	public InputSource resolveEntity(String publicId, String systemId)
			throws IOException, SAXException {
		// Auto-generated method stub
		return super.resolveEntity(publicId, systemId);
	}


	@Override
	public void setDocumentLocator(Locator locator) {
		// Auto-generated method stub
		super.setDocumentLocator(locator);
	}


	@Override
	public void skippedEntity(String name) throws SAXException {
		// Auto-generated method stub
		super.skippedEntity(name);
	}


	@Override
	public void startPrefixMapping(String prefix, String uri)
			throws SAXException {
		// Auto-generated method stub
		super.startPrefixMapping(prefix, uri);
	}


	@Override
	public void warning(SAXParseException e) throws SAXException {
//		String errMsg = "";
//		if (e != null) {
//			errMsg = "It came with the error:\n" + e.getMessage();
//		}
//		this.caller.logWarning(
//				GDBMessages.DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA, 
//				"An error occured while reading the data sent by the web service.\n" +
//				errMsg);
	}


	@Override
	public void endDocument() throws SAXException {
		this.parsingIsOver = true;
	}


	@Override
	public void ignorableWhitespace(char[] ch, int start, int length)
			throws SAXException {
		// Auto-generated method stub
//		super.ignorableWhitespace(ch, start, length);
	}


	@Override
	public void notationDecl(String name, String publicId, String systemId)
			throws SAXException {
		// Auto-generated method stub
		super.notationDecl(name, publicId, systemId);
	}


	@Override
	public void startDocument() throws SAXException {
		// Auto-generated method stub
		super.startDocument();
	}


	@Override
	public void unparsedEntityDecl(String name, String publicId,
			String systemId, String notationName) throws SAXException {
		// Auto-generated method stub
		super.unparsedEntityDecl(name, publicId, systemId, notationName);
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
}
