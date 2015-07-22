/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.gaiandb.webservices.patternmatcher.AttributeMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.TagMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.TagPattern;
import com.ibm.gaiandb.webservices.patternmatcher.ValueMatcher;
import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * The purpose of this class is to provide a tool understanding the name of the 
 * read tags in order to generate logical table based on the read data.
 * 
 * @author remi - IBM Hursley
 *
 */
public class LogicalTableGeneratorHandler extends DefaultHandler {

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
	
	private static final String IGNORE_CLOSED_TAG = "&&&"; // forbidden char in XML
	
	
	// -------------------------------------------------------------------------- Dynamic

	private ArrayList<TagPattern> scannedElements = new ArrayList<TagPattern>();
	
	/** 
	 * The TagMatchers which are gonna be generated while scanning the stream. 
	 * They will then be used as matchers in the MatcherManager 
	 * {@link generatedMatcher}.
	 */
	private ArrayList<TagMatcher> matchersValueMatchers = new ArrayList<TagMatcher>();
	private ArrayList<String> columnNamesValueMatching = new ArrayList<String>();
	
	private ArrayList<TagMatcher> matchersAttributeMatchers = new ArrayList<TagMatcher>();
	private ArrayList<String> columnNamesAttributeMatching = new ArrayList<String>();
	
	private Tag lastStartTag = null;
	private String lastClosedTag = IGNORE_CLOSED_TAG;
	
	/** Once the stream has been parsed, the class ignore the other read values. */
	private boolean ignore = false;
	
	/** Confirm the generation of the logical table is completed. */
	private Semaphore sem = new Semaphore(0);

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	public LogicalTableGeneratorHandler() {
	}
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	public ArrayList<TagMatcher> getValueMatchers() {
		return matchersValueMatchers;
	}

	public String[] getColumnNamesValueMatching() {
		return columnNamesValueMatching.toArray(new String[0]);
	}
			
	public ArrayList<TagMatcher> getAttributeMatchers() {
		return matchersAttributeMatchers;
	}

	public String[] getColumnNamesAttributeMatching() {
		return columnNamesAttributeMatching.toArray(new String[0]);
	}

	public Semaphore getSemaphoreMatchingComplete() {
		return sem;
	}

	@Override
	public void startElement(String uri, String localName,String qName, 
            Attributes attributes) throws SAXException {
		// If the logical table definition is still not over
		if (!this.ignore) {
			if (qName.equalsIgnoreCase(this.lastClosedTag)) {
				// The logical table definition is done
//				this.objectToReturnName = qName;
				this.ignore = true;
				this.sem.release();
			}
			else {
				// still need to define logical table
				TagPattern currentTag = new TagPattern(qName);
				this.scannedElements.add(currentTag);
				
				this.lastClosedTag = IGNORE_CLOSED_TAG;
			}
			this.lastStartTag = new Tag(qName, attributes);
		}
	}

	@Override
	public void endElement(String uri, String localName,
		String qName) throws SAXException {
		if(!this.ignore) {
			if (IGNORE_CLOSED_TAG.equals(this.lastClosedTag)) { // /!\ Check on the object, not on the value
				// The last one was a startElement
				// => We are at the end of a branch
				
				// --- Saves the tag pattern ---
				// - The value pattern
				// Column values
				this.columnNamesValueMatching.add(qName);
				// Adds the value matcher for this end of branch
				ArrayList<TagPattern> patterns = new ArrayList<TagPattern>(this.scannedElements);
				this.matchersValueMatchers.add(new ValueMatcher(patterns));
				
				// - The Attribute pattern
				// For each attribute of the last start element visited
				HashMap<String, String> tagsAttributes = this.lastStartTag.getAttributes();
				if (tagsAttributes != null) {
					Set<String> attributesNamingColumns = tagsAttributes.keySet();
					
					for (String attributeNamingColumn : attributesNamingColumns) {
						// Column values
						this.columnNamesAttributeMatching.add(attributeNamingColumn);
						
						// Adds the attribute matcher
						ArrayList<TagPattern> attributePatterns = 
									new ArrayList<TagPattern>(this.scannedElements);
						this.matchersAttributeMatchers.add(
								new AttributeMatcher(attributePatterns,
													attributeNamingColumn,
													attributePatterns.size() - 1
								)
						);
					}	
				}
			}
			this.lastClosedTag = qName;
			
			int nbElements = this.scannedElements.size();
			this.scannedElements.remove(nbElements-1); // removes the last element
			
			if (nbElements == 1) {
				// We are currently closing the root
//				this.objectToReturnName = qName;
				this.sem.release();
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
