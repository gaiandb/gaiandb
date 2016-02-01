/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.patternmatcher;

import java.util.ArrayList;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * The purpose of this class is to provide a TagMatcher able to check and
 * select a tag's value, base on a sequence of tags.
 * 
 * @author remi - IBM Hursley
 *
 */
public class ValueMatcher extends TagMatcher {

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
	
	private static final Logger logger = new Logger( "ValueMatcher", 50 );
	
	/** 
	 * True if the matcher has read all the right tags and can read the value. 
	 * False otherwise.
	 */
	private boolean canReadValue = false;
	
	/** Buffer saving the value which has to be read and returned by the object. */
	private StringBuilder valueToRead = new StringBuilder();
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public

	/**
	 * Creates a AttributeMatcher object.
	 * @param tags
	 * 			Pattern that the object will match.
	 * @param attributeToMatch
	 * 			The attribute the object will return
	 * @param matchingLevel
	 */
	public ValueMatcher(ArrayList<TagPattern> tags) {
		
		super(tags);
		this.currentDepth = 0;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	@Override
	public String getIn(Tag openingTag) {
		
		if (this.isCurrentlyMatching//) {  || this.currentDepth == this.lastMatchedDepth+1
				&& this.currentDepth < this.patternToMatch.size()) {// if (
			// Gets the tag the matcher wants to see
			TagPattern tagMeantToBeMatched = this.patternToMatch.get(this.currentDepth);
	
			// passes in the next level
			this.currentDepth++;
			
			// Checks if the current tag matches with the one from the pattern
			if (tagMeantToBeMatched.isMatching(openingTag)) {
				
				logger.logDetail("Matched tag: " + openingTag
						+ ", depth: " + this.currentDepth + ", patternDepth: " + this.patternToMatch.size());
				
				// Checks if all the pattern has been fully matched
				if (this.currentDepth == (this.patternToMatch.size())){
					// All the pattern is matched, the next read value can be considered 
					// as a value to read
					this.canReadValue  = true;
					this.valueToRead = new StringBuilder();
				}
				
				// If it is not fully matched, saves that it matches
				this.lastMatchedDepth = this.currentDepth;
			}
			else {
				this.isCurrentlyMatching = false;
				this.canReadValue = false;
			}
		}
		else {
			// passes in the next level
			this.currentDepth++;
			
			if (this.canReadValue) {
				this.valueToRead.append(openingTag.toString());
			}
		}
				
		return null;
	}

	@Override
	public String getValue(String value) {
		if (this.canReadValue) {
			this.valueToRead.append(value);
		}
		return null;
	}
	
	@Override
	public String getOut(String closingTagName) {
		
		String ret = null;
		if (this.currentDepth == this.lastMatchedDepth
				&& this.canReadValue) {
			this.canReadValue = false;
			
			ret = this.valueToRead.toString();
			this.valueToRead = new StringBuilder();
		}
		else {
			Tag closingTag = new Tag(closingTagName, Tag.END);
			
			logger.logDetail("Reached depth of patternToMatch. Extracting Tag Value: " + closingTag.toString());
			
			if (this.canReadValue) {
				this.valueToRead.append(closingTag.toString());
			}
		}
		
		this.currentDepth--;
		if (this.currentDepth <= this.lastMatchedDepth) {
			this.lastMatchedDepth = this.currentDepth;
			this.isCurrentlyMatching = true;
		}
		
		return ret;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
}
