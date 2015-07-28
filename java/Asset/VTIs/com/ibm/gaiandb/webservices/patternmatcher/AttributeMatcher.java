/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.patternmatcher;

import java.util.ArrayList;

//import com.ibm.gaiandb.webservices.XmlElement;
//import com.ibm.gaiandb.webservices.parser.NonParsableStringException;
//import com.ibm.gaiandb.webservices.parser.properties.GenericWsPropertiesParser;
import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * The purpose of this class is to provide a TagMatcher able to check and
 * select one specific attribute, in a sequence of tags.
 * 
 * @author remi - IBM Hursley
 *
 */
public class AttributeMatcher extends TagMatcher {

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
	
	/** The attribute the matcher will look for. */
	private final String attributeToMatch;
	
	/** The attribute's value the matcher will look for. */
	private String valueToMatch = null;
	
	/** The level of the tag which attribute has to be matched. */
	private final int matchingDepth;

	private boolean valueFound = false;
	
	
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
	 * 			The name of the attribute the object will match the value of.
	 * @param matchingLevel
	 * 			The level of the attribute which has to be matched. Zero based.
	 */
	public AttributeMatcher(ArrayList<TagPattern> tags, 
			String attributeToMatch, 
			int matchingLevel) {
		
		super(tags);
		this.lastMatchedDepth = -1;
		this.currentDepth = 0;
		this.attributeToMatch = attributeToMatch;
		this.matchingDepth = matchingLevel;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	
	public String getAttributeToMatch() {
		return attributeToMatch;
	}


	public int getMatchingDepth() {
		return matchingDepth;
	}
	
	@Override
	public String getIn(Tag openingTag) {
		if (this.isCurrentlyMatching && this.currentDepth < this.patternToMatch.size()) {
			
			// Gets the tag the matcher wants to see (regarding to the current depth)
			TagPattern tagMeantToBeMatched = this.patternToMatch.get(this.currentDepth);
			
			// Checks if the current tag matches with the one from the pattern
			if (tagMeantToBeMatched.isMatching(openingTag)) {
				
				// Saves that it matches
				this.lastMatchedDepth = this.currentDepth;
				
				// Checks if the level currently read is the one the requested value 
				// is (or can be) located in
				if (this.currentDepth == this.matchingDepth
						&& !this.valueFound) {
					
					this.valueToMatch = openingTag.getAttributesValue(this.attributeToMatch);
					this.valueFound  = true;
					
					if (this.valueToMatch == null) {
						// the attribute doesn't exist for this tag. 
						// That's annoying, but no value will be returned
					}
				}
				
				// Checks if all the pattern has been fully matched
				if (this.currentDepth == (this.patternToMatch.size() - 1) && this.valueFound) {
					// -1 because the first depth index is 0, and the tag stored at this
					// level will represent 1 tag, so nbTags's value will be 1
					// The -1 resolves this problem
					
					// passes in the next level
					this.currentDepth++;
					this.isCurrentlyMatching = false;
					this.valueFound = false;
					String ret = null;
					
					if (this.valueToMatch != null) {
						ret = new String(this.valueToMatch);
						this.valueToMatch = null;		
					}
							
					// All the pattern is matched
					return ret;
					
				}
			}
			else {
				this.isCurrentlyMatching = false;
			}
		}
			
		// passes in the next level
		this.currentDepth++;		
		return null;
	}

	/** 
	 * Returns null.
	 * @return null. 
	 */
	@Override
	public String getValue(String value) {
		return null;
	}
	
	@Override
	public String getOut(String closingTagName) {
		this.currentDepth--;
		if (this.currentDepth <= this.lastMatchedDepth + 1) {
			this.isCurrentlyMatching = true;
		}
		if (this.currentDepth <= this.lastMatchedDepth) {
			this.lastMatchedDepth = this.currentDepth - 1;
		}
		return null;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	
}
