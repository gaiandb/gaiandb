/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.patternmatcher;

import java.util.ArrayList;

import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * The purpose of this class is to provide a generic object, matching tags while 
 * reading a formated document.
 * 
 * @author remi - IBM Hursley
 *
 */
public abstract class TagMatcher {

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
	
	/** 
	 * The sequence of Tags the TagMatcher will try to look for when scanning 
	 * a formated file . 
	 */
	protected ArrayList<TagPattern> patternToMatch = null;
	
	/** The depth of tag which is currently being matched. Zero based. */
	protected int currentDepth = 0;
	
	/** The depth of tag the TagMatcher has lost the matching. */
	protected int lastMatchedDepth = -1;
	
	/** If it is still matching the Tags */
	protected boolean isCurrentlyMatching = true;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * Creates a TagMatcher object, which pattern to match is given as a parameter.
	 * 
	 * @param tags
	 * 			Pattern that the object will match.
	 */
	protected TagMatcher(ArrayList<TagPattern> tags) {
		this.patternToMatch = tags;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Returns the TagPattern list defining the object.
	 * @return the TagPattern list defining the object.
	 */
	public ArrayList<TagPattern> getPatternSequence() {
		return this.patternToMatch;
	}
	
	/** 
	 * Behaviour of the TagMatcher when it gets into an opening tag.
	 * @return The value which had to be matched. null if not found.
	 */
	public abstract String getIn(Tag openingTag);

	/** 
	 * Behaviour of the TagMatcher when it gets into an closing tag.
	 * @return The value which had to be matched. null if not found.
	 */
	public abstract String getOut(String closingTagName);

	/** 
	 * Behaviour of the TagMatcher when it gets the value located between 
	 * two tags. 
	 * @return The value which had to be matched. null if not found.
	 */
	public abstract String getValue(String value);
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
