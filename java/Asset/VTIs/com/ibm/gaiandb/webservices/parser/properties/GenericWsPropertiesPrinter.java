/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.properties;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import static com.ibm.gaiandb.webservices.parser.extractors.PPElementExtractor.ATTRIBUTE_TO_FIND_MARKER;

import com.ibm.gaiandb.utils.Pair;
import com.ibm.gaiandb.webservices.patternmatcher.AttributeMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.TagMatcher;
import com.ibm.gaiandb.webservices.patternmatcher.TagPattern;
import com.ibm.gaiandb.webservices.patternmatcher.ValueMatcher;

/**
 * The purpose of this class is to print a Tag Matcher into a String.
 * 
 * @author remi - IBM Hursley
 *
 */
public class GenericWsPropertiesPrinter {

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

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Return the String value of the tagMatcher in the gaian tree routing language.
	 * 
	 * @param tagMatcher
	 * 			TagMatcher to print.
	 * 
	 * @return the String value of the tagMatcher in the gaian tree routing language.
	 */
	public String printTagMatcher(TagMatcher tagMatcher) {
		
		StringBuilder ret = new StringBuilder();
		
		if (tagMatcher instanceof ValueMatcher) {
			ValueMatcher myTagMatcher = (ValueMatcher)tagMatcher;
			ArrayList<TagPattern> pattern = myTagMatcher.getPatternSequence();
			
			if (pattern == null)
				return "";
			
			for (TagPattern tag : pattern) {
				// name of the tag
				ret.append('<').append(tag.getName());
				
				//attributes of the tag
				Map<String, Pair<AttributeComparator, String>> atts = tag.getAttributePattern();
				Set<String> attsKeys = atts.keySet();
				
				for (String attKey : attsKeys) {
					String opKey = atts.get(attKey).getFirst().getSymbol();
					String valKey = atts.get(attKey).getSecond();
					
					ret.append(' ').append(attKey)
									.append(opKey)
									.append('"').append(valKey).append('"');
				}
				
				// close the tag
				ret.append('>');
			}
		}
		else if (tagMatcher instanceof AttributeMatcher) {
			AttributeMatcher myTagMatcher = (AttributeMatcher)tagMatcher;
			ArrayList<TagPattern> pattern = myTagMatcher.getPatternSequence();
			
			if (pattern == null)
				return "";
			
			int depth = 0;
			
			for (TagPattern tag : pattern) {
				// name of the tag
				ret.append('<').append(tag.getName());
				
				//attributes of the tag
				Map<String, Pair<AttributeComparator, String>> atts = tag.getAttributePattern();
				Set<String> attsKeys = atts.keySet();
				
				for (String attKey : attsKeys) {
					String opKey = atts.get(attKey).getFirst().getSymbol();
					String valKey = atts.get(attKey).getSecond();
					
					ret.append(' ').append(attKey)
									.append(opKey)
									.append('"').append(valKey).append('"');
				}
				
				// Attribute to look for - in case
				if (depth == myTagMatcher.getMatchingDepth()) {
					ret.append(' ').append(myTagMatcher.getAttributeToMatch())
					.append('=')
					.append(ATTRIBUTE_TO_FIND_MARKER); // PPElementExtractor.ATTRIBUTE_TO_FIND_MARKER
				}
				
				// close the tag
				ret.append('>');
				depth++;
			}
		}
		return ret.toString();
	}

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
