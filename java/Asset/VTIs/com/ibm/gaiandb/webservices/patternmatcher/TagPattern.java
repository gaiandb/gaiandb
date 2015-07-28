/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.patternmatcher;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

//import org.xml.sax.Attributes;

import com.ibm.gaiandb.utils.Pair;
import com.ibm.gaiandb.webservices.parser.AttributeDefinition;
import com.ibm.gaiandb.webservices.parser.properties.AttributeComparator;
import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * The purpose of this class is to define a pattern that the 
 * {@link com.ibm.gaiandb.webservices.scanner.Tag} objects can match when 
 * executing queries in the configuration file of GAIANDB. This tool 
 * will be used for defining whether the tag met in a xml file are 
 * matching the model given in the config file.  
 * 
 * @author remi - IBM Hursley
 *
 */
public class TagPattern extends Tag {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";
	
	
	/** Tag which is always equal to any other Tags. */
	public static final TagPattern JOCKER = new TagPattern();
	
	/** The "name" of the JOCKER tag. */
	public static final String JOCKER_NAME = "*";
	
	
	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/** To know if the Tag is a jocker or not. */
//	protected boolean isJocker = false;
	
	/** 
	 * <p>
	 * Hashmap defining the attributes found in the tag. 
	 * <p>
	 * The map key is the name of the attributes. The map value is a pair, which 
	 * first element is the operation written for the attribute, and the second
	 * is the value that the attribute has to be compared with.  
	 */
	private HashMap<String, Pair<AttributeComparator, String>> attributePattern = null;

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/** 
	 * Creates a TagPattern object.
	 * @param name 
	 * 			The name of the tag.
	 */
	public TagPattern(String name) {
		super(name, (HashMap<String, String>)null);
		this.attributePattern = new HashMap<String, Pair<AttributeComparator, String>>();
	}
	
	public TagPattern(String name, int position) {
		this(name);
		this.indexPosition = position;
	}
	
	
	// -------------------------------------------------------------------------- Private
	
	/**
	 * Generates a Jocker Tag. A tag which is always equals to any other tags.
	 */
	private TagPattern() {
		super(JOCKER_NAME, (HashMap<String,String>)null, Tag.NO_INDEX_POSITION);
//		this.isJocker = true;
	}
	
	
	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Adds an attribute in the current tag.
	 * 
	 * @param atteibute
	 * 			The name of the attribute to be inserted.
	 * @param value
	 * 			The value of the attribute to be inserted. 
	 */
	public void addAttribute(AttributeDefinition attribute){
		this.attributePattern.put(attribute.getAttribute(), 
				new Pair<AttributeComparator, String>(attribute.getComparator(), attribute.getValue()));
	}
	
	/**
	 * <p>
	 * Checks if a Tag matches the current TagPattern object. the current object has
	 * to be a subset of the tag given in parameter. It takes in consideration the name
	 * of the tag, the attributes names and values, the position if has been set in the
	 * current object.
	 * <p>
	 * The tagPattern &#60;div class="test"&#62; is matching the tag 
	 * &#60;div class="test" id="123"&#62; but not the tag &#60;span class=test"&#62;.
	 */
	 // FYI: In HTML &#60; = '<' and &#62; = '>'
	public boolean isMatching(Tag tag) {
		
//		if (this.isJocker) {
//			return true; 
//		}
		
		if (!this.name.equals(TagPattern.JOCKER_NAME)
				&& !this.name.equals(tag.getName())) {
			return false;
		}
		
		if (this.indexPosition != Tag.NO_INDEX_POSITION
				&& this.indexPosition != tag.getPosition()) {
			return false;
		}
		
		// if the attributes are empty or null
		if ((this.attributePattern == null || this.attributePattern.size() == 0)) {
			return true;
		}
		
		Set<String>attrs = this.attributePattern.keySet();
		for (String attr : attrs) {
			Pair<AttributeComparator, String> val = this.attributePattern.get(attr);
			if (val != null) {
				AttributeComparator comp = val.getFirst();
				String value = tag.getAttributes().get(attr);
				String reference = val.getSecond();
				if (value == null || !comp.validates(value, reference)) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	public Map<String, Pair<AttributeComparator, String>> getAttributePattern() {
		return attributePattern;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
