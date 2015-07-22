/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner;

import java.util.HashMap;
import java.util.Set;

import org.xml.sax.Attributes;

/**
 * 
 * The purpose of this class is to provide a model of the tags which can be found 
 * in a XML document. They represent the tag itself <name attribute=value> but do 
 * not present either the content or the hierarchy of the tag. It does include the 
 * position index within the parent tag though.   
 * 
 * @author remi - IBM Hursley
 *
 */
public class Tag {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";
	
	
	/** Defines that a tag has no children registered. */
	public static final int NO_CHILD = -1;
	
	/** 
	 * Defines that the position index within the parent tag has 
	 * not been given for a tag. 
	 */
	public static final int NO_INDEX_POSITION = -1;
	
	/** Defines the type of an end tag. */
	public static final TagType END = TagType.END_TAG;
	
	
	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/** Name of the tag. */
	protected String name;

	/**
	 * Attributes of the tag. The key String is the name of the of the tag's attribute 
	 * and the value String is the value of this attribute. <br/>
	 * CAUTION: <b>This attribute is not supposed to have null values.</b>
	 */
	protected HashMap<String, String> attributes = null;

	/** Number of children which have been counted in the tag. NO_CHILD if not given.  */
	private int nbFoundChilren = Tag.NO_CHILD;
	
	/** Position index within the parent tag. NO_INDEX_POSITION if not given. */
	protected int indexPosition = Tag.NO_INDEX_POSITION;
	
	/** Type of the tag. TagType.START_TAG by default. */
	private TagType type = TagType.START_TAG;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	/** Defines the differnet kinds of tag which can be found. */
	private enum TagType { START_TAG, END_TAG, AUTO_CLOSING_TAG };
	
	
	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * Creates a Tag object. 
	 * @throws IllegalArgumentException if the parameter 'name' is null.
	 */
	public Tag(String name) {
		super();
		
		if (name == null) {
			throw new IllegalArgumentException("the name of the Tag object " +
					"cannot be set up at null.");
		}
		
		this.name = name;
	}
	
	public Tag(String name, HashMap<String, String> attributes) {
		this(name);
		this.attributes = attributes;
		this.nbFoundChilren = Tag.NO_CHILD;
	}
	
	public Tag(String name, TagType type) {
		this(name, (HashMap<String, String>)null);
		this.type = type;
	}
	
	/**
	 * Creates a Tag object. 
	 * @throws IllegalArgumentException if the parameter 'name' is null.
	 */
	public Tag(String name, HashMap<String, String> attributes, int indexPosition) {
		this(name, attributes);
		this.indexPosition = indexPosition;
	}
	
	public Tag(String name, Attributes attr) {
		this.name = name;
		this.attributes = new HashMap<String, String>();
		if (attr != null) {
			int nbAttributes = attr.getLength();
			for (int i = 0; i < nbAttributes; i++) {
				String type = attr.getQName(i);
				String value = attr.getValue(i);
				this.attributes.put(type, value);
			}
		}
	}
	
	public Tag(String name, Attributes attr, int indexPosition) {
		this(name, attr);
		this.indexPosition = indexPosition;
	}
	
	
	// -------------------------------------------------------------------------- Private
	
	
	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Returns the name of the tag.
	 * @return the name of the tag.
	 */
	public String getName() {
		return this.name;
	}
	
	/**
	 * Returns the attributes of the tag. Can return null if 
	 * the attributes have not been initiated.
	 * @return the attributes of the tag. Returns null if the 
	 * attributes have not been initiated.
	 */
	public HashMap<String, String> getAttributes() {
		return this.attributes;
	}
	
	/**
	 * Returns the index position of the tag.
	 * @return the index position of the tag.
	 */
	public int getPosition() {
		return this.indexPosition;
	}

	/**
	 * Returns the number of children which have been counted so far. This number 
	 * might increase with the scan of a xml file. Returns NO_CHILD if no children
	 * have been added. 
	 * 
	 * @return  the number of children which have been counted so far. NO_CHILD if 
	 * no children have been added. 
	 */
	public int getLastChildIndex() {
		return this.nbFoundChilren;
	}
	
	/** Increases the  number of tag children found for the current tag. */ 
	public void addChild() {
		this.nbFoundChilren++;
	}
	
	/**
	 * Returns the value of the tag's attribute, given as a parameter. null if the 
	 * attribute doesn't exist.
	 * 
	 * @param attributeToMatch
	 * 			The attribute which value is requested.
	 * 
	 * @return the value of the tag's attribute, given as a parameter. null if the 
	 * attribute doesn't exist.
	 */
	public String getAttributesValue(String attributeToMatch) {
		return this.attributes.get(attributeToMatch);
	}

	@Override
	public int hashCode() {
		StringBuilder build = new StringBuilder(name);
		
		Set<String> attrs = this.attributes.keySet();
		for (String attr : attrs) {
			build.append(attr);
		}
		
		return build.toString().hashCode();
	}
	
	/**
	 * Prints the current tag depending of its type attribute value.
	 */
	@Override
	public String toString() {
		
		StringBuilder ret = new StringBuilder("<");
		
		if (this.type == TagType.END_TAG) { ret.append('/'); }
		
		ret.append(this.name);
		
		if (this.type == TagType.END_TAG) { ret.append('>'); return ret.toString(); }
		
		if (this.attributes != null) {
			Set<String> attr = this.attributes.keySet();
			for (String att : attr) {
				ret.append(" " + att + "=\"" + this.attributes.get(att) + "\"");
			}
		}
		
		if (this.type == TagType.AUTO_CLOSING_TAG) {
			ret.append(" /");
		}
		
		ret.append('>');
		return ret.toString();
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
