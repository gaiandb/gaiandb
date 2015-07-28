/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner.json;

import java.util.ArrayList;

import org.xml.sax.Attributes;

import com.ibm.gaiandb.utils.Pair;

/**
 * <p>
 * This class is an implementation of the interface {@link Attributes}. It provides 
 * class which is used when parsing Json files using the
 * <a href="file:///C:/Users/IBM_ADMIN/Documents/Workspace/Utils/Juno/juno-javadocs-5.0.0.30/index.html">
 * juno</a> api.
 * <p>
 * Only the methods getLength(), getQName(int), getType(int) et getValue(int) are
 * implemented. The others are auto-generated and only returning null or 0;
 * 
 * @author remi - IBM Hursley
 *
 */
public class JsonAttributes implements Attributes {

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
	 * The different attributes stored in the object. The first value of the pair is
	 * the name of the attribute, the second one is its value. It can be an Integer,
	 * a String, a Long, an ObjectMap, an ObjectList...
	 */
	private ArrayList<Pair<String, Object>> attributes = 
									new ArrayList<Pair<String, Object>>();
	
	
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
	 * Adds an attribute and its value the the object.
	 * @param qName
	 * 			The name of the attribute to add.
	 * @param value
	 * 			The value of the attribute. It can be an Integer, a String, 
	 * a Long, an ObjectMap, an ObjectList...
	 */
	public void addAttribute(String qName, Object value) {
		this.attributes.add(new Pair<String, Object>(qName, value));
	}

	/** Not implemented. */
	@Override
	public int getIndex(String qName) {
		// Auto-generated method stub
		return 0;
	}

	/** Not implemented. */
	@Override
	public int getIndex(String uri, String localName) {
		return 0;
	}

	/**
	 * Returns the number of attributes contained in the object.
	 * @return the number of attributes contained in the object.
	 */
	@Override
	public int getLength() {
		return this.attributes.size();
	}

	/** Not implemented. */
	@Override
	public String getLocalName(int index) {
		return null;
	}

	/**
	 * Returns the name of the attribute which index is given as a parameter.
	 * The index does not represent the order in which they appear in the parsed
	 * json document.
	 * @return the name of the attribute which index is given as a parameter.
	 */
	@Override
	public String getQName(int index) {
		return this.attributes.get(index).getFirst();
	}

	/**
	 * Returns the value's type of the attribute which index is given as a parameter.
	 * The index does not represent the order in which they appear in the parsed
	 * json document.
	 * @return the value's type of the attribute which index is given as a parameter.
	 */
	@Override
	public String getType(int index) {
		return this.attributes.get(index).getSecond().getClass().getName();
	}

	/** Not implemented. */
	@Override
	public String getType(String qName) {
		// Auto-generated method stub
		return null;
	}

	/** Not implemented. */
	@Override
	public String getType(String uri, String localName) {
		// Auto-generated method stub
		return null;
	}

	/** Not implemented. */
	@Override
	public String getURI(int index) {
		// Auto-generated method stub
		return null;
	}

	/**
	 * Returns the value of the attribute which index is given as a parameter.
	 * The index does not represent the order in which they appear in the parsed
	 * json document.
	 * @return the value of the attribute which index is given as a parameter.
	 */
	@Override
	public String getValue(int index) {
		return this.attributes.get(index).getSecond().toString();
	}

	/** Not implemented. */
	@Override
	public String getValue(String qName) {
		// Auto-generated method stub
		return null;
	}

	/** Not implemented. */
	@Override
	public String getValue(String uri, String localName) {
		// Auto-generated method stub
		return null;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
