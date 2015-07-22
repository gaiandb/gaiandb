/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser;

import com.ibm.gaiandb.webservices.parser.properties.AttributeComparator;

/**
 * <p>
 * This class represents an attribute definition in which one the value 
 * doesn't have to be affected to the attriute. Indeed, the comparator 
 * can be chosen and thus, an object can represent an affectation ('=')
 * or a comparison ('<', '>', '!='...) or any other relation defined as
 * a {@link AttributeComparator} object.
 * <p>
 * The generated objects cannot be modified, they  are built on read-only.
 * 
 * @author remi - IBM Hursley
 * 
 * @see {@link AttributeComparator}
 */
public class AttributeDefinition {

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
	
	/** The name of the attribute in this declaration. */
	private final String attribute;
	
	/** The operation applied to the attribute in this declaration. */
	private final AttributeComparator comparator;
	
	/** The value the attribute is compared to in this definition. */
	private final String value;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/** 
	 * Creates a AttributeDefinition object. 
	 * @param attribute
	 * 			The name of the attribute in this declaration.
	 * @param comparator
	 * 			The operation applied to the attribute in this declaration.
	 * @param value
	 * 			The value the attribute is compared to in this definition.
	 */
	public AttributeDefinition(String attribute,
			AttributeComparator comparator, String value) {
		super();
		this.attribute = attribute;
		this.comparator = comparator;
		this.value = value;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * Returns the attribute's name of the attribute definition.
	 * @return the attribute's name of the attribute definition.
	 */
	public String getAttribute() {
		return attribute;
	}

	/**
	 * Returns the comparator of the attribute definition.
	 * @return the comparator of the attribute definition.
	 */
	public AttributeComparator getComparator() {
		return comparator;
	}

	/**
	 * Returns the value the attribute is compared to.
	 * @return the value the attribute is compared to.
	 */
	public String getValue() {
		return value;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
