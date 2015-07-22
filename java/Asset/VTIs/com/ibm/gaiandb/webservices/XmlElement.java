/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices;

/**
 * Names different elements found in the XML definition. 
 * 
 * @author remi - IBM Hursley
 *
 */
public enum XmlElement {

	// ----------------------------------------------------------------------------------
	// --------------------------------------------------------------------------- VALUES

	// =========================================================================== Public
	
	/** Value found between an opening tag and a closing one. */
	VALUE,
	
	/** Attribute found in a tag. */
	TAG_ATTIBUTE,
	
//	/** Name found in a tag. */
//	TAG_NAME,
	
	/** If it cannot be defined as an XML element. */
	ERROR_TAG, 
	
	/** If it has not been defined yet. */
	UNDEFINED;
}
