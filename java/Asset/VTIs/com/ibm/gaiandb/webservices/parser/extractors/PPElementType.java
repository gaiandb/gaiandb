/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.extractors;

/**
 * The different elements which can be extracted from a tag pattern in the 
 * gaian tree routing language.
 * 
 * @author remi - IBM Hursley
 *
 */
public enum PPElementType {

	// ----------------------------------------------------------------------------------
	// --------------------------------------------------------------------------- VALUES

	// =========================================================================== Public
	
	ATTRIBUTE,
	
	ATTRIBUTE_TO_FIND,
	
	POSITION,
	
	NAME, 
	
	NONE;

}
