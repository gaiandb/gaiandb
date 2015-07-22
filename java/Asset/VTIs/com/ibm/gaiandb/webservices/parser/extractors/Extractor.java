/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.extractors;

import com.ibm.gaiandb.webservices.parser.NonParsableStringException;

/**
 * The purpose of this class is to provide a pattern to extract elements 
 * from a String having a specific format. 
 * 
 * @author remi - IBM Hursley
 *
 */
public abstract class Extractor {

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
	 * Defines whether or not the string given as a parameter can be extracted.
	 * @return true if the element can be extracted, false otherwise.
	 */
	public abstract boolean canExtract(String element);
	
	/**
	 * Extract an object from the String given as a parameter.
	 * 
	 * @param element
	 * 			the String to parse.
	 * 
	 * @return an Object generated from the String given as a paramter.
	 * 
	 * @throws NonParsableStringException if the string given cannot be 
	 * parsed into an object. This exception can be avoided by using the 
	 * method canExtract(element) first:					<br/>
	 * 	if (myExtractor.canExtract(A_STRING)) {				<br/>&nbsp;&nbsp;&nbsp;&nbsp;
	 * 		Object o = myExtractor.extract(A_STRING);		<br/>
	 * 	}
	 * 
	 */
	public abstract Object extract(String element) throws NonParsableStringException;

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
