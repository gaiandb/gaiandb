/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser;

/**
 * Exception thrown when a GenericWS property is not in the right format.
 * 
 * @author remi - IBM Hursley
 *
 */
public class NonParsableStringException extends Exception {
	
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

	/** From superclass. */
	private static final long serialVersionUID = -1214351041385393168L;
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	/** 
	 * Type of the exception. has to be a value in GDBMessages.DSWRAPPER_GENERICWS_*.
	 */
	private String type;

	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/** 
	 * Creates a new NonParsableStringException object.
	 * 
	 * @param type
	 * 			Type of the exception. has to be a value in 
	 * GDBMessages.DSWRAPPER_GENERICWS_*.
	 * @param msg
	 * 			Message presented by the Exception.
	 */
	
	public NonParsableStringException(String type, String msg) {
		super(msg);
		this.type = type;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Returns the type of the exception.
	 * @return the type of the exception.
	 */
	public String getType() {
		return this.type;
	}

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
