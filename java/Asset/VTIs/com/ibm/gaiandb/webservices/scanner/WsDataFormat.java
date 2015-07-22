/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner;

/**
 * Defines the format of the data which can be received using the Web services.
 * It currently support XML and JSON.
 * 
 * @author remi - IBM Hursley
 *
 */
public enum WsDataFormat {

	// ----------------------------------------------------------------------------------
	// --------------------------------------------------------------------------- VALUES

	// =========================================================================== Public
	JSON,
	XML,
	UNKNOWN_FORMAT;
}
