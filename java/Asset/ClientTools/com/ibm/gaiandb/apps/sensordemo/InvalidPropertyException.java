/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensordemo;

/**
 * Encapsulates the standard <code>Exception</code> for the sole purpose of
 * separating invalid properties from other exceptions.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class InvalidPropertyException extends Exception {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = -2623977174422060560L;

	public InvalidPropertyException() {	}

	public InvalidPropertyException(String message) {
		super(message);
	}

	public InvalidPropertyException(Throwable cause) {
		super(cause);
	}

	public InvalidPropertyException(String message, Throwable cause) {
		super(message, cause);
	}
}
