/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.clientseeker;

/**
 * Exception class thrown when Fabric resources (Fabric Managers, Registries) are
 * not discovered.
 * 
 * @author DavidBarker
 *
 */
public class ResourceNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";	

	/**
	 * Default constructor.
	 */
	public ResourceNotFoundException() {
	}

	/**
	 * Constructor.
	 * 
	 * @param message - the error message to include.
	 */
	public ResourceNotFoundException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructor.
	 * 
	 * @param cause - the root cause exception to include.
	 */
	public ResourceNotFoundException(Throwable cause) {
		super(cause);
	}

	/**
	 * Constructor.
	 * 
	 * @param message - the error message to include.
	 * @param cause - the root cause exception to include.
	 */
	public ResourceNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}
}
