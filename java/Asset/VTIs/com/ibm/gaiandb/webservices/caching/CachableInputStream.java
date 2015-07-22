/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.caching;

import java.io.IOException;
import java.io.InputStream;

/**
 * The purpose of this class is to provide an InputStream which can be cached when 
 * being executed. The cache will be done in memory. When read, the stream will also 
 * copy its values into a StringCacher in order to get the value back when calling it 
 * again.
 * 
 * @author remi - IBM Hursley
 *
 */
public class CachableInputStream extends InputStream {

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
	
	/** The InputStream to cache. */
	private InputStream is;
	
	/** 
	 * The String builder which will be used as a stamp for loading the values 
	 * of the stream into the memory.
	 */
	private StringBuilder builder;
	
	/** The cacher managing the expiration of the cached value. */
	private StringCacher cacher; 

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	public CachableInputStream(InputStream is, int timeOut) {
		super();
		this.is = is;
		this.builder = new StringBuilder();
		this.cacher = new StringCacher(this.builder, timeOut);
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * Returns the cacher managing the read stream.
	 * @return the cacher managing the read stream.
	 */
	public StringCacher getCacher() {
		return this.cacher;
	}
	
	/**
	 * Reads the stream and cache the value before returning it.
	 * @return The next read byte.
	 */
	@Override
	public int read() throws IOException {
		int currentByte = this.is.read();
//		this.stringWriter.write(currentByte);
		if (currentByte != -1) {
			this.builder.append((char)currentByte);
		}
		else {
			this.cacher = new StringCacher(this.builder, System.currentTimeMillis());
		}
		return currentByte;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
