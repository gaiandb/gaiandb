/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.caching;

/**
 * This class defines an interface for caching data within gaiandb.
 * 
 * @author remi - IBM Hursley
 *
 * @param <TYPE_TO_CACHE> the type of data to cache.
 */
public abstract class Cacher<TYPE_TO_CACHE> {

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
	
	private static final long SECOND = 1000;
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	/** deadline for the validity of the cache. */
	private long expiringTime;
	
	/** time the data is supposed to be cached. */
	private long expiringPeriod;

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	// ------------------------------------------------------------------------ Protected
	
	/**
	 * @param dataToCach
	 * @param timeOut
	 * 			time the data will stay cached in seconds.
	 */
	protected Cacher(long timeOut) {
		this.expiringPeriod = timeOut * Cacher.SECOND;
		this.resetExpiring();
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Returns the data which has been cached.
	 * @return the data which has been cached.
	 */
	public abstract TYPE_TO_CACHE getCachedData();
	
	/**
	 * Returns true if the data is up to date and false if it has expired. 
	 * @return true if the data is up to date and false if it has expired. 
	 */
	public boolean hasExpired() {
		return System.currentTimeMillis() > this.expiringTime;
	}
	
	/**
	 * Reset the time the cache will expire. 
	 */
	public void resetExpiring() {
		this.expiringTime = System.currentTimeMillis() + this.expiringPeriod;
	}
	

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
