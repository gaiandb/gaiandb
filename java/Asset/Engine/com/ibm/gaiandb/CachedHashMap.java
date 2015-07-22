/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author DavidVyvyan
 */
public class CachedHashMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = -5176468539211767236L;

	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
    
	private final int cacheSize;
	
	/**
	 * Creates a new CachedHashMap object.
	 * @param cacheSize 
	 * 			Number of records that can be stored into the map.
	 */
	public CachedHashMap( int cacheSize ) {
		this.cacheSize = cacheSize;
	}
	
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() > cacheSize;
	}
}
