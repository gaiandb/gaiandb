/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.common;

/**
 * General utilities class
 * 
 * @author lengelle
 *
 */
public abstract class Util
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
    
	/**
	 * Return a subarray of the array given as a parameter delimited by the two other
	 * parameters.
	 * 
	 * @param array
	 * @param start
	 * @param length
	 * @return
	 */
    public static byte[] getSubArray( byte[] array, int start, int length )
    {
    	byte[] subArray = new byte[ length ];
    	System.arraycopy( array, start, subArray, 0, length );
    	return subArray;
    }
    
}
