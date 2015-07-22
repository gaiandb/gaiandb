/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;

import java.io.ByteArrayOutputStream;

/**
 * SpecificByteArrayOutputStream extends java.io.ByteArrayOutputStream
 * SpecificByteArrayOutputStream just adds a method : byte[] toByteArray( int length )
 * Otherwise it has exactly the same behavior as ByteArrayOutputStream
 * 
 * @author lengelle
 *
 */
public class SpecificByteArrayOutputStream extends ByteArrayOutputStream
{
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	public SpecificByteArrayOutputStream( int size )
	{
		super( size );
	}
	
	
	/**
	 * Creates a newly allocated byte array, which size is given as a parameter.
	 * The first part of the valid contents of the buffer has been copied into it.
	 * 
	 * @param length
	 * @return
	 */
    public byte[] toByteArray( int length )
    {
    	byte[] array = new byte[length];
    	System.arraycopy( buf, 0, array, 0, length );
    	return array;
    }
}
