/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;

/**
 * An Exception class for UDP driver server
 * 
 * @author lengelle
 *
 */
public class UDPDriverServerException extends Exception
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
    private static final long serialVersionUID = 1L ;
    
    public UDPDriverServerException( String errorMessage )
    {
        super( errorMessage );
    }

    public UDPDriverServerException( String errorMessage, Throwable e )
    {
        super( errorMessage, e );
    }
}
