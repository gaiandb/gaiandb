/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.common.protocol;

/**
 * An Exception class for UDP driver protocol
 * 
 * @author lengelle
 *
 */
public class UDPProtocolException extends Exception
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
    private static final long serialVersionUID = 1L ;
    
    public UDPProtocolException( String errorMessage )
    {
        super( errorMessage );
    }
    
    public UDPProtocolException( String errorMessage, Exception e )
    {
        super( errorMessage, e );
    }
}
