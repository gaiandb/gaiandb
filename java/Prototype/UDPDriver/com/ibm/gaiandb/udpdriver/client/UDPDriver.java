/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.gaiandb.udpdriver.client;



import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * Implementation of java.sql.Driver
 * 
 * @author lengelle
 *
 */
public class UDPDriver implements Driver
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPDriver", 25 );
	
	/**
	 * Label for the URL parameter
	 */
	private static String DATAGRAM_SIZE_PARAMETER_LABEL = "datagramSize";
	
	/**
	 * Label for the URL parameter
	 */
	private static String TIMEOUT_PARAMETER_LABEL = "timeout";
	
	// Default values
	/**
	 * The datagram size, in bytes
	 */
	public static int DATAGRAM_SIZE = 1450; // Bytes
	
	/**
	 * Timeout, in milli-seconds, used for the lost datagram detection for the first
	 * interaction between the server and the client.
	 * 
	 *  - this is the default timeout used by client apps when connecting... must be larger than the inter-node client timeout...
	 */
	public static long FIRST_TIMEOUT = 5000; // Milli-seconds
	
	/**
	 * Margin timeout, in milli-seconds, which is always added to the timeout values
	 * used for the lost datagram detection.
	 */
	public static long TIMEOUT_MARGIN = 1000; // Milli-seconds
	
	/**
	 * Prefix for the connection URL this driver recognizes 
	 */
    public static final String UDP_DRIVER_URL_PREFIX = "jdbc:gaiandb:udp";
    
    /**
     * Register the driver to the java.sql.DriverManager
     */
    static
    {
        try
        {
            java.sql.DriverManager.registerDriver( new UDPDriver() );
        }
        catch( Exception e )
        {
        	logger.logException( GDBMessages.NETDRIVER_CLIENT_DRIVER_REGISTER_ERROR, "java.sql.DriverManager.registerDriver(new UDPDriver()) failed.", e );
        }
    }

    
    /* (non-Javadoc)
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    public boolean acceptsURL( String url ) throws SQLException
    {
    	logger.logInfo("Checking if url is accepted by UDPDriver: " + url);
        return url.startsWith( UDPDriver.UDP_DRIVER_URL_PREFIX );
    }


    /* (non-Javadoc)
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    public Connection connect( String url, Properties info ) throws SQLException
    {
        if ( !acceptsURL( url ) ) return null;
    	
        String user = info.getProperty( "user" );
        String password = info.getProperty( "password" );

        if ( !user.equals("gaiandb") || !password.equals("passw0rd") )
        	throw new SQLException( "UDPDriver - connect() failed. Wrong login/password." );
        
        try {
	        int index2 = url.indexOf( "/", url.indexOf( "/" )+1 );
	        int index = url.indexOf( ":", ++index2 );
	        
	        String address = url.substring( index2, index ); // e.g. "127.0.0.1"
	        index2 = url.indexOf( "/", ++index );
	        
	        String port = url.substring( index, index2 ); // e.g. "7414"        
	        
	        index = url.indexOf( ";", index2 );
	        
	        String arg;
	        String value;
	        while ( index != -1 )
	        {
	            index2 = url.indexOf( "=", ++index );
	            
	            arg = url.substring( index, index2 );
	            index = url.indexOf( ";" , ++index2 );
	            
	            if ( index==-1 )
	            {
	                value = url.substring( index2, url.length() );
	            }
	            else
	            {
	                value = url.substring( index2, index );
	            }
	            
	            initializeParameter( arg, value );
	        }

	        return new UDPConnection( address, port ) ;
	        
        } catch ( Exception e ) {
        	throw new SQLException("UDPDriver - connect() failed (Incorrect url: " + url + "): " + e);
        }
    }

    
    private void initializeParameter( String parameter, String value )
    {
        if ( parameter.equals( DATAGRAM_SIZE_PARAMETER_LABEL ) )
        {
            DATAGRAM_SIZE = Integer.parseInt( value );
        }
        
        if ( parameter.equals( TIMEOUT_PARAMETER_LABEL ) )
        {
            FIRST_TIMEOUT = Integer.parseInt( value );
        }
    }
    
    
    
    
    
    public int getMajorVersion()
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getMajorVersion() : Unimplemented method." );
        return 0 ;
    }

    
    public int getMinorVersion()
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getMinorVersion() : Unimplemented method." );
        return 0 ;
    }

    
    public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getPropertyInfo( String url, Properties info ) : Unimplemented method." );
        return null ;
    }

    
    public boolean jdbcCompliant()
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "jdbcCompliant() : Unimplemented method." );
        return false ;
    }


	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getParentLogger() : Unimplemented method." );
		return null;
	}

}
