/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.lite;



import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */

public class LiteDriver implements Driver {

	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "LiteDriver", 25 );
	
	/**
	 * Prefix for the connection URL this driver recognizes 
	 */
    public static final String LITE_DRIVER_URL_PREFIX = "jdbc:gaiandb:lite";
    
    /**
     * Register the driver to the java.sql.DriverManager
     */
    static
    {
        try { java.sql.DriverManager.registerDriver( new LiteDriver() ); }
        catch( Exception e ){
        	logger.logException( GDBMessages.NETDRIVER_DRIVER_REGISTER_ERROR, "java.sql.DriverManager.registerDriver(new LiteDriver()) failed: ", e );
        }
    }
    
    /* (non-Javadoc)
     * @see java.sql.Driver#acceptsURL(java.lang.String)
     */
    public boolean acceptsURL( String url ) throws SQLException {
    	logger.logInfo("Checking if url is accepted by LiteDriver: " + url);
    	return url.equals( LiteDriver.LITE_DRIVER_URL_PREFIX );
    }

	public Connection connect(String url, Properties info) throws SQLException {
        if ( !acceptsURL( url ) ) return null; // return null to indicate this driver does not recognise this url and make the Driver Manager try another driver
//        if ( !acceptsURL( url ) )
//            throw new SQLException( "LiteDriver - connect() failed - URL is not accepted: " + url );
		return new LiteConnection();
	}

	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}

	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
}
