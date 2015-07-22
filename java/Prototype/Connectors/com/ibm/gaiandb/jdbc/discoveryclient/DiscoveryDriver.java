/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.jdbc.discoveryclient;



import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author PaulDStone
 */

public class DiscoveryDriver implements java.sql.Driver {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
    public static final String SEEKER_DRIVER_URL_PREFIX = "jdbc:gaiandb:seeker";


	private static final Logger logger = new Logger( "DiscoveryDriver", 25 );

	/**
     * Register the driver to the java.sql.DriverManager
     */
    static
    {
    	try { java.sql.DriverManager.registerDriver( new DiscoveryDriver() ); }

    	catch( Exception e ){
    		logger.logException( GDBMessages.DISCOVERY_DRIVER_REGISTER_ERROR, "java.sql.DriverManager.registerDriver(new DiscoveryDriver()) failed: ", e );
    	}
	
    }

	public DiscoveryDriver() {
	}

	public boolean acceptsURL(String url) throws SQLException {
    	logger.logDetail("Checking if url is accepted by DiscoveryDriver: " + url);
    	return url.startsWith( DiscoveryDriver.SEEKER_DRIVER_URL_PREFIX );
	}

	public Connection connect(String url, Properties info) throws SQLException {
        if ( !acceptsURL( url ) ) return null; // return null to indicate this driver does not recognise this url and make the Driver Manager try another driver

        if (null == info) info = new java.util.Properties();
        
        String[] urlProps = url.split(";");
        for ( String prop : urlProps ) {
        	int idx = prop.indexOf('=');
        	if ( -1 == idx ) continue;
        	info.put(prop.substring(0, idx), prop.substring(idx+1));
        }

        GaianConnectionSeeker seeker = new GaianConnectionSeeker();
        // find a suitable gaian connection.
        return seeker.discoverGaianConnection(info);		// TODO Auto-generated method stub
	}

	public int getMajorVersion() {
		return 1;
	}

	public int getMinorVersion() {
		return 0;
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return null;
	}

	public boolean jdbcCompliant() {
		return false;
	}

	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
}
