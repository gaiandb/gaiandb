/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.jdbc.discoveryclient.DiscoveryDriver;

/**
 * @author DavidV
 */
public class SQLDiscoveryClientRunner extends SQLRunner {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";
    
	private static final String DEFAULT_USR = GaianDBConfig.GAIAN_NODE_DEFAULT_USR;
	private static final String DEFAULT_PWD = GaianDBConfig.GAIAN_NODE_DEFAULT_PWD;

    protected static final String DBMS = "gaiandb";
    protected static final String URL_PREFIX = DiscoveryDriver.SEEKER_DRIVER_URL_PREFIX;

    private static final String driver  = "com.ibm.gaiandb.jdbc.discoveryclient.DiscoveryDriver";
    
    private Properties properties = new Properties();
        
    static boolean isDriverLoaded = false;

    public SQLDiscoveryClientRunner() {
    	super( DEFAULT_USR, DEFAULT_PWD, null, -1, null );
    	USAGE =
    		"\nArguments: " + BASE_ARGS + " [-createdb|-nocreatedb] [-standalone] <sql queries | queries files>*" +
    		"\nDefault host: " + DEFAULT_HOST +
    		"\nDefault port: " + DEFAULT_PORT + " (when overriding this, the default database used will be '" + DEFAULT_DATABASE + "<port>')" +
    		"\nDefault database: " + DEFAULT_DATABASE +
    		"\nDefault usr (=schema): " + DEFAULT_USR +
    		"\nDefault pwd: " + DEFAULT_PWD +
    		"\n-createdb|-nocreatedb: Create database if it doesn't already exist. Use -nocreatedb to disable this" +
    		"\n-standalone: Connect to Derby directly using the Embedded driver. NOTE: This option is incompatible with any host or port setting" +
    		COMMON_USAGE;
    }
    
    public SQLDiscoveryClientRunner( String usr, String pwd, String db ) {
    	
    	super( DEFAULT_USR, DEFAULT_PWD, null, -1, null );
    	standalone = true;
    	createdb=true;
    	mUsr = usr;
    	mPwd = pwd;
    	mDatabase = db;
    }
    
    public SQLDiscoveryClientRunner( String url ) {
    	
    	super( DEFAULT_USR, DEFAULT_PWD, null, -1, null );
    	this.url = url;
    }
    
    public static void main(String[] args) {
    	
    	new SQLDiscoveryClientRunner().processArgs( args );
    }
    
    /**
     * Connect to a database.
     * 
     * @param url the URL of the database
     * @param username the username to use
     * @param password the password for the user
     * @throws SQLException 
     * @throws SQLException if there was a problem connecting to the database
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Connection sqlConnect() throws SQLException {
            	
    	if ( isDriverLoaded && null != url ) {
    		printInfo("sqlConnect() url: " + url + (null==mUsr?", usr: " + mUsr:"") + "...\n");
    	} else {
    	
	    	if ( null == url ) {
		    	loadDriver( driver );
		    	isDriverLoaded = true;
		    	url = URL_PREFIX;
	    	}
	
	    	if ( null == mUsr ) properties.remove("user"); else properties.setProperty("user", mUsr);
	    	if ( null == mPwd ) properties.remove("password"); else properties.setProperty("password", mPwd);
	    	
			printInfo("\nConnecting to " + DBMS + " database: " + url + (null==mUsr?"":", usr: "+mUsr) + "...\n");
    	}
    	
    	return DriverManager.getConnection( url, properties );
    }
    
}
