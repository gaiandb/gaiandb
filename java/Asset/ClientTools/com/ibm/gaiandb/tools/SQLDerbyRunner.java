/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;

/**
 * @author DavidV
 */
public class SQLDerbyRunner extends SQLRunner {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
    
	private static final String DEFAULT_USR = GaianDBConfig.GAIAN_NODE_DEFAULT_USR;
	private static final String DEFAULT_PWD = GaianDBConfig.GAIAN_NODE_DEFAULT_PWD;
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = GaianNode.DEFAULT_PORT;
	private static final String DEFAULT_DATABASE = GaianDBConfig.GAIANDB_NAME;
	    
    protected static final String DBMS   = "derby";

    private static final String ndriver  = GaianDBConfig.DERBY_CLIENT_DRIVER;
    private static final String edriver  = GaianDBConfig.DERBY_EMBEDDED_DRIVER;
        
    static boolean isDriverLoaded = false;

    public SQLDerbyRunner() {
    	super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE );
    	USAGE =
    		"\nArguments: " + BASE_ARGS + " [-createdb|-nocreatedb] [-standalone] [-ssl=<off|basic|peerAuthentication>] <sql queries | queries files>*" +
    		"\nDefault host: " + DEFAULT_HOST +
    		"\nDefault port: " + DEFAULT_PORT + " (when overriding this, the default database used will be '" + DEFAULT_DATABASE + "<port>')" +
    		"\nDefault database: " + DEFAULT_DATABASE +
    		"\nDefault usr (=schema): " + DEFAULT_USR +
    		"\nDefault pwd: " + DEFAULT_PWD +
    		"\n-createdb|-nocreatedb: Create database if it doesn't already exist. Use -nocreatedb to disable this" +
    		"\n-ssl=<sslMode>: Used to encrypt query/results with SSL/HTTPS. See Derby docs for more info. sslMode can be: off, basic or peerAuthentication" +
    		"\n-standalone: Connect to Derby directly using the Embedded driver. NOTE: This option is incompatible with any host or port setting" +
    		COMMON_USAGE;
    }
    
    public SQLDerbyRunner( String usr, String pwd, String db ) {
    	
    	super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE );
    	standalone = true;
    	createdb=true;
    	mUsr = usr;
    	mPwd = pwd;
    	mDatabase = db;
    }

    public SQLDerbyRunner( String usr, String pwd, String db, int port ) {
    	
    	super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, port, DEFAULT_DATABASE );
    	standalone = false; // a port is set
    	createdb=true;
    	mUsr = usr;
    	mPwd = pwd;
    	mDatabase = db;
    }
    
    public SQLDerbyRunner( String url ) {
    	
    	super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE );
    	this.url = url;
    }
	    
    public static void main(String[] args) {
    	
    	new SQLDerbyRunner().processArgs( args );
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
    		return DriverManager.getConnection( url, mUsr, mPwd );
    	}
    	
    	Connection c = null;
    	
    	if ( null == url ) {
	    	if ( standalone ) {
	        	loadDriver( edriver );
	        	url = "jdbc:" + DBMS + ":" + mDatabase + ";create=" + createdb + (upgrade?";upgrade=true":"");
	        	
	    	} else {
	        	loadDriver( ndriver );
	    		url = "jdbc:" + DBMS + "://" + mHost + ":" + mPort + "/" + mDatabase + ";create=" + createdb
	    			+ (upgrade?";upgrade=true":"") + (null!=sslMode?";ssl="+sslMode:"");
	    	}
    	}
    	
		printInfo("\nConnecting to " + DBMS + " database: " + url + (null==mUsr?"":", usr: "+mUsr) + "...\n");

		if ( null == mUsr )
			c = DriverManager.getConnection( url );
		else {
//			printStream.println("url=" + url + ";user="+mUsr+";password="+mPwd);
			c = DriverManager.getConnection( url, mUsr, mPwd ); // + ";user="+mUsr+";password="+mPwd ); 
		}
		
    	isDriverLoaded = true;
    	
    	return c;
    }
    
}
