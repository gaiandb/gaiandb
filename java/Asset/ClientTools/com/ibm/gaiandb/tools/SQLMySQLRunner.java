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

/**
 * @author DavidV
 */
public class SQLMySQLRunner extends SQLRunner {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final String DEFAULT_USR = "usr";
	private static final String DEFAULT_PWD = "pwd";
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 3306;
	private static final String DEFAULT_DATABASE = "test";
	    
    protected String DBMS     = "mysql";
    private static final String driver  = "com.mysql.jdbc.Driver";
        
    static boolean isDriverLoaded = false;

    public SQLMySQLRunner() {
    	super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE );    	
    	USAGE =
    		"\nArguments: " + BASE_ARGS + " <sql queries | queries files>*" +
    		"\nDefault host: " + DEFAULT_HOST +
    		"\nDefault port: " + DEFAULT_PORT + " (when overriding this, a database name should also be specified)" +
    		"\nDefault database: " + DEFAULT_DATABASE +
    		"\nDefault usr (=schema): " + DEFAULT_USR +
    		"\nDefault pwd: " + DEFAULT_PWD +
    		COMMON_USAGE;
    }

    public static void main( String[] args ) {
            
    	new SQLMySQLRunner().processArgs( args );
    }
    
    /**
     * Connect to a database.
     * 
     * @param url the URL of the database
     * @param username the username to use
     * @param password the password for the user
     * @throws SQLException if there was a problem connecting to the database
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Connection sqlConnect() throws SQLException {
            	
    	if ( isDriverLoaded ) {
    		return DriverManager.getConnection( url, mUsr, mPwd );
    	}
    	
    	Connection c = null;
    	
    	loadDriver( driver );
    	url = "jdbc:" + DBMS + "://" + mHost + ":" + mPort + "/" + mDatabase;
    	
		System.out.println("\nConnecting to " + DBMS + " using url: " + url + "\n");
		c = DriverManager.getConnection( url, mUsr, mPwd );
    	isDriverLoaded = true;
    	
    	return c;
    }
    
}
