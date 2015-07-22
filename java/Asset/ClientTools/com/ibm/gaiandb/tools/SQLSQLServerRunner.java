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
public class SQLSQLServerRunner extends SQLRunner {

	
//  private static final String DBMS     = "microsoft:sqlserver";  // Microsoft driver 2000
//  private static final String table    = "littleblackbook"; // flightpaths littleblackbook dair_rowset_1
//  private static final String driver   = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
//  private static final String host     = "localhost";
//  private static final String port     = "1433";
//  private static final String database = "ogsadai"; // camera
//  private static final String username = "sa";
//  private static final String password = "ogsadai";
//  private static final String procName = "proc3";
//  private static final String url = "jdbc:" + DBMS + "://" + host + ":" + port + ";databaseName=" + database;

//  private static final String DBMS     = "sqlserver"; // Microsoft driver 2005 - beta 2
//  private static final String table    = "littleblackbook"; // flightpaths littleblackbook dair_rowset_1
//  private static final String driver   = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
//  private static final String host     = "localhost";
//  private static final String port     = "1433";
//  private static final String database = "ogsadai"; // camera
//  private static final String username = "sa";
//  private static final String password = "ogsadai";
//  private static final String procName = "proc3";
//  private static final String url = "jdbc:" + DBMS + "://" + host + ":" + port + ";databaseName=" + database;
  
//  private static final String DBMS     = "jtds:sqlserver";
//  private static final String table    = "littleblackbook"; // flightpaths littleblackbook dair_rowset_1
//  private static final String driver   = "net.sourceforge.jtds.jdbc.Driver";
//  private static final String host     = "localhost";
//  private static final String port     = "1433";
//  private static final String database = "ogsadai"; // camera
//  private static final String username = "sa";
//  private static final String password = "ogsadai";
//  private static final String procName = "proc3";
//  private static final String url = "jdbc:" + DBMS + "://" + host + ":" + port + "/" + database;

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final String DEFAULT_USR = "usr";
	private static final String DEFAULT_PWD = "pwd";
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 1433;
	private static final String DEFAULT_DATABASE = "pubs";
	    
    protected static final String DBMS     = "sqlserver";
    private static final String driver  = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        
    static boolean isDriverLoaded = false;

    public SQLSQLServerRunner() {
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
            
    	new SQLSQLServerRunner().processArgs( args );
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
    	url = "jdbc:" + DBMS + "://" + mHost + ":" + mPort + ";databaseName=" + mDatabase;
    	
		System.out.println("\nConnecting to " + DBMS + " using url: " + url + "\n");
		c = DriverManager.getConnection( url, mUsr, mPwd );
    	isDriverLoaded = true;
    	
    	return c;
    }
    
}
