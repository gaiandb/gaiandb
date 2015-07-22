/*
 * (C) Copyright IBM Corp. 2010
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
import com.ibm.gaiandb.udpdriver.client.UDPDriver;

public class SQLUDPRunner extends SQLRunner
{
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final String DEFAULT_USR = GaianDBConfig.GAIAN_NODE_DEFAULT_USR;
	private static final String DEFAULT_PWD = GaianDBConfig.GAIAN_NODE_DEFAULT_PWD;
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = GaianNode.DEFAULT_PORT;
	private static final String DEFAULT_DATABASE = GaianDBConfig.GAIANDB_NAME;
	
	protected static final String DBMS = "gaiandb";
	
	private static final String udpdriver  = GaianDBConfig.GDB_UDP_DRIVER;

	static boolean isDriverLoaded = false;
	
	public SQLUDPRunner()
	{
    	super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE );
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

	public SQLUDPRunner( String usr, String pwd, String db )
	{
		super( DEFAULT_USR, DEFAULT_PWD, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE );
    	createdb=true;
    	mUsr = usr;
    	mPwd = pwd;
    	mDatabase = db;
	}

	
	
	public Connection sqlConnect() throws SQLException
	{
		if ( !isDriverLoaded )
		{
			if ( !GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE ) loadDriver( udpdriver );
			isDriverLoaded = true;
		}
		
		if ( url == null )
		{
			url = UDPDriver.UDP_DRIVER_URL_PREFIX + "//" + mHost + ":" + mPort + "/" + mDatabase;
		}
		
		printInfo("\nConnecting to " + DBMS + " database: " + url + (null==mUsr?"":", usr: "+mUsr) + "...\n");
		
		Connection c = null;
		if ( mUsr == null )
		{
			c = DriverManager.getConnection( url );
		}
		else 
		{
			c = DriverManager.getConnection( url, mUsr, mPwd );
		}
		
		return c;
	}
	

    public static void main( String[] args )
    {
    	new SQLUDPRunner().processArgs( args );
    }
}
