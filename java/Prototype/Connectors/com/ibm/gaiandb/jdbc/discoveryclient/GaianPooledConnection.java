/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.jdbc.discoveryclient;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

/**
 * @author Paul D Stone
 *
 */

public class GaianPooledConnection implements PooledConnection {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	// The JDBC connection to the discovered gaian database,
	Connection connection = null;
	
	// Creates a PooledConnection object encapsulating the database connection. 
	public GaianPooledConnection() throws SQLException {
		java.util.Properties info = new java.util.Properties();
		GaianConnectionSeeker seeker = new GaianConnectionSeeker();
 
		// find a suitable gaian connection.
		connection = seeker.discoverGaianConnection(info);
	}

	// Creates a PooledConnection object encapsulating the database connection. 
	// Use the defined User and Password to connect to the database.
	public GaianPooledConnection(String theUser, String thePassword) throws SQLException {
		java.util.Properties info = new java.util.Properties();
		info.setProperty("user", theUser);
		info.setProperty("password", thePassword);
		
		GaianConnectionSeeker seeker = new GaianConnectionSeeker();

		// find a suitable gaian connection.
		connection = seeker.discoverGaianConnection(info);
	}
	
	/* 
	 * Returns the connection managed by this object
	 */
	public Connection getConnection() throws SQLException {
		return connection;
	}
	
	/* 
	 * Closes the connection managed by this object
	 */
	public void close() throws SQLException {
		connection.close();

	}

	/* 
	 * Event listeners are not supported in this version of the driver
	 */	
	public void addConnectionEventListener(ConnectionEventListener theListener) {
	}

	/* 
	 * Event listeners are not supported in this version of the driver
	 */	
	public void addStatementEventListener(StatementEventListener listener) {
	}


	/* 
	 * Event listeners are not supported in this version of the driver
	 */
	public void removeConnectionEventListener(
			ConnectionEventListener theListener) {
	}

	/* 
	 * Event listeners are not supported in this version of the driver
	 */
	public void removeStatementEventListener(StatementEventListener listener) {
	}

}
