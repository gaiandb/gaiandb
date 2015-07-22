/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.jdbc.discoveryclient;

import java.sql.SQLException;
import javax.sql.PooledConnection;

/**
 * @author Paul D Stone
 *
 */
public class ConnectionPoolDataSource extends org.apache.derby.jdbc.ClientConnectionPoolDataSource {

	private static final long serialVersionUID = 1L;
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	// no initialisation required in the constructor.
	public ConnectionPoolDataSource() {
	}

	// Retrieve a PooledConnection object encapsulating the actual database connection. 
	public PooledConnection getPooledConnection() throws SQLException {
		PooledConnection newPooledConnection = new GaianPooledConnection();

		return newPooledConnection;
	}

	// Retrieve a PooledConnection object encapsulating the actual database connection. 
	// Use the defined User and Password to connect to the database.
	public PooledConnection getPooledConnection(String theUser,
			String thePassword) throws SQLException {
		PooledConnection newPooledConnection = new GaianPooledConnection(theUser, thePassword);

		return newPooledConnection;
	}

}
