/*
 * (C) Copyright IBM Corp. 2015
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.SQLException;
import java.util.Properties;

/**
 * @author DavidVyvyan
 */
public class GaianAuthenticator implements org.apache.derby.authentication.UserAuthenticator {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2015";

	private static final Logger logger = new Logger( "GaianAuthenticator", 20 );
	
	@Override
	public boolean authenticateUser(String usr, String pwd, String dbName, Properties props) throws SQLException {
		
		if ( null == usr ) usr = props.getProperty("user");
		if ( null == pwd ) pwd = props.getProperty("password");
		
		if ( null == usr || null == pwd ) return false;
		
		logger.logThreadDetail("Entered authenticateUser(), usr: " + usr + ", db: " + dbName);
		
		// If usr is the value of gaian property 'GAIAN_NODE_USR', then request Gaian lookup of 'GAIAN_NODE_PWD' or value in derby.properties
		if ( usr.equals(GaianDBConfig.getGaianNodeUser()) ) return pwd.equals( GaianDBConfig.getGaianNodePassword() );
		
		// Otherwise - validate passwords using default derby builtin scheme - to allow backwards compatibility.
		// i.e. any usr/pwd combinations specified at startup in plain text in derby.properties
		return pwd.equals( GaianDBConfig.getDerbyProperties().getProperty("derby.user."+usr) );
	}
}
