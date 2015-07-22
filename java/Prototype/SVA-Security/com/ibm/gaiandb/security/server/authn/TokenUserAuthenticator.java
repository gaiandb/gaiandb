/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.server.authn;
/**
 * An authenticator for the GaianDB token authentication strategy.

 * Actors in the token authentication protocol are:
 * 	1. Client (initiates JDBC connection);
 * 	2. Derby database;
 * 	3. Token Manager (issues, authenticates and revokes token).
 * 
 * The authentication protocol is:
 * 	1. Client establishes Security Token (ST) with Token Manager;
 * 	2. Client creates Connection (C1) with Derby (anonymously);
 * 	3. Client sends ST to Derby over C1 in a stored procedure call;
 * 	4. Derby generates a Session identity[1] (SID) and stores UID[2], ST and SID internally;
 * 	5. Derby returns SID to Client over C1;
 * 	6. Client terminates C1;
 * 	7. Client establishes Connection (C2) with Derby with UID and SID;
 * 	8. Derby verifies UID and SID (by comparing UID with user name in SID) and authenticates ST.
 * 
 * [1] SID must be expressed in a String object.
 * [2] Server extracts UID from ST.
 * 
 * Security comment: This class (and any implementation) should not expose the tokens beyond the scope of this class (and its subclasses) 
 */

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.UUID;

import org.apache.derby.authentication.UserAuthenticator;
import org.apache.derby.impl.jdbc.authentication.BasicAuthenticationServiceImpl;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.security.common.SecurityToken;

public abstract class TokenUserAuthenticator implements UserAuthenticator {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final Logger logger = new Logger( "TokenUserAuthenticator", 30 );
	
	static final String GAIAN_DEFAULTUID = "APP";
	static final String GAIAN_SESSIONID = "SID";
	static final String GAIAN_DID_KEY = "domain";
	
	protected static TokenUserAuthenticator tua;

	private Hashtable<String, String> sids = new Hashtable<String,String>();  // session id's and user names

	/**
	 * authenticate the user using the credentials passed. If secure authentication fails, fallback to basic authentication.
	 * @param String userName
	 * @param String passwordOrSid
	 * @param String dbName
	 * @param Properties info
	 * @returns boolean whether the user was authenticated or not
	 */
	@Override
	public boolean authenticateUser(String userName, String passwordOrSid, String dbName, Properties info) throws SQLException {
		assert(userName != null);
		boolean res = false;

		// first call Derby DEFAULT authenticator
		if (userName.equals(GAIAN_DEFAULTUID)) res = true; // handle initial client connection
		else {
			// handle "returning user" connection; expecting UID and SID
			if (null != passwordOrSid) {
				// expecting UID and SID
				String pUID = userName;
				String pSID = passwordOrSid;
				String idRes = this.sids.get(pSID);
				if (null != idRes && idRes.equals(pUID)) res = true;
			}
			BasicAuthenticationServiceImpl basAuth = new BasicAuthenticationServiceImpl(); 
			
			if (!res) {
				logger.logInfo("Couldn't authenticate securely, falling back to basic auth");
				res = basAuth.authenticateUser(userName, passwordOrSid, dbName, new Properties());  // drop back to Basic if no asserted id
			}
		}
		return res;
	}

	protected abstract boolean authenticateToken(SecurityToken token);
	
	/**
	 * sets the token and returns the secure id from it.
	 * @param SecurityToken token
	 * @return String the secure id
	 */
	public static String setToken(SecurityToken token) {
		String sid=null;
		
		if (tua.authenticateToken(token)) {
			String tid=token.getId();
			if (null != tid) {
				// generate unique SID
				sid = UUID.randomUUID().toString();  // implementation uses Java's UUID
				tua.sids.put(sid, tid);  // add new SID to table
			}
		}
		
		return sid;
	}
}
