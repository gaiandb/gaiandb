/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.server.authn;
/*
 * An authenticator for the GaianDB identity assertion strategy.
 */
import java.sql.SQLException;
import java.util.Properties;

import org.apache.derby.authentication.UserAuthenticator;
import org.apache.derby.impl.jdbc.authentication.BasicAuthenticationServiceImpl;

import com.ibm.gaiandb.Logger;

public class TrustedUserAuthenticator implements UserAuthenticator {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final Logger logger = new Logger( "TrustedUserAuthenticator", 30 );
	
	private static final String GAIAN_PROXYUID_KEY = "proxy-user";
	private static final String GAIAN_PROXYPWD_KEY = "proxy-pwd";
	
	private static final BasicAuthenticationServiceImpl basAuth = new BasicAuthenticationServiceImpl(); 

	/**
	 * authenticate a user with the credentials provided, fallback to basic authentication if needed
	 * @param String userName
	 * @param String passwordOrSid
	 * @param String dbName
	 * @param Properties info
	 * @return boolean whether or not the user is authenticated.
	 */
	@Override
	public boolean authenticateUser(String userName, String passwordOrSid, String dbName, Properties info) throws SQLException {
		boolean res = false;

		// authenticate assertor's identity, checking first that an identity has been asserted!
		if (null != userName && null != info) {
			String proxyUID = info.getProperty(GAIAN_PROXYUID_KEY);
			String proxyPwd = info.getProperty(GAIAN_PROXYPWD_KEY);

			if (null != proxyUID && null != proxyPwd) {
				// first call Derby DEFAULT authenticator
				res = basAuth.authenticateUser(proxyUID, proxyPwd, dbName, new Properties());
			}
		}
		if (!res) {
			logger.logInfo("Couldn't authenticate securely, falling back to basic auth");
			res = basAuth.authenticateUser(userName, passwordOrSid, dbName, new Properties());  // drop back to Basic if no asserted id
		}
		
		return res;
	}

}
