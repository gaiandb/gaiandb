/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.server.authn;
/**
 * An authenticator for GaianDB Kerberos token support.
 */

import java.sql.SQLException;
import java.util.Properties;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.security.common.KerberosToken;
import com.ibm.gaiandb.security.common.SecurityToken;

public class KerberosUserAuthenticator extends TokenUserAuthenticator {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	public KerberosUserAuthenticator() {
		// must instantiate due to static reference in super
		TokenUserAuthenticator.tua = this;
	}
	
	/**
	 * @param String userName
	 * @param String passwordOrSid
	 * @param String dbName
	 * @param Properties info
	 * @return boolean whether the user has been authenticated or not
	 */
	public boolean authenticateUser(String userName, String passwordOrSid, String dbName, Properties info) throws SQLException {
		// replace REALM delimiter, if present
		String uid = userName.replaceAll("\"", "");
		return super.authenticateUser(uid, passwordOrSid, dbName, info);
	}
	
	/**
	 * validate a Token
	 * @param SecurityToken the token to validate
	 * @return boolean whether the token was validated or not
	 */
	@Override
	protected boolean authenticateToken(SecurityToken pToken) {
		// authenticate token
		assert(pToken.getClass().equals(KerberosToken.class));  // check for type of GaianSecurityToken
		boolean res=false;
		
		// is the KerberosToken valid?
		if (!pToken.isValid()) return false;
		
		String authMode = GaianDBConfig.getDerbyAuthMode();
		
		// authenticate token; get authentication mode
		if (GaianDBConfig.DERBY_AUTH_MODE_DEFAULT.equals( authMode )) { 
			// default authentication mode -- KDC auth?	
			res=true;
		} else if (GaianDBConfig.DERBY_AUTH_MODE_ID_ASSERT.equals( authMode ) ) {
			// identity assertion mode	
			res=true;
		}

		return res;
	}
}

