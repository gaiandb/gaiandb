/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.security.common;

import java.io.IOException;

import javax.security.auth.kerberos.KerberosTicket;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

public class KerberosToken extends SecurityToken {
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final Logger logger = new Logger( "KerberosToken", 30 );
	
	public static final String TNAME = "javax.security.auth.kerberos.KerberosTicket";  // assume JAAS Kerberos Ticket
	private KerberosTicket token=null;
	
	/**
	 * 
	 * @param pToken
	 */
	public KerberosToken(byte[] pToken) {
		this.set(pToken);
		super.setName(TNAME);
	}
	
	/**
	 * 
	 * @param pToken
	 */
	public KerberosToken(KerberosTicket pToken) {
		this.set(pToken);
		super.setName(TNAME);
	}
	
	/**
	 * 
	 * @param obj a Kerberos ticket
	 */
	public void set(KerberosTicket obj) {
		if (null != obj) this.token = (KerberosTicket)obj;
	}
	
	/**
	 * @returns boolean is token valid
	 */
	@Override
	public boolean isValid() {
		boolean res = false;
		
		if (null != this.token) {
			// check that ticket is current (at the very least)
			res = this.token.isCurrent();
		} else {
			logger.logWarning(GDBMessages.SECURITY_INVALID_TOKEN, "Could not get a valid secure token");
		}

		return res;
	}
	
	/**
	 * @return String an identity from the Kerberos token
	 */
	@Override
	public String getId() {
		return (this.token!=null ? this.token.getClient().getName() : null);
	}
		
	/**
	 * @param pToken a token
	 * set the kerberos token
	 */
	@Override
	public void set(byte[] pToken) {
		// cast from byte array to KerberosTicket
		Object obj=null;
		try {
			obj = super.getObject(pToken);
		} catch (IOException ioe) {
			logger.logException(GDBMessages.SECURITY_TOKEN_IO_EXCEPTION, "Could not get the secure token", ioe);
		} catch (ClassNotFoundException cnfe) {
			logger.logException(GDBMessages.SECURITY_TOKEN_CLASS_NOT_FOUND, "Could not get the secure token", cnfe);
		}
		if (null != obj) this.token = (KerberosTicket)obj;
	}

	/**
	 * @return byte[] get the kerberos token
	 */
	@Override
	public byte[] get() {
		byte[] ret = null;
		
		if (null != this.token)
			try {
				ret = super.getBytes(this.token);
			} catch (IOException e) {
				logger.logException(GDBMessages.SECURITY_INVALID_TOKEN, "Could not find a valid kerberos token", e);
			}

		return ret;
	}
}
