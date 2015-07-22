/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.server;

import java.security.PrivilegedAction;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

public class GSSServerPrivilegedAction implements PrivilegedAction<byte[]> {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final Logger logger = new Logger( "GSSServerPrivilegedAction", 30 );
	
	private static final String GSSMAN_NAME = "gaianServer";
	private byte[] inToken;
	
	/**
	 * Connects using the credentials given
	 * @returns byte[] byte array containing the secure token
	 */
	public byte[] run() {
		byte[] outToken = null;
		
		try {
			GSSManager manager = GSSManager.getInstance();
			GSSName serverName =
				manager.createName(GSSMAN_NAME,
						GSSName.NT_HOSTBASED_SERVICE);
			GSSCredential serverCreds =
				manager.createCredential(serverName,
						GSSCredential.INDEFINITE_LIFETIME,
						createKerberosOid(),
						GSSCredential.ACCEPT_ONLY);
			GSSContext secContext = manager.createContext(serverCreds);

			// Loop while the context is still not established
			while (!secContext.isEstablished()) {
				outToken = 
					secContext.acceptSecContext(inToken, 0, inToken.length);
			}
			secContext.dispose();

		} catch (GSSException gsse) {
			logger.logException(GDBMessages.SECURITY_NO_CONTEXT, "Could not create a secure context", gsse);
			outToken = null;
		}

		return outToken;
	}
	
	/**
	 * Creates a Kerberos Oid
	 * @return a new Kerberos Oid
	 * @throws GSSException
	 */
	private Oid createKerberosOid() throws GSSException {
        return new Oid("1.2.840.113554.1.2.2");
    }
	
	/**
	 * set the secure token
	 * @param pToken a secure token
	 */
	public void setToken(byte[] pToken) {
		this.inToken = pToken;
	}
}
