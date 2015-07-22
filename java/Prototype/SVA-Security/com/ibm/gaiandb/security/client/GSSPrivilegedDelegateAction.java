/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.client;

/*
 * Acquire delegate token.
 */
import java.security.PrivilegedAction;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

public class GSSPrivilegedDelegateAction implements PrivilegedAction<byte[]> {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final Logger logger = new Logger( "GSSPrivilegedDelegateAction", 30 );
	private static final String GSSMAN_NAME = "xpclient";
	private static final String GSSSERVICE_NAME = "http@gaiandb.securedom.local";
	
	/**
	 * Connects using the credentials given
	 * @returns byte[] byte array containing the secure token
	 */
	@Override
	public byte[] run() {

		byte[] outToken = null;
			
		try {
			GSSManager manager = GSSManager.getInstance();
			GSSName clientName = manager.createName(GSSMAN_NAME, GSSName.NT_USER_NAME);
			GSSCredential clientCred = manager.createCredential(clientName,
					8 * 3600,
					createKerberosOid(),
					GSSCredential.INITIATE_ONLY);

			GSSName serverName = manager.createName(GSSSERVICE_NAME, GSSName.NT_HOSTBASED_SERVICE);

			GSSContext context = manager.createContext(serverName,
					createKerberosOid(),
					clientCred,
					GSSContext.DEFAULT_LIFETIME);
			context.requestMutualAuth(true);
			context.requestConf(false);
			context.requestInteg(true);

			outToken = context.initSecContext(null, 0, 0);

 			context.dispose();
 			context = null;

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
}
