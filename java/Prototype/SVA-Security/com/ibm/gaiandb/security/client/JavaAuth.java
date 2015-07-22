/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.client;
/**
 * This authenticator uses the JAAS framework.
 */
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class JavaAuth {
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private Subject subj = null;
	private Properties props = new Properties();
	private static final String CLASSNAME = JavaAuth.class.getName();

	/**
	 * Returns the authenticated subject from a LoginContext
	 * @param cbh
	 * @throws LoginException
	 */
	public JavaAuth(CallbackHandler cbh) throws LoginException {
		LoginContext lc = null;
		
		if (null == cbh) lc = new LoginContext(CLASSNAME);
		else lc = new LoginContext(CLASSNAME, cbh);

		// attempt authentication
		lc.login();

		this.subj = lc.getSubject();
	}

	/**
	 * Get the subject credentials
	 * @return a Properties object with the subject credentials
	 */
	public Properties getSubjectCreds() {
		// Let's see what Principals we have:
		for (Principal p: this.subj.getPrincipals()) {
			props.put(p.getClass().getName(),p);			
		}
		
		// collect public credentials
		for (Object o: this.subj.getPublicCredentials()) {
			props.put(o.getClass().getName(),o);			
		}

		// collect private credentials
		for (Object o: this.subj.getPrivateCredentials()) {
			props.put(o.getClass().getName(),o);
		}

		// get GSS context token
		PrivilegedAction<byte[]> action = new GSSPrivilegedDelegateAction();
		byte[] token=Subject.doAs(this.subj, action);
		
		if (token!=null) props.put(token.getClass().getName(), token);

		return props;

	}

	/**
	 * Get Suject credentials for a given name
	 * @param pName
	 * @return the subject credentials
	 */
	public Object getToken(String pName) {
		if (this.props.isEmpty()) this.getSubjectCreds();
		// return named token
		return this.props.get(pName);
	}

}
