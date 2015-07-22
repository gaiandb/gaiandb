/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.ws;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Abstract class accessing a web service.
 * 
 * @author remi - IBM Hursley
 *
 */
public abstract class WebService {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";

	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	protected URL receiver;
	protected URLConnection connection;
	protected String username=null;
	protected String password=null;
	protected String userInfo=null;

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	// -------------------------------------------------------------------------- Private
	/*
	 * This method now extracts the userinfo for the openConnection().
	 * The url (without userinfo is also kept use for later in openconnection().
	 * One can set up these  (username,password  by using the getter/setter 
	 * between this method call and the OpenConnection call. 
	 */
	protected WebService(String url) throws MalformedURLException {		
		this.receiver = new URL(url);
		userInfo=this.receiver.getUserInfo();
		if (userInfo != null) //have username and password in hostname? 
		{
			String[] tokens=userInfo.split(":");
			username=(tokens[0] != null) ? tokens[0] : null;
			password=(tokens[1] != null) ? tokens[1] : null;
			//System.out.println("user info " + userInfo+"   username="+username+"   password:"+password);
		}
	}
	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	/**
	 * This method now  assign the userinfo to the connection for authentication
	 */
	public void openConnection() throws IOException {
		this.connection = this.receiver.openConnection();
		if (this.userInfo != null) {
		    String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userInfo.getBytes());
		    //new String(new Base64().encode(userInfo.getBytes()));
		    this.connection.setRequestProperty("Authorization", basicAuth);
		}
	}
	
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUserInfo() {
		return userInfo;
	}
	public void setUserInfo(String userInfo) {
		this.userInfo = userInfo;
	}

	public abstract InputStream getInputStream() throws IOException;

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
