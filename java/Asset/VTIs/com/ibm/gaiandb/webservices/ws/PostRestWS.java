/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.ws;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;


/**
 * Access a REST/GET web service.
 * 
 * @author remi - IBM Hursley
 *
 */
public class PostRestWS extends RestWS {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";

	// -------------------------------------------------------------------------- Dynamic
	
	/** The data to send to the server when sending the web service. */
	private String postRequest;

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public

	public PostRestWS(String url, String request) throws MalformedURLException {
		super(url);
//System.out.println(request);
		this.postRequest = request;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
//	public void setPostRequest() {
//	}

	@Override
	public InputStream getInputStream() throws IOException {
		
		// Sets the connection as a POST ws
		this.connection.setDoOutput(true);
		if (this.connection instanceof HttpURLConnection) {
			((HttpURLConnection)this.connection).setRequestMethod("POST");
		}
		
		// Send the POST data on the connection
		OutputStreamWriter out = new OutputStreamWriter(
				this.connection.getOutputStream());
		out.write(this.postRequest);
		out.close();
        
	    // Returns reading of the request
        return this.connection.getInputStream();
	}
	

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
