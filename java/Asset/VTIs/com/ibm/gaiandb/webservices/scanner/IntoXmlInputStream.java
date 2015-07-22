/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * The purpose of this stream is to add the tag &#60;xml&#62; at the beginning of 
 * a stream and the tag &#60;/xml&#62; at its end. Thus, the stream is transformed 
 * into an xml value.
 * 
 * @author remi - IBM Hursley
 *
 */
public class IntoXmlInputStream extends InputStream {

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

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/** The stream to convert into xml. */
	private InputStream is;
	
	/** The first tag to add to the stream. */
	private InputStream startTag = new ByteArrayInputStream("<xml>".getBytes());
	
	/** Checks if the tag starting the stream has been read or not yet. */
	private boolean startTagRead = false;

	/** Checks if the main stream (is) has been read or not yet. */
	private boolean valueRead = false;

	/** The last tag to add to the stream. */
	private InputStream endTag = new ByteArrayInputStream("</xml>".getBytes());

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	public IntoXmlInputStream (InputStream is) {
		this.is = is;
	}
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	@Override
	public int read() throws IOException {
		// Reads the stream including the start tag
		if(!startTagRead) {
			int ret = startTag.read();
			if (ret > -1) {
				return ret;
			}
			else {
				startTagRead = true;
			}
		}
		// Reads the main stream is
		if(!valueRead) {
			int ret = is.read();
			if (ret > -1) {
				return ret;
			}
			else {
				valueRead = true;
			}
		}
		// Reads the stream including the end tag
		return endTag.read();
	}

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
