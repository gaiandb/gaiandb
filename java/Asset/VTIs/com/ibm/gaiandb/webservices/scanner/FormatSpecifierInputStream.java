/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner;

import java.io.IOException;
import java.io.InputStream;
//import java.text.Normalizer.Form;

import junit.framework.Assert;

/**
 * 
 * TODO - comment Type
 * 
 * @author remi - IBM Hursley
 *
 */
public class FormatSpecifierInputStream extends InputStream {

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
	
	/** The InputStream containing the data to specify the format of. */
	private InputStream is;
	
	/** The next value to return. */
	private int nextValue = (int)' '; // init value. Never read.

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public

	public FormatSpecifierInputStream(InputStream is) {
		super();
		this.is = is;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * Returns {@link WsDataFormat.XML} if the first non-space character is a '<', 
	 * {@link WsDataFormat.JSON} if it is a '{', {@link WsDataFormat.WRONG_FORMAT} 
	 * otherwise or if the InputStream is null. When called, all the space 
	 * characters which still have to be read before the first non-space one
	 * will be ignored of the read() method. 
	 * 
	 * @return {@link WsDataFormat.XML} if the first non-space character is a '<', 
	 * {@link WsDataFormat.JSON} if it is a '{', {@link WsDataFormat.WRONG_FORMAT} 
	 * otherwise or if the InputStream is null.
	 */
	public WsDataFormat defineFormat() {
		
		if (this.is != null) {
			// Ignore all spaces at the beginning of the file
			while (("" + (char)this.nextValue).matches("\\s")) {
				try {
					this.nextValue = is.read();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			// Checks the first read character of the stream
			if (this.nextValue == (int)'<') {
				return WsDataFormat.XML;
			}
			if (this.nextValue == (int)'{' || this.nextValue == (int)'[') {
				return WsDataFormat.JSON;
			}
		}
		return WsDataFormat.UNKNOWN_FORMAT;
		
	}
	
	@Override
	public int read() throws IOException {
		int ret = this.nextValue;
		
		// Saves next value to return
		this.nextValue = this.is.read();
		
		// And return the current one
		return ret;
	}

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic


	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TESTS
	
	/**
	 * Test the class.
	 */
	public static void testFormatSpecifierInputStream () {
		
		// Stream containing XML
		InputStream ix = new InputStream() {
			private int[] tab = {(int)' ', (int)'\t', (int)'\n', (int)'\r', (int)'<', (int)'a'};
			private int i = 0;
			@Override
			public int read() throws IOException {
				if (i < 6) return tab[i++];
				return -1;
			}
		};
		
		// Stream containing JSON
		InputStream ij = new InputStream() {
			private int[] tab = {(int)' ', (int)'\t', (int)'\n', (int)'\r', (int)'{', (int)'a'};
			private int i = 0;
			@Override
			public int read() throws IOException {
				if (i < 6) return tab[i++];
				return -1;
			}
		};
		
		// Stream containing JSON
		InputStream ij2 = new InputStream() {
			private int[] tab = {(int)' ', (int)'\t', (int)'\n', (int)'\r', (int)'[', (int)'{', (int)'a'};
			private int i = 0;
			@Override
			public int read() throws IOException {
				if (i < 7) return tab[i++];
				return -1;
			}
		};
		
		// Stream containing nothing
		InputStream in = new InputStream() {
			private int[] tab = {(int)' ', (int)'\t', (int)'\n', (int)'\r', (int)'t', (int)'a'};
			private int i = 0;
			@Override
			public int read() throws IOException {
				if (i < 6) return tab[i++];
				return -1;
			}
		};
		
		char read = ' ';
		
		try {
			FormatSpecifierInputStream fx = new FormatSpecifierInputStream(ix);
			fx.defineFormat();
			Assert.assertEquals("The stream should define a XML format", WsDataFormat.XML, fx.defineFormat());
			read = (char)fx.read();
			Assert.assertEquals("The stream.read() should read a '<' instead of a '" + read + "'", '<', read);
			read = (char)fx.read();
			Assert.assertEquals("The stream.read() should read a 'a' instead of a " + read + "'", 'a', read);
			Assert.assertEquals("The stream.read() should read a -1 (end of stream)", -1, fx.read());
		} catch (IOException ioe) {
			Assert.fail("An error occured while readin the stream");
//			Assert.
		}
		
		try {
			FormatSpecifierInputStream fj = new FormatSpecifierInputStream(ij);
			Assert.assertEquals("The stream should define a JSON format", WsDataFormat.JSON, fj.defineFormat());
			read = (char)fj.read();
			Assert.assertEquals("The stream.read() should read a '{' instead of a " + read + "'", '{', read);
			read = (char)fj.read();
			Assert.assertEquals("The stream.read() should read a 'a' instead of a " + read + "'", 'a', read);
			Assert.assertEquals("The stream.read() should read a -1 (end of stream)", -1, fj.read());
		} catch (IOException ioe) {
			Assert.fail("An error occured while readin the stream");
		}
			
		try { 
			FormatSpecifierInputStream fj2 = new FormatSpecifierInputStream(ij2);
			Assert.assertEquals("The stream should define a JSON format", WsDataFormat.JSON, fj2.defineFormat());
			read = (char)fj2.read();
			Assert.assertEquals("The stream.read() should read a '[' instead of a " + read + "'", '[', read);
			read = (char)fj2.read();
			Assert.assertEquals("The stream.read() should read a '{' instead of a " + read + "'", '{', read);
			read = (char)fj2.read();
			Assert.assertEquals("The stream.read() should read a 'a' instead of a " + read + "'", 'a', read);
			Assert.assertEquals("The stream.read() should read a -1 (end of stream)", -1, fj2.read());
		} catch (IOException ioe) {
			Assert.fail("An error occured while readin the stream");
		}
			
		try{
			FormatSpecifierInputStream fn = new FormatSpecifierInputStream(in);
			Assert.assertEquals("The stream should define a UNKNOWN_FORMAT format", WsDataFormat.UNKNOWN_FORMAT, fn.defineFormat());
			read = (char)fn.read();
			Assert.assertEquals("The stream.read() should read a 't' instead of a " + read + "'", 't', read);
			read = (char)fn.read();
			Assert.assertEquals("The stream.read() should read a 'a' instead of a " + read + "'", 'a', read);
			Assert.assertEquals("The stream.read() should read a -1 (end of stream)", -1, fn.read());
		} catch (IOException ioe) {
			Assert.fail("An error occured while readin the stream");
		}
		
		// stream null
		FormatSpecifierInputStream fnull = new FormatSpecifierInputStream(null);
		Assert.assertEquals("The stream should define a UNKNOWN_FORMAT format", WsDataFormat.UNKNOWN_FORMAT, fnull.defineFormat());
	}
}
