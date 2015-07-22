/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.scanner.sax;

import java.io.IOException;
import java.io.InputStream;

/**
 * The purpose of this class is to generate a XHTML InputStream from a HTML InputStream. 
 * 
 * @author remi - IBM Hursley
 *
 */
public class HTMLFilterInputStream extends InputStream {

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
	
	/** 
	 * Used as a default value when the index going through the buffer doesn't 
	 * point to any values.
	 */
	private static final int NO_VALUES_TO_READ = -1;
	
	/**
	 * Used as a default returned value when a filter does not have to be applied 
	 * on a given character.
	 */
	private static final int FILTER_NOT_RELEVANT = -1;
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	private InputStream inputStreamToFilter;
	
	private byte[] valuesToRead;
	
	private int indexValueToRead;

	private String[] tagsContentToRemove = new String[0];
	
	private String[] tagsToRemove = new String[0];//{"meta"}, "link"}; //new String[0];

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	public HTMLFilterInputStream(InputStream inputStreamToFilter) {
		super();
System.out.println("Apply Filter");
		this.inputStreamToFilter = inputStreamToFilter;
		this.valuesToRead = new byte[0];
		this.indexValueToRead = NO_VALUES_TO_READ;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	public void setTagContentsToRemove(String[] contents) {
		this.tagsContentToRemove = contents;
		
	}

	public void setTagsToRemove(String[] tagNames) {
		this.tagsToRemove = tagNames;
		
	}

	@Override
	public int read() throws IOException {
		
		int nextValue = this.readAndFilter();
		
		// Checks the result of the conversion (int)-1 > char > int
		if (nextValue == 65535) nextValue = -1; 
		
//		char c = (char)nextValue;
//System.out.println("" + c);
		return nextValue;//(int)c;
	}
	
	@Override
	public void close() throws IOException {
		this.inputStreamToFilter.close();
		super.close();
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	private int readAndFilter() throws IOException {
		
		// Checks if the buffer contains values to read
		if (this.indexValueToRead != NO_VALUES_TO_READ
				&& this.indexValueToRead < this.valuesToRead.length) {
			// read the first value of the buffer
			int ret = (int)this.valuesToRead[this.indexValueToRead];
			
			// Updates index
			this.indexValueToRead++;
			if (this.indexValueToRead >= this.valuesToRead.length) {
				this.valuesToRead = new byte[0];
				this.indexValueToRead = NO_VALUES_TO_READ;
			}
//System.out.println("read:" + new Character((char)ret).toString());	
			
			return ret;
		}
		// If the buffer is empty 
		else {
			// If it is the beginning of a tag: "<"
			char currentChar = (char)this.inputStreamToFilter.read();
			
			if (currentChar == '<') {
				StringBuilder valueToReadIfNormalTag = new StringBuilder();
				valueToReadIfNormalTag.append(currentChar);
				
				// --- Checks if it is a end tag
				currentChar = (char)this.inputStreamToFilter.read();
				if (currentChar == '/') {
					
					valueToReadIfNormalTag.append(currentChar);
					// If it is, we do not consider this as a tag to remove
					// So we store the read value and keep going
					this.valuesToRead = valueToReadIfNormalTag.toString().getBytes();
					this.indexValueToRead = 0;
									
					return this.readAndFilter();
				}
				
				// --- Scans the name of the tag
				// Remove whites
				while (currentChar == ' ' || currentChar == '\t' 
						|| currentChar == '\r' || currentChar == '\n' 
						|| currentChar == '\f') {
					valueToReadIfNormalTag.append(currentChar);
					currentChar = (char)this.inputStreamToFilter.read();
				}
				
				// Build the name of the tag
				StringBuilder nameTag = new StringBuilder();
				while (currentChar != '>' && currentChar != ' ' 
						&& currentChar != '\t' && currentChar != '\r'
						 && currentChar != '\n' && currentChar != '\f') {
					valueToReadIfNormalTag.append(currentChar);
					nameTag.append(currentChar);
					currentChar = (char)this.inputStreamToFilter.read();
				}
				valueToReadIfNormalTag.append(currentChar);
				
				// --- If it is a tag which content has to be removed
				String currentTagName = nameTag.toString().toLowerCase();
				String currentTagNameLowerStr = currentTagName.toLowerCase();
				for (String tagContentToRemove : 
						this.tagsContentToRemove ) {
					if (tagContentToRemove.equals(currentTagNameLowerStr)) {
						
						// scans until we find the corresponding end tag
						this.ignoreUntilEndTag(currentTagName);
						
						// The char just after the end of the end tag just opened
						currentChar = (char)this.readAndFilter();
						
//System.out.println("read:" + new Character((char)currentChar).toString());
						// return the next char
						return (int)currentChar;
					}
				}
				
				// Else, if it is the name of a tag to remove
				for (String tagContentToRemove : 
						this.tagsToRemove ) {
					if (tagContentToRemove.equals(currentTagName)) {
						
						// scans until the end of the tag
						this.ignoreCurrentTag(currentChar);
						
						currentChar = (char)this.readAndFilter();
						
//System.out.println("read:" + new Character((char)currentChar).toString());
						// return the next char
						return (int)currentChar;
					}
				}
				
				// Else, if it a normal tag, need to fill the buffer
				// Problem: going to return first char of valueToReadIfNormalTag 
				// and buffer all the others (plus the last read value stampNam)
				
				this.valuesToRead = valueToReadIfNormalTag.toString().getBytes();
				this.indexValueToRead = 0;
				
				return this.readAndFilter();
			
			}
			// If it is something else, 
			else {
//System.out.println("read:" + new Character((char)currentChar).toString());
				// returns the value
				return this.filterChar(currentChar);
			}
		}
	}
	
	/**
	 * Calls the method this.inputStreamToFilter.read() until the end of the end tag
	 * which name is given as a parameter.
	 * @param endTag
	 * 			end tag name delimiting when the ignoring has to stop.
	 * @throws IOException if any exceptions occur during the 
	 * this.inputStreamToFilter.read().
	 */
	private void ignoreUntilEndTag(String endTag) throws IOException {
		
		char stamp = (char)this.inputStreamToFilter.read();
		boolean hasBeFound = false;
		
		while (!hasBeFound) {
			// Does nothing until it find a '<'
			while (stamp != '<') {
				stamp = (char)this.inputStreamToFilter.read();
			}
			stamp = (char)this.inputStreamToFilter.read();

			// Checks if it is a closig tag
			if (stamp == '/') {
				
				// Remove whites before the name
				stamp = (char)this.inputStreamToFilter.read();
				while (stamp == ' ' || stamp == '\t' 
						|| stamp == '\r' || stamp == '\n' 
						|| stamp == '\f') {
					stamp = (char)this.inputStreamToFilter.read();
				}
				
				// Gets the name
				StringBuilder nameTag = new StringBuilder();
				while (stamp != '>' && stamp != ' ' 
						&& stamp != '\t' && stamp != '\n' 
						&& stamp != '\f' && stamp != '\r') {
					nameTag.append(stamp);
					stamp = (char)this.inputStreamToFilter.read();
				}
				
				// Checks if it closes the tag we are ignoring
				if (nameTag.toString().equalsIgnoreCase(endTag)) {
					// Ignore all whites until (including) the end of the tag
					while (stamp != '>') {
						stamp = (char)this.inputStreamToFilter.read();
					}
					hasBeFound = true;
				}
				// If it is not the right tag that it is closing, keep going!
			}
		}
	}

	/**
	 * Calls the method this.inputStreamToFilter.read() until the end of the 
	 * current tag.
	 * @param lastReadChar
	 * 			last char read before the call of this method.
	 * @throws IOException if any exceptions occur during the 
	 * this.inputStreamToFilter.read().
	 */
	private void ignoreCurrentTag(char lastReadChar) throws IOException {
		while (lastReadChar != '>') {
			lastReadChar = (char)this.inputStreamToFilter.read();
		}
	}

	/** 
	 * Checks if it is a specific char and add values to the buffer if so.
	 * @param charToTest
	 * 			The character to test.
	 * @return charToTest if no filter is relevant for this char, the first 
	 * character of the buffer if it changes have to be done.
	 * @throws IOException if any exceptions occur during the 
	 * this.inputStreamToFilter.read().
	 */
	private int filterChar(int charToTest) throws IOException {

		if (charToTest == -1) return charToTest;
		
		char testedChar = (char)charToTest;
		
		int filtered = this.filterCharAnd(testedChar);
		if (filtered != FILTER_NOT_RELEVANT) { 
			return filtered;
		}
		
		return charToTest;
	}
	
	/**
	 * If the charToTest equals '&', checks if it is the beginning of 
	 * '&amp;' (XML value for '&'), '&lt;' (XML value for '<') or for 
	 * '>', '"' or ''', and if not, add "amp;" to the buffer and returns 
	 * '&'.
	 * @param charToTest
	 * 			Character to test.
	 * @return FILTER_NOT_RELEVANT if charToTest is different of '&',
	 * '&' otherwise.
	 * @throws IOException if any exceptions occur during the 
	 * this.inputStreamToFilter.read().
	 */
	private int filterCharAnd(char charToTest) throws IOException {
		
		if (charToTest != '&') {
			return FILTER_NOT_RELEVANT;
		}
		
		StringBuilder toBuffer = new StringBuilder();
		toBuffer.append((char)charToTest);
		charToTest = (char)this.inputStreamToFilter.read();
		
		// If the '&' is before a space character
		if ((char)charToTest == ' ' || (char)charToTest == '\t'
			|| (char)charToTest == '\r' || (char)charToTest == '\n'
			|| (char)charToTest == '\f') {
			toBuffer.append("amp;");
			toBuffer.append(charToTest);
			
			this.valuesToRead = toBuffer.toString().getBytes();
			this.indexValueToRead = 0;
			return this.readAndFilter();
		}
		
		// gets 4 more char and compare the generated string with 
		// the definition of the specific XML characters.
		toBuffer.append(charToTest);
		for (int i = 0; i < 4; i++) {
			toBuffer.append((char)this.inputStreamToFilter.read());
		}
		
		// replace '&' by "&amp;" if it is an illegal '&' in the current file
		String currentBuffer = toBuffer.toString();
		if (!(currentBuffer.toLowerCase().startsWith("&amp;")
				|| currentBuffer.toLowerCase().startsWith("&lt;")
				|| currentBuffer.toLowerCase().startsWith("&gt;")
				|| currentBuffer.toLowerCase().startsWith("&quot;")
				|| currentBuffer.toLowerCase().startsWith("&#39"))) {
			currentBuffer.replaceAll("&", "&amp;");
			
		}
		
		// Loads the buffer
		this.valuesToRead = currentBuffer.getBytes();
		this.indexValueToRead = 0;
		
		return this.readAndFilter();
	}

}
