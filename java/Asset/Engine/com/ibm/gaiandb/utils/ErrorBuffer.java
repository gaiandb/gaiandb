/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.utils;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class implements a buffer of strings to hold error messages so that they 
 * can be logged together rather than separately.
 * The number of messages is limited to a maximum, past that either the latest or the
 * oldest messages are discarded (depending on the specified Overflow Strategy.
 * 
 * @author Paul Stone
 */
public class ErrorBuffer {
	
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";

	public enum overflowStrategy { KEEP_FIRST, KEEP_LAST };

	ArrayList<String> messageBuffer = new ArrayList<String>();
	int maxSize;
	overflowStrategy onOverflow;
	int nextMessageNumber; //this is only maintained with the KEEP_LAST strategy
	
	/**
	 * This method creates an error buffer of default maximum size (10) and a strategy 
	 * to keep the first messages if more than 10 are added. 
	 * @param vtiArgs - contains the Datasource "_ARGS" field from the config file.
	 */
	public ErrorBuffer(){
		this(10,overflowStrategy.KEEP_FIRST);
	}

	/**
	 * This method creates an error buffer of the specified maximum size and a strategy 
	 * to keep the first messages if more than the maximum are added. 
	 * @param maxSize - specifies the maximum number of messages that the buffer can hold.
	 */
	public ErrorBuffer(int maxSize){
		this(maxSize,overflowStrategy.KEEP_FIRST);
	}
	
	/**
	 * This method creates an error buffer of the specified maximum size and overflow strategy 
	 * @param maxSize - specifies the maximum number of messages that the buffer can hold.
	 * @param onOverflow - specifies which messages to keep and discard when more than the maximum are added..
	 */
	public ErrorBuffer(int maxSize, overflowStrategy onOverflow){
		messageBuffer = new ArrayList<String>(maxSize);
		this.maxSize = maxSize;
		this.onOverflow = onOverflow;
		nextMessageNumber=0;
	}
	
	/**
	 * This method adds a message to the buffer, respecting the overflowStrategy specified
	 * when the buffer was  created.
	 * @param errorMessage - specifies the error message to add to the buffer..
	 */
	public void add(String errorMessage) {
		if (messageBuffer.size()<maxSize) {
			messageBuffer.add(errorMessage);
		} else {
			switch (onOverflow){
			case KEEP_FIRST:
				//ignore the latest error message
				break;
			case KEEP_LAST:
				// maintain a circular buffer of the latest messages.
				messageBuffer.add(nextMessageNumber,errorMessage);
				// calculate the next message position, using modulus to wrap around to the start again.
				nextMessageNumber = (nextMessageNumber+1) % maxSize;
				break;
			}
		}
	}
	
	/**
	 * This method determines whether the buffer contains any errors.
	 * @return boolean - true if an error has been added to the buffer.
	 */
	public boolean errorExists(){
		return messageBuffer.size()>0;
	}
	
	/**
	 * This method returns an iterator which allows access to the error messages in the buffer.
	 * @return Iterator<String> - A class to iterate over all the error messages in the buffer.
	 */
	public Iterator<String> getErrorMessageIterator(){
		return messageBuffer.iterator();
	}
}


