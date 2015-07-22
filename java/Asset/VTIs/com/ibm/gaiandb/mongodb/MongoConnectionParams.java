/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.mongodb;

import java.util.IllegalFormatException;

/**
 * This class holds the necessary parameters to connect to a remote MongoDB process.
 * 
 * @author Paul Stone
 */
public class MongoConnectionParams {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	// Instance variables needed to connect to mongoDB
	private String userName = null;
	private String password = null;
	private String hostAddress = null;
	private Integer hostPort   = null;
	private String databaseName = null;
	private String collectionName = null;

	/**
	 * Constructs a MongoConnectionDetails object from the URL as passed
	 * to the setltformongodb Stored Procedure. Individual connection parameters
	 * may be accessed using the get methods of this class.
	 * 
	 * @param StoredProcURL - Expected format is: 
	 * 		{user}:{password}@{MongoURL}:{Port}/{Database}/{Collection}
	 * 		The {user}:{password}@ portion is optional
	 * 
	 * if user and password are not passed, they will be set as null.
	 * 
	 * @exception IllegalFormatException - if the URL passed is an invalid format.
	 * 
	 */
	public MongoConnectionParams(String StoredProcURL) throws IllegalArgumentException{
		super();
		String[] splitTokens = StoredProcURL.split("@"); // The @ separates the user and password from the rest.
		
		String mandatoryToken; // the part of the URL following the optional user:password. 
		
		if (splitTokens.length > 2) {
			 // only one or two tokens are expected
			throw new IllegalArgumentException("Invalid URL: Only 1 '@' symbol is allowed, found "+(splitTokens.length-1));
		} else if (splitTokens.length == 2) {
			//the first portion should be the username and password
			String[] userPasswordTokens = splitTokens[0].split(":");
			if (userPasswordTokens.length != 2) {
				// we expect 2 tokens - user and password
				throw new IllegalArgumentException("Invalid URL: Expected 2 tokens split by ':' for usr:pwd, found "+userPasswordTokens.length);
			} else {
				userName = userPasswordTokens[0];
				password = userPasswordTokens[1];
			}
			mandatoryToken = splitTokens[1];
		} else {
			mandatoryToken = splitTokens[0];
		}
		
		//now split out the mandatory components
		String [] locationTokens = mandatoryToken.split("/");
		if (locationTokens.length < 2 || locationTokens.length > 3) {
			 // we expect 3 tokens - hostname:port, database and collection.
			throw new IllegalArgumentException("Invalid URL: Expected 2 or 3 tokens split by '/' for host:port/database[/collection], found "+locationTokens.length);
		} else {
			String [] hostPortTokens = locationTokens[0].split(":");
			if (hostPortTokens.length != 2) {
				 // we expect 2 tokens - host and port.
				throw new IllegalArgumentException("Invalid URL: Expected 2 tokens split by ':' for host:port, found "+hostPortTokens.length);
			} else {
				hostAddress = hostPortTokens[0];
				hostPort = Integer.parseInt(hostPortTokens[1]);
			}
			
			databaseName = locationTokens[1];
			if ( 3 == locationTokens.length ) collectionName = locationTokens[2];
		}
	}
	
	/**
	 * Constructs a MongoConnectionDetails object from the set of parameters held in the 
	 * Gaian Configuration file.
	 * 
	 * @param hostAddress
	 * @param hostPort
	 * @param databaseName
	 * @param collectionName
	 * @param userName
	 * @param password
	 * 
	 * if user and password are not passed, they will be set as null.
	 * 
	 * @exception IllegalFormatException - if the URL passed is an invalid format.
	 * 
	 */
	public MongoConnectionParams(String hostAddress, Integer hostPort, String databaseName, String collectionName, String userName, String password) {
		this.hostAddress = hostAddress;
		this.hostPort   = hostPort;
		this.databaseName = databaseName;
		this.collectionName = collectionName;
		this.userName = userName;
		this.password = password;
	}	
	
	/**
	 * @return the user name
	 */
	public String getUserName() {
		return userName;
	}
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	/**
	 * @return the hostAddress
	 */
	public String getHostAddress() {
		return hostAddress;
	}
	/**
	 * @return the hostPort
	 */
	public Integer getHostPort() {
		return hostPort;
	}
	/**
	 * @return the databaseName
	 */
	public String getDatabaseName() {
		return databaseName;
	}
	/**
	 * @return the collectionName
	 */
	public String getCollectionName() {
		return collectionName;
	}
}
