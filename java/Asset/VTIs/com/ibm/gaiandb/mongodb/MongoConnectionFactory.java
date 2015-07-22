/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.mongodb;

import java.net.ConnectException;
import java.net.UnknownHostException;

import javax.naming.AuthenticationException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * This class holds code allowing connection to a remote MongoDB process.
 * 
 * @author Paul Stone
 */
public class MongoConnectionFactory {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	/**
	 * Returns a handle to a Mongo Collection object - which may be used to execute mongoDB queries.
	 * 
	 * @param connectionParams - object containing all the parameters needed to connect to MongoDB
	 * @exception UnknownHostException - if we cannot connect to the mongoDB host specified
	 * @exception AuthenticationException - if authentication with the mongoDB process fails.
	 * 
	 */
	public static DBCollection getMongoCollection (MongoConnectionParams connectionParams) throws Exception{

		String collectionName = connectionParams.getCollectionName();
		
		if ( null == collectionName )
			throw new Exception("Missing collection name. Check URL syntax matches 'usr:pwd@host:port/database/collection'");
		
		// Now we should have an authenticated connection the mongo database, validate that the collection exists.
		DBCollection mongoCollection = getMongoDB(connectionParams).getCollection( collectionName );
		
		return mongoCollection;
	}
	
	/**
	 * Returns a handle to a Mongo DB object - which may be used to manage collections.
	 * 
	 * @param connectionParams - object containing all the parameters needed to connect to MongoDB
	 * @exception UnknownHostException - if we cannot connect to the mongoDB host specified
	 * @exception AuthenticationException - if authentication with the mongoDB process fails.
	 * 
	 */
	public static DB getMongoDB (MongoConnectionParams connectionParams) throws Exception{
		
		// connect to the mongo process
		MongoClient mongoClient = new MongoClient(connectionParams.getHostAddress(), connectionParams.getHostPort());
		if (mongoClient == null) throw new ConnectException(MongoMessages.DSWRAPPER_MONGODB_CONNECTION_ERROR);

		// Connect to the specific database
		DB mongoDb = mongoClient.getDB(connectionParams.getDatabaseName()); // TBD possibly try to reuse these.
		if (mongoDb == null) throw new ConnectException(MongoMessages.DSWRAPPER_MONGODB_DATABASE_CONN_ERROR);

		// Authenticate if the configuration parameters have been set
		String userName = connectionParams.getUserName();
		String password = connectionParams.getPassword();

		if (userName != null && password != null) {
			boolean authenticated = mongoDb.authenticate(userName, password.toCharArray());
			if (!authenticated){
				throw new AuthenticationException(MongoMessages.DSWRAPPER_MONGODB_AUTHENTICATION_ERROR);
			}
		}

		return mongoDb;
	}
	
	/**
	 * Closes the Mongo Collection object - frees up any resources held by the collection and the related
	 * connection object.
	 * 
	 * @param connectionParams - object containing all the parameters needed to connect to MongoDB
	 * @exception UnknownHostException - if we cannot connect to the mongoDB host specified
	 * @exception AuthenticationException - if authentication with the mongoDB process fails.
	 * 
	 */
	public static void closeMongoCollection (DBCollection mongoCollection) {
		//we have to close the mongo client object, get a reference to it via the database object.
		DB mongoDB = mongoCollection.getDB();
		MongoClient mongoClient = (MongoClient)mongoDB.getMongo();
		mongoClient.close();
		
	}
}
