/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.ibm.gaiandb.GaianNode;

/**
 * The ConnectionManager provides and recycles java.sql.Connection from the Derby embedded driver.
 * It implements a Connection pool. When it is ask to provide a Connection while the pool 
 * is empty, it creates a new Connection object and returns it.
 * 
 * It is strongly advised to return all Connection to the ConnectionManager after usage.
 * 
 * @author lengelle
 *
 */
public class ConnectionManager
{
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final String DERBY_DRIVER_URL_PREFIX = "jdbc:derby:";
	private static final String USER = "gaiandb";
	private static final String PASSWORD = "passw0rd";
	
	private int maxConnections;
	private BlockingQueue<Connection> connectionQueue; 
	private String localDataBaseName;
	
	/**
	 * Instantiates a new ConnectionManager
	 * 
	 * @param localDataBaseName
	 * @param maxConnections
	 */
	public ConnectionManager( String localDataBaseName, int maxConnections )
	{
		this.localDataBaseName = localDataBaseName;
		this.maxConnections = maxConnections;
		
		this.connectionQueue = new ArrayBlockingQueue<Connection>( this.maxConnections );
	}
	
	
	/**
	 * Obtain a Connection from the pool.
	 * If the pool is empty, a new Connection is created and returned.
	 * 
	 * @return
	 * @throws UDPDriverServerException
	 */
	public Connection getConnection() throws UDPDriverServerException
	{
		try
		{
			Connection connection = connectionQueue.poll();
			
			if ( connection == null )
			{
				connection = DriverManager.getConnection( GaianNode.isLite() ? "jdbc:gaiandb:lite" :
					DERBY_DRIVER_URL_PREFIX+localDataBaseName, USER, PASSWORD );
			}
			
			return connection;
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ConnectionManager getConnection() failed. ", e );
		}
	}

	
	/**
	 * Return a connection to the pool.
	 * If the pool is full, the connection is closed and discarded.
	 * 
	 * @param connection
	 * @throws UDPDriverServerException
	 */
	public void releaseConnection( Connection connection ) throws UDPDriverServerException
	{
		try
		{
			if ( !connectionQueue.offer( connection ) )
			{
				connection.close();
			}
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ConnectionManager releaseConnection() failed. ", e );
		}
	}
}
