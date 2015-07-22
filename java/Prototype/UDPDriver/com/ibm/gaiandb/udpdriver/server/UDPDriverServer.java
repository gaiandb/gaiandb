/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.udpdriver.common.SocketHelper;

/**
 * The UDPDriverServer has to be launch on the machine where the database is defined.
 * Once started, it accepts remote connection from UDP driver clients. 
 * 
 * @author lengelle
 *
 */
public class UDPDriverServer
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	public static final String DS_EXECUTOR_THREAD_PREFIX = "UDPDriverServerThread-";

	/**
	 * The datagram size, in bytes
	 */
	public static int DATAGRAM_SIZE = 1450;
	
	/**
	 * The size of the pool of java.sql.Connection objects obtained from the Derby embedded driver.
	 * A Connection is used to process a client PreparedStatement queries.
	 */
	public static int MAX_CONNECTIONS_FOR_PREPAREDSTATEMENT = 40;
	
	/**
	 * The size of the pool of java.sql.Connection objects obtained from the Derby embedded driver.
	 * A Connection is used to process a client PreparedStatement queries.
	 */
	public static int MAX_CONNECTIONS_FOR_STATEMENT = 20;

    private int serverPort;
    private String serverAddress;
    private String localDataBasePath;

    
    /**
     * Instantiates a new UDPDriverServer object.
     * The UDPDriverServer will attempts to connect to the 'localDataBaseName' using the Derby embedded driver.
     * It also will listen to client request on a socket bound to 'address' and 'port'
     * 
     * @param address
     * @param port
     * @param localDataBasePath
     */
    public UDPDriverServer( String address, int port, String localDataBasePath )
    {
        this.serverAddress = address;
        this.serverPort = port;
        this.localDataBasePath = localDataBasePath;
    }


    /**
     * Starts the UDPDriverServer.
     * 
     * @throws UDPDriverServerException
     */
    public void start() throws UDPDriverServerException
    {

        try
        {
        	// Socket initialization
        	SocketHelper serverSocket = new SocketHelper( serverAddress, serverPort );


            // JDBC ConnectionManagers initialization
            ConnectionManager connectionManagerForPreparedStatement = new ConnectionManager( localDataBasePath, MAX_CONNECTIONS_FOR_PREPAREDSTATEMENT );
            ConnectionManager connectionManagerForStatement = new ConnectionManager( localDataBasePath, MAX_CONNECTIONS_FOR_STATEMENT );


            // ThreadPool initialization
            Executor executor = Executors.newCachedThreadPool( new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger( 1 );
                public Thread newThread( Runnable r ) { return new Thread( r, DS_EXECUTOR_THREAD_PREFIX+threadNumber.getAndIncrement() ); }
        	});


            ServerListener l = new ServerListener( serverSocket, executor, connectionManagerForPreparedStatement, connectionManagerForStatement );
            l.start();

        }
        catch( Exception e )
        {
            throw new UDPDriverServerException( "UDPDriverServer start() failed." + Util.getStackTraceDigest(e), e );
        }
    }

    public void setDatagramSize( int maxDatagramSize )
    {
    	DATAGRAM_SIZE = maxDatagramSize;
    }

    public void setMaximumPreparedStatementPoolSize( int poolSize )
    {
    	MAX_CONNECTIONS_FOR_PREPAREDSTATEMENT = poolSize;
    }

    public void setMaximumStatementPoolSize( int poolSize )
    {
    	MAX_CONNECTIONS_FOR_STATEMENT = poolSize;
    }

}
