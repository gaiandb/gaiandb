/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.gaiandb.udpdriver.client;

import java.net.InetAddress;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.common.SocketHelper;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;

/**
 * Implementation of java.sql.Connection
 * 
 * @author lengelle
 *
 */
public class UDPConnection implements Connection
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPConnection", 25 );
	
    //Query Id management.
    private static int QUERY_COUNTER = 0;
    private static int QUERY_COUNTER_MODULUS = 50000;
    
    /**
     * Returns a unique String ID.
     * @return
     */
    static synchronized public String createQueryID()
    {
        QUERY_COUNTER = QUERY_COUNTER+1 % QUERY_COUNTER_MODULUS;
        return ""+QUERY_COUNTER+System.currentTimeMillis();
    }

    /**
     * The server address
     */
    private InetAddress serverAddress;
    
    /**
     * The server port
     */
    private int serverPort;
    
    /**
     * The concurrent structure that contains the incoming messages for each query
     */
    private ConcurrentMap< String, BlockingQueue<Message> > map;
    private ClientListener listener;
    private SocketHelper clientSocket;
    

    /**
     * Creates a new UDPConnection objects.
     * It represents a connection between the client which has instantiated this object and a
     * distant server, specified by the parameters serverAddress and serverPort.
     * 
     * This instantiates and starts a new ClientListener.
     * This instantiates a new concurrent structure shared by the ClientListener and the 
     * future Statement and PreparedStatement objects this Connection could generate.
     * 
     * @param serverAddress the address of the distant server
     * @param serverPort the port of the distant server
     * @throws SQLException
     */
    public UDPConnection( String serverAddress, String serverPort ) throws SQLException
    {
        try
        {
            this.serverAddress = InetAddress.getByName( serverAddress );
            this.serverPort = Integer.decode( serverPort );
            
            map = new ConcurrentHashMap< String, BlockingQueue<Message> >();
            clientSocket = new SocketHelper();
            
            listener = new ClientListener( clientSocket, map );
            listener.start();
        }
        catch( Exception e )
        {
            throw new SQLException( "UDPConnection instantiation failed(). "+ e );
        }
    }
    
    /**
     * Return the concurrent structure
     * @return
     */
    public ConcurrentMap< String, BlockingQueue<Message> > getMap()
    {
        return map;
    }
    


    /* (non-Javadoc)
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement() throws SQLException
    {
        return new UDPStatement( this );
    }
    

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement( String sql ) throws SQLException
    {
        return new UDPPreparedStatement( this, sql );
    }
    
    
    /* (non-Javadoc)
     * @see java.sql.Connection#close()
     */
    public synchronized void close() throws SQLException
    {
    	try
    	{
    		if ( map==null )
    		{
    			return;
    		}
    		
    		SocketHelper sh = new SocketHelper();
    		sh.send( ClientListener.STOP_MESSAGE, InetAddress.getLocalHost(), clientSocket.getLocalPort() );
    		sh.close();
    		
	        map = null;
	        clientSocket = null;
	        serverAddress = null;
	        serverPort = -1;
    	}
    	catch( Exception e )
    	{
    		throw new SQLException( "UDPConnection close() failed. "+ e );
    	}
    }
    
    /**
     * Return the address of the server this connection is bound to
     * @return
     */
    public InetAddress getAddress()
    {
        return serverAddress;
    }
    
    /**
     * Return the port of the server this connection is bound to
     * @return
     */
    public int getPort()
    {
        return serverPort;
    }
    
    
    /* (non-Javadoc)
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed() throws SQLException
    {
    	return ( map == null );
    }
    
    /**
     * Sends the message given as a parameter to the server.
     * 
     * @param message
     * @throws SQLException
     */
    public void sendMessage( byte[] message ) throws SQLException
    {
        try
        {
            if ( message.length > UDPDriver.DATAGRAM_SIZE )
            {
                logger.logWarning( GDBMessages.NETDRIVER_MESSAGE_TOO_LONG, "A message longer than the current datagram size has been sent. Message type : " + message[0] );
            }
            
//            System.out.println("Sending to serverAddress " + serverAddress + ", port " + serverPort + ", message: " + new String(message));

            clientSocket.send( message, serverAddress, serverPort );
        }
        catch( Exception e )
        {
            throw new SQLException( "UDPConnection sendMessage() failed. "+ e );
        }
    }
    
    /**
     * Helper method used by the Statement and PreparedStatement.
     * It tries to return a message from the map having the expected queryID, sequenceNumber and type
     * before the timeout exceeds.
     * If no message is found or received during this time, it returns null.
     * 
     * @param queryID
     * @param sequenceNumberExpected
     * @param typeExpected
     * @param timeout
     * @return
     * @throws UDPDriverClientException
     */
    public Message retreiveMessageFromMap( String queryID, int sequenceNumberExpected, int typeExpected, long timeout ) throws UDPDriverClientException
    {
    	try
    	{
    		Message m = map.get( queryID ).poll( timeout, TimeUnit.MILLISECONDS );
    		while ( m!=null && ( m.getSequenceNumber()!=sequenceNumberExpected || m.getType()!=typeExpected ) )
    		{
    			logger.logWarning(GDBMessages.NETDRIVER_MESSAGE_CONTAINS_UNWANTED_TYPES, "Throwing away message containing unwanted " + 
    					( m.getType()!=typeExpected ? "type: " + m.getType() + " != expected: " + typeExpected :
    						"sequence number: " + m.getSequenceNumber() + " != expected: " + sequenceNumberExpected ) );
    			
    			m = map.get( queryID ).poll( timeout, TimeUnit.MILLISECONDS );
    		}
    		return m;
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverClientException( "UDPConnection getMessage() failed. ", e );
    	}
    }
    
    
    
    
    
    
    
    
    
    
    
    public void clearWarnings() throws SQLException
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "clearWarnings() : Unimplemented method." );
        
    }

    public void commit() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "commit() : Unimplemented method." );
        
    }

    public Array createArrayOf( String typeName, Object[] elements )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createArrayOf( String typeName, Object[] elements ) : Unimplemented method." );
        return null ;
    }

    public Blob createBlob() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createBlob() : Unimplemented method." );
        return null ;
    }

    public Clob createClob() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createClob() : Unimplemented method." );
        return null ;
    }

    public NClob createNClob() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createNClob() : Unimplemented method." );
        return null ;
    }

    public SQLXML createSQLXML() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createSQLXML() : Unimplemented method." );
        return null ;
    }


    public Statement createStatement( int resultSetType,
            int resultSetConcurrency ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createStatement( int resultSetType, int resultSetConcurrency ) : Unimplemented method." );
        return null ;
    }

    
    public Statement createStatement( int resultSetType,
            int resultSetConcurrency, int resultSetHoldability )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) : Unimplemented method." );
        return null ;
    }

    
    public Struct createStruct( String typeName, Object[] attributes )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "createStruct( String typeName, Object[] attributes ) : Unimplemented method." );
        return null ;
    }

    
    public boolean getAutoCommit() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getAutoCommit() : Unimplemented method." );
        return false ;
    }

    
    public String getCatalog() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getCatalog() : Unimplemented method." );
        return null ;
    }

    
    public Properties getClientInfo() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getClientInfo() : Unimplemented method." );
        return null ;
    }

    
    public String getClientInfo( String name ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getClientInfo( String name ) : Unimplemented method." );
        return null ;
    }

    
    public int getHoldability() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getHoldability() : Unimplemented method." );
        return 0 ;
    }

    
    public DatabaseMetaData getMetaData() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getMetaData() : Unimplemented method." );
        return null ;
    }

    
    public int getTransactionIsolation() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTransactionIsolation() : Unimplemented method." );
        return 0 ;
    }

    
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTypeMap() : Unimplemented method." );
        return null ;
    }

    
    public SQLWarning getWarnings() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getWarnings() : Unimplemented method." );
        return null ;
    }

    
    public boolean isReadOnly() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isReadOnly() : Unimplemented method." );
        return false ;
    }

    
    public boolean isValid( int timeout ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isValid( int timeout ) : Unimplemented method." );
        return false ;
    }

    
    public String nativeSQL( String sql ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "nativeSQL( String sql ) : Unimplemented method." );
        return null ;
    }

    
    public CallableStatement prepareCall( String sql ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareCall( String sql ) : Unimplemented method." );
        return null ;
    }

    
    public CallableStatement prepareCall( String sql, int resultSetType,
            int resultSetConcurrency ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareCall( String sql, int resultSetType, int resultSetConcurrency ) : Unimplemented method." );
        return null ;
    }

    
    public CallableStatement prepareCall( String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareCall( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) : Unimplemented method." );
        return null ;
    }

    
    public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareStatement( String sql, int autoGeneratedKeys ) : Unimplemented method." );
        return null ;
    }

    
    public PreparedStatement prepareStatement( String sql, int[] columnIndexes )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareStatement( String sql, int[] columnIndexes ) : Unimplemented method." );
        return null ;
    }

    
    public PreparedStatement prepareStatement( String sql, String[] columnNames )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareStatement( String sql, String[] columnNames ) : Unimplemented method." );
        return null ;
    }

    
    public PreparedStatement prepareStatement( String sql, int resultSetType,
            int resultSetConcurrency ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) : Unimplemented method." );
        return null ;
    }

    
    public PreparedStatement prepareStatement( String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) : Unimplemented method." );
        return null ;
    }

    
    public void releaseSavepoint( Savepoint savepoint ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "releaseSavepoint( Savepoint savepoint ) : Unimplemented method." );
        
    }

    
    public void rollback() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "rollback() : Unimplemented method." );
        
    }

    
    public void rollback( Savepoint savepoint ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "rollback( Savepoint savepoint ) : Unimplemented method." );
        
    }

    
    public void setAutoCommit( boolean autoCommit ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setAutoCommit( boolean autoCommit ) : Unimplemented method." );
        
    }

    
    public void setCatalog( String catalog ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setCatalog( String catalog ) : Unimplemented method." );
        
    }

    
    public void setClientInfo( Properties properties )
            throws SQLClientInfoException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setClientInfo( Properties properties ) : Unimplemented method." );
        
    }

    
    public void setClientInfo( String name, String value )
            throws SQLClientInfoException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setClientInfo( String name, String value ) : Unimplemented method." );
        
    }

    
    public void setHoldability( int holdability ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setHoldability( int holdability ) : Unimplemented method." );
        
    }

    
    public void setReadOnly( boolean readOnly ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setReadOnly( boolean readOnly ) : Unimplemented method." );
        
    }

    
    public Savepoint setSavepoint() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setSavepoint() : Unimplemented method." );
        return null ;
    }

    
    public Savepoint setSavepoint( String name ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setSavepoint( String name ) : Unimplemented method." );
        return null ;
    }

    
    public void setTransactionIsolation( int level ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setTransactionIsolation( int level ) : Unimplemented method." );
        
    }

    
    public void setTypeMap( Map<String, Class<?>> map ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setTypeMap( Map<String, Class<?>> map ) : Unimplemented method." );
        
    }

    
    public boolean isWrapperFor( Class<?> iface ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isWrapperFor( Class<?> iface ) : Unimplemented method." );
        return false ;
    }

    
    public <T> T unwrap( Class<T> iface ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "unwrap( Class<T> iface ) : Unimplemented method." );
        return null ;
    }

	public void abort(Executor executor) throws SQLException {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "abort(Executor executor) : Unimplemented method." );
	}

	public int getNetworkTimeout() throws SQLException {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNetworkTimeout() : Unimplemented method." );
		return 0;
	}

	public String getSchema() throws SQLException {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getSchema() : Unimplemented method." );
		return null;
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setNetworkTimeout(Executor executor, int milliseconds) : Unimplemented method." );
	}

	public void setSchema(String schema) throws SQLException {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setSchema(String schema) : Unimplemented method." );
	}

}
