/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.gaiandb.udpdriver.client;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.udpdriver.common.protocol.MetaData;
import com.ibm.gaiandb.udpdriver.common.protocol.QueryRequest;

/**
 * Implementation of java.sql.Driver
 * 
 * @author lengelle
 *
 */
public class UDPStatement implements Statement
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPStatement", 25 );

    protected static final int BLOCKING_QUEUE_MAX_SIZE = 5;

    protected UDPConnection connection;
    protected String queryID;
    protected int messageSequenceNumber;
    protected long adaptiveTimeout;
	protected UDPResultSet resultSet;
    protected UDPResultSetMetaData resultSetMetaData;
    
    /**
     * Instantiates a UDPStatement object.
     * 
     * @param connection UDPConnection object that produced this statement
     * @throws SQLException
     */
    public UDPStatement( UDPConnection connection ) throws SQLException
    {
        this.connection = connection;
        this.queryID = "";
        messageSequenceNumber = -1;

        this.resultSet = null;
        this.resultSetMetaData = null;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    public ResultSet executeQuery( String sql ) throws SQLException
    {
        try
        {
            queryID = createQueryId();
            messageSequenceNumber = 0;
            
            createNewEntryInMap();

            // Send the query
            QueryRequest queryRequest = MessageFactory.getQueryRequestMessage( queryID );
            queryRequest.setSequenceNumber( messageSequenceNumber );
            queryRequest.setStatementType( Message.STATEMENT );
            queryRequest.setQuery( sql );

            MetaData metaData = sendQueryRequestAndReturnMetaData( queryRequest );
            
            resultSetMetaData = new UDPResultSetMetaData( metaData );
            MessageFactory.returnMessage( queryRequest );

            // We can't recycle the previous UDPResultSet object because it is link to the previous
            // meta-data. Between two execution of executeQuery(), the meta-data could have changed.
            if ( resultSet != null )
            {
            	resultSet.clean();
            	resultSet = null;
            }
            resultSet = new UDPResultSet( resultSetMetaData, this, metaData );
            return resultSet;
        }
        catch( Exception e )
        {
            throw new SQLException( "UDPStatement : executeQuery() failed. "+ e );
        }
    }
    
    /**
     * 
     * Sends a QueryRequest to the server.
     * Waits for the response MetaData and returns it if received.
     * Else considers that a datagram was lost and throws an exception.
     * 
     * The timeout used for this round trip is : UDPDriver.FIRST_TIMEOUT.
     * If successful, the new timeout will be the time taken by this round trip + UDPDriver.MARGIN_TIMEOUT.
     * 
     * @param queryRequest
     * @return
     * @throws SQLException
     */
	protected MetaData sendQueryRequestAndReturnMetaData( QueryRequest queryRequest ) throws SQLException
	{
		try
		{
	        // Send the query
	        long clock1 = System.currentTimeMillis();
	        connection.sendMessage( queryRequest.serializeMessage() );

	        // Wait for the first response : the Meta-Data
	        Message message = connection.retreiveMessageFromMap( 
	        		queryID, ++messageSequenceNumber, Message.META_DATA, getQueryTimeout()*1000) ; //UDPDriver.FIRST_TIMEOUT );
	        if ( message == null )
	        {
	        	throw new SQLException( "Datagram lost on initial query execution !" );
	        }
	        MetaData metaData = ( MetaData )message;
	        long clock2 = System.currentTimeMillis();
	        
	        adaptiveTimeout = ( clock2 - clock1 ) + UDPDriver.TIMEOUT_MARGIN;
	        
	        return metaData;
		}
		catch( Exception e )
		{
			throw new SQLException( e.getMessage() );
		}
	}
    

    public InetAddress getServerAddress()
    {
        return connection.getAddress();
    }

    public int getServerPort()
    {
        return connection.getPort();
    }

    protected String createQueryId()
    {
        return UDPConnection.createQueryID();
    }

    /**
     * Creates a new entry in the concurrent structure provided by the UDPConnection.
     * The key of this new entry is the queryID.
     * 
     * @throws SQLException
     */
    protected void createNewEntryInMap() throws SQLException
    {
        if ( connection.getMap().containsKey( queryID ) )
        {
            throw new SQLException( "UDPStatement : createNewEntryInMap() Map already contains the key." );
        }
        connection.getMap().put( queryID , new ArrayBlockingQueue<Message>( BLOCKING_QUEUE_MAX_SIZE ) );
    }


    /* (non-Javadoc)
     * @see java.sql.Statement#close()
     */
    public void close() throws SQLException
    {
        if ( connection.getMap()!= null && queryID != null )
        {
            connection.getMap().remove( queryID );
        }

        if ( resultSet != null  )
		{
			resultSet.close();
			resultSet.clean();
        }

        resultSetMetaData = null;
    }
    
    
    /* (non-Javadoc)
     * @see java.sql.Statement#getConnection()
     */
    public Connection getConnection() throws SQLException
    {
        return connection;
    }

    /**
     * Return the UDPConnection object that produced this Statement object.
     * 
     * @return
     */
    public UDPConnection getUDPConnection()
    {
        return connection;
    }
    
    
    /* (non-Javadoc)
     * @see java.sql.Statement#getResultSet()
     */
    public ResultSet getResultSet() throws SQLException
    {
        return resultSet;
    }

    
	public long getAdaptiveTimeout()
	{
		return adaptiveTimeout;
	}

	
	public void setAdaptiveTimeout( long adaptiveTimeout )
	{
		this.adaptiveTimeout = adaptiveTimeout;
	}
	
	
    public int getUpdateCount() throws SQLException
    {
//        logger.logAlways( "UDPStatement getUpdateCount(): Unimplemented method." );
        return -1;
    }
	
    public boolean getMoreResults() throws SQLException
    {
    	return true;
    }
    
    
    public SQLWarning getWarnings() throws SQLException
    {
//        logger.logAlways( "UDPStatement getWarnings(): Unimplemented method." );
        return null;
    }
    
    private int timeout = -1; //(int) (UDPDriver.FIRST_TIMEOUT/1000);

    public void setQueryTimeout( int seconds ) throws SQLException
    {
//        logger.logAlways( "UDPStatement setQueryTimeout( int seconds ): Unimplemented method." );
//    	if ( 5 < seconds ) 
    		timeout = seconds;
    }

    public int getQueryTimeout() throws SQLException
    {
//        logger.logAlways( "UDPStatement getQueryTimeout(): Unimplemented method." );
        return -1 < timeout ? timeout : GaianDBConfig.getNetworkDriverGDBUDPTimeout()/1000;
    }
    









    public void addBatch( String sql ) throws SQLException
    {
    	logger.logAlways( "UDPStatement addBatch( String sql ): Unimplemented method ." );

    }


    public void cancel() throws SQLException
    {
        logger.logAlways( "UDPStatement cancel(): Unimplemented method." );

    }


    public void clearBatch() throws SQLException
    {
        logger.logAlways( "UDPStatement clearBatch(): Unimplemented method." );

    }


    public void clearWarnings() throws SQLException
    {
        logger.logAlways( "UDPStatement clearWarnings(): Unimplemented method." );

    }


    public boolean execute( String sql ) throws SQLException
    {
        logger.logAlways( "UDPStatement execute( String sql ): Unimplemented method." );
        return false ;
    }


    public boolean execute( String sql, int autoGeneratedKeys )
            throws SQLException
    {
        logger.logAlways( "UDPStatement execute( String sql, int autoGeneratedKeys ): Unimplemented method." );
        return false ;
    }


    public boolean execute( String sql, int[] columnIndexes )
            throws SQLException
    {
        logger.logAlways( "UDPStatement execute( String sql, int[] columnIndexes ): Unimplemented method." );
        return false ;
    }


    public boolean execute( String sql, String[] columnNames )
            throws SQLException
    {
        logger.logAlways( "UDPStatement execute( String sql, String[] columnNames ): Unimplemented method." );
        return false ;
    }


    public int[] executeBatch() throws SQLException
    {
        logger.logAlways( "UDPStatement executeBatch(): Unimplemented method." );
        return null ;
    }


    public int executeUpdate( String sql ) throws SQLException
    {
        logger.logAlways( "UDPStatement executeUpdate( String sql ): Unimplemented method." );
        return 0 ;
    }


    public int executeUpdate( String sql, int autoGeneratedKeys )
            throws SQLException
    {
        logger.logAlways( "UDPStatement executeUpdate( String sql, int autoGeneratedKeys ): Unimplemented method." );
        return 0 ;
    }


    public int executeUpdate( String sql, int[] columnIndexes )
            throws SQLException
    {
        logger.logAlways( "UDPStatement executeUpdate( String sql, int[] columnIndexes ): Unimplemented method." );
        return 0 ;
    }


    public int executeUpdate( String sql, String[] columnNames )
            throws SQLException
    {
        logger.logAlways( "UDPStatement executeUpdate( String sql, String[] columnNames ): Unimplemented method." );
        return 0 ;
    }


    public int getFetchDirection() throws SQLException
    {
        logger.logAlways( "UDPStatement getFetchDirection(): Unimplemented method." );
        return 0 ;
    }


    public int getFetchSize() throws SQLException
    {
        logger.logAlways( "UDPStatement getFetchSize(): Unimplemented method." );
        return 0 ;
    }


    public ResultSet getGeneratedKeys() throws SQLException
    {
        logger.logAlways( "UDPStatement getGeneratedKeys(): Unimplemented method." );
        return null ;
    }


    public int getMaxFieldSize() throws SQLException
    {
        logger.logAlways( "UDPStatement getMaxFieldSize(): Unimplemented method." );
        return 0 ;
    }


    public int getMaxRows() throws SQLException
    {
        logger.logAlways( "UDPStatement getMaxRows(): Unimplemented method." );
        return 0 ;
    }


    public boolean getMoreResults( int current ) throws SQLException
    {
        logger.logAlways( "UDPStatement getMoreResults( int current ): Unimplemented method." );
        return false ;
    }
    

    public int getResultSetConcurrency() throws SQLException
    {
        logger.logAlways( "UDPStatement getResultSetConcurrency(): Unimplemented method." );
        return 0 ;
    }


    public int getResultSetHoldability() throws SQLException
    {
        logger.logAlways( "UDPStatement getResultSetHoldability(): Unimplemented method." );
        return 0 ;
    }


    public int getResultSetType() throws SQLException
    {
        logger.logAlways( "UDPStatement getResultSetType(): Unimplemented method." );
        return 0 ;
    }


    public boolean isClosed() throws SQLException
    {
        logger.logAlways( "UDPStatement isClosed(): Unimplemented method." );
        return false ;
    }


    public boolean isPoolable() throws SQLException
    {
        logger.logAlways( "UDPStatement isPoolable(): Unimplemented method." );
        return false ;
    }


    public void setCursorName( String name ) throws SQLException
    {
        logger.logAlways( "UDPStatement setCursorName( String name ): Unimplemented method." );

    }


    public void setEscapeProcessing( boolean enable ) throws SQLException
    {
        logger.logAlways( "UDPStatement setEscapeProcessing( boolean enable ): Unimplemented method." );

    }


    public void setFetchDirection( int direction ) throws SQLException
    {
        logger.logAlways( "UDPStatement setFetchDirection( int direction ): Unimplemented method." );

    }


    public void setFetchSize( int rows ) throws SQLException
    {
        logger.logAlways( "UDPStatement setFetchSize( int rows ): Unimplemented method." );

    }


    public void setMaxFieldSize( int max ) throws SQLException
    {
        logger.logAlways( "UDPStatement setMaxFieldSize( int max ): Unimplemented method." );

    }


    public void setMaxRows( int max ) throws SQLException
    {
        logger.logAlways( "UDPStatement setMaxRows( int max ): Unimplemented method." );

    }


    public void setPoolable( boolean poolable ) throws SQLException
    {
        logger.logAlways( "UDPStatement setPoolable( boolean poolable ): Unimplemented method." );

    }


    public boolean isWrapperFor( Class<?> iface ) throws SQLException
    {
        logger.logAlways( "UDPStatement isWrapperFor( Class<?> iface ): Unimplemented method." );
        return false ;
    }


    public <T> T unwrap( Class<T> iface ) throws SQLException
    {
        logger.logAlways( "UDPStatement unwrap( Class<T> iface ): Unimplemented method." );
        return null ;
    }

	public void closeOnCompletion() throws SQLException {
        logger.logAlways( "UDPStatement closeOnCompletion(): Unimplemented method." );
		
	}

	public boolean isCloseOnCompletion() throws SQLException {
        logger.logAlways( "UDPStatement isCloseOnCompletion(): Unimplemented method." );
		return false;
	}

}
