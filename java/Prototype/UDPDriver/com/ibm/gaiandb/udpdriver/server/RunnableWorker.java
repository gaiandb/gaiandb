/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;



import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.common.SocketHelper;
import com.ibm.gaiandb.udpdriver.common.protocol.CloseRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.udpdriver.common.protocol.MetaData;
import com.ibm.gaiandb.udpdriver.common.protocol.NextValuesRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.NextValuesResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.QueryRequest;

/**
 * RunnableWorker aims to be run on a thread from a thread pool.
 * 
 * It is started by the server listener and processes a protocol Message.
 * 
 * It contains a static concurrent map , shared with the other workers, containing all
 * the information about each client, identified by their query ID, and their current state (ClientState object).
 * Depending on the type of message it has to process, a RunnableWorker may need to modify this structure to
 * add/modify information about a client.
 * 
 * Once it has processed the message, the RunnableWorker has finished its job and the thread will
 * be return to the pool.
 * 
 * Because multiple RunnableWorker could access a ClientState at the same time, this object is
 * protected by a Semaphore. A RunnableWorker has to acquire the permit to modify the ClientState.
 * 
 * @author lengelle
 *
 */
public class RunnableWorker implements Runnable
{
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "RunnableWorker", 25 );
	
	/**
	 * The concurrent structure that contains the ClientState associated with each query
	 */
    private static ConcurrentMap< String, ClientState > clientMap = new ConcurrentHashMap< String, ClientState >();
    
    private SocketHelper socket;
    private Message message;
    private ConnectionManager connectionManagerForPreparedStatement;
    private ConnectionManager connectionManagerForStatement;
    
    public RunnableWorker(  Message message, SocketHelper socket, ConnectionManager connectionManagerForPreparedStatement, ConnectionManager connectionManagerForStatement ) throws SQLException
    {
        this.socket = socket;
        this.message = message;
        this.connectionManagerForPreparedStatement = connectionManagerForPreparedStatement;
        this.connectionManagerForStatement = connectionManagerForStatement;
    }
    
    /**
     * Process the message.
     */
    public void run()
    {
        try
        {
            switch ( message.getType() )
            {
            
                case Message.QUERY_REQUEST :
                	
                	QueryRequest queryRequest = ( QueryRequest )message;

                	processQueryRequest( queryRequest );
                	
                    break;
                
                
                case Message.EXECUTE_QUERY_REQUEST :
                	
                	ExecuteQueryRequest executeQueryRequest = ( ExecuteQueryRequest )message;
                	
                	processExecuteQueryRequest( executeQueryRequest );
                    
                	break;
                    
                    
                case Message.NEXT_VALUES_REQUEST :
            
                	NextValuesRequest nextValuesRequest = ( NextValuesRequest )message;
                	
                	processNextValuesRequest( nextValuesRequest );
                    
                    break;
                    
                    
                case Message.CLOSE_REQUEST :
                	
                	CloseRequest closeRequest = ( CloseRequest )message;
                	
                	processCloseRequest( closeRequest );
                	
                	break;
                    
                default:
                	
                	throw new UDPDriverServerException( "Message is not conformed." );
            }
        	
        }
        catch( Exception e )
        {
        	logger.logException( GDBMessages.NETDRIVER_SERVER_INTERRUPTED_ERROR, "RunnableWorker Interrupted End." , e );
        }
    }
    
    
    /**
     * Process a QueryRequest.
     * 
     * @param queryRequest
     * @throws UDPDriverServerException
     */
    private void processQueryRequest( QueryRequest queryRequest ) throws UDPDriverServerException
    {
    	try
    	{
    		// Create a ClientState
    		ClientState clientState = createClientState( queryRequest );
    		clientState.acquirePermit();
    		
            // Build the response
    		MetaData response = clientState.processQueryRequest( queryRequest );
    		if ( response == null )
    		{
    			clientState.releasePermit();
    			return;
    		}
    		
    		String key;
    		if ( queryRequest.getStatementType() == Message.STATEMENT )
    		{
        		if ( !response.containsLastValues() )
        		{
        			// Register the ClientState
        			key = getKeyFromMessage( queryRequest );
        			if ( !createNewEntryInMap( key, clientState, clientMap ) )
        			{
        				// Probably a duplicate message
        				clientState.releasePermit();
        				return;
        			}
        			
        			sendMessage( response.serializeMessage(), queryRequest.getEmittingAddress(), queryRequest.getEmittingPort() );
        			
        			clientState.serializeNextValues();
        		}
        		else
        		{
        			sendMessage( response.serializeMessage(), queryRequest.getEmittingAddress(), queryRequest.getEmittingPort() );
        			cleanClientState( clientState, connectionManagerForStatement );
        		}
    		}
    		else
    		{
    			// Register the ClientState
    			key = getKeyFromMessage( queryRequest );
    			if ( !createNewEntryInMap( key, clientState, clientMap ) )
    			{
    				// Probably a duplicate message
    				clientState.releasePermit();
    				return;
    			}
    			
    			sendMessage( response.serializeMessage(), queryRequest.getEmittingAddress(), queryRequest.getEmittingPort() );
    		}

    		clientState.releasePermit();
    		
    		MessageFactory.returnMessage( queryRequest );
    		MessageFactory.returnMessage( response );

    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ThreadWorker processQueryRequest() failed: " + e.getMessage(), e );
    	}
    }
    
    /**
     * Process a ExecuteQueryRequest.
     * 
     * @param executeQueryRequest
     * @throws UDPDriverServerException
     */
    private void processExecuteQueryRequest( ExecuteQueryRequest executeQueryRequest ) throws UDPDriverServerException
    {
    	try
    	{
    		// Obtain a ClientState from the map
    		String key = getKeyFromMessage( executeQueryRequest );
    		ClientState clientState = getEntryFromMap( key, clientMap );
        	if ( clientState == null )
        	{
        		logger.logThreadWarning(GDBMessages.NETDRIVER_CLIENT_QUERY_STATE_LOOKUP_ERROR, "Unable to lookup client query state on server. " +
        				"This node may have been recycled (ignoring client request), queryID: " + executeQueryRequest.getQueryID());
        		return;
        	}
    		
        	clientState.acquirePermit();
        	
            // Build the response
    		ExecuteQueryResponse response = clientState.processExecuteQueryRequest( executeQueryRequest );
        	if ( response == null )
        	{
        		clientState.releasePermit();
        		logger.logThreadWarning(GDBMessages.NETDRIVER_MESSAGE_RESPONSE_BUILD_ERROR, "Unable to build response message (ignoring client request), queryID: " + executeQueryRequest.getQueryID());
        		return;
        	}
    		
    		sendMessage( response.serializeMessage(), executeQueryRequest.getEmittingAddress(), executeQueryRequest.getEmittingPort() );
    		
    		if ( !response.containsLastValues() )
    		{
            	clientState.serializeNextValues();
    		}
    		
    		clientState.releasePermit();
    		
        	MessageFactory.returnMessage( executeQueryRequest );
        	MessageFactory.returnMessage( response );

    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ThreadWorker processExecuteQueryRequest() failed.", e );
    	}
    }
    
    
    /**
     * Process a NextValuesRequest
     * 
     * @param nextValuesRequest
     * @throws UDPDriverServerException
     */
    private void processNextValuesRequest( NextValuesRequest nextValuesRequest ) throws UDPDriverServerException
    {
    	try
    	{
    		// Obtain a ClientState from the map
    		String key = getKeyFromMessage( nextValuesRequest );
        	ClientState clientState = getEntryFromMap( key, clientMap );
        	if ( clientState == null )
        	{
        		return;
        	}
        	
        	clientState.acquirePermit();
    		
        	// Build the response
        	NextValuesResponse response = clientState.processNextValuesRequest( nextValuesRequest );
    		if ( response == null )
    		{
    			clientState.releasePermit();
    			return;
    		}
        	
        	if ( response.containsLastValues() )
        	{
        		if ( clientState.getStatementType() == Message.STATEMENT )
        		{
        			cleanClientState( clientState, connectionManagerForStatement );
        			removeEntryFromMap( key, clientMap );
        		}
        		
        		sendMessage( response.serializeMessage(), nextValuesRequest.getEmittingAddress(), nextValuesRequest.getEmittingPort() );
        		
            	MessageFactory.returnMessage( nextValuesRequest );
            	MessageFactory.returnMessage( response );
            	
        	}
        	else
        	{
        		sendMessage( response.serializeMessage(), nextValuesRequest.getEmittingAddress(), nextValuesRequest.getEmittingPort() );
        		
            	MessageFactory.returnMessage( nextValuesRequest );
            	MessageFactory.returnMessage( response );
            	
            	clientState.serializeNextValues();
            	
        	}
        	
        	clientState.releasePermit();
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ThreadWorker processNextValuesRequest() failed.", e );
    	}
    }
    
    
    /**
     * Process a CloseRequest
     * 
     * @param closeRequest
     * @throws UDPDriverServerException
     */
    private void processCloseRequest( CloseRequest closeRequest ) throws UDPDriverServerException
    {
    	try
    	{
        	// Obtain a ClientState from the map
    		String key = getKeyFromMessage( closeRequest );
        	ClientState clientState = getEntryFromMap( key, clientMap );
        	if ( clientState == null )
        	{
        		return;
        	}
        	
        	clientState.acquirePermit();
        	
        	cleanClientState( clientState, connectionManagerForPreparedStatement );
        	removeEntryFromMap( key, clientMap );
        	
        	clientState.releasePermit();
        	
        	MessageFactory.returnMessage( closeRequest );
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ThreadWorker processCloseRequest() failed.", e );
    	}
    }
    
    
    private void cleanClientState( ClientState clientState, ConnectionManager connectionManager ) throws UDPDriverServerException
    {
    	try
    	{
        	Connection connection = clientState.processCloseRequest();
        	if ( connection == null )
        	{
        		return;
        	}
        	connectionManager.releaseConnection( connection );
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ThreadWorker removeClientState() failed.", e );
    	}
    }

    
    private ClientState createClientState( QueryRequest queryRequest ) throws UDPDriverServerException
    {
        try
        {
        	ConnectionManager connectionManager = null;
        	
        	if ( queryRequest.getStatementType() == Message.STATEMENT )
        	{
        		connectionManager = connectionManagerForStatement;
        	}
        	else
        	{
        		connectionManager = connectionManagerForPreparedStatement;
        	}
        	
        	Connection connection = connectionManager.getConnection();
        	PreparedStatement preparedStatement = connection.prepareStatement( queryRequest.getQuery() );
        	
        	ClientState clientState = new ClientState( preparedStatement, queryRequest.getStatementType() );
        	
            return clientState;
        }
        catch( Exception e )
        {
            throw new UDPDriverServerException( "ThreadWorker createClientState() failed." + e.getMessage(), e );
        }
    }
    
    /**
     * Create a new entry in the map.
     * Return true if successful.
     * 
     * @param key
     * @param clientState
     * @param map
     * @return
     * @throws UDPDriverServerException
     */
    private boolean createNewEntryInMap( String key, ClientState clientState, ConcurrentMap< String, ClientState > map ) throws UDPDriverServerException
    {
    	if ( map == null )
    	{
    		throw new UDPDriverServerException( "ThreadWorker createNewEntryInMap() failed. Map is null." );
    	}
    	
        if ( !map.containsKey( key ) )
        {
        	map.put( key, clientState );
        	return true;
        }
        
        return false;
    }
    
    /**
     * Get a reference on a ClientState in the map.
     * Return null if the ClientState associated with the key is not found.
     * 
     * @param key
     * @param map
     * @return
     * @throws UDPDriverServerException
     */
    private ClientState getEntryFromMap( String key, ConcurrentMap< String, ClientState > map ) throws UDPDriverServerException
    {
    	if ( map == null )
    	{
    		throw new UDPDriverServerException( "ThreadWorker getEntryFromMap() failed. Map is null." );
    	}
    	
    	ClientState clientState = map.get( key );
    	
    	return clientState;
    }

    private void removeEntryFromMap( String key, ConcurrentMap< String, ClientState > map ) throws UDPDriverServerException
    {
    	if ( map == null )
    	{
    		throw new UDPDriverServerException( "ThreadWorker removeEntryFromMap() failed. Map is null." );
    	}
    	
    	map.remove( key );
    }    
    
    /**
     * Create a unique key from a message.
     * The key is a concatenation of the queryID, the client address and the client port.
     * 
     * @param message
     * @return
     */
    private String getKeyFromMessage( Message message )
    {
    	String queryID = message.getQueryID();
    	
    	StringBuffer sb = new StringBuffer( message.getQueryID().length() + 15 ); // Approximatively port + host
    	sb.append( message.getEmittingAddress() );
    	sb.append( message.getEmittingPort() );
    	sb.append( queryID );
    	
    	return sb.toString();
    }
    
    /**
     * Send a message through the network.
     * If the message size exceeds the datagram size, a warning is logged.
     * 
     * @param message
     * @param address
     * @param port
     * @throws UDPDriverServerException
     */
    private void sendMessage( byte[] message, InetAddress address, int port ) throws UDPDriverServerException
    {
    	try
    	{
	    	if ( message.length > UDPDriverServer.DATAGRAM_SIZE )
	    	{
	    		//For debug purpose, because it is an expensive operation
	    		//ResponseWithValues rwv = (ResponseWithValues)MessageFactory.getMessage( message );	
	    		//logger.logWarning( "A message longer than the current datagram size has been sent. Message type : " + message[0] +" NumberOfRows: "+rwv.getNumberOfRows()+" totalSize: "+message.length+" compared to datagramSize: "+UDPDriverServer.DATAGRAM_SIZE );
	    		logger.logWarning( GDBMessages.NETDRIVER_MESSAGE_LONGER_THAN_DATAGRAM, "A message longer than the current datagram size has been sent. Message type : " + message[0] );
	    		//MessageFactory.returnMessage( rwv );
	    	}

	    	socket.send( message, address, port );
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "RunnableWorker sendMessage() failed.", e );
    	}
    }
    

}
