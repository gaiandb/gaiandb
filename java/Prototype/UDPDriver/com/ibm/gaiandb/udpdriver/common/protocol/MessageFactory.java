/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.common.protocol;

import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * MessageFactory implements message pools. Each message obtained from MessageFactory has
 * to be return to the pool by using the static method 
 * MessageFactory.returnMessage(message);
 *
 * Example:
 * QueryRequest qr = MessageFactory.getQueryRequestMessage( queryID );
 * ...
 * ...
 * MessageFactory.returnMessage(qr);
 * 
 * @author lengelle
 *
 */
public abstract class MessageFactory
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final int MESSAGE_POOL_MAX_SIZE = 30;
	
	private static ArrayBlockingQueue<QueryRequest> queryRequestList;
	private static ArrayBlockingQueue<MetaData> metaDataList;
	private static ArrayBlockingQueue<ExecuteQueryRequest> executeQueryRequestList;
	private static ArrayBlockingQueue<ExecuteQueryResponse> executeQueryResponseList;
	private static ArrayBlockingQueue<NextValuesRequest> nextValuesRequestList;
	private static ArrayBlockingQueue<NextValuesResponse> nextValuesResponseList;
	private static ArrayBlockingQueue<CloseRequest> closeRequestList;
//	private static ArrayBlockingQueue<QueryStatusReport> queryStatusReportList; DRVV
	
	static
	{
		queryRequestList = new ArrayBlockingQueue<QueryRequest>( MESSAGE_POOL_MAX_SIZE );
		metaDataList = new ArrayBlockingQueue<MetaData>( MESSAGE_POOL_MAX_SIZE );
		executeQueryRequestList = new ArrayBlockingQueue<ExecuteQueryRequest>( MESSAGE_POOL_MAX_SIZE );
		executeQueryResponseList = new ArrayBlockingQueue<ExecuteQueryResponse>( MESSAGE_POOL_MAX_SIZE );
		nextValuesRequestList = new ArrayBlockingQueue<NextValuesRequest>( MESSAGE_POOL_MAX_SIZE );
		nextValuesResponseList = new ArrayBlockingQueue<NextValuesResponse>( MESSAGE_POOL_MAX_SIZE );
		closeRequestList = new ArrayBlockingQueue<CloseRequest>( MESSAGE_POOL_MAX_SIZE );
	}
	
	/**
	 * Returns a Message object according to the data provided as a parameter.
	 * 
	 * @param data
	 * @param emittingAdress
	 * @param emittingPort
	 * @return
	 * @throws UDPProtocolException
	 */
    public static Message getMessage( byte[] data, InetAddress emittingAdress, int emittingPort ) throws UDPProtocolException
    {
    	try
    	{
	        int messageType = new Integer( data[0] );
	        Message message = null;
	        
	        switch ( messageType )
	        {
	            case Message.QUERY_REQUEST : 
	            	message = getQueryRequestMessage();
	            	break;
	            	
	            case Message.META_DATA :
	            	message = getMetaDataMessage();
	            	break;
	            	
	            case Message.EXECUTE_QUERY_REQUEST : 
	            	message = getExecuteQueryRequestMessage();
	            	break;
	            	
	            case Message.EXECUTE_QUERY_RESPONSE : 
	            	message = getExecuteQueryResponseMessage();
	            	break;
	            	
	            case Message.NEXT_VALUES_REQUEST : 
	            	message = getNextValuesRequestMessage();
	            	break;
	            	
	            case Message.NEXT_VALUES_RESPONSE : 
	            	message = getNextValuesResponseMessage(); 
	            	break;
	            	
	            case Message.CLOSE_REQUEST : 
	            	message = getCloseRequestMessage();
	            	break;

//	            case Message.QUERY_STATUS_REPORT : 
//	            	message = getCloseRequestMessage();
//	            	break; // DRVV
	            	
	            default : 
	            	return null;
	        }
	        
        	message.initializeWithData( data, emittingAdress, emittingPort );
        	return message;
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "MessageFactory - createMessage() failed.", e );
    	}
    }
    
    
    public static Message getMessage( byte[] data ) throws UDPProtocolException
    {
        return getMessage( data, null, -1 );
    }
    
    
    public static QueryRequest getQueryRequestMessage( String queryID ) throws UDPProtocolException
    {
    	QueryRequest queryRequest = getQueryRequestMessage();
    	queryRequest.setQueryID( queryID );
    	return queryRequest;
    }
    
    public static MetaData getMetaDataMessage( String queryID ) throws UDPProtocolException
    {
    	MetaData metaData = getMetaDataMessage();
    	metaData.setQueryID( queryID );
    	return metaData;
    }
    
    public static ExecuteQueryRequest getExecuteQueryRequestMessage( String queryID ) throws UDPProtocolException
    {
    	ExecuteQueryRequest executeQueryRequest = getExecuteQueryRequestMessage();
    	executeQueryRequest.setQueryID( queryID );
    	return executeQueryRequest;
    }
    
    public static ExecuteQueryResponse getExecuteQueryResponseMessage( String queryID ) throws UDPProtocolException
    {
    	ExecuteQueryResponse executeQueryResponse = getExecuteQueryResponseMessage();
    	executeQueryResponse.setQueryID( queryID );
    	return executeQueryResponse;
    }
    
    public static NextValuesRequest getNextValuesRequestMessage( String queryID ) throws UDPProtocolException
    {
    	NextValuesRequest nextValuesRequest = getNextValuesRequestMessage();
    	nextValuesRequest.setQueryID( queryID );
    	return nextValuesRequest;
    }
    
    public static NextValuesResponse getNextValuesResponseMessage( String queryID ) throws UDPProtocolException
    {
    	NextValuesResponse nextValuesResponse = getNextValuesResponseMessage();
    	nextValuesResponse.setQueryID( queryID );
    	return nextValuesResponse;
    }
    
    public static CloseRequest getCloseRequestMessage( String queryID ) throws UDPProtocolException
    {
    	CloseRequest closeRequest = getCloseRequestMessage();
    	closeRequest.setQueryID( queryID );
    	return closeRequest;
    }
    
    
    
    
    public static QueryRequest getQueryRequestMessage() throws UDPProtocolException
    {
    	QueryRequest message = queryRequestList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new QueryRequest();
    	}
    }
    
    
    public static MetaData getMetaDataMessage() throws UDPProtocolException
    {
    	MetaData message = metaDataList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new MetaData();
    	}
    }
    
    
    public static ExecuteQueryRequest getExecuteQueryRequestMessage() throws UDPProtocolException
    {
    	ExecuteQueryRequest message = executeQueryRequestList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new ExecuteQueryRequest();
    	}
    }
    
    
    public static ExecuteQueryResponse getExecuteQueryResponseMessage() throws UDPProtocolException
    {
    	ExecuteQueryResponse message = executeQueryResponseList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new ExecuteQueryResponse();
    	}
    }
    
    
    public static NextValuesRequest getNextValuesRequestMessage() throws UDPProtocolException
    {
    	NextValuesRequest message = nextValuesRequestList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new NextValuesRequest();
    	}
    }
    

    public static NextValuesResponse getNextValuesResponseMessage() throws UDPProtocolException
    {
    	NextValuesResponse message = nextValuesResponseList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new NextValuesResponse();
    	}
    }

    
    public static CloseRequest getCloseRequestMessage() throws UDPProtocolException
    {
    	CloseRequest message = closeRequestList.poll();
    	
    	if ( message != null )
    	{
    		return message;
    	}
    	else
    	{
    		return new CloseRequest();
    	}
    }
    
//    public static CloseRequest getQueryStatusReportMessage() throws UDPProtocolException
//    {
//    	CloseRequest message = closeRequestList.poll();
//    	
//    	if ( message != null )
//    	{
//    		return message;
//    	}
//    	else
//    	{
//    		return new CloseRequest();
//    	}
//    }    
    
    
    public static void returnMessage( Message message )
    {
        int messageType = message.getType();
        
        switch ( messageType )
        {
            case Message.QUERY_REQUEST : returnQueryRequest( ( QueryRequest )message ); return;
            case Message.META_DATA : returnMetaData( ( MetaData )message ); return;
            case Message.EXECUTE_QUERY_REQUEST : returnExecuteQueryRequest( ( ExecuteQueryRequest )message ); return;
            case Message.EXECUTE_QUERY_RESPONSE : returnExecuteQueryResponse( ( ExecuteQueryResponse )message ); return;
            case Message.NEXT_VALUES_REQUEST : returnNextValuesRequest( ( NextValuesRequest )message ); return;
            case Message.NEXT_VALUES_RESPONSE : returnNextValuesResponse( ( NextValuesResponse )message ); return;
            case Message.CLOSE_REQUEST : returnCloseRequest( ( CloseRequest )message ); break;
            
            default : return;
        }
    }
    
    
    private static void returnQueryRequest( QueryRequest queryRequest )
    {
    	queryRequest.clean();
    	
    	if ( !queryRequestList.offer( queryRequest ) )
    	{
    		queryRequest = null;
    	}
    }
    
    
    private static void returnMetaData( MetaData metaData )
    {
    	metaData.clean();
    	
    	if ( !metaDataList.offer( metaData ) )
    	{
    		metaData = null;
    	}
    }
    
    
    private static void returnExecuteQueryRequest( ExecuteQueryRequest executeQueryRequest )
    {
    	executeQueryRequest.clean();
    	
    	if ( !executeQueryRequestList.offer( executeQueryRequest ) )
    	{
    		executeQueryRequest = null;
    	}
    }
    
    
    private static void returnExecuteQueryResponse( ExecuteQueryResponse executeQueryResponse )
    {
    	executeQueryResponse.clean();
    	
    	if ( !executeQueryResponseList.offer( executeQueryResponse ) )
    	{
    		executeQueryResponse = null;
    	}
    }
    
    
    private static void returnNextValuesRequest( NextValuesRequest nextValuesRequest )
    {
    	nextValuesRequest.clean();
    	
    	if ( !nextValuesRequestList.offer( nextValuesRequest ) )
    	{
    		nextValuesRequest = null;
    	}
    }
    
    
    private static void returnNextValuesResponse( NextValuesResponse nextValuesResponse )
    {
    	nextValuesResponse.clean();
    	
    	if ( !nextValuesResponseList.offer( nextValuesResponse ) )
    	{
    		nextValuesResponse = null;
    	}
    }
    
    
    private static void returnCloseRequest( CloseRequest closeRequest )
    {
    	closeRequest.clean();
    	
    	if ( !closeRequestList.offer( closeRequest ) )
    	{
    		closeRequest = null;
    	}
    }
    
}
