/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.derby.vti.IFastPath;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.udpdriver.common.protocol.MetaData;
import com.ibm.gaiandb.udpdriver.common.protocol.NextValuesRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.NextValuesResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.QueryRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues;

/**
 * ClientState contains information about a client query.
 * 
 * A reference to the Embedded Connection object associated with the query is stored here.
 * 
 * Because multiple RunnableWorkers could access a ClientState at the same time and so execute 
 * non thread safe method at the same time, the ClientState implements a
 * Semaphore. The RunnableWorkers have to acquire the permit to modify the ClientState.
 * 
 * ClientState uses a ValuesEncoder to transform the database records into their binary format.
 * 
 * @author lengelle
 *
 */
public class ClientState
{
	private static final Logger logger = new Logger( "ClientState", 30 );
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	public static int NUMBER_OF_PRESERIALIZED_MESSAGE = 1;
	
	private PreparedStatement preparedStatement;
	private ValuesEncoder valuesEncoder;
	private Connection connection;
	private int statementType;
	
	private int numberOfColumns;
	private int[] columnTypes;
	private ArrayList<Integer> nullableColumns;
	
	private String queryID;
	private ArrayBlockingQueue<NextValuesResponse> nextResponseToSend;
	
	private int lastMessageSequenceNumber;
	
	private Semaphore semaphore;
	
	
	public void acquirePermit() throws Exception
	{
		semaphore.acquire();
	}
	
	public void releasePermit() throws Exception
	{
		semaphore.release();
	}
	
	/**
	 * Instantiates a new ClientState.
	 * 
	 * @param preparedStatement
	 * @param statementType
	 * @throws UDPDriverServerException
	 */
	public ClientState( PreparedStatement preparedStatement, int statementType ) throws UDPDriverServerException
	{
		try
		{
			this.preparedStatement = preparedStatement;
			this.statementType = statementType;
			
			queryID = null;
			lastMessageSequenceNumber = -1;
			valuesEncoder = null;
			connection = preparedStatement.getConnection();
			nextResponseToSend = new ArrayBlockingQueue<NextValuesResponse>( NUMBER_OF_PRESERIALIZED_MESSAGE );
			
			semaphore = new Semaphore( 1 );
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ClientState ClientState() failed.", e );
		}
	}
	
	
	public int getStatementType()
	{
		return statementType;
	}
	
	
	/**
	 * Process a QueryRequest
	 * Return the MetaData message
	 * 
	 * @param queryRequest
	 * @return
	 * @throws UDPDriverServerException
	 */
    public MetaData processQueryRequest( QueryRequest queryRequest ) throws UDPDriverServerException
    {
        try
        {
        	queryID = queryRequest.getQueryID();
        	
        	MetaData response = MessageFactory.getMetaDataMessage( queryRequest.getQueryID() );
        	lastMessageSequenceNumber = queryRequest.getSequenceNumber() + 1;
        	response.setSequenceNumber( lastMessageSequenceNumber );
        	
            ResultSetMetaData rsmd = preparedStatement.getMetaData();
            if ( null == rsmd )
            	throw new Exception("Failed to resolve columns definition ResultSetMetaData from underlying RDBMS for query: " + queryRequest.getQuery());
            
            ParameterMetaData pmd = preparedStatement.getParameterMetaData();
            
            numberOfColumns = rsmd.getColumnCount();
            
            // Set the number of parameters in the response
            response.setNumberOfParameters( pmd.getParameterCount() );
            
            // Set the number of columns in the response
            response.setNumberOfColumns( numberOfColumns );
            
            if ( numberOfColumns > 0 )
            {
            	// Set the column types in the response
            	columnTypes = new int[numberOfColumns];
            	for ( int i=0; i<columnTypes.length; ++i )
            	{
            		columnTypes[i] = rsmd.getColumnType( i+1 );
            	}
            	response.setColumnTypes( columnTypes );
            	
            	
            	// Set the number of nullable columns and their index in the response
            	nullableColumns = new ArrayList<Integer>( numberOfColumns );
                for( int i=0; i<numberOfColumns; ++i )
                {
                    if ( rsmd.isNullable( i+1 ) == 1 )
                    {
                    	nullableColumns.add( i+1 );
                    	response.addNullableColumnIndex( i+1 );
                    }
                }

                response.setNumberOfNullableColumns( nullableColumns.size() );
            	
                // Set the column names
                String[] columnNames = new String[numberOfColumns];
                for( int i=0; i<columnNames.length; ++i )
                {
                	columnNames[i] = rsmd.getColumnName( i+1 );
                }
                response.setColumnNames( columnNames );
                
                // Set the column scale
                int[] columnScale = new int[numberOfColumns];
            	for ( int i=0; i<columnScale.length; ++i )
            	{
            		columnScale[i] = rsmd.getScale( i+1 );
            	}
            	response.setColumnScale( columnScale );
            	
                // Set the column precision
            	int[] columnPrecision = new int[numberOfColumns];
            	for ( int i=0; i<columnPrecision.length; ++i )
            	{
            		columnPrecision[i] = rsmd.getPrecision( i+1 );
            	}
            	response.setColumnPrecision( columnPrecision );

            	// Set the column display size
            	int[] columnDisplaySize = new int[numberOfColumns];
            	for ( int i=0; i<columnDisplaySize.length; ++i )
            	{
            		columnDisplaySize[i] = rsmd.getColumnDisplaySize( i+1 );
            	}
            	response.setColumnDisplaySize( columnDisplaySize );
            	
                // If simple statement, execute and send the first values
            	if ( queryRequest.getStatementType() == Message.STATEMENT )
            	{
                    // Execute the statement
                    executeStatement();
                    
                    // Build the response message adding records from database
                    nextValues( response, valuesEncoder );
            	}
            }
            
            return response;
        }
        catch( Exception e )
        {
            throw new UDPDriverServerException( "ClientState processQueryRequest() failed.", e );
        }
    }
    
    /**
     * Process a ExecuteQueryRequest
     * Return a ExecuteQueryResponse
     * 
     * @param executeQueryRequest
     * @return
     * @throws UDPDriverServerException
     */
    public ExecuteQueryResponse processExecuteQueryRequest( ExecuteQueryRequest executeQueryRequest ) throws UDPDriverServerException
    {
        try
        {
        	
        	// If it is a re-execution, a NextValuesResponse could remain in the queue
        	nextResponseToSend.clear();
        	
        	if ( executeQueryRequest.getSequenceNumber() <= lastMessageSequenceNumber )
        	{
        		// Duplicate message
        		logger.logThreadWarning(GDBMessages.NETDRIVER_CLIENT_MESSAGE_DUPLICATE, "Detected duplicate client message (ignored). Sequence number: " + executeQueryRequest.getSequenceNumber());
        		return null;
        	}
        	
        	ExecuteQueryResponse response = MessageFactory.getExecuteQueryResponseMessage( queryID );
        	lastMessageSequenceNumber = executeQueryRequest.getSequenceNumber() + 1;
        	response.setSequenceNumber( lastMessageSequenceNumber );
        	
        	int[] types = executeQueryRequest.getExecutiveParameterTypes();
            String[] values = executeQueryRequest.getExecutiveParameters();
        	
        	
            if ( types!=null && values!=null ) // Query with parameters '?'
            {
                for ( int i=0; i<values.length; ++i )
                {
                    this.setValueInPreparedStatement( types[i], values[i], i+1 );
                }
            }
            
            // Execute the statement
            executeStatement();
            
            // Build the response message adding records from database
            nextValues( response, valuesEncoder );
            
            return response;
        }
        catch( Exception e )
        {
            throw new UDPDriverServerException( "ClientState processExecuteQueryRequest() failed.", e );
        }
    }
	
    /**
     * Process a NextValuesRequest
     * Return a NextValuesResponse
     * 
     * @param nextValuesRequest
     * @return
     * @throws UDPDriverServerException
     */
    public NextValuesResponse processNextValuesRequest( NextValuesRequest nextValuesRequest ) throws UDPDriverServerException
    {
    	try
    	{
        	if ( nextValuesRequest.getSequenceNumber() <= lastMessageSequenceNumber )
        	{
        		// Duplicate message
        		return null;
        	}
        	
        	if ( connection == null )
        	{
    			// A processCloseRequest has already been executed
    			return null;
        	}
    		
    		// Wait for the response
    		NextValuesResponse response = nextResponseToSend.take();
    		lastMessageSequenceNumber = nextValuesRequest.getSequenceNumber() + 1;
    		response.setSequenceNumber( lastMessageSequenceNumber );
    		
    		return response;
    	}
    	catch( Exception e )
        {
            throw new UDPDriverServerException( "ClientState processNextValuesRequest() failed.", e );
        }
    }
    
    /**
     * Process a CloseRequest.
     * Return the embedded Connection reference which is not used anymore.
     * 
     * @return
     * @throws UDPDriverServerException
     */
    public Connection processCloseRequest() throws UDPDriverServerException
    {
    	try
    	{
    		if ( connection == null )
    		{
    			// A processCloseRequest has already been executed
    			// This execution is probably due to a duplicate CloseRequest message
    			return null;
    		}
    		
        	Connection connectionToReturn = connection;
        	clean();
        	return connectionToReturn;
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ClientState processCloseRequest() failed.", e );
    	}
    }
    
    
    /**
     * Encode the next values of the ResultSet into the ResponseWithValues given as a parameter,
     * thanks to the ValuesEncoder given as a parameter.
     * 
     * @param response
     * @param valuesEncoder
     * @throws UDPDriverServerException
     */
    private void nextValues( ResponseWithValues response, ValuesEncoder valuesEncoder ) throws UDPDriverServerException
    {
    	try
    	{
        	valuesEncoder.encodeNextValues( response );

        	if ( response.containsLastValues() )
            {
        		if ( ! ( preparedStatement instanceof IFastPath ) )
        			preparedStatement.getResultSet().close();
            }
        	
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ClientState nextValues() failed.", e );
    	}
    }
    
    
    /**
     * Serialize the next values and place them into the buffer 'nextResponseToSend'
     * 
     * @throws UDPDriverServerException
     */
    public void serializeNextValues() throws UDPDriverServerException
    {
    	try
    	{
    		if ( connection == null )
    		{
    			// A processCloseRequest has already been executed
    			return;
    		}
    		
            if ( valuesEncoder == null )
            {
            	throw new UDPDriverServerException( "ValuesEncoder is null." );
            }
    		
    		NextValuesResponse nextValuesResponse = MessageFactory.getNextValuesResponseMessage( queryID );
    		
    		nextValues( nextValuesResponse, valuesEncoder );
    		
    		nextResponseToSend.add( nextValuesResponse );
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ClientState serializeNextValues() failed.", e );
    	}
    }
    
    
    /**
     * Execute the Statement
     * 
     * @throws UDPDriverServerException
     */
    private void executeStatement() throws UDPDriverServerException
    {
    	try
    	{
//    		ResultSet resultSet = preparedStatement.executeQuery(); // now done in ValuesEncoder
    		
            if ( null == valuesEncoder )
            	valuesEncoder = new ValuesEncoder( preparedStatement, UDPDriverServer.DATAGRAM_SIZE, columnTypes, nullableColumns );
            else
            	valuesEncoder.reExecutePreparedStatementQuery();
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ClientState executeStatement() failed.", e );
    	}
    }
    
    /**
     * Clean the ClientState
     * 
     * @throws UDPDriverServerException
     */
	private void clean() throws UDPDriverServerException
	{
		try
		{
			preparedStatement.close();
			preparedStatement = null;
			connection = null;
			valuesEncoder = null;
			nextResponseToSend.clear();
			nextResponseToSend = null;
			columnTypes = null;
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ClientState clean() failed.", e );
		}
	}
	
	/**
	 * Set the parameter values to the PreapredStatement
	 * 
	 * @param type
	 * @param value
	 * @param index
	 * @throws UDPDriverServerException
	 */
    private void setValueInPreparedStatement( int type, String value, int index ) throws UDPDriverServerException
    {
        try
        {
            switch ( type )
            {
                case Types.INTEGER : preparedStatement.setInt( index, Integer.decode( value ) ); break;
                case Types.VARCHAR : preparedStatement.setString( index, value ); break;
                case Types.VARBINARY : preparedStatement.setBytes( index, value.getBytes() ); break;
                
                default : 
                	throw new UDPDriverServerException( "PreparedStatement parameter setter for this type is not implemented: " + type );
            }
        }
        catch( Exception e )
        {
            throw new UDPDriverServerException( "ClientState setValueInPreparedStatement() failed.", e );
        }
    }
    
}
