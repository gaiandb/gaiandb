/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.client;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.udpdriver.common.protocol.CloseRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.udpdriver.common.protocol.MetaData;
import com.ibm.gaiandb.udpdriver.common.protocol.QueryRequest;

/**
 * Implementation of java.sql.PreparedStatement
 * 
 * @author lengelle
 *
 */
public class UDPPreparedStatement extends UDPStatement implements PreparedStatement
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPPreparedStatement", 25 );

    private String query;
    private ParameterMetaData parameterMetaData;
    private int[] parameterTypes;
    private String[] parameterValues;

    /**
     * Instantiates a UDPPreparedStatement object.
     * 
     * @param connection UDPConnection object that produced this statement
     * @param sql SQL query
     * @throws SQLException
     */
    public UDPPreparedStatement( UDPConnection connection, String sql ) throws SQLException
    {
        super( connection );

        query = sql;
        queryID = this.createQueryId();
        messageSequenceNumber = 0;

        createNewEntryInMap();
        prepareQuery(); // Initialize resultSetMetaData and parameterMetaData

        if ( parameterMetaData.getParameterCount()!=0 )
        {
            parameterTypes = new int[ parameterMetaData.getParameterCount() ];
            parameterValues = new String[ parameterMetaData.getParameterCount() ];
        }
    }

    private void prepareQuery() throws SQLException
    {
        try
        {
        	// Prepare and send a QueryRequest
            QueryRequest queryRequest = MessageFactory.getQueryRequestMessage( queryID );
            queryRequest.setSequenceNumber( messageSequenceNumber );
            queryRequest.setStatementType( Message.PREPARED_STATEMENT );
            queryRequest.setQuery( query );

            MetaData metaData = sendQueryRequestAndReturnMetaData( queryRequest );            

            MessageFactory.returnMessage( queryRequest );
            
            resultSetMetaData = new UDPResultSetMetaData( metaData );
            parameterMetaData = new UDPParameterMetaData( metaData );

            MessageFactory.returnMessage( metaData );
        }
        catch( Exception e )
        {
            throw new SQLException( "UDPPreparedStatement prepareQuery() failed. "+ e );
        }
    }

    public void setInt( int parameterIndex, int theInt ) throws SQLException
    {
        // Careful : No security check in this version
        parameterTypes[ parameterIndex-1 ] = Types.INTEGER;
        parameterValues[ parameterIndex-1 ] = ""+theInt;
    }


    public void setString( int parameterIndex, String theString ) throws SQLException
    {
        // Careful : No security check in this version
        parameterTypes[ parameterIndex-1 ] = Types.VARCHAR;
        parameterValues[ parameterIndex-1 ] = theString;
    }


    public void setBytes( int parameterIndex, byte[] theBytes ) throws SQLException
    {
        // Careful : No security check in this version
        parameterTypes[ parameterIndex-1 ] = Types.VARBINARY;
        parameterValues[ parameterIndex-1 ] = new String( theBytes );
    }


    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#executeQuery()
     */
    public ResultSet executeQuery() throws SQLException
    {
        try
        {
            // Build and send an ExecuteQueryRequest
        	ExecuteQueryRequest executeQueryRequest = MessageFactory.getExecuteQueryRequestMessage( queryID );
        	
        	if ( resultSet == null )
        	{
        		executeQueryRequest.setSequenceNumber( ++messageSequenceNumber );
        	}
        	else
        	{
        		// If it is a re-execution, a NextValuesRequest message may have been sent
        		// in advance to the server. In this case the server is preparing, or has already 
        		// prepared and sent back, a response with the previous sequence number + 1.
        		// The solution adopted here is to put a higher sequence number to make a gap between
        		// the latest sequence number sent and the new one.
        		
        		messageSequenceNumber = resultSet.getMessageSequenceNumber();
        		executeQueryRequest.setSequenceNumber( messageSequenceNumber+=5 );
        	}
        	
        	
        	int parameterCount = parameterMetaData.getParameterCount();

        	if ( parameterCount > 0 )
        	{
        		if ( parameterTypes==null || parameterValues==null || parameterTypes.length!=parameterCount || parameterValues.length!=parameterCount )
        		{
        			throw new SQLException( "UDPPreparedStatement executeQuery() failed. PreparedStatement parameters are not correct." );
        		}

        		executeQueryRequest.setNumberOfParameters( parameterCount );
        		executeQueryRequest.setExecutiveParameterTypes( parameterTypes );
        		executeQueryRequest.setExecutiveParameters( parameterValues );
        	}

        	long clock1 = System.currentTimeMillis();
            connection.sendMessage( executeQueryRequest.serializeMessage() );
            MessageFactory.returnMessage( executeQueryRequest );

            // Wait for an ExecuteQueryResponse
            // DRV - 21/09/2011 - Removed use of adaptiveTimeout for ACITA 2011
            // Reason: We need the client app timeout to be consistently higher than the inter-node timeout.
            // Hence if a node disconnect at some point, partial results can still be retrieved from the remaining nodes and we don't just get a "Datagram lost" message...
            Message message = connection.retreiveMessageFromMap( 
            		queryID, ++messageSequenceNumber, Message.EXECUTE_QUERY_RESPONSE, getQueryTimeout()*1000 ); //UDPDriver.FIRST_TIMEOUT ); //adaptiveTimeout );
            if ( message == null )
            {
            	throw new SQLException( "Datagram lost on query execution !" );
            }
            ExecuteQueryResponse executeQueryResponse = ( ExecuteQueryResponse )message;
            long clock2 = System.currentTimeMillis();
            
            adaptiveTimeout = Math.max( adaptiveTimeout, ( ( clock2 - clock1 ) + UDPDriver.TIMEOUT_MARGIN ) );

            if ( resultSet == null )
            {
                resultSet = new UDPResultSet( resultSetMetaData, this, executeQueryResponse );
            }
            else
            {
                resultSet.recycle( executeQueryResponse );
            }

            return resultSet;
        }
        catch( Exception e )
        {
            throw new SQLException( "UDPPreparedStatement executeQuery() failed. "+ e );
        }
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#getMetaData()
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return resultSetMetaData;
    }


    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#execute()
     */
    public boolean execute() throws SQLException
    {
    	resultSet = ( UDPResultSet ) executeQuery();
        return true;
    }


    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.client.UDPStatement#close()
     */
    public void close() throws SQLException
    {
    	try
    	{
    		if ( resultSet != null )
    		{
    			// Send a CLOSE_REQUEST to the server
    			
        		// A NextValuesRequest message may have been sent
        		// in advance to the server. In this case the server is preparing, or has already 
        		// prepared and sent back, a response with the previous sequence number + 1.
        		// The solution adopted here is to put a higher sequence number to make a gap between
        		// the latest sequence number sent and the new one.
    			
    			CloseRequest closeRequest = MessageFactory.getCloseRequestMessage( queryID );
    			closeRequest.setSequenceNumber( resultSet.getMessageSequenceNumber() + 5 );
    			connection.sendMessage( closeRequest.serializeMessage() );
    			MessageFactory.returnMessage( closeRequest );
    		}
    		
    		super.close();

	        resultSetMetaData = null;
	        query = null;
	        parameterMetaData = null;
	        parameterTypes = null;
	        parameterValues = null;
    	}
    	catch( Exception e )
    	{
    		throw new SQLException( "UDPPreparedStatement close() failed. "+ e );
    	}
    }




    
    
    
    








    public void addBatch() throws SQLException
    {
    	logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void clearParameters() throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public int executeUpdate() throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );
        return 0 ;
    }


    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );
        return null ;
    }


    public void setArray( int parameterIndex, Array theArray )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setAsciiStream( int parameterIndex, InputStream x )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setAsciiStream( int parameterIndex, InputStream theInputStream,
            int length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setAsciiStream( int parameterIndex, InputStream x, long length )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBigDecimal( int parameterIndex, BigDecimal theBigDecimal )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBinaryStream( int parameterIndex, InputStream x )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBinaryStream( int parameterIndex,
            InputStream theInputStream, int length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBinaryStream( int parameterIndex, InputStream x, long length )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBlob( int parameterIndex, Blob theBlob ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBlob( int parameterIndex, InputStream inputStream )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBlob( int parameterIndex, InputStream inputStream,
            long length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setBoolean( int parameterIndex, boolean theBoolean )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setByte( int parameterIndex, byte theByte ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setCharacterStream( int parameterIndex, Reader reader )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setCharacterStream( int parameterIndex, Reader reader,
            int length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setCharacterStream( int parameterIndex, Reader reader,
            long length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setClob( int parameterIndex, Clob theClob ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setClob( int parameterIndex, Reader reader )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setClob( int parameterIndex, Reader reader, long length )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setDate( int parameterIndex, Date theDate ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setDate( int parameterIndex, Date theDate, Calendar cal )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setDouble( int parameterIndex, double theDouble )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setFloat( int parameterIndex, float theFloat )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setLong( int parameterIndex, long theLong ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNCharacterStream( int parameterIndex, Reader value )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNCharacterStream( int parameterIndex, Reader value,
            long length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNClob( int parameterIndex, NClob value ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNClob( int parameterIndex, Reader reader )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNClob( int parameterIndex, Reader reader, long length )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNString( int parameterIndex, String value )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNull( int parameterIndex, int sqlType ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setNull( int paramIndex, int sqlType, String typeName )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setObject( int parameterIndex, Object theObject )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setObject( int parameterIndex, Object theObject,
            int targetSqlType ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setObject( int parameterIndex, Object theObject,
            int targetSqlType, int scale ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setRef( int parameterIndex, Ref theRef ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setRowId( int parameterIndex, RowId x ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setSQLXML( int parameterIndex, SQLXML xmlObject )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setShort( int parameterIndex, short theShort )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setTime( int parameterIndex, Time theTime ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setTime( int parameterIndex, Time theTime, Calendar cal )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setTimestamp( int parameterIndex, Timestamp theTimestamp )
            throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setTimestamp( int parameterIndex, Timestamp theTimestamp,
            Calendar cal ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setURL( int parameterIndex, URL theURL ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }


    public void setUnicodeStream( int parameterIndex,
            InputStream theInputStream, int length ) throws SQLException
    {
        logger.logAlways( "UDPPreparedStatement : Unimplemented method." );

    }








}
