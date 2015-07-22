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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.common.protocol.ExecuteQueryResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.udpdriver.common.protocol.NextValuesRequest;
import com.ibm.gaiandb.udpdriver.common.protocol.NextValuesResponse;
import com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues;

/**
 * Implementation of java.sql.ResultSet
 * UDPResultSet internally uses a ValuesDecoder object to deserialize the records.
 * 
 * @author lengelle
 *
 */
public class UDPResultSet implements ResultSet
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPResultSet", 25 );
	
    private UDPStatement statement;
    private UDPResultSetMetaData metaData;
    private String queryID;
    private int messageSequenceNumber;
	private int wasNull;
    private boolean isClosed;
	private DataValueDescriptor[] currentRow;
	private NextValuesRequest cachedNextValuesRequest;
	private ValuesDecoder valuesDecoder;
	
	/**
	 * Creates a new UDPResultSet object.
	 * 
	 * @param metaData meta data associated with the query
	 * @param statement statement which has generated the UDPResultSet
	 * @param responseWithValues first message containing serialized values
	 * @throws SQLException
	 */
	public UDPResultSet( UDPResultSetMetaData metaData, UDPStatement statement, ResponseWithValues responseWithValues ) throws SQLException
	{
	    try
	    {
	        this.metaData = metaData;
	        this.statement = statement;
	        
	        queryID = responseWithValues.getQueryID();
	        messageSequenceNumber = responseWithValues.getSequenceNumber();
	        
	        wasNull = -1;
	        isClosed = false;
	        currentRow = null;
	        cachedNextValuesRequest = null;
	        
	        valuesDecoder = new ValuesDecoder( this.metaData, responseWithValues );
	        
            if ( !responseWithValues.containsLastValues() )
            {
            	sendRequestToServer( ++messageSequenceNumber );
            }
	    }
	    catch( Exception e )
	    {
	        throw new SQLException( "UDPResultSet - constructor failed. "+ e );
	    }
	}
	
	
    public boolean next() throws SQLException
    {
        try
        {
            currentRow = valuesDecoder.decodeOneRow( currentRow );
            if ( currentRow == null ) // End of datagram is reached
            {
                if ( valuesDecoder.isDecodingLastShipement() )
                {
                    // End
                    currentRow = null;
                    return false;
                }
                else
                {
                    //Get the next message from map
                    Message message = statement.getUDPConnection().retreiveMessageFromMap( 
                    		queryID, ++messageSequenceNumber, Message.NEXT_VALUES_RESPONSE, getStatement().getQueryTimeout()*1000 );
//                    		UDPDriver.FIRST_TIMEOUT ); //statement.getAdaptiveTimeout() );
                    if ( message == null )
                    {
                    	throw new SQLException( "Datagram lost while fetching records !" );
                    }
                    ResponseWithValues newResponse = ( NextValuesResponse )message;
                	
                    
                    // Eventually send a new request to the server
                    if ( !newResponse.containsLastValues() )
                    {
                    	sendRequestToServer( ++messageSequenceNumber );
                    }
                    
                    valuesDecoder.setNewResponseWithValues( newResponse );
                    return next();
                }
            }
            
            return true;
        }
        catch( Exception e )
        { 
            throw new SQLException( "UDPResultSet next() failed. "+e );
        }
    }

    
    private void sendRequestToServer( int sequenceNumber ) throws Exception
    {
        if ( cachedNextValuesRequest==null )
        {
        	cachedNextValuesRequest = MessageFactory.getNextValuesRequestMessage( queryID );
        }
        
    	cachedNextValuesRequest.setSequenceNumber( sequenceNumber );
        statement.getUDPConnection().sendMessage( cachedNextValuesRequest.serializeMessage() );
    }
   
   
    public void close() throws SQLException
    {
        try
        {
            wasNull = -1;
            isClosed = true;
            currentRow = null;
        }
        catch( Exception e )
        {
            throw new SQLException( "UDPResultSet : close failed. "+ e );
        }
    }
   
    /**
     * Clean the UDPResultSet.
     * Once cleaned, the UDPResultSet could no be reused.
     * 
     * @throws SQLException
     */
   public void clean() throws SQLException
   {
       try
       {
           metaData = null;
           statement = null;
           queryID = null;
           messageSequenceNumber = -1;
           wasNull = -1;
           isClosed = true;
           currentRow = null;
           
           if ( cachedNextValuesRequest != null )
           {
               MessageFactory.returnMessage( cachedNextValuesRequest );
           }
           cachedNextValuesRequest = null;
           
           valuesDecoder.close();
           valuesDecoder = null;
       }
       catch( Exception e )
       {
           throw new SQLException( "UDPResultSet : clean failed. "+ e );
       }
   }
   
   /**
    * Recycle the UDPResultSet for the same query with a new message containing serialized values.
    * 
    * @param executeQueryResponse
    * @throws UDPDriverClientException
    */
   public void recycle( ExecuteQueryResponse executeQueryResponse ) throws UDPDriverClientException
   {
       try
       {
           wasNull = -1;
           isClosed = false;
           currentRow = null;
           messageSequenceNumber = executeQueryResponse.getSequenceNumber();
           valuesDecoder.setNewResponseWithValues( executeQueryResponse );
           
           if ( !executeQueryResponse.containsLastValues() )
           {
               sendRequestToServer( ++messageSequenceNumber );
           }
       }
       catch( Exception e )
       {
           throw new UDPDriverClientException( "UDPResultSet : recycle() failed.", e );
       }
   }
    
    
    public Statement getStatement() throws SQLException
    {
        return statement;
    }
    
    
    public int getMessageSequenceNumber()
    {
		return messageSequenceNumber;
	}


	public void setMessageSequenceNumber( int messageSequenceNumber )
	{
		this.messageSequenceNumber = messageSequenceNumber;
	}

    
    public int getInt( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
            return currentRow[ columnIndex-1 ].getInt();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getInt() failed. "+ e + " Value is: " + currentRow[ columnIndex-1 ] );
        }
    }
    
    
    public String getString( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getString();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getString() failed. "+ e );
        }
    }
    
    
    public boolean getBoolean( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getBoolean();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getBoolean() failed. "+ e );
        }
    }
    
    
    public byte getByte( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getByte();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getByte() failed. "+ e );
        }
    }
    
    
    public Date getDate( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getDate( new GregorianCalendar() );
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getDate() failed. "+ e );
        }
    }
    

    public Time getTime( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getTime( new GregorianCalendar() );
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getTime() failed. "+ e );
        }
    }
    

    public Timestamp getTimestamp( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getTimestamp( new GregorianCalendar() );
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getTimestamp() failed. "+ e );
        }
    }
    
    
    public double getDouble( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getDouble();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getDouble() failed. "+ e );
        }
    }
    
    
    public long getLong( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getLong();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getLong() failed. "+ e );
        }
    }
    
    
    public float getFloat( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getFloat();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getFloat() failed. "+ e );
        }
    }
    

    public short getShort( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getShort();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getShort() failed. "+ e );
        }
    }
    

    public byte[] getBytes( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getBytes();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getBytes() failed. "+ e );
        }
    }
    
    
    public Object getObject( int columnIndex ) throws SQLException
    {
        try
        {
        	wasNull = currentRow[ columnIndex-1 ].isNull() ? 1 : 0;
        	return currentRow[ columnIndex-1 ].getObject();
        }
        catch ( Exception e )
        {
            throw new SQLException( "UDPResultSet : getObject() failed. "+ e );
        }
    }
    
    
    public boolean isClosed() throws SQLException
    {
        return isClosed;
    }
    
    
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return metaData;
    }

    
    public boolean wasNull() throws SQLException
    {
        if ( wasNull == -1 )
        {
            throw new SQLException( "UDPResultSet - wasNull() failed. You must read a value first. " );
        }
        return ( wasNull == 1 );
    }
    
    
    public SQLWarning getWarnings() throws SQLException
    {
        return null ;
    }
    
    public boolean absolute( int row ) throws SQLException
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "absolute() : Unimplemented method." );
        return false ;
    }

    
    public void afterLast() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "afterLast() : Unimplemented method." );
        
    }

    
    public void beforeFirst() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "beforeFirst() : Unimplemented method." );
        
    }

    
    public void cancelRowUpdates() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "cancelRowUpdates() : Unimplemented method." );
        
    }

    
    public void clearWarnings() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "clearWarnings() : Unimplemented method." );
        
    }

    
    public void deleteRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "deleteRow() : Unimplemented method." );
        
    }

    
    public int findColumn( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "findColumn( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public boolean first() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "first() : Unimplemented method." );
        return false ;
    }

    
    public Array getArray( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getArray( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public Array getArray( String colName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getArray( String colName ) : Unimplemented method." );
        return null ;
    }

    
    public InputStream getAsciiStream( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getAsciiStream( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public InputStream getAsciiStream( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getAsciiStream( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public BigDecimal getBigDecimal( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBigDecimal( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public BigDecimal getBigDecimal( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBigDecimal( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public BigDecimal getBigDecimal( int columnIndex, int scale )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBigDecimal( int columnIndex, int scale ) : Unimplemented method." );
        return null ;
    }

    
    public BigDecimal getBigDecimal( String columnName, int scale )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBigDecimal( String columnName, int scale ) : Unimplemented method." );
        return null ;
    }

    
    public InputStream getBinaryStream( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBinaryStream( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public InputStream getBinaryStream( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBinaryStream( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Blob getBlob( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBlob( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public Blob getBlob( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBlob( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public boolean getBoolean( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBoolean( String columnName ) : Unimplemented method." );
        return false ;
    }

    
    public byte getByte( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getByte( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public byte[] getBytes( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getBytes( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Reader getCharacterStream( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getCharacterStream( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public Reader getCharacterStream( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getCharacterStream( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Clob getClob( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getClob( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public Clob getClob( String colName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getClob( String colName ) : Unimplemented method." );
        return null ;
    }

    
    public int getConcurrency() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getConcurrency() : Unimplemented method." );
        return 0 ;
    }

    
    public String getCursorName() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getCursorName() : Unimplemented method." );
        return null ;
    }

    
    public Date getDate( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getDate( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Date getDate( int columnIndex, Calendar cal ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getDate( int columnIndex, Calendar cal ) : Unimplemented method." );
        return null ;
    }

    
    public Date getDate( String columnName, Calendar cal ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getDate( String columnName, Calendar cal ) : Unimplemented method." );
        return null ;
    }


    
    public double getDouble( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getDouble( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public int getFetchDirection() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getFetchDirection() : Unimplemented method." );
        return 0 ;
    }

    
    public int getFetchSize() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getFetchSize() : Unimplemented method." );
        return 0 ;
    }

    
    public float getFloat( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getFloat( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public int getHoldability() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getHoldability() : Unimplemented method." );
        return 0 ;
    }

    
    public int getInt( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getInt( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public long getLong( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getLong( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public Reader getNCharacterStream( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNCharacterStream( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public Reader getNCharacterStream( String columnLabel ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNCharacterStream( String columnLabel ) : Unimplemented method." );
        return null ;
    }

    
    public NClob getNClob( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNClob( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public NClob getNClob( String columnLabel ) throws SQLException
    {
        logger.logWarning(  GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNClob( String columnLabel ) : Unimplemented method." );
        return null ;
    }

    
    public String getNString( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNString( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public String getNString( String columnLabel ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getNString( String columnLabel ) : Unimplemented method." );
        return null ;
    }

    
    public Object getObject( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getObject( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Object getObject( int columnIndex, Map<String, Class<?>> map )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getObject( int columnIndex, Map<String, Class<?>> map ) : Unimplemented method." );
        return null ;
    }

    
    public Object getObject( String columnName, Map<String, Class<?>> map )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getObject( String columnName, Map<String, Class<?>> map ) : Unimplemented method." );
        return null ;
    }

    
    public Ref getRef( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getRef( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public Ref getRef( String colName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getRef( String colName ) : Unimplemented method." );
        return null ;
    }

    
    public int getRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getRow() : Unimplemented method." );
        return 0 ;
    }

    
    public RowId getRowId( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getRowId( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public RowId getRowId( String columnLabel ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getRowId( String columnLabel ) : Unimplemented method." );
        return null ;
    }

    
    public SQLXML getSQLXML( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getSQLXML( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public SQLXML getSQLXML( String columnLabel ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getSQLXML( String columnLabel ) : Unimplemented method." );
        return null ;
    }

    
    public short getShort( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getShort( String columnName ) : Unimplemented method." );
        return 0 ;
    }

    
    public String getString( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getString( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Time getTime( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTime( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Time getTime( int columnIndex, Calendar cal ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTime( int columnIndex, Calendar cal ) : Unimplemented method." );
        return null ;
    }

    
    public Time getTime( String columnName, Calendar cal ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTime( String columnName, Calendar cal ) : Unimplemented method." );
        return null ;
    }

    
    public Timestamp getTimestamp( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTimestamp( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public Timestamp getTimestamp( int columnIndex, Calendar cal )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTimestamp( int columnIndex, Calendar cal ) : Unimplemented method." );
        return null ;
    }

    
    public Timestamp getTimestamp( String columnName, Calendar cal )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTimestamp( String columnName, Calendar cal ) : Unimplemented method." );
        return null ;
    }

    
    public int getType() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getType() : Unimplemented method." );
        return 0 ;
    }

    
    public URL getURL( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getURL( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public URL getURL( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getURL( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public InputStream getUnicodeStream( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getUnicodeStream( int columnIndex ) : Unimplemented method." );
        return null ;
    }

    
    public InputStream getUnicodeStream( String columnName )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getUnicodeStream( String columnName ) : Unimplemented method." );
        return null ;
    }

    
    public void insertRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "insertRow() : Unimplemented method." );
        
    }

    
    public boolean isAfterLast() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isAfterLast() : Unimplemented method." );
        return false ;
    }

    
    public boolean isBeforeFirst() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isBeforeFirst() : Unimplemented method." );
        return false ;
    }

    
    public boolean isFirst() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isFirst() : Unimplemented method." );
        return false ;
    }

    
    public boolean isLast() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isLast() : Unimplemented method." );
        return false ;
    }

    
    public boolean last() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "last() : Unimplemented method." );
        return false ;
    }

    
    public void moveToCurrentRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "moveToCurrentRow() : Unimplemented method." );
        
    }

    
    public void moveToInsertRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "moveToInsertRow() : Unimplemented method." );
        
    }

    
    public boolean previous() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "previous() : Unimplemented method." );
        return false ;
    }

    
    public void refreshRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "refreshRow() : Unimplemented method." );
        
    }

    
    public boolean relative( int rows ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "relative( int rows ) : Unimplemented method." );
        return false ;
    }

    
    public boolean rowDeleted() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "rowDeleted() : Unimplemented method." );
        return false ;
    }

    
    public boolean rowInserted() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "rowInserted() : Unimplemented method." );
        return false ;
    }

    
    public boolean rowUpdated() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "rowUpdated() : Unimplemented method." );
        return false ;
    }

    
    public void setFetchDirection( int direction ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setFetchDirection( int direction ) : Unimplemented method." );
        
    }

    
    public void setFetchSize( int rows ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "setFetchSize( int rows ) : Unimplemented method." );
        
    }

    
    public void updateArray( int columnIndex, Array x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateArray( int columnIndex, Array x ) : Unimplemented method." );
        
    }

    
    public void updateArray( String columnName, Array x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateArray( String columnName, Array x ) : Unimplemented method." );
        
    }

    
    public void updateAsciiStream( int columnIndex, InputStream x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateAsciiStream( int columnIndex, InputStream x ) : Unimplemented method." );
        
    }

    
    public void updateAsciiStream( String columnLabel, InputStream x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateAsciiStream( String columnLabel, InputStream x ) : Unimplemented method." );
        
    }

    
    public void updateAsciiStream( int columnIndex, InputStream x, int length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateAsciiStream( int columnIndex, InputStream x, int length ) : Unimplemented method." );
        
    }

    
    public void updateAsciiStream( String columnName, InputStream x, int length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateAsciiStream( String columnName, InputStream x, int length ) : Unimplemented method." );
        
    }

    
    public void updateAsciiStream( int columnIndex, InputStream x, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateAsciiStream( int columnIndex, InputStream x, long length ) : Unimplemented method." );
        
    }

    
    public void updateAsciiStream( String columnLabel, InputStream x,
            long length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateAsciiStream( String columnLabel, InputStream x, long length ) : Unimplemented method." );
        
    }

    
    public void updateBigDecimal( int columnIndex, BigDecimal x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBigDecimal( int columnIndex, BigDecimal x ) : Unimplemented method." );
        
    }

    
    public void updateBigDecimal( String columnName, BigDecimal x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBigDecimal( String columnName, BigDecimal x ) : Unimplemented method." );
        
    }

    
    public void updateBinaryStream( int columnIndex, InputStream x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBinaryStream( int columnIndex, InputStream x ) : Unimplemented method." );
        
    }

    
    public void updateBinaryStream( String columnLabel, InputStream x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBinaryStream( String columnLabel, InputStream x ) : Unimplemented method." );
        
    }

    
    public void updateBinaryStream( int columnIndex, InputStream x, int length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBinaryStream( int columnIndex, InputStream x, int length ) : Unimplemented method." );
        
    }

    
    public void updateBinaryStream( String columnName, InputStream x, int length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBinaryStream( String columnName, InputStream x, int length ) : Unimplemented method." );
        
    }

    
    public void updateBinaryStream( int columnIndex, InputStream x, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBinaryStream( int columnIndex, InputStream x, long length ) : Unimplemented method." );
        
    }

    
    public void updateBinaryStream( String columnLabel, InputStream x,
            long length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBinaryStream( String columnLabel, InputStream x, long length ) : Unimplemented method." );
        
    }

    
    public void updateBlob( int columnIndex, Blob x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBlob( int columnIndex, Blob x ) : Unimplemented method." );
        
    }

    
    public void updateBlob( String columnName, Blob x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBlob( String columnName, Blob x ) : Unimplemented method." );
        
    }

    
    public void updateBlob( int columnIndex, InputStream inputStream )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBlob( int columnIndex, InputStream inputStream ) : Unimplemented method." );
        
    }

    
    public void updateBlob( String columnLabel, InputStream inputStream )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBlob( String columnLabel, InputStream inputStream ) : Unimplemented method." );
        
    }

    
    public void updateBlob( int columnIndex, InputStream inputStream,
            long length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBlob( int columnIndex, InputStream inputStream, long length ) : Unimplemented method." );
        
    }

    
    public void updateBlob( String columnLabel, InputStream inputStream,
            long length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBlob( String columnLabel, InputStream inputStream, long length ) : Unimplemented method." );
        
    }

    
    public void updateBoolean( int columnIndex, boolean x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBoolean( int columnIndex, boolean x ) : Unimplemented method." );
        
    }

    
    public void updateBoolean( String columnName, boolean x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBoolean( String columnName, boolean x ) : Unimplemented method." );
        
    }

    
    public void updateByte( int columnIndex, byte x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateByte( int columnIndex, byte x ) : Unimplemented method." );
        
    }

    
    public void updateByte( String columnName, byte x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateByte( String columnName, byte x ) : Unimplemented method." );
        
    }

    
    public void updateBytes( int columnIndex, byte[] x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBytes( int columnIndex, byte[] x ) : Unimplemented method." );
        
    }

    
    public void updateBytes( String columnName, byte[] x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateBytes( String columnName, byte[] x ) : Unimplemented method." );
        
    }

    
    public void updateCharacterStream( int columnIndex, Reader x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateCharacterStream( int columnIndex, Reader x ) : Unimplemented method." );
        
    }

    
    public void updateCharacterStream( String columnLabel, Reader reader )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateCharacterStream( String columnLabel, Reader reader ) : Unimplemented method." );
        
    }

    
    public void updateCharacterStream( int columnIndex, Reader x, int length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateCharacterStream( int columnIndex, Reader x, int length ) : Unimplemented method." );
        
    }

    
    public void updateCharacterStream( String columnName, Reader reader,
            int length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateCharacterStream( String columnName, Reader reader, int length ) : Unimplemented method." );
        
    }

    
    public void updateCharacterStream( int columnIndex, Reader x, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateCharacterStream( int columnIndex, Reader x, long length ) : Unimplemented method." );
        
    }

    
    public void updateCharacterStream( String columnLabel, Reader reader,
            long length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateCharacterStream( String columnLabel, Reader reader, long length ) : Unimplemented method." );
        
    }

    
    public void updateClob( int columnIndex, Clob x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateClob( int columnIndex, Clob x ) : Unimplemented method." );
        
    }

    
    public void updateClob( String columnName, Clob x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateClob( String columnName, Clob x ) : Unimplemented method." );
        
    }

    
    public void updateClob( int columnIndex, Reader reader )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateClob( int columnIndex, Reader reader ) : Unimplemented method." );
        
    }

    
    public void updateClob( String columnLabel, Reader reader )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateClob( String columnLabel, Reader reader ) : Unimplemented method." );
        
    }

    
    public void updateClob( int columnIndex, Reader reader, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateClob( int columnIndex, Reader reader, long length ) : Unimplemented method." );
        
    }

    
    public void updateClob( String columnLabel, Reader reader, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateClob( String columnLabel, Reader reader, long length ) : Unimplemented method." );
        
    }

    
    public void updateDate( int columnIndex, Date x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateDate( int columnIndex, Date x ) : Unimplemented method." );
        
    }

    
    public void updateDate( String columnName, Date x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateDate( String columnName, Date x ) : Unimplemented method." );
        
    }

    
    public void updateDouble( int columnIndex, double x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateDouble( int columnIndex, double x ) : Unimplemented method." );
        
    }

    
    public void updateDouble( String columnName, double x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateDouble( String columnName, double x ) : Unimplemented method." );
        
    }

    
    public void updateFloat( int columnIndex, float x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateFloat( int columnIndex, float x ) : Unimplemented method." );
        
    }

    
    public void updateFloat( String columnName, float x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateFloat( String columnName, float x ) : Unimplemented method." );
        
    }

    
    public void updateInt( int columnIndex, int x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateInt( int columnIndex, int x ) : Unimplemented method." );
        
    }

    
    public void updateInt( String columnName, int x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateInt( String columnName, int x ) : Unimplemented method." );
        
    }

    
    public void updateLong( int columnIndex, long x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateLong( int columnIndex, long x ) : Unimplemented method." );
        
    }

    
    public void updateLong( String columnName, long x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateLong( String columnName, long x ) : Unimplemented method." );
        
    }

    
    public void updateNCharacterStream( int columnIndex, Reader x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNCharacterStream( int columnIndex, Reader x ) : Unimplemented method." );
        
    }

    
    public void updateNCharacterStream( String columnLabel, Reader reader )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNCharacterStream( String columnLabel, Reader reader ) : Unimplemented method." );
        
    }

    
    public void updateNCharacterStream( int columnIndex, Reader x, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNCharacterStream( int columnIndex, Reader x, long length ) : Unimplemented method." );
        
    }

    
    public void updateNCharacterStream( String columnLabel, Reader reader,
            long length ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNCharacterStream( String columnLabel, Reader reader, long length ) : Unimplemented method." );
        
    }

    
    public void updateNClob( int columnIndex, NClob clob ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNClob( int columnIndex, NClob clob ) : Unimplemented method." );
        
    }

    
    public void updateNClob( String columnLabel, NClob clob )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNClob( String columnLabel, NClob clob ) : Unimplemented method." );
        
    }

    
    public void updateNClob( int columnIndex, Reader reader )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNClob( int columnIndex, Reader reader ) : Unimplemented method." );
        
    }

    
    public void updateNClob( String columnLabel, Reader reader )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNClob( String columnLabel, Reader reader ) : Unimplemented method." );
        
    }

    
    public void updateNClob( int columnIndex, Reader reader, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNClob( int columnIndex, Reader reader, long length ) : Unimplemented method." );
        
    }

    
    public void updateNClob( String columnLabel, Reader reader, long length )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNClob( String columnLabel, Reader reader, long length ) : Unimplemented method." );
        
    }

    
    public void updateNString( int columnIndex, String string )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNString( int columnIndex, String string ) : Unimplemented method." );
        
    }

    
    public void updateNString( String columnLabel, String string )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNString( String columnLabel, String string ) : Unimplemented method." );
        
    }

    
    public void updateNull( int columnIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNull( int columnIndex ) : Unimplemented method." );
        
    }

    
    public void updateNull( String columnName ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateNull( String columnName ) : Unimplemented method." );
        
    }

    
    public void updateObject( int columnIndex, Object x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateObject( int columnIndex, Object x ) : Unimplemented method." );
        
    }

    
    public void updateObject( String columnName, Object x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateObject( String columnName, Object x ) : Unimplemented method." );
        
    }

    
    public void updateObject( int columnIndex, Object x, int scale )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateObject( int columnIndex, Object x, int scale ) : Unimplemented method." );
        
    }

    
    public void updateObject( String columnName, Object x, int scale )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateObject( String columnName, Object x, int scale ) : Unimplemented method." );
        
    }

    
    public void updateRef( int columnIndex, Ref x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateRef( int columnIndex, Ref x ) : Unimplemented method." );
        
    }

    
    public void updateRef( String columnName, Ref x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateRef( String columnName, Ref x ) : Unimplemented method." );
        
    }

    
    public void updateRow() throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateRow() : Unimplemented method." );
        
    }

    
    public void updateRowId( int columnIndex, RowId x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateRowId( int columnIndex, RowId x ) : Unimplemented method." );
        
    }

    
    public void updateRowId( String columnLabel, RowId x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateRowId( String columnLabel, RowId x ) : Unimplemented method." );
        
    }

    
    public void updateSQLXML( int columnIndex, SQLXML xmlObject )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateSQLXML( int columnIndex, SQLXML xmlObject ) : Unimplemented method." );
        
    }

    
    public void updateSQLXML( String columnLabel, SQLXML xmlObject )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateSQLXML( String columnLabel, SQLXML xmlObject ) : Unimplemented method." );
        
    }

    
    public void updateShort( int columnIndex, short x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateShort( int columnIndex, short x ) : Unimplemented method." );
        
    }

    
    public void updateShort( String columnName, short x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateShort( String columnName, short x ) : Unimplemented method." );
        
    }

    
    public void updateString( int columnIndex, String x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateString( int columnIndex, String x ) : Unimplemented method." );
        
    }

    
    public void updateString( String columnName, String x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateString( String columnName, String x ) : Unimplemented method." );
        
    }

    
    public void updateTime( int columnIndex, Time x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateTime( int columnIndex, Time x ) : Unimplemented method." );
        
    }

    
    public void updateTime( String columnName, Time x ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateTime( String columnName, Time x ) : Unimplemented method." );
        
    }

    
    public void updateTimestamp( int columnIndex, Timestamp x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateTimestamp( int columnIndex, Timestamp x ) : Unimplemented method." );
        
    }

    
    public void updateTimestamp( String columnName, Timestamp x )
            throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "updateTimestamp( String columnName, Timestamp x ) : Unimplemented method." );
        
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


	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getObject(int columnIndex, Class<T> type) : Unimplemented method." );
		return null;
	}


	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getObject(String columnLabel, Class<T> type) : Unimplemented method." );
		return null;
	}
}

