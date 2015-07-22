/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.gaiandb.udpdriver.common.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;

/**
 * QueryRequest protocol message.
 * 
 * The binary format is :
 * |messageHeader|statementType|SQLQuery
 * 
 * @author lengelle
 *
 */
public class QueryRequest extends Message
{
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private int statementType;
	private String query;
	
	
    /**
     * Creates a new QueryRequest
     */
    public QueryRequest()
    {
    	super();
    	
    	statementType = -1;
    	query = null;
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#initializeWithData(byte[], java.net.InetAddress, int)
     */
    public void initializeWithData( byte[] data, InetAddress emittingAdress, int emittingPort ) throws UDPProtocolException
    {
    	super.initializeWithData( data, emittingAdress, emittingPort );
    	
        try
        {
        	deserializeData();
        }
        catch( Exception e )
        {
        	throw new UDPProtocolException( "QueryRequest - constructor failed.", e );
        }
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#clean()
     */
    public void clean()
    {
    	super.clean();
    }
    

    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#deserializeData()
     */
    protected void deserializeData() throws IOException
    {
    	ByteArrayInputStream bais = new ByteArrayInputStream( binaryData );
    	DataInputStream dis = new DataInputStream( bais );
    	
    	dis.readByte(); // the message type
    	queryID = dis.readUTF();
    	sequenceNumber = dis.readInt();
    	statementType = dis.readInt();
    	query = dis.readUTF();

    	dis.close();
    	bais.close();
    }
    
    
    public String getQuery()
    {
    	return query;
    }
    
    
    public int getStatementType()
    {
        return statementType;
    }

    
    public void setQuery( String query )
    {
    	this.query = query;
    }
    
    
    public void setStatementType( int statementType )
    {
    	this.statementType = statementType;
    }
    
    
    public int getType()
    {
        return Message.QUERY_REQUEST;
    }


    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#serializeMessage()
     */
    public byte[] serializeMessage() throws UDPProtocolException
    {
    	try
    	{
        	ByteArrayOutputStream baos = new ByteArrayOutputStream( BYTE_ARRAY_INITIAL_SIZE );
        	DataOutputStream daos = new DataOutputStream( baos );
        	
        	if ( queryID==null || sequenceNumber==-1 )
        	{
        		throw new UDPProtocolException( "The fields 'queryID' or 'sequenceNumber' are not initialized. " );
        	}
        	
        	daos.writeByte( getType() );
        	daos.writeUTF( queryID );
        	daos.writeInt( sequenceNumber );
        	
        	daos.writeInt( getStatementType() );
        	daos.writeUTF( query );
        	
        	daos.close();
        	byte[] serialized = baos.toByteArray();
        	baos.close();
        	
        	return serialized;
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "QueryResquest - serializeMessage() failed. ", e );
    	}
    }

}
