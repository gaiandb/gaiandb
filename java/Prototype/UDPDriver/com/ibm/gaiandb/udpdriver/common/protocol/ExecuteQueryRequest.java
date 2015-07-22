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
 * ExecuteQueryRequest protocol message.
 * 
 * The binary format is :
 * |messageHeader|numberOfParameters|parameterTypes|parametersValues|
 * 
 * @author lengelle
 *
 */
public class ExecuteQueryRequest extends Message
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";

	private int numberOfParameters;
	private int[] executiveParameterTypes;
	private String[] executiveParameters;
	
    /**
     * Creates a new ExecuteQueryRequest
     */
    protected ExecuteQueryRequest()
    {
    	super();
    	
    	numberOfParameters = 0;
    	executiveParameterTypes = null;
    	executiveParameters = null;
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
        	throw new UDPProtocolException( "ExecuteQueryRequest - constructor failed.", e );
        }
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#clean()
     */
    public void clean()
    {
        super.clean();
        
        numberOfParameters = 0;
        executiveParameterTypes = null;
        executiveParameters = null;
    }
    

    public int getType()
    {
        return Message.EXECUTE_QUERY_REQUEST;
    }

    
	public int getNumberOfParameters()
	{
		return numberOfParameters;
	}


	public void setNumberOfParameters( int numberOfParameters )
	{
		this.numberOfParameters = numberOfParameters;
	}
    
    
    public int[] getExecutiveParameterTypes()
    {
        return executiveParameterTypes;
    }
    
    
    public String[] getExecutiveParameters()
    {
        return executiveParameters;
    }
    
    
	public void setExecutiveParameterTypes( int[] executiveParamterTypes )
	{
		this.executiveParameterTypes = executiveParamterTypes;
	}


	public void setExecutiveParameters( String[] executiveParameters )
	{
		this.executiveParameters = executiveParameters;
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
    	numberOfParameters = dis.readInt();
    	
    	if ( numberOfParameters > 0 )
    	{
    		executiveParameterTypes = new int[numberOfParameters];
    		for ( int i=0; i<numberOfParameters; ++i )
    		{
    			executiveParameterTypes[i] = dis.readInt();
    		}
    		
    		executiveParameters = new String[numberOfParameters];
    		for ( int i=0; i<numberOfParameters; ++i )
    		{
    			executiveParameters[i] = dis.readUTF();
    		}
    	}
    	
    	dis.close();
    	bais.close();
    	
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
        	daos.writeInt( numberOfParameters );
        	
        	if ( numberOfParameters > 0 )
        	{
    	    	if ( executiveParameterTypes == null )
    	    	{
    	    		throw new UDPProtocolException( "executiveParameterTypes field is not initilized." );
    	    	}
    	    	
        		for ( int i=0; i<numberOfParameters; ++i )
        		{
        			daos.writeInt( executiveParameterTypes[i] );
        		}
        		
    	    	if ( executiveParameters == null )
    	    	{
    	    		throw new UDPProtocolException( "executiveParameters field is not initilized." );
    	    	}
    	    	
        		for ( int i=0; i<numberOfParameters; ++i )
        		{
        			daos.writeUTF( executiveParameters[i] );
        		}
        	}
        	
        	daos.close();
        	byte[] serialized = baos.toByteArray();
        	baos.close();
        	
        	return serialized;
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "ExecuteQueryRequest - serializeMessage() failed. ", e );
    	}
    	
    }

}

