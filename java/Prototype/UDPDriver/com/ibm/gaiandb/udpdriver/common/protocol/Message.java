/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.common.protocol;

import java.net.InetAddress;

import com.ibm.gaiandb.udpdriver.client.UDPDriver;

/**
 * Abstract class for the protocol messages.
 * Protocol messages aim to facilitate the usage of data that are sent on the network
 * through UDP datagrams.
 * Protocol messages are in charge of their serialization/deserialization.
 * 
 * The header of all messages has the following binary format:
 * |messageType|queryID|sequenceNumber|
 * 
 * @author lengelle
 *
 */
public abstract class Message
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
    //Static declarations
    public static final int QUERY_REQUEST = 0;
    public static final int META_DATA = 1;
    public static final int EXECUTE_QUERY_REQUEST = 2;
    public static final int EXECUTE_QUERY_RESPONSE = 3;
    public static final int NEXT_VALUES_REQUEST = 4;
    public static final int NEXT_VALUES_RESPONSE = 5;
    public static final int CLOSE_REQUEST = 6;
//    public static final int QUERY_STATUS_REPORT = 7; DRVV

	public static final int STATEMENT = 1;
	public static final int PREPARED_STATEMENT = 2;
	
	protected static final int LIST_INITIAL_SIZE = 100;
	protected static final int BYTE_ARRAY_INITIAL_SIZE = UDPDriver.DATAGRAM_SIZE;
    
	protected InetAddress emittingAddress;
    protected int emittingPort;
    
    protected String queryID;
    protected int sequenceNumber;
	protected byte[] binaryData;
    

    protected Message()
    { 
    	queryID = null;
    	sequenceNumber = -1;
    	emittingAddress = null;
    	emittingPort = -1;
    	binaryData = null;
    }
    
    
    /**
     * Initialize the message attributes with data coming from the network.
     * 
     * @param data
     * @param emittingAdress
     * @param emittingPort
     * @throws UDPProtocolException
     */
    public void initializeWithData( byte[] data, InetAddress emittingAdress, int emittingPort ) throws UDPProtocolException
    {
        this.emittingAddress = emittingAdress;
        this.emittingPort = emittingPort;
        binaryData = data;
    }
    
    
    /**
     * Return the message type.
     * @return the message type
     */
    public abstract int getType();
    

    /**
     * Serialize the message attributes and return them in a byte[]
     * 
     * @return
     * @throws UDPProtocolException
     */
    public abstract byte[] serializeMessage() throws UDPProtocolException;
    
    /**
     * Deserialize the data from the byte array attribute : binaryData
     * 
     * @throws UDPProtocolException
     */
    protected abstract void deserializeData() throws Exception;
    
    public void setQueryID( String queryID )
    {
    	this.queryID = queryID;
    }
    
    
    public String getQueryID()
	{
		return queryID;
	}    
    
    
    public int getSequenceNumber()
    {
		return sequenceNumber;
	}


	public void setSequenceNumber( int sequenceNumber )
	{
		this.sequenceNumber = sequenceNumber;
	}
    
    
    public InetAddress getEmittingAddress()
    {
        return emittingAddress ;
    }
    
    
    public int getEmittingPort()
    {
        return emittingPort ;
    }

    /**
     * Clean the message attributes.
     */
    public void clean()
    {
    	queryID = null;
    	sequenceNumber = -1;
    	emittingAddress = null;
    	emittingPort = -1;
    	binaryData = null;
    }

}
