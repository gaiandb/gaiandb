/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.gaiandb.udpdriver.common.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;

/**
 * ResponseWithValues is a subclass of Message.
 * It groups useful attributes and methods for messages containing database records.
 * 
 * The binary format of database records is :
 * |containsLastValues|numberOfRows|numberOfNullValues|nullValues|serializedDVDs|
 * 
 * @author lengelle
 *
 */
public abstract class ResponseWithValues extends Message
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	
    protected boolean containsLastValues;
	protected int numberOfRows;
    protected int numberOfNullValues;
    protected byte[] serializedDVDs;
    protected ArrayList<Integer> nullValues;
    
	
    protected ResponseWithValues()
    { 
    	super();
    	
    	containsLastValues = false;
    	numberOfRows = 0;
    	numberOfNullValues = 0;
    	nullValues = new ArrayList<Integer>( LIST_INITIAL_SIZE );
    	serializedDVDs = null;
    }

    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#initializeWithData(byte[], java.net.InetAddress, int)
     */
    public void initializeWithData( byte[] data, InetAddress emittingAdress, int emittingPort ) throws UDPProtocolException
    {
    	super.initializeWithData( data, emittingAdress, emittingPort );
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#clean()
     */
    public void clean()
    {
    	super.clean();
    	
    	containsLastValues = false;
    	numberOfRows = 0;
    	numberOfNullValues = 0;
    	nullValues.clear();
    	serializedDVDs = null;
    }
    
    
    public void setContainsLastValues( boolean containsLastValues )
    {
		this.containsLastValues = containsLastValues;
	}

	public void setNumberOfRows( int numberOfRows )
	{
		this.numberOfRows = numberOfRows;
	}

	public void setNumberOfNullValues( int numberOfNullValues )
	{
		this.numberOfNullValues = numberOfNullValues;
	}

	public void addNullValue( int nullValue )
	{
		nullValues.add( nullValue );
	}
	
	public void removeLastNullValue()
	{
		nullValues.remove( nullValues.size() - 1 );
	}

	public void setSerializedDVDs( byte[] values )
	{
		this.serializedDVDs = values;
	}

	
    public boolean containsLastValues()
    {
    	return containsLastValues;
    }

    
    public int getNumberOfRows()
    {
    	return numberOfRows;
    }
    
    
    public byte[] getBinaryValues()
    {
    	return serializedDVDs;
    }
    
    
    public int getNumberOfNullValues()
    {
    	return numberOfNullValues;
    }
    
    
    public ArrayList<Integer> getNullValues()
    {
    	return nullValues;
    }
    
    /**
     * Returns the exact number of bytes taken by the serialized header format without serializing the header.
     * 
     * @return
     * @throws UDPProtocolException
     */
    public abstract int estimateHeaderSize() throws UDPProtocolException;
    
    /**
     * Returns the exact number of bytes taken by the ResponseWithValues attributes 
     * 
     * @return
     */
    protected int estimateHeaderSizeHelper()
    {
    	int acc = 1; // containsLastValues
    	acc += 4; // numberOfRows
    	
    	if ( numberOfRows > 0 )
    	{
    		acc += 4; // numberOfNullValues
    		acc += numberOfNullValues * 4; // nullValues
    	}
    	
    	return acc;
    }
    
    /**
     * Helper method which serialized the ResponseWithValues attributes only.
     * 
     * @param daos
     * @throws UDPProtocolException
     */
    protected void serializeValuesHelper( DataOutputStream daos ) throws UDPProtocolException
    {
    	try
    	{
        	daos.writeBoolean( containsLastValues );
        	daos.writeInt( numberOfRows );
        	
        	if ( numberOfRows > 0 )
        	{
    	    	if ( numberOfNullValues != nullValues.size() )
    	    	{
    	    		throw new UDPProtocolException( "numberOfNullValues field is not initilized." );
    	    	}
        		daos.writeInt( numberOfNullValues );
        		
        		if ( numberOfNullValues > 0 )
        		{
        			for ( int i=0; i<numberOfNullValues; ++i )
        			{
        				daos.writeInt( nullValues.get( i ) );
        			}
        		}
        		
    	    	if ( serializedDVDs == null )
    	    	{
    	    		throw new UDPProtocolException( "serializedDVDs field is not initilized." );
    	    	}
            	daos.writeInt( serializedDVDs.length );
            	daos.write( serializedDVDs );
        	}
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "ResponseWithValues serializeValuesHelper() failed. ", e );
    	}
    }
    
    /**
     * Helper method which deserialized the ResponseWithValues attributes only.
     * 
     * @param dis
     * @throws UDPProtocolException
     */
    protected void deserializedValuesHelper( DataInputStream dis ) throws UDPProtocolException
    {
    	try
    	{
        	containsLastValues = dis.readBoolean();
        	numberOfRows = dis.readInt();
        	
        	if ( numberOfRows > 0 )
        	{
        		numberOfNullValues = dis.readInt();
        		
        		for ( int i=0; i<numberOfNullValues; ++i )
        		{
        			nullValues.add( dis.readInt() );
        		}
        		
        		serializedDVDs = new byte[ dis.readInt() ];
        		dis.read( serializedDVDs );
        	}
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "ResponseWithValues deserializedValuesHelper() failed. ", e );
    	}
    }


}
