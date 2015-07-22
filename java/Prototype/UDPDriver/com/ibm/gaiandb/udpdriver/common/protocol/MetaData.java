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
import java.net.InetAddress;
import java.util.ArrayList;

/**
 * MetaData protocol message.
 * In addition to meta data information, MetaData could contain serialized database records.
 * 
 * The binary format is :
 * |messageHeader|numberOfParameters|numberOfColumns|columnTypes|numberOfNullableColumns|nullableColumns|columnNames|columnScale|columnPrecision|columnDisplaySize|containDatabaseRecords|
 * 
 * if containDatabaseRecords==true, then there is |responseWithValuesBinaryFormat| at the end.
 * 
 * @author lengelle
 *
 */
public class MetaData extends ResponseWithValues
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	ByteArrayOutputStream baos ;
	DataOutputStream daos ;
	boolean metaDataHeaderSerializationDone;
	
    private int numberOfParameters;
    private int numberOfColumns;
    private int numberOfNullableColumns;
    private int[] columnTypeTab;
	private int[] columnScale;
	private int[] columnDisplaySize;
	private int[] columnPrecision;
	private ArrayList<Integer> nullableColumnList;
    
    private String[] columnNameTab;
    
    
    /**
     * Creates a new MetaData
     */
    public MetaData()
    {
    	super();
    	
        numberOfParameters = -1;
        numberOfColumns = -1;
        columnTypeTab = null;
        numberOfNullableColumns = -1;
        nullableColumnList = new ArrayList<Integer>( LIST_INITIAL_SIZE );
        columnNameTab = null;
        
        metaDataHeaderSerializationDone = false;
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues#initializeWithData(byte[], java.net.InetAddress, int)
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
        	throw new UDPProtocolException( "MetaData - constructor failed.", e );
        }
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues#clean()
     */
    public void clean()
    {
    	super.clean();
    	
        numberOfParameters = -1;
        numberOfColumns = -1;
        columnTypeTab = null;
        numberOfNullableColumns = -1;
        nullableColumnList.clear();
        columnNameTab = null;
    	columnScale = null;
    	columnDisplaySize = null;
    	columnPrecision = null;
        
        metaDataHeaderSerializationDone = false;
        baos = null;
        daos = null;
    }
    
    
    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#deserializeData()
     */
    protected void deserializeData() throws UDPProtocolException
    {
    	try
    	{
        	ByteArrayInputStream bais = new ByteArrayInputStream( binaryData );
        	DataInputStream dis = new DataInputStream( bais );
        	
        	dis.readByte(); // the message type
        	queryID = dis.readUTF();
        	sequenceNumber = dis.readInt();
        	
        	numberOfParameters = dis.readInt();
        	numberOfColumns = dis.readInt();
        	
        	columnTypeTab = new int[numberOfColumns];
        	for ( int i=0; i<numberOfColumns; ++i )
        	{
        		columnTypeTab[i] = dis.readInt();
        	}
        	
        	numberOfNullableColumns = dis.readInt();
    		for ( int i=0; i<numberOfNullableColumns; ++i )
    		{
    			nullableColumnList.add( dis.readInt() );
    		}
    		
    		columnNameTab = new String[numberOfColumns];
    		for ( int i=0; i<numberOfColumns; ++i )
    		{
    			columnNameTab[i] = dis.readUTF();
    		}
        	
    		columnScale = new int[numberOfColumns];
        	for ( int i=0; i<numberOfColumns; ++i )
        	{
        		columnScale[i] = dis.readInt();
        	}
        	
    		columnPrecision = new int[numberOfColumns];
        	for ( int i=0; i<numberOfColumns; ++i )
        	{
        		columnPrecision[i] = dis.readInt();
        	}
        	
    		columnDisplaySize = new int[numberOfColumns];
        	for ( int i=0; i<numberOfColumns; ++i )
        	{
        		columnDisplaySize[i] = dis.readInt();
        	}
    		
    		if ( dis.readBoolean() )
    		{
    			// There are serialized values in the message
    			deserializedValuesHelper( dis );
    		}
        	
        	dis.close();
        	bais.close();
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "MetaData deserializeData() failed. ", e );
    	}
    }
    
    
    public int getType()
    {
        return Message.META_DATA;
    }

    
    public int getNumberOfColumns()
    {
        return numberOfColumns;
    }
    
    
    public int getNumberOfNullableColumns()
    {
        return numberOfNullableColumns;
    }
    
    
    public ArrayList<Integer> getNullableColumns()
    {
        return nullableColumnList;
    }
    
    
    public String[] getColumnNames()
    {
        return columnNameTab;
    }
    

    public int[] getColumnTypes()
    {
        return columnTypeTab;
    }
    
    
    public int getNumberOfParameters()
    {
        return numberOfParameters;
    }
    
    
    public void setNumberOfColumns( int numberOfColumns )
    {
    	this.numberOfColumns = numberOfColumns;
    }
    
    
    public void setNumberOfParameters( int numberOfParameters )
    {
		this.numberOfParameters = numberOfParameters;
	}
    
    
    public void setNumberOfNullableColumns( int numberOfNullableColumns )
    {
		this.numberOfNullableColumns = numberOfNullableColumns;
	}


	public void addNullableColumnIndex( int nullableColumnIndex )
	{
		nullableColumnList.add( nullableColumnIndex );
	}


	public void setColumnTypes( int[] columnTypes )
	{
		this.columnTypeTab = columnTypes;
	}


	public void setColumnNames( String[] columnNames )
	{
		this.columnNameTab = columnNames;
	}
	
	
	public int[] getColumnScale()
	{
		return columnScale;
	}


	public void setColumnScale( int[] columnScale )
	{
		this.columnScale = columnScale;
	}
	
	
	public int[] getColumnDisplaySize()
	{
		return columnDisplaySize;
	}


	public void setColumnDisplaySize( int[] columnDisplaySize )
	{
		this.columnDisplaySize = columnDisplaySize;
	}
	
	
	public int[] getColumnPrecision()
	{
		return columnPrecision;
	}


	public void setColumnPrecision( int[] columnPrecision )
	{
		this.columnPrecision = columnPrecision;
	}
	

    /* (non-Javadoc)
     * @see com.ibm.gaiandb.udpdriver.common.protocol.Message#serializeMessage()
     */
    public byte[] serializeMessage() throws UDPProtocolException
	{
    	try
    	{
        	serializeMetaDataHeader();
        	
        	if ( numberOfRows > 0 )
        	{
        		daos.writeBoolean( true );
        		serializeValuesHelper( daos );
        	}
        	else
        	{
        		daos.writeBoolean( false );
        	}
        	
        	daos.close();
        	byte[] serialized = baos.toByteArray();
        	baos.close();
        	
        	return serialized;
    	}
    	catch( Exception e )
    	{
    		throw new UDPProtocolException( "MetaData - serializeMessage() failed. ", e );
    	}
	}
	
	/**
	 * Serialize only the meta data attributes into the stream.
	 * 
	 * @throws UDPProtocolException
	 */
	private void serializeMetaDataHeader() throws UDPProtocolException
	{
		try
		{
			if ( !metaDataHeaderSerializationDone )
			{
		    	baos = new ByteArrayOutputStream( BYTE_ARRAY_INITIAL_SIZE );
		    	daos = new DataOutputStream( baos );
		    	
	        	if ( queryID==null || sequenceNumber==-1 )
	        	{
	        		throw new UDPProtocolException( "The fields 'queryID' or 'sequenceNumber' are not initialized. " );
	        	}
	        	
	        	daos.writeByte( getType() );
	        	daos.writeUTF( queryID );
	        	daos.writeInt( sequenceNumber );

		    	if ( numberOfParameters < 0 )
		    	{
		    		throw new UDPProtocolException( "numberOfParameters field is not initilized." );
		    	}
		    	daos.writeInt( numberOfParameters );
		    	
		    	if ( numberOfColumns <= 0 )
		    	{
		    		throw new UDPProtocolException( "numberOfColumns field is not initilized." );
		    	}
		    	daos.writeInt( numberOfColumns );
		    	
		    	if ( columnTypeTab == null )
		    	{
		    		throw new UDPProtocolException( "columnTypeTab field is not initilized." );
		    	}
		    	for ( int i=0; i<numberOfColumns; ++i )
		    	{
		    		daos.writeInt( columnTypeTab[i] );
		    	}
		    	
		    	if ( numberOfNullableColumns < 0 )
		    	{
		    		throw new UDPProtocolException( "numberOfNullableColumns field is not initilized." );
		    	}
		    	daos.writeInt( numberOfNullableColumns );
		    	
		    	if ( numberOfNullableColumns > 0 )
		    	{
		    		if ( nullableColumnList == null )
		    		{
		    			throw new UDPProtocolException( "nullableColumnList field is not initilized." );
		    		}
		        	for ( int i=0; i<numberOfNullableColumns; ++i )
		        	{
		        		daos.writeInt( nullableColumnList.get( i ) );
		        	}
		    	}
		    	
		    	if ( columnNameTab == null )
		    	{
		    		throw new UDPProtocolException( "columnNameTab field is not initilized." );
		    	}
		    	for ( int i=0; i<numberOfColumns; ++i )
		    	{
		    		daos.writeUTF( columnNameTab[i] );
		    	}
		    	
		    	if ( columnScale == null )
		    	{
		    		throw new UDPProtocolException( "columnScale field is not initilized." );
		    	}
		    	for ( int i=0; i<numberOfColumns; ++i )
		    	{
		    		daos.writeInt( columnScale[i] );
		    	}
		    	
		    	if ( columnPrecision == null )
		    	{
		    		throw new UDPProtocolException( "columnPrecision field is not initilized." );
		    	}
		    	for ( int i=0; i<numberOfColumns; ++i )
		    	{
		    		daos.writeInt( columnPrecision[i] );
		    	}
		    	
		    	if ( columnDisplaySize == null )
		    	{
		    		throw new UDPProtocolException( "columnDisplaySize field is not initilized." );
		    	}
		    	for ( int i=0; i<numberOfColumns; ++i )
		    	{
		    		daos.writeInt( columnDisplaySize[i] );
		    	}
		    	
		    	metaDataHeaderSerializationDone = true;
			}
		}
		catch( Exception e )
		{
			throw new UDPProtocolException( "MetaData - serializeMetaDataHeader() failed. ", e );
		}
	}
	
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues#estimateHeaderSize()
	 */
	public int estimateHeaderSize() throws UDPProtocolException
	{
		try
		{
			serializeMetaDataHeader();
			
			// metaData|boolean containsValues|ResponseWithValuesHeader
			return baos.size() + 1 + estimateHeaderSizeHelper();
		}
		catch( Exception e )
		{
			throw new UDPProtocolException( "MetaData - estimateHeaderSize() failed.", e );
		}
	}

}
