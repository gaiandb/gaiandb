/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.client;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.sql.Types;
import java.util.List;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.udpdriver.common.RowsFilter;
import com.ibm.gaiandb.udpdriver.common.protocol.MessageFactory;
import com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues;

/**
 * This object deserialize database records received, in their binary format, from the UDP driver server.
 * Because the meta data are different for each queries, a instance of ValuesDecoder can be used only for a complete processing of one query.
 *
 * @author lengelle
 */
public class ValuesDecoder
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
    private UDPResultSetMetaData metaData;
    private boolean[] columnsWithChar;
    private ResponseWithValues response;

    // Deserialization fields
    private int coefficient;
    private int nullValuesIndex;
    private int numberOfRowRead;
    private int numberOfNullValues;

    private List<Integer> nullValues;

    private ByteArrayInputStream bais;
    private ObjectInput ois;

    /**
     * Instantiates a ValuesDecoder.
     * 
     * @param metaData meta data associated with the query
     * @param response protocol message containing the serialized values
     * @throws UDPDriverClientException
     */
    public ValuesDecoder( UDPResultSetMetaData metaData, ResponseWithValues response ) throws UDPDriverClientException
    {
        try
        {
            this.metaData = metaData;
            this.response = response;

            //Set a boolean array indicating CHAR, VARCHAR, LONGVARCHAR columns.
            columnsWithChar = new boolean[ this.metaData.getColumnCount() ];
            int type;
            for ( int i=0; i<columnsWithChar.length; ++i )
            {
                type = this.metaData.getColumnType( i+1 );
                if ( type==Types.CHAR || type==Types.VARCHAR || type==Types.LONGVARCHAR )
                {
                    columnsWithChar[i] = true;
                }
                else
                {
                    columnsWithChar[i] = false;
                }
            }

            initializeDeserializationFields();

        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "ValuesDecoder - constructor failed. ", e );
        }
    }

    /**
     * Returns a DVD row from the message.
     * If the last records has already been deserialized, returns null.
     * 
     * Takes in parameter a DVD array to recycle. If null, a new DVD array will be
     * instantiated.
     * 
     * @param toRecycle
     * @return
     * @throws UDPDriverClientException
     */
    public DataValueDescriptor[] decodeOneRow( DataValueDescriptor[] toRecycle ) throws UDPDriverClientException
    {
        try
        {
            if ( numberOfRowRead==response.getNumberOfRows() )
            {
                return null;
            }

            DataValueDescriptor[] dvdr = null;
            if ( toRecycle==null )
            {
                dvdr = createNewDVDRowFromMetaData( metaData );
            }
            else
            {
                dvdr = toRecycle;
            }

            DataValueDescriptor dvdTemp = null;

            for( int i=0; i<dvdr.length; ++i )
            {
                dvdTemp = dvdr[i];

                if ( numberOfNullValues!=0 && nullValuesIndex<numberOfNullValues && nullValues.get( nullValuesIndex ).equals( i + coefficient ) )
                {
                    dvdTemp.setToNull();
                    ++nullValuesIndex;
                }
                else
                {
                    dvdTemp.readExternal( ois );

                    //Due to derby serialization issue with empty string "".
                    if ( columnsWithChar[i] && dvdTemp.getLength()==1 )
                    {
                        if ( dvdTemp.getString().equals( "\0" ) )
                        {
                            dvdTemp.setValue( "" );
                        }
                    }
                }

                dvdr[i] = dvdTemp;
            }

            ++numberOfRowRead;
            coefficient = coefficient + dvdr.length;

            return dvdr;
        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "ValuesDecoder - decodeOneRow() failed. ", e );
        }
    }

    /**
     * Set a new protocol message containing serialized values.
     * This new message replaces the previous one.
     * 
     * @param newResponseWithValues
     * @throws UDPDriverClientException
     */
    public void setNewResponseWithValues( ResponseWithValues newResponseWithValues ) throws UDPDriverClientException
    {
        try
        {
            MessageFactory.returnMessage( response );
            response = newResponseWithValues;
            initializeDeserializationFields();
        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "setNewResponseWithValues - closeStreams() failed. ", e );
        }
    }


    private void closeStreams() throws UDPDriverClientException
    {
        try
        {
        	if ( ois != null )
        	{
        		ois.close();
        		ois = null;
        	}

        	if ( bais != null )
        	{
        		bais.close();
        		bais = null;
        	}
        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "ValuesDecoder - closeStreams() failed. ", e );
        }
    }


    private void initializeDeserializationFields() throws UDPDriverClientException
    {
        try
        {
            coefficient = 0;
            nullValuesIndex = 0;
            numberOfRowRead = 0;

            numberOfNullValues = response.getNumberOfNullValues();
            nullValues = response.getNullValues();

            closeStreams();
            
            if ( response.getNumberOfRows()>0 && response.getBinaryValues()!=null )
            {
                bais = new ByteArrayInputStream( response.getBinaryValues() );
                ois = new ObjectInputStream( bais );
            }
        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "ValuesDecoder - initializeDeserializationFields() failed. ", e );
        }
    }


    private DataValueDescriptor[] createNewDVDRowFromMetaData( UDPResultSetMetaData metaData ) throws UDPDriverClientException
    {
        try
        {
            DataValueDescriptor[] dvdr = null;
            int numberOfColumns = metaData.getColumnCount();

            if ( numberOfColumns != 0 )
            {
                dvdr = new DataValueDescriptor[ numberOfColumns ];
                for ( int i=0; i<dvdr.length; ++i )
                {
                    dvdr[i] = RowsFilter.constructDVDMatchingJDBCType( metaData.getColumnType( i+1 ) );
                }
            }
            return dvdr;
        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "ValuesDecoder - createNewDVDRowFromMetaData() failed. ", e );
        }
    }

    public boolean isDecodingLastShipement()
    {
        return response.containsLastValues();
    }

    /**
     * Close the ValuesDecoder.
     * Once closed, the ValuesDecoder could not be used anymore.
     * 
     * @throws UDPDriverClientException
     */
    public void close() throws UDPDriverClientException
    {
        try
        {
            MessageFactory.returnMessage( response );
            response = null;
            columnsWithChar = null;
            nullValues = null;
            closeStreams();
        }
        catch( Exception e )
        {
            throw new UDPDriverClientException( "ValuesDecoder - close() failed. ", e );
        }
    }
    

}
