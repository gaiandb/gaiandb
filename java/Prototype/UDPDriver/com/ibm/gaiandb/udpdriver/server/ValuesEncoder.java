/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.server;

import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;

import com.ibm.gaiandb.udpdriver.common.RowsFilter;
import com.ibm.gaiandb.udpdriver.common.protocol.Message;
import com.ibm.gaiandb.udpdriver.common.protocol.ResponseWithValues;

/**
 * ValuesEncoder transforms database records into a binary format allowing them to be sent
 * through the network.
 * It fetches the records from a java.sql.ResultSet, obtained from the Derby Embedded driver.
 * 
 * A ValuesEncoder instance is linked to a query because of the meta-data. So it could be re-used 
 * in case of re-execution of the query re-executed (PreapredStatement)by setting the new 
 * ResultSet object with the reUseWithNewResultSet() method.
 * 
 * @author lengelle
 *
 */
public class ValuesEncoder
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	
	private int maxLength;
	private int numberOfColumns;
	
	private int[] columnTypes;
	
	private DataValueDescriptor[] currentRow;
	private DataValueDescriptor[] recycleRow;
	
	private boolean[] nullableColumns;
	private boolean[] columnsWithChar;
	
	
	/**
	 * Instantiates a new ValuesEncoder.
	 * 
	 * @param resultSet
	 * @param maxLength the maximum size expected for the records serialized form
	 * @param columnTypes
	 * @param nullableColumnsList
	 * @throws UDPDriverServerException
	 */
	public ValuesEncoder( /*ResultSet resultSet,*/ 
			PreparedStatement preparedStatement, int maxLength, int[] columnTypes, 
			ArrayList<Integer> nullableColumnsList ) throws UDPDriverServerException
	{
		try
		{
//	    	if ( resultSet==null )
//	    	{
//	    		throw new UDPDriverServerException( "ResultSet is null." );
//	    	}
//	        this.resultSet = resultSet;
	        
	        this.preparedStatement = preparedStatement;
	        reExecutePreparedStatementQuery();
	        
	        this.maxLength = maxLength;
	        numberOfColumns = columnTypes.length;
	        this.columnTypes = columnTypes;
	        currentRow = null;

	        // Set a boolean array indicating nullable columns
	        nullableColumns = new boolean[ numberOfColumns ];
	        int nullableColumnsListSize = nullableColumnsList.size();
	        int nullableColumnsListIndex = 0;
	        for ( int i=0; i<nullableColumns.length; ++i )
	        {
	        	if ( nullableColumnsList != null && nullableColumnsListSize != 0 && nullableColumnsListIndex<nullableColumnsListSize && nullableColumnsList.get( nullableColumnsListIndex ).equals( i+1 ) )
	        	{
	        		nullableColumns[i] = true;
	        		++nullableColumnsListIndex;
	        	}
	        	else
	        	{
	        		nullableColumns[i] = false;
	        	}
	        }
	        
	        
	        // Set a boolean array indicating CHAR, VARCHAR, LONGVARCHAR columns
	        columnsWithChar = new boolean[ numberOfColumns ];
	        for ( int i=0; i<columnsWithChar.length; ++i )
	        {
	        	if ( columnTypes[i]==Types.CHAR || columnTypes[i]==Types.VARCHAR || columnTypes[i]==Types.LONGVARCHAR )
	        	{
	        		columnsWithChar[i] = true;
	        	}
	        	else
	        	{
	        		columnsWithChar[i] = false;
	        	}
	        }
	        
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ValuesEncoder constructor failed.", e );
		}
	}
	
	/**
	 * Re-use the ValuesEncoder for a re-execution of the query.
	 * 
	 * @param resultSet the new ResultSet
	 * @throws UDPDriverServerException
	 */
//    public void reUseWithNewResultSet( ResultSet resultSet ) throws UDPDriverServerException
//    {
//    	try
//    	{
//        	this.resultSet = resultSet;
//        	currentRow = null;
//    	}
//    	catch( Exception e )
//    	{
//    		throw new UDPDriverServerException( "ValuesEncoder clean() failed.", e );
//    	}
//    }
	
    public void reExecutePreparedStatementQuery() throws UDPDriverServerException
    {
    	try
    	{
    		if ( preparedStatement instanceof IFastPath ) {
    			((IFastPath)preparedStatement).executeAsFastPath();
    		} else {
        		resultSet = preparedStatement.executeQuery();
    	    	if ( resultSet==null )
    	    	{
    	    		throw new UDPDriverServerException( "ResultSet is null." );
    	    	}
    		}

        	currentRow = null;
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ValuesEncoder clean() failed.", e );
    	}
    }
	
    /**
     * Encode the next database records into the ResponseWithValues given as a parameter.
     * 
     * @param response
     * @throws UDPDriverServerException
     */
    public void encodeNextValues( ResponseWithValues response ) throws UDPDriverServerException
    {
    	try
    	{
    		int responseSize;

    		boolean containsLastValuesCopy;
    		int numberOfRowsCopy;
    		int numberOfNullValuesCopy;
    		int baosSizeCopy;
    		
			SpecificByteArrayOutputStream baos = new SpecificByteArrayOutputStream( maxLength );
			ObjectOutput oos = new ObjectOutputStream( baos );
    		
    		do
    		{
				// Fetch a row in the ResultSet
				if ( currentRow==null )
				{
					currentRow = fetchOneRowInResult( recycleRow );
					
					if ( currentRow==null ) // No more row in the ResultSet
					{
						response.setContainsLastValues( true );
						oos.flush();
						response.setSerializedDVDs( baos.toByteArray() );
						
						oos.close();
						baos.close();
		                return;
					}
				}
    			
				// Copy the current values
				containsLastValuesCopy = response.containsLastValues();
				numberOfRowsCopy = response.getNumberOfRows();
				numberOfNullValuesCopy = response.getNumberOfNullValues();
				baosSizeCopy = baos.size();
				
				// Add new values to the response
				addRowToResponse( response, oos, currentRow );
				
				// Calculate the new size
				oos.flush();
				responseSize = response.estimateHeaderSize() + baos.size();
				
				
				recycleRow = currentRow;
				currentRow = null;
    		}
    		while( responseSize <= maxLength );

    		// Detection of the case where one serialized row is longer than maxLength for MetaData
        	if ( response.getNumberOfRows()==1 && response.getType()!=Message.META_DATA )
        	{
        		response.setSerializedDVDs( baos.toByteArray() );
        		return;
        	}
        	
    		currentRow = recycleRow;
    		recycleRow = null;
    		
    		// Put the copies in the response
    		response.setContainsLastValues( containsLastValuesCopy );
    		response.setNumberOfRows( numberOfRowsCopy );
    		
    		// Remove the latest null values from the list
			if ( response.getNumberOfNullValues() > 0 )
			{
				for ( int i=0; i<( response.getNumberOfNullValues() - numberOfNullValuesCopy ); ++i )
				{
					response.removeLastNullValue();
				}
			}
			
			response.setNumberOfNullValues( numberOfNullValuesCopy );
			if ( baosSizeCopy > 4 )
			{
				response.setSerializedDVDs( baos.toByteArray( baosSizeCopy ) );
			}
			
			oos.close();
			baos.close();
			
			return;
    	}
    	catch( Exception e )
    	{
    		throw new UDPDriverServerException( "ValuesEncoder encodeNextValues() failed.", e );
    	}
    }
	
	
	private void addRowToResponse( ResponseWithValues response, ObjectOutput oos, DataValueDescriptor[] dvdr ) throws UDPDriverServerException
	{
		try
		{
			DataValueDescriptor dvdTemp = null;
			
			int currentNumberOfRows = response.getNumberOfRows();
			int currentNumberOfNullValues = response.getNumberOfNullValues();
			int coefficient = currentNumberOfRows * dvdr.length;
			
            for( int i=0; i<dvdr.length; ++i )
            {
            	dvdTemp = dvdr[i];
            	
                if ( dvdTemp.isNull() )
                {
                    ++currentNumberOfNullValues;
                    response.addNullValue( i + coefficient );
                }
                else
                {
                    // Due to derby serialization issue with empty string ""
                    if ( columnsWithChar[ i ] && dvdTemp.getLength()==1 )
                    {
                    	// Double if because getString() is an expensive operation
                    	if ( dvdTemp.getString().equals( "" ) )
                    	{
                    		dvdTemp.setValue( "\0" );
                    	}
                    }
                    
                    dvdTemp.writeExternal( oos );
                }
            }
			
            response.setContainsLastValues( false );
            response.setNumberOfRows( ++currentNumberOfRows );
            response.setNumberOfNullValues( currentNumberOfNullValues );
            
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ValuesEncoder addRowToResponse() failed.", e );
		}
	}
	
	
	private DataValueDescriptor[] fetchOneRowInResult( DataValueDescriptor[] rowToPopulate ) throws UDPDriverServerException
	{
		try
		{
			if ( preparedStatement instanceof IFastPath ) {

		        if ( null == rowToPopulate )
		        	rowToPopulate = createNewRowTemplate();
				
				IFastPath fp = (IFastPath) preparedStatement;
				if ( IFastPath.SCAN_COMPLETED == fp.nextRow(rowToPopulate) )
					return null; // any newly created rowToPopulate will be dereferenced and garbage collected
				
			} else {
				
		        if ( !resultSet.next() )
		        	return null;
		        
		        if ( null == rowToPopulate )
		        	rowToPopulate = createNewRowTemplate();
		        
		        for ( int i=0; i<numberOfColumns; i++ )
		        	rowToPopulate[i].setValueFromResultSet( resultSet, i+1, nullableColumns[i] );	
			}
	        
	        return rowToPopulate;	        
		}
		catch( Exception e )
		{
			throw new UDPDriverServerException( "ValuesEncoder fetchOneRowInResultSet(DVD[]) failed.", e );
		}
	}
	
	private DataValueDescriptor[] createNewRowTemplate() {
    	// initialise row
		DataValueDescriptor[] dvdr = new DataValueDescriptor[ numberOfColumns ];
    	for ( int i=0; i<numberOfColumns; i++ )
    		dvdr[i] = RowsFilter.constructDVDMatchingJDBCType( columnTypes[i] );
    	return dvdr;
	}
    
}
