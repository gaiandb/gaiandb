/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;



import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */
public class InMemoryRows implements GaianChildVTI {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "InMemoryRows", 30 );
	
//	private DataValueDescriptor[][] rows = new DataValueDescriptor[0][], result;
	
	private ArrayList<DataValueDescriptor[]> rows;
	private ArrayList<DataValueDescriptor[]> result;
	
	// We use ArrayList instead of Vector because we don't need or want the overhead of synchronization
//	private ArrayList rows, result;
	
	private ConcurrentMap<Integer, SortedMap<DataValueDescriptor, Object>> indexes;
	
	private int rowCount;
	
	private int index = 0;

	// logical projection is used to only set columns that are selected in the query, which is not necessarily
	// all the physical columns mapped to the logical table.
	private int[] logicalProjection = null;
    private int[] columnsMapping = null;
    
    private int colCount = 0;
    
    // NOTE: The loaded in-memory 'rows' are shared between all instances of InMemoryRows
    // These will change when a reload is done, so synchronization is required.
    // This is why the execute() and reinitialise() methods of each VTIWrapper are synchronized.
    // However this means that the rows Vector can only be worked with within mthods called by one or
    // the other of these methods, i.e. setExtractConditions(), but definitely not in next()
//	public InMemoryRows( Vector rows ) {
//		this.rows = rows;
//	}
	
	/*
	 * This method should not be called whilst the rows are being processed...
	 */
//	public void setRowsAndIndexes( DataValueDescriptor[][] rows, Hashtable indexes ) {
	public void setRowsAndIndexes( ArrayList<DataValueDescriptor[]> rows, ConcurrentMap<Integer, SortedMap<DataValueDescriptor, Object>> indexes ) {
	
		this.rows = rows;
		this.indexes = indexes;
	}
	
	/**
	 * qualifiers and pushedProjection respectively represent predicates structure and queried column ids.
	 * These both reference column ids that are relative to the physical data.
	 */
	public void setExtractConditions( Qualifier[][] qualifiers, int[] logicalProjection, int[] columnsMapping ) throws Exception {
		
		this.logicalProjection = logicalProjection;
    	this.columnsMapping = columnsMapping;
    	
    	logger.logInfo("All query's logical col ids: " + Util.intArrayAsString(logicalProjection) +
				", columnsMapping: " + Util.intArrayAsString(columnsMapping) );
    	
    	colCount = logicalProjection.length;
    	
    	if ( null == qualifiers || 0 == qualifiers.length )
    		result = new ArrayList<DataValueDescriptor[]>( rows ); // Disassociate rows from result, so that a cleanup cannot impact the fetch.
    	else {
    		// Get a copy of the qualifiers structure, but with column ids swapped to the physical ones.
    		// Cannot modify the orignal qualifiers structure as is being shared between physical nodes.
        	Qualifier[][] mappedQualifiers = RowsFilter.getQualifiersDeepCopyWithColumnsMapped( qualifiers, columnsMapping );

        	logger.logInfo("New Physical Column Mapped Qualifiers: " + RowsFilter.reconstructSQLWhereClause( mappedQualifiers ) +
        			" #colmappings: " + Util.intArrayAsString(columnsMapping));
        	
        	// Apply indexes - and prune the mappedQualifiers from them
        	SortedMap<DataValueDescriptor, Object> remainingMap = RowsFilter.applyAndPruneIndexQualifiers( indexes, mappedQualifiers );
	    	
	    	if ( null != remainingMap ) {
	    		
	    		// The index reduced the set of keys - now filter the rest using remaining predicates - if any
	    			    		
		    	// Get values - this just creates an inner class impl of AbstractCollection that can work on the TreeMap's values collection
		    	// Getting the values() here does not parse the results...
		    	Collection<Object> c = remainingMap.values();
		    	
		        result = new ArrayList<DataValueDescriptor[]>( c.size() );
		        
		        Iterator<Object> it = c.iterator();
		        if ( 0 == mappedQualifiers.length )
			        while (it.hasNext()) {
			        	Object keyRows = it.next();
			        	
			        	if ( keyRows instanceof ArrayList<?> ) {
			        		ArrayList<?> a = (ArrayList<?>) keyRows;
			        		int alen = a.size();
			        		for ( int i=0; i<alen; i++ )
			        			result.add( (DataValueDescriptor[]) a.get(i) );
			        	} else
			        		result.add( (DataValueDescriptor[]) keyRows );
			        }
		        else
			        while (it.hasNext()) {
			        	Object keyRows = it.next();
			        	
			        	if ( keyRows instanceof ArrayList<?> ) {
			        		ArrayList<?> a = (ArrayList<?>) keyRows;
			        		int alen = a.size();
			        		for ( int i=0; i<alen; i++ ) {
			        			DataValueDescriptor[] row = (DataValueDescriptor[])a.get(i);
			        			if ( RowsFilter.testQualifiers( row, mappedQualifiers ) )
			        				result.add( row );
			        		}
			        	} else
			        		if ( RowsFilter.testQualifiers( (DataValueDescriptor[])keyRows, mappedQualifiers ) )
			        			result.add( (DataValueDescriptor[]) keyRows );
			        }
		        
        		logger.logInfo("Applied remaining predicates on submap of index");
		    	
		    	// Copy into array - will parse results through AbstractCollection
//		    	result = (DataValueDescriptor[][]) c.toArray( new DataValueDescriptor[c.size()][] );
	    	
	    	} else { //if ( 0 != mappedQualifiers.length ) {
	    	
	    		// The index didn't help - and we have qualifiers
	    		// Apply table scan using all qualifiers
	    			    		
        		int len = rows.size();
		        result = new ArrayList<DataValueDescriptor[]>( len/10 );
        		
        		for ( int i=0; i<len; i++ ) {
					DataValueDescriptor[] row = (DataValueDescriptor[]) rows.get(i);
		    		if ( RowsFilter.testQualifiers( row, mappedQualifiers ) )
		    			result.add( row );
				}
				
        		logger.logInfo("Applied remaining predicates on full table scan");
        		
				// This is a System.arrayCopy
//		    	result = (DataValueDescriptor[][]) resultList.toArray( new DataValueDescriptor[resultList.size()][] );
	    	}
    	}
    	
    	rowCount = result.size();
    	logger.logInfo("Determined InMemoryRows rowCount = " + rowCount + ", index set to " + index);
    }
    
    /**
     * This method is not implemented yet - it could be used in future to combine indexes...
     * It would AND 2 submaps, each of which had been derived from separate indexes (indexed by a different column).
     * 
     * @param t1
     * @param t2
     * @return
     */
//    private DataValueDescriptor[] mergeTrees( SortedMap<DataValueDescriptor, Object> t1, SortedMap<DataValueDescriptor, Object> t2 ) {
//    	
//    	// Algo 1:
//    	// Iterate over smallest tree's values, inserting them into a new tree indexed by the larger tree's column.
//    	// Identify the overlapping range between the new tree and the larger tree - get the values.
//    	
//    	// Algo 2 (alternative):
//    	// Get the keySet from the smaller tree and the values from the larger one
//    	// Iterate over values, building a HashSet by picking out for each row the column that the smaller set is indexed by.
//    	// keySet.retailAll( <new HashSet> )
//    	// Get the values from the smaller TreeMap, which should now be even smaller as changes in the keySet are reflected in the Map.
//    	
//    	return null;
//    }
    
	public void setArgs(String[] args) {
		// No variable arguments processing
	}

//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#next()
//	 */
//	public boolean next() throws SQLException {
//		
//		if ( rowCount <= index ) return false;
//		index++;
//		return true;
//	}
	
	public boolean fetchNextRow( DataValueDescriptor[] row ) throws SQLException {
				
		if ( rowCount <= index ) return false;
		
    	DataValueDescriptor[] nextRow = (DataValueDescriptor[]) result.get( index++ ); // result[index++];
    	
		if ( Logger.LOG_ALL == Logger.logLevel )
			logger.logDetail("New in-mem result row: " + Arrays.asList(nextRow) );
    	
    	for (int i=0; i<colCount; i++) {
    		int lcolID = logicalProjection[i]-1;
    		int pcolID = columnsMapping[lcolID]; // pcolID is the in-mem table col index
    		// Check if column is not in the in memory rows. 
    		// This happens for columns that were not in the pushed projection for this query.
    		// Derby is expected to ignore these columns in the row array.
    		// This may also happen when the column is actually queried (as it exists on other physical sources but not this one)
    		// A null will be returned in the column for this source in that case - so the user will have a partial result.
			// Note that we don't distinguish between null values and non-existant columns... the fact that a column does not
			// exist for a given source is considered as it having a null value for it.
//    		if ( 0 > pcolID ) continue; // if the col is not mapped then pcolID will be numcols+1, which is a null dvd
    		    		
    		try {

    			if ( Logger.LOG_ALL == Logger.logLevel )
    				logger.logDetail("Setting InMemory col " + (pcolID+1) + " as type " + row[lcolID].getTypeName());
    			
    			row[lcolID].setValue( nextRow[pcolID] ); // note that nextRow[number of pcols] is a null dvd.. this is used for a non-existant pcol
    		
    		} catch ( ArrayIndexOutOfBoundsException e ) {
    			logger.logException( GDBMessages.RESULT_LOGICAL_COLUMN_REF_ERROR, "Error referencing Logical column " + (i+1) + // must be 1-based from user pt of view
    					" which does not exist in physical table. Null ResultSet will be returned for this node", e);
				return false;
	    	} catch (Exception e) {
	    		throw new SQLException( "Unable to set value for in-memory row's column id " + (pcolID+1) + 
						" to the intended DVD column type: " + row[lcolID].getTypeName() + ", cause: " + e );
			}
    	}
		
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSet#close()
	 */
	public void close() { reinitialise(); }
	
	@Override
	public boolean reinitialise() {
		if ( null != result && result != rows ) {
			result.clear();
			result.trimToSize();
		}
		result = null;
		index = 0;
		return true;
	}

	public boolean isBeforeFirst() {
		return 0 == index;
	}
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSet#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		return null;
	}

	public int getRowCount() throws Exception {
		index = rowCount; // invalidate this structure until close() has been called
		return rowCount;
	}

	public boolean isScrollable() {
		return true;
	}	
}
