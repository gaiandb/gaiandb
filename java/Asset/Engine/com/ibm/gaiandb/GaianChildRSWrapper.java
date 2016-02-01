/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.iapi.types.SQLBlob;

import com.ibm.db2j.VTI60;
import com.ibm.gaiandb.diags.GDBMessages;

public class GaianChildRSWrapper implements GaianChildVTI {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "GaianChildRSWrapper", 30 );
	
	private final ResultSet resultSet;
	private int updateCount = -2;
	private int[] projectedColumns = null;
	private Qualifier[][] qualifiers = null; // Used in cases where they couldn't be pushed down to the RDBMS (e.g. for a procedure call)
	
	private final ResultSetMetaData rsmd;
	private final int rsColCount;
	
	private int rowIndex = 0;
	
	private Connection parentCalledProcedureConnection = null;
		
	public GaianChildRSWrapper( int updateCount ) throws SQLException {
		this( null );
		this.updateCount = updateCount;
	}

	public GaianChildRSWrapper( ResultSet resultSet, Connection parentCalledProcedureConnection ) throws SQLException {
		this( resultSet );
		this.parentCalledProcedureConnection = parentCalledProcedureConnection;
	}
	
	public GaianChildRSWrapper( ResultSet resultSet ) throws SQLException {		
		this.resultSet = resultSet;		
		rsmd = null == resultSet ? null : resultSet.getMetaData();
		rsColCount = null == rsmd ? 0 : rsmd.getColumnCount();
		
		// include all columns by default
		this.projectedColumns = new int[ rsColCount ];
		for ( int i=0; i<rsColCount; i++ ) this.projectedColumns[i] = i+1; // 1-based
	}

	public boolean fetchNextRow(DataValueDescriptor[] dvdr) throws SQLException {

		if ( null == qualifiers ) return fetchNextRow2(dvdr);
		
    	boolean areQualifiersMet = false;
    	while ( !areQualifiersMet ) {
    		if ( false == fetchNextRow2(dvdr) ) return false;
    		
			areQualifiersMet = RowsFilter.testQualifiers( dvdr, qualifiers );
	
			if ( Logger.LOG_ALL == Logger.logLevel )
				logger.logDetail("Qualifiers check on row: " + Arrays.asList(dvdr) + ", result = " + areQualifiersMet);
    	}
    	return true;
	}
	
	private boolean fetchNextRow2(DataValueDescriptor[] dvdr) throws SQLException {
		
		if ( null == resultSet ) { // special case - result set wrapper used to hold update count only
			if ( -2 == updateCount ) return false; // unset
			// Just one update count
			try {
				dvdr[0].setValue(updateCount);
				updateCount = -2;
			} catch (StandardException e) {
				logger.logWarning(GDBMessages.ENGINE_ROW_NEXT_FETCH_ERROR, "Unable to set dvdr[0] with updateCount (resultSet was null) - returning no rows");
				return false;
			}
			return true;
		}
		
		if ( false == resultSet.next() ) {
//			logDerbyThreadInfo("nextFastPathRow(): No more rows in ResultSet, returning SCAN_COMPLETED");
			return false;
		}
		rowIndex++;
		
		int numQueriedLTCols = projectedColumns.length;
		
		// Deal with the count(*) case... we actually issued a "select 1 from <physcial_table>..." which gets empty rows
		if ( 0 == numQueriedLTCols ) return true; // We got a row with nothing to fill in. The caller just counts empty rows.
		
//		ResultSetMetaData rsmd = resultSet.getMetaData();
//		int colCount = rsmd.getColumnCount();
//		int[] projectedColumns = ( currentNodeResult.getOriginatingVTI().isGaianNode() ? allProjectedColumns : physicalProjectedColumns );
		
//    	logger.logInfo("colCount " + colCount + ", projectedColumns " + GaianTable.toString(projectedColumns));
		
		// The column mapping for RDB ResultSets was dealt with by the reconstituted sql "WHERE" clause which was passed to the RDBMS.
		
		// Cycle through the columns of the queried physical projection, i.e. the queried column ids of the logical table which are mapped
		// to physical columns. These are held in projectedColumns[].		
		// Skip all those that were previously determined to be missing. For those that were not, set the appropriate column in the 
		// returned dvdr row with the value from the next ResultSet column.
		// Notes:
		//  - ltProjIdx is the index into projectedColumns[] of the next logical column ID of a queried physical column (which may be missing)
		//	- rsColID is the column id from the ResultSet resulting from the physical query (where missing cols will have been omitted)
		//  - The dvdr row of DataValueDescriptors is sized to potentially accommodate all of the columns defined in the logical table
		
		for (int ltProjIdx=0, rsColID=0; ltProjIdx < numQueriedLTCols; ltProjIdx++, rsColID++) {
			
			int ltColID = projectedColumns[ltProjIdx]-1; // deduce the next 0-based logical column index that is actually being queried
			
			if ( null != pcolsMissing ) {
//				System.out.println(
//						"Physical RS colCount " + colCount + ", rsColIdx " + rsColID + 
//						"\n\t, ltProjIdx (incremented independantly for missing cols) " + ltProjIdx + 
//						"\n\t, colID (LT index in returned row) " + ltColID +
//						"\n\t, pcolsMissing[ltProjIdx] = " + pcolsMissing[ltProjIdx]);
				
				// Cycle through queried cols until one is defined in the physical table, setting all missing ones
				// to null in the corresponding cols in the returned dvdr.
				// Stop when we reach a non null column or return if all queried cols have been dealt with.
				// The resultSet index will remain the same while the queried col index increases.
				
				while ( pcolsMissing[ltProjIdx] ) {
					dvdr[ltColID].setToNull();
					// If the new projection index is equal to the number of physical cols queried, then we have dealt with them all.
					if ( ++ltProjIdx >= pcolsMissing.length )
						return true;
					
//					System.out.println("ltColID " + ltColID + 
//							", was missing in pcolsMissing " + Util.boolArrayAsString(pcolsMissing) + 
//							", getting next ltcol from projectedColumns " + Util.intArrayAsString(projectedColumns) + 
//							", ltProjIdx = " + ltProjIdx);
					
					ltColID = projectedColumns[ltProjIdx]-1; // switch from 1-based to 0-based
				}
			}
			
//			logDerbyThreadInfo("dvdrsize = " + dvdr.length + ", colID = " + colID);
			try {
				if ( dvdr[ltColID] instanceof SQLBlob ) {
					dvdr[ltColID].setValue(resultSet.getBytes(rsColID+1));
					byte[] bytes = dvdr[ltColID].getBytes();
					logger.logInfo("Extracted blob bytes from SQLBlob column: " +
							(null==bytes ? "set to null" : "blob size = " + bytes.length) );
					// Unable to get the following working right now...
//					Blob blob = resultSet.getBlob(rsColID+1);
//					dvdr[ltColID].setValue(new MyResetableStream(blob.getBinaryStream()), (int) blob.length()); //resultSet.getBinaryStream(rsColID+1)
				} else
					dvdr[ltColID].setValueFromResultSet(
						resultSet, rsColID+1, rsmd.isNullable(rsColID+1) != ResultSetMetaData.columnNoNulls);
			} catch (Exception e) {
				String type1 = rsmd.getColumnTypeName(rsColID+1);
				String type2 = ltColID >= dvdr.length || null == dvdr[ltColID] ? "null" : 
					dvdr[ltColID].getTypeName() + (dvdr[ltColID].isNull() ? " (isNull)" : "");
				
				String w = "Unable to convert value from ResultSet column " + (rsColID+1) + " type " +
							type1 + " to LT col " + (ltColID+1) + " type " + type2 + ", cause: ";
				logger.logException(GDBMessages.ENGINE_CONVERT_VALUE_ERROR, w, e);

				logger.logInfo("rsColCount " + rsColCount + ", projectedColumns " + Util.intArrayAsString(projectedColumns));
				logger.logInfo("Input dvdr: " + Arrays.asList(dvdr));
				
				// DRV - 30/01/2012 - Set to null and just continue trying to fill in more columns and rows even if a type conversion error occurs
				dvdr[ltColID].setToNull();
//				return false;
			}
		}
		
		return true;
	}
	
	public class MyResetableStream extends InputStream implements Resetable {

		private InputStream is = null;
		
		private int[] cachedBytes = new int[1000];
		private int numBytesCached = 0, pos = 0;
		
		public MyResetableStream(InputStream is) {
			super();
			this.is = is;
			logger.logInfo("Entered MyResetableStream(); Mark isSupported ? " + is.markSupported());
//			if ( is.markSupported() ) is.mark(1000);
		}

		public void closeStream() {
			logger.logInfo("Entered closeStream()");
			try {
				is.close();
			} catch (IOException e) {
				logger.logException(GDBMessages.ENGINE_CLOSE_STREAM_ERROR, "Unable to close Stream", e);
			}
		}

		public void initStream() throws StandardException {
			logger.logInfo("Entered initStream()");
			numBytesCached = 0; pos = 0;
		}

		public void resetStream() throws IOException, StandardException {
			logger.logInfo("Entered resetStream(), numBytesCached = " + numBytesCached);
//			if ( is.markSupported() ) is.reset();
			pos = 0;
		}

		@Override
		public int read() throws IOException {
			logger.logInfo("Entered read()");
			if ( pos < numBytesCached ) return cachedBytes[pos++];
			int b = is.read();
			if ( numBytesCached < cachedBytes.length ) cachedBytes[numBytesCached++] = b;
			return b;
		}		
	}
	
//	public boolean fetchNextRow(Object[] rawrow) throws SQLException {
//		
//		if ( false == resultSet.next() ) {
////			logDerbyThreadInfo("nextFastPathRow(): No more rows in ResultSet, returning SCAN_COMPLETED");
//			return false;
//		}
//		
//		// Deal with the count(*) case... we actually issued a "select 1 from <physcial_table>..." which gets empty rows
//		if ( 0 == projectedColumns.length )
//			return true; // We got a row with nothing to fill in. The caller just counts empty rows.
//		
////    	logger.logInfo("colCount " + colCount + ", projectedColumns " + GaianTable.toString(projectedColumns));
//					
//		// Cycle through the ResultSet column values for the next row, positionning them appropriately in the given row of DataValueDescriptors.
//		// The column mapping for RDB ResultSets was dealt with by the reconstitued sql "WHERE" clause which was passed to the remote RDB.
//		for (int i=0; i<colCount; i++) {
//			// The row of DataValueDescriptors is sized to potentially accomodate all of the columns defined in the logical table.
//			int colID = projectedColumns[i]-1;
////			logDerbyThreadInfo("dvdrsize = " + dvdr.length + ", colID = " + colID);
//			try {
//				rawrow[colID] = resultSet.getObject(i+1);
//			} catch (Exception e) {
//				String type1 = rsmd.getColumnTypeName(i+1);
//				String type2 = null == rawrow[colID] ? "null" : rawrow[colID].getClass().getSimpleName();
//				
//				String w = "Unable to convert value from ResultSet column " + (i+1) + " type " +
//							type1 + " to LT col " + (colID+1) + " type " + type2 + ", cause: ";
//				logger.logException(w, e);
//
//				logger.logInfo("colCount " + colCount + ", projectedColumns " + Util.intArrayAsString(projectedColumns));
//				logger.logInfo("Input dvdr: " + Arrays.asList(rawrow));
//				
//				return false;
//			}
//		}
//		
//		return true;
//	}

	public void close() throws SQLException {
		if ( null != resultSet ) {
			resultSet.close();
//			logger.logThreadInfo("ResultSet object closure: " + resultSet);
		}
		// resultSet is not necessarily Scrollable - do not reset rowIndex.
	}

	public boolean reinitialise() { return false; } // cannot re-execute this GaianChildVTI
	
	public boolean isBeforeFirst() {
		return 0 == rowIndex;
	}
	
	public Statement getStatementForCleanup() throws SQLException {
		if ( null == resultSet ) return null;
		// Handle the possibility that the result may come from a stored procedure call.. in which case we want to recycle the
		// top level connection instead of the one that is generated within the procedure..
		return // null == parentCalledProcedureConnection ? resultSet.getStatement() : parentCalledProcedureConnection.createStatement();
			new VTI60() {
				private final Statement s = resultSet.getStatement();
				public void close() throws SQLException { if (null!=s) s.close(); }
				public Connection getConnection() throws SQLException {
					return null == parentCalledProcedureConnection && null != s ? s.getConnection() : parentCalledProcedureConnection;
				}
			};
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		if ( null == resultSet ) return null;
		return resultSet.getMetaData();
	}

	public void setArgs(String[] args) {
	}

	// pcolsMissing identifies missing physical cols for given lt col names - hence null - Array is indexed by logical col id.
	// This defines which of the queried columns to be passed back were omitted from the query against the physical source (due to being missing)
	boolean[] pcolsMissing = null;
	
	public void setExtractConditions(Qualifier[][] qualifiers, int[] projectedCols, int[] physicalColumnsMapping) throws Exception {
		
		this.qualifiers = qualifiers;		
		if ( null != projectedCols ) this.projectedColumns = projectedCols;
		
		// mapping of logical -> physical col ids
		if ( null == physicalColumnsMapping ) return; // no cols mapping, e.g. this may be a propagated query or a sub-query
		
//		System.out.println("Mapped physical col ids: " + Util.intArrayAsString(physicalColumnsMapping) + ", Projected cols: " + Util.intArrayAsString(projectedColumns));
		
		// If there are fewer columns in the result set than the number of physical cols referenced in the query, then some are missing in this data source.
		if ( rsColCount < projectedColumns.length ) {
			pcolsMissing = new boolean[projectedColumns.length]; // initialises to false, so no cols are missing
			
//			System.out.println("Missing columns detected! count " + (projectedColumns.length-colCount) + 
//					" (projected physical cols: " + projectedColumns.length + ", rs cols: " + colCount + ")");
			// find the missing ones - they will be the max index, which is 1 more than the max physical col index - see VTIWrapper.refreshColumnsMapping
			int max = 0;
			for ( int i : physicalColumnsMapping ) max = Math.max(max, i);
			// missing cols are those where the physical cols for the given queried logical cols are equal to the max column value (i.e. out of range)
			for ( int i=0; i<pcolsMissing.length; i++ ) {
				int queriedCol = projectedColumns[i]-1; // switch from 1-based to 0-based
				pcolsMissing[i] = max == physicalColumnsMapping[queriedCol]; // 0-based
			}
			
			logger.logInfo("Deduced pcolsMissing in projectedColumns: " + Util.boolArrayAsString(pcolsMissing));
		}
	}

	public int getRowCount() throws SQLException {
		
		if ( null == resultSet ) return 1;
		if ( ! resultSet.isBeforeFirst() )
			throw new SQLException("getRowCount(): Cursor is not at the start of the ResultSet");
		
		while ( resultSet.next() ) rowIndex++;
		return rowIndex;
	}

	public boolean isScrollable() {
		return false;
	}
}
