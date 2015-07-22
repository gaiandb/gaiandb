/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;



import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfigProcedures;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

public class GaianConfig extends VTI60 implements VTICosting, IFastPath, GaianChildVTI {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "GaianConfig", 20 );

	private final String configRequest;
	private String additionalParams = null;
	
	private DataValueDescriptor[][] resultRows = null;
	private int index = 0;
	
	// search sring -> latest row count
	private static Map<String, Long> estimatedRowCounts = 
		Collections.synchronizedMap( new CachedHashMap<String, Long>( GaianTable.QRY_METADATA_CACHE_SIZE ) );
	
	private ResultSetMetaData rsmd;
		
	/**
	 * @param configRequest
	 * @throws Exception 
	 */
	public GaianConfig(String configRequest) throws Exception {
		super();
		logger.logDetail("Entered GaianConfig(configRequest) constructor");
		this.configRequest = configRequest;
	}
	
	/**
	 * @param configRequest
	 * @param additionalParams - String - CSV list of additional parameters for the request. Null if none.
	 * @throws Exception 
	 */
	public GaianConfig(String configRequest, String additionalParams) throws Exception {
		super();
		logger.logDetail("Entered GaianConfig(configRequest, additionalParams) constructor");
		this.configRequest = configRequest;
		this.additionalParams = additionalParams;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setArgs(java.lang.String[])
	 */
	public void setArgs(String[] args) {
//		if ( null != args && 0 < args.length ) this.configRequest = args[0];
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setExtractConditions(org.apache.derby.iapi.store.access.Qualifier[][], int[])
	 */
	public void setExtractConditions(Qualifier[][] qualifiers, int[] logicalProjection, int[] columnsMapping) throws SQLException {
		// Qualifiers and column mappings are not supported for this VTI - as it is mostly used by the API list commands...
		// No need to set qualifiers - the search string acts as a filter instead.
		// Also ignore mappedColumns as column names are expected to be the same in the logical table.
		executeAsFastPath();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#executeAsFastPath()
	 */
	public boolean executeAsFastPath() throws SQLException {
		
		logger.logDetail("Entered GaianConfig.executeAsFastPath()");
		
		if ( null != resultRows ) {
			logger.logDetail("Old results still in memory - closing..");
			close();
//			return true;
		}
		
		index = 0;
		
		try {
			resultRows = GaianDBConfigProcedures.getResultSetForUtilityRequest( configRequest, additionalParams );
		} catch (Exception e) {
			logger.logException(GDBMessages.ENGINE_GET_ROWS_FOR_CONFIG_ERROR, "Unable to get rows for config request: " + configRequest + ", cause: ", e);
			throw new SQLException( e.getMessage() );
		}
		
		logger.logDetail("Exiting GaianConfig.executeAsFastPath(), resultRows.length: " + resultRows.length);
	
		return true;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#nextRow(org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		
		if ( 0 == index )
			logger.logDetail("Fetching rows for GaianConfig('" + configRequest + "')");
		
		if ( index >= resultRows.length ) {
//			logger.logDetail("Fetch complete for GaianConfig('" + configRequest + "')");
			// Keep track of row count for this search string
			Long previousCount = estimatedRowCounts.get( configRequest );
			if ( null == previousCount || index > previousCount.longValue() )
				estimatedRowCounts.put( configRequest, new Long( index ) );
			index = 0;
			return IFastPath.SCAN_COMPLETED;
		}
		
		System.arraycopy(resultRows[index++], 0, dvdr, 0, resultRows[0].length);
//		for ( int i=0; i<numCols; i++ ) dvdr[i].setValue( row[i] );
		
		return IFastPath.GOT_ROW;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#currentRow(java.sql.ResultSet, org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public void currentRow(ResultSet arg0, DataValueDescriptor[] arg1) throws StandardException, SQLException {
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#rowsDone()
	 */
	public void rowsDone() throws StandardException, SQLException {
		close();
	}
	
    /**
     *  Provide the metadata for the query against the given table. Cloudscape
	 *  calls this method when it compiles the SQL-J statement that uses this
	 *  VTI class.
     *
     *   @return the result set metadata for the query
	 * 
     *   @exception SQLException thrown by JDBC calls
     */
    public ResultSetMetaData getMetaData() throws SQLException {
		
		if ( null == rsmd ) {
			try {
				rsmd = (ResultSetMetaData) GaianDBConfigProcedures.configRequestResultDefs.get(
						Util.splitByCommas(configRequest)[0]);
			} catch (Exception e) {
				throw new SQLException(e+"");
			}
		}

		logger.logDetail("getMetaData(): " + rsmd + " - " + configRequest);
		return rsmd;
    }

	public void close() {
		logger.logDetail("GaianConfig.close()");
		reinitialise();
	}
	
	/**
	 * Re-initialises and tells us whether the VTI can be re-executed
	 */
	@Override public boolean reinitialise() {
		logger.logDetail("GaianConfig.reinitialise()");
		if ( null != resultRows ) {
			for (int i=0; i<resultRows.length; i++) {
				for (int j=0; j<resultRows[i].length; j++)
					resultRows[i][j] = null;
				resultRows[i] = null;
			}
			resultRows = null;
		}
		index = 0;	
		return true;
	}
	
	public boolean isBeforeFirst() {
		return 0 == index;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedRowCount(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
	
		Long l = estimatedRowCounts.get( configRequest );
		double val = null == l ? 1000 : l.doubleValue() * 10; // multiply by 10 as this is XML!
		
		logger.logDetail("getEstimatedRowCount() returning: " + val);
		return val;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedCostPerInstantiation(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException {
		int rc = 0;
		logger.logDetail("getEstimatedCostPerInstantiation() returning: " + rc);
		return rc;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#supportsMultipleInstantiations(org.apache.derby.vti.VTIEnvironment)
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException {
		// For us to return true, we must ensure that results can be fetched repeatedly.
		// i.e. that the cursor is set back to the first row every time we reach the end of it.
		boolean rc = true;
		
		logger.logDetail("supportsMultipleInstantiations() returning: " + rc);
		return rc;
	}

	public boolean fetchNextRow(DataValueDescriptor[] row) throws Exception {
		return IFastPath.GOT_ROW == nextRow(row);
	}

	public int getRowCount() throws Exception {
		return null == resultRows ? 0 : resultRows.length;
	}

	public boolean isScrollable() {
		return true;
	}
	
	@Override
	public int getResultSetType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return ResultSet.CONCUR_UPDATABLE;
	}
}
