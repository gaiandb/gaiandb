/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import java.util.Arrays;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.Logger;

public abstract class SQLResultFilterX implements SQLResultFilter {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "SQLResultFilterX", 25 );
	
	/**
	 * Applies a transformation function to the columns in each row as required by the filter.
	 * An array of rows is passed in, each of which have generic SQL columns in Derby DataValueDescriptor column wrappers.
	 * A single row size and indices match the whole logical table definition, but only the queried columns will be populated.
	 * These are given when 'setQueriedColumns' is called. 
	 * 
	 * The corresponding physical table's columns can be derived using its columnMappings, which are 0-based, e.g.
	 * If columMappings = { 2, 0, 5 }, then it means that the first 3 logical table's columns target columns 3, 1 and 6 in 
	 * the dataSource which is currently being processed. Notice the column ids are numbered from 0 in the array, but from 1
	 * externally with JDBC.
	 * 
	 * Note that all calls to nextQueriedDataSource() (for every data source in the query) will have completed 
	 * before filterRowsBatch() is called for the first time.
	 * The dataSourceID should be consistent with one of those passed in during preceding calls to nextQueriedDataSource().
	 * 
	 * @param rows: A double array holding a batch of rows, each holding all logical table columns.
	 * @return A resulting double array with excluded or modified rows. The number of rows returned may be less or equals to the number passed in.
	 */
	public abstract DataValueDescriptor[][] filterRowsBatch( String dataSourceID, DataValueDescriptor[][] rows );
	
	/**
	 * Overloaded method - includes the data source ID as well - so as not to have to do a reverse lookup
	 * Note that all invocations of this method (for all data sources) will be made before the very first invocation of filterRowsBatch().
	 * Use the dataSourceID to store the correct columnMappings[] array for use when filtering. 
	 * 
	 * @param dataSourceID
	 * @param dataSourceDescription
	 * @param columnMappings
	 * @return
	 */
	public abstract int nextQueriedDataSource( String dataSourceID, String dataSourceDescription, int[] columnMappings );
	
	/**
	 * This method is intended to be called by VTIs (such as ICAREST, SpatialQuery, or custom ones), unlike nextQueriedDataSource().
	 * It sets the data source wrapper for the next query to be run and returns the maximum number of rows that may be returned by the wrapper.
	 * If the filter wishes to refuse that this data source wrapper be queried (e.g. based on user privileges) it may return 0, 
	 * in which case it will be skipped.
	 * 
	 * @param wrapperID: String identifying the data source wrapper - e.g. CSV list containing VTI class name and possibly arguments
	 * @return the max number of rows that may be returned to this user from this data source wrapper, or -1 if unlimited
	 */
	public abstract int setDataSourceWrapper(String wrapperID);

	
//	public final int[] getSupportedOperations() {
//		final int[] rc = getSupportedOperationsImpl();
//		logger.logInfo("Evaluated SQLResultFilterX.getSupportedOperationsImpl(), result: " + Util.intArrayAsString(rc));
//		return rc;
//	}
//	protected abstract int[] getSupportedOperationsImpl();

	// The operation OP_ID_SET_FORWARDING_PATH_RETURN_IS_QUERY_ALLOWED is called for execution and all subsequent re-executions of queries.
	// When the executeOperation() method is invoked with this operation ID, it must return a Boolean object specifying if the query can proceed on this node.
	public static final String OP_ID_SET_FORWARDING_PATH_RETURN_IS_QUERY_ALLOWED = "OP_ID_SET_FORWARDING_PATH_RETURN_IS_QUERY_ALLOWED";
	public static final String OP_ID_SET_ACCESS_CLUSTERS_RETURN_IS_QUERY_ALLOWED = "OP_ID_SET_ACCESS_CLUSTERS_RETURN_IS_QUERY_ALLOWED";
	
	public static final String OP_ID_SET_AUTHENTICATED_DERBY_USER_RETURN_IS_QUERY_ALLOWED = "OP_ID_SET_AUTHENTICATED_DERBY_USER_RETURN_IS_QUERY_ALLOWED";
	
	public final Object executeOperation( String opID, Object... args ) {
		logger.logDetail("Entering SQLResultFilterX.executeOperation(" + opID + ", " + Arrays.asList(args) + ")");
		return executeOperationImpl( opID, args );
	}
	
	protected abstract Object executeOperationImpl( String opID, Object... args );
}
