/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.diags;


// This class lists all of the Unique Error codes, with their documentation. 
// 
// Each error code begins with a token that identifies the functionality of the code 
// 		DISCOVERY, DSWRAPPER, ENGINE... 
//
// The error codes are grouped by category in alphabetical order. 

public class GDBMessages {
	// Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------
	// logException() calls
	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------

	// db2j/AbstractVTI.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to initialise cache tables. Caching is disabled.
	 * <br/><br/>
	 * <b>Reason:</b> The cache.expires property for this VTI has been disabled in the configuration file. A value < 1 will disable the cache.
	 * <br/><br/> 
	 * <b>Action:</b> Check that the cache.expires property for this VTI has not been disabled in the configuration file.
	 * Check the GaianDB configuration file and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_CACHE_TABLES_INIT_ERROR = "DSWRAPPER_CACHE_TABLES_INIT_ERROR";

	/**
	 * 
	 * <p>
	 * <b>Warning:</b> Unable to drop cache table.
	 * <br/><br/>
	 * <b>Reason:</b> The cache table might be in used by another query. 
	 * <br/><br/>
	 * <b>Action:</b> Wait for all queries to finish and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DROP_CACHE_TABLES_WARNING = "DSWRAPPER_DROP_CACHE_TABLES_WARNING";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to recycle connection after dropping cache table.
	 * <br/><br/>
	 * <b>Reason:</b> The connection to the database cannot be recycled into the pool. GaianDB will retry later.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_RECYCLE_CONNECTION_ERROR = "DSWRAPPER_RECYCLE_CONNECTION_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve VTI metadata definition.
	 * <br/><br/>
	 * <b>Reason:</b> The VTI cannot process the information in the data source to build a view.
	 * <br/><br/> 
	 * <b>Action:</b> Check your source object and verify that GaianDB has been configured correctly to read it. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_METADATA_RESOLVE_ERROR = "DSWRAPPER_METADATA_RESOLVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to prepare insert statement for caching a set of records, the cache has been invalidated.
	 * <br/><br/>
	 * <b>Reason:</b> The cache for this query has been invalidated. It may be possible that the cache table has been corrupted or dropped outside of the GaianDB's process.
	 * <br/><br/>
	 * <b>Action:</b> Check that no other process interferes with the internal GaianDB tables and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_CACHE_INSERT_STATEMENT_ERROR = "DSWRAPPER_CACHE_INSERT_STATEMENT_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to cache row, the cache has been invalidated.
	 * <br/><br/>
	 * <b>Reason:</b> The cache for this query has been invalidated. It may be possible that the cache table has been corrupted or dropped outside of the GaianDB's process.
	 * <br/><br/>
	 * <b>Action:</b> Check that no other process interferes with the internal GaianDB tables and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_ROW_CACHE_ERROR = "DSWRAPPER_ROW_CACHE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to fetch row or scroll back to beginning of ResultSet.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the cache table, GaianDB cannot process the ResultSet further. 
	 * <br/><br/>
	 * <b>Action:</b> Check your data source and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_ROW_FETCH_ERROR = "DSWRAPPER_ROW_FETCH_ERROR";

	
	// db2j/AbstractDurableCacheVTI.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to initialise durable cache tables. Caching is disabled.
	 * <br/><br/>
	 * <b>Reason:</b> An error occurred whilst trying to initialise the cache table.
	 * <br/><br/> 
	 * <b>Action:</b> Check that the cache.expires property for this VTI has not been disabled in the configuration file.
	 * Check the GaianDB configuration file and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DURABLE_CACHE_TABLES_INIT_ERROR = "DSWRAPPER_DURABLE_CACHE_TABLES_INIT_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to check for existing valid durable cache table to use.
	 * <br/><br/>
	 * <b>Reason:</b> An error occurred whilst trying to check for an existing valid durable cache table to use.
	 * <br/><br/> 
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DURABLE_CACHE_LOOKUP_ERROR = "DSWRAPPER_DURABLE_CACHE_LOOKUP_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to invalidate the durable cache table.
	 * <br/><br/>
	 * <b>Reason:</b> An error occurred whilst trying to invalidate the durable cache table.
	 * <br/><br/> 
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DURABLE_CACHE_INVALIDATE_ERROR = "DSWRAPPER_DURABLE_CACHE_INVALIDATE_ERROR";
	
	/**
	 * 
	 * <p>
	 * <b>Warning:</b> Unable to delete the durable cache table.
	 * <br/><br/>
	 * <b>Reason:</b> The cache table might be in used by another query. 
	 * <br/><br/>
	 * <b>Action:</b> Wait for all queries to finish and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DURABLE_CACHE_DELETE_WARNING = "DSWRAPPER_DURABLE_CACHE_DELETE_WARNING";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to recycle connection after deleting cache table.
	 * <br/><br/>
	 * <b>Reason:</b> The connection to the database cannot be recycled into the pool. GaianDB will retry later.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DURABLE_CACHE_RECYCLE_CONNECTION_ERROR = "DSWRAPPER_DURABLE_CACHE_RECYCLE_CONNECTION_ERROR";
	
	// db2j/EntityAssociations.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to process distributed query and/or its results.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Check your data source has not been corrupted, and that the query issued is valid for the data source.
	 * Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_DIST_QUERY_PROCESS_ERROR = "DSWRAPPER_DIST_QUERY_PROCESS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to load Entity Matrix Joiner associations or write them to output file.
	 * <br/><br/>
	 * <b>Reason:</b> It could be that GaianDB wasn't configured properly.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check that the chosen destination for the output file is in a directory that GaianDB can write to.
	 * Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_MATRIX_JOINER_LOAD_WRITE_ERROR = "DSWRAPPER_MATRIX_JOINER_LOAD_WRITE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to get EntityMatrixJoiner from the file name.
	 * <br/><br/>
	 * <b>Reason:</b> It could be that GaianDB wasn't configured properly.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/	
	public static final String DSWRAPPER_ENTITYMATRIXJOINER_GET_WARNING = "DSWRAPPER_ENTITYMATRIXJOINER_GET_WARNING";
	/**
	 * <p>
	 * <b>Error:</b> Unexpected Exception occurred while getting the next row from the ResultSet.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot process the complete ResultSet.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_NEXT_ROW_GET_ERROR = "DSWRAPPER_NEXT_ROW_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unexpected Exception whilst setting restricted group size.
	 * <br/><br/>
	 * <b>Reason:</b> 
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_RESTRICTED_GPSIZE_SET_ERROR = "DSWRAPPER_RESTRICTED_GPSIZE_SET_ERROR";

	// db2j/FileImport.java
	/**
	 * <p>
	 * <b>Error:</b> Error referencing a Logical column which does not exist in physical table. Null ResultSet will be returned for this node.
	 * <br/><br/>
	 * <b>Reason:</b> There might be a problem with the data source file for this logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the data source for this logical table is defined properly in the GaianDB configuration file, and its data is not corrupted. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_LOGICAL_COLUMN_REF_ERROR = "DSWRAPPER_LOGICAL_COLUMN_REF_ERROR";

	// db2j/GaianTable.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to unzip blob.
	 * <br/><br/>
	 * <b>Reason:</b> The blob data may not be a zip file, or may be corrupted.
	 * <br/><br/>
	 * <b>Action:</b> Check the data in the original data source. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_BLOB_UNZIP_ERROR = "ENGINE_BLOB_UNZIP_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot reload the configuration file.
	 * <br/><br/>
	 * <b>Reason:</b> The GaianDB configuration failed to reload. The file might have been corrupted, or unexpected data has been entered.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_CONFIG_RELOAD_ERROR = "ENGINE_CONFIG_RELOAD_ERROR";

	
	/**
	 * <p>
	 * <b>Warning:</b> Rejecting query from another node.
	 * <br/><br/>
	 * <b>Reason:</b> The requesting node does not have authorisation to query on this GaianDB node.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration security and authorisation settings. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_DISALLOWED_NODE_ERROR = "ENGINE_DISALLOWED_NODE_ERROR";
	
	
	/**
	 * <p>
	 * <b>Error:</b> GaianDB cannot process the query.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the query received or issued. GaianDB cannot proceed with it.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Review the query issued or received by the GaianDB node.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_EXEC_AS_FAST_PATH_ERROR = "ENGINE_EXEC_AS_FAST_PATH_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to retrieve column information.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot retrieve column information for the query received.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Review the query issued or received by the GaianDB node.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_EXPLAIN_COLUMN_SET_ERROR = "ENGINE_EXPLAIN_COLUMN_SET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to open explain file for writing.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot write to the specified file for this explain query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Check the GaianDB configuration file and try again.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_EXPLAIN_FILE_OPEN_ERROR = "ENGINE_EXPLAIN_FILE_OPEN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception in GaianResult initialisation.
	 * <br/><br/>
	 * <b>Reason:</b> An exception has occurred while GaianDB was processing the ResultSet for a query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_GAIAN_RESULT_ERROR = "ENGINE_GAIAN_RESULT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Column information isn't correct.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot process the columns for this ResultSet.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration setting for this logical table. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_LEAF_QUALIFIERS_TEST_ERROR = "ENGINE_LEAF_QUALIFIERS_TEST_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to construct empty GaianResultSetMetaData, aborting query.
	 * <br/><br/>
	 * <b>Reason:</b> An exception occurred in GaianDB, the query processing is being aborted.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_METADATA_CONSTRUCT_ERROR = "ENGINE_METADATA_CONSTRUCT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Caught Exception in ResultSet processing, returning SCAN_COMPLETED.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB has caught an exception while processing a ResultSet, the node will stop processing and return a SCAN_COMPLETED status.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_NEXT_ROW_ERROR = "ENGINE_NEXT_ROW_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> There is an error in the constant column for this logical table.
	 * <br/><br/>
	 * <b>Reason:</b> The constant column definition for this logical table isn't correct.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_NODE_QUALIFIERS_TEST_ERROR = "ENGINE_NODE_QUALIFIERS_TEST_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to write performance logs to file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot write performance data to the specified file.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_PERFORMANCE_LOGS_WRITE_ERROR = "ENGINE_PERFORMANCE_LOGS_WRITE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to setup physical projected columns from the physical data source.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the layout of the physical data source. GaianDB cannot understand it.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation for supported data sources. For database, check that the types used in the layout are supported by GaianDB. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_PHYSICAL_PROJECTED_COLUMNS_SETUP_ERROR = "ENGINE_PHYSICAL_PROJECTED_COLUMNS_SETUP_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to setup projected columns for the logical table.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the layout of the logical table. GaianDB cannot create the structure.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_PROJECTED_COLUMNS_SETUP_ERROR = "ENGINE_PROJECTED_COLUMNS_SETUP_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to setup Qualifier[][] structure for the logical table.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the predicates passed as a Qualifier[][] from Derby. GaianDB cannot create the structure.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_QUALIFIERS_SETUP_ERROR = "ENGINE_QUALIFIERS_SETUP_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Exception occurred while checking if child nodes meet the qualifiers requirements for this table.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the SQL qualifiers in the query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_QUALIFIERS_PRUNE_ERROR = "ENGINE_QUALIFIERS_PRUNE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception refreshing registry and VTIs.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot refresh the registry and VTI definitions for data sources.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_REGISTRY_REFRESH_ERROR = "ENGINE_REGISTRY_REFRESH_ERROR";

	// db2j/ICAREST.java
	/**
	 * <p>
	 * <b>Error:</b> Invalid ICAREST function specified.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_INVALID_FUNCTION_ERROR = "DSWRAPPER_ICAREST_INVALID_FUNCTION_ERROR";
	
	/**
	 * Not in use - reserved for future use.
	 * <p> 
	 * <b>Error:</b>
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_ICAREST_EXECUTE_ERROR = "DSWRAPPER_ICAREST_EXECUTE_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> The raw ICAREST Query has been restricted to a maximum number of results.
	 * <br/><br/>
	 * <b>Reason:</b> The result of the query is being restricted.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file for the ICAREST setup properties. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_PARTIAL_RESULT = "DSWRAPPER_ICAREST_PARTIAL_RESULT";
	
	/**
	 * <p>
	 * <b>Error:</b> The raw ICAREST Query encountered an error while fetching a row.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_ROW_FETCH_ERROR = "DSWRAPPER_ICAREST_ROW_FETCH_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> An error occurred while attempting to read from the cache.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_CACHE_READ_ERROR = "DSWRAPPER_ICAREST_CACHE_READ_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> An empty resultset was received while attempting to read from the cache. This indicates that a problem has occurred.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check for exceptions in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_CACHE_READ_EMPTY = "DSWRAPPER_ICAREST_CACHE_READ_EMPTY";
	
	/**
	 * <p>
	 * <b>Error:</b> Field truncation was performed for ICAREST query.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB had to truncate some fields from the ResultSet.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation for information on supported types and their sizes. If possible, adapt the ICAREST results to be within the types boundaries. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_DATA_TRUNCATION_OCCURRED = "DSWRAPPER_ICAREST_DATA_TRUNCATION_OCCURRED";
	
	/**
	 * <p>
	 * <b>Error:</b> SQL errors occurred while trying to cache the results.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB could not cache the results from the ICAREST query.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation for information on supported types and their sizes. If possible, adapt the ICAREST results to be within the types boundaries. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_CACHE_ROWS_ERROR = "DSWRAPPER_ICAREST_CACHE_ROWS_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> The url parameter for this function type has not been specified.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_URL_PARAMETER_NOT_SPECIFIED = "DSWRAPPER_ICAREST_URL_PARAMETER_NOT_SPECIFIED";
	
	/**
	 * <p>
	 * <b>Error:</b> Invalid fetch batch size parameter value specified.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_INVALID_FETCH_BATCH_SIZE_PARAMETER = "DSWRAPPER_ICAREST_INVALID_FETCH_BATCH_SIZE_PARAMETER";
	
	/**
	 * <p>
	 * <b>Error:</b> Invalid fetch buffer size parameter value specified.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ICAREST_INVALID_FETCH_BUFFER_SIZE_PARAMETER = "DSWRAPPER_ICAREST_INVALID_FETCH_BUFFER_SIZE_PARAMETER";
	
	/**
	 * <p>
	 * <b>Error:</b> The value read in the property file cannot be converted into the right format.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The value read in the configuration file has a format which cannot be converted by the application..
	 * <br/><br/>
	 * <b>Action:</b> Check and correct the format of the property in the config file. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_FORMAT_ATTRIBUTE_DEFINITON = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_FORMAT_ATTRIBUTE_DEFINITON";
	
	/**
	 * <p>
	 * <b>Error:</b> The value read from the config file is not recognized by the application.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The value of the property is to be chosen from a set of values and the read one is none of them.
	 * <br/><br/>
	 * <b>Action:</b> Check the possible values for the given property and change the currently-set value. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_VALUE";
	
	/**
	 * <p>
	 * <b>Error:</b> The value read from the config file is not the name of a file which exists or can 
	 * be opened.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The file either does not exist or its access rights do not allow it to be read by the application.
	 * <br/><br/>
	 * <b>Action:</b> Change the file's access rights. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_FILE_NOT_FOUND = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_FILE_NOT_FOUND";
	
	/**
	 * <p>
	 * <b>Error:</b> A column definition is requiring to look for 2 attributes while only one is allowed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * An attribute definition is defined as: GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 att1=?&#62;&#60;t2 att2=?&#62;
	 * with at least two attributes to find ( attr=? ).
	 * <br/><br/>
	 * <b>Action:</b> Change definition for : GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 att1=?&#62;&#60;t2&#62;	<br/>
	 * or : GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 att1=?&#62;&#60;t2 att2=value2&#62;							<br/>
	 * But only one attribute to look for is allowed.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_ATTRIBUTE_TO_LOOK_FOR_DEFINITION = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_ATTRIBUTE_TO_LOOK_FOR_DEFINITION";
	
	/**
	 * <p>
	 * <b>Error:</b> The url given cannot be opened.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The url does not give an access to any servers.
	 * <br/><br/>
	 * <b>Action:</b> Doucle check and change the url or the access rights to the server by GAIANDB. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_URL = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_URL";
	/**
	 * <p>
	 * <b>Error:</b> A column definition has two names while only one is allowed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * A tag definition is defined as: GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 t2 att1="val1"&#62;
	 * with at least two tag names ( t1, t2 ).
	 * <br/><br/>
	 * <b>Action:</b> Change definition for : GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 att1="val1"&#62	<br/>
	 * or : GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t2 att1="val1"&#62;										<br/>
	 * But only one name is allowed.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_NAME_DEFINITION = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_NAME_DEFINITION";
	
	/**
	 * <p>
	 * <b>Error:</b> A column definition has two position while only one is allowed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * A tag definition is defined as: GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 [pos1] [pos2]&#62;
	 * with at least two tag positions ( pos1, pos2 ).
	 * <br/><br/>
	 * <b>Action:</b> Change definition for : GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 [pos1]&#62;		<br/>
	 * or : GenericWS.mine.CX.XML_LOCATE_EXPRESSION=&#60;t1 [pos2]&#62;											<br/>
	 * But only one position is allowed.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_POSITION_DEFINITION = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_POSITION_DEFINITION";
	
	/**
	 * <p>
	 * <b>Error:</b> A property has a tag definition which name is missing.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * In the configuration file, a property defining a tag has been given, without naming the tag.
	 * <br/><br/>
	 * <b>Action:</b> In the config file, check which tag doesn't have any name and give one. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_NAME_NOT_DEFINED = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_NAME_NOT_DEFINED";
	
	/**
	 * <p>
	 * <b>Error:</b> A property has a tag definition which has a wrong format.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * In the config file, one of the properties GenericWS.mine.CX.XML_LOCATE_EXPRESSION
	 * has a tag definition which has a format not reecognized by the aplication. 
	 * <br/><br/>
	 * <b>Action:</b> The error message should tell which element of the tag definition has 
	 * the wrong format. Pick it and correct it. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT = "DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT";
	
	/**
	 * <p>
	 * <b>Error:</b> Missing property in configuration file.
	 * <br/><br/>
	 * <b>Reason:</b> A required property has not been written in the 
	 * configuration file. 
	 * <br/><br/>
	 * <b>Action:</b> The error message should tell which property is
	 * missing. Add it to the coniguration file.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_MISSING_PROPERTY = "DSWRAPPER_GENERICWS_MISSING_PROPERTY";
	
	/**
	 * <p>
	 * <b>Error:</b> The value read from the web service cannot be converted in the right format.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The value read from the web service cannot be converted in the right format.
	 * <br/><br/>
	 * <b>Action:</b> Redefine the format of the data displayed in the logical table 
	 * which is requested. Logical table clomun's format might be numeric or date
	 * while the returned data is only a non-parsable string. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_WRONG_VALUES_FORMAT_IN_FILE = "DSWRAPPER_GENERICWS_WRONG_VALUES_FORMAT_IN_FILE";
	
	/**
	 * <b>Error:</b> Wrong format in the data received from the web service call.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The server sending the data does not provide a stream which content can be parsed in XML or JSON
	 * object.
	 * <br/><br/>
	 * <b>Action:</b> If the received data is in HTML format, the HTML filter can 
	 * be used, especially for removing some tags from the data. For processing, the 
	 * property <b> GenericWS.myPrefix.applyHtmlFilter=TRUE </b> can be applyed. The property
	 * <b> GenericWS.myPrefix.ELT_CONTENT_TO_RMV=&#60;tagName1&#62;,&#60;tagName2&#62; </b>
	 * will remove all the tags having the name "tagName1" or "tagName2" and all their 
	 * content until the end of their end tag. The property <b>
	 * GenericWS.myPrefix.ELT_TO_RMV=&#60;tagName1&#62;,&#60;tagName2&#62; </b> 
	 * will remove only the tag having the name "tagName1" or "tagName2", (from the 
	 * character "&#60;" to the character "&#62;") but will not remove the content of 
	 * the element.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA = "DSWRAPPER_GENERICWS_WRONG_FORMAT_FOR_RECEIVED_DATA";
	
	/**
	 * <p>
	 * <b>Error:</b> Process trying to access a shared data has been killed. Another process could be waiting for 
	 * reading a data which is not going to be writen, and the application mightenter in dead lock.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * An application might have been killing processes around. 
	 * <br/><br/>
	 * <b>Action:</b> If the application is frozen, restart GaianDB. Otherwise, ignore it.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_KILLED_PROCESS = "DSWRAPPER_GENERICWS_KILLED_PROCESS";
	
	/**
	 * <p>
	 * <b>Error:</b> The current thread has been interrupted while 
	 * it was waiting for reading a value.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The VTI has been stopped or has crashed.
	 * <br/><br/>
	 * <b>Action:</b> Restart the reqest. And check that no other 
	 * application could access and stop the current one. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_THREAD_SYNCHRONIZATION = "DSWRAPPER_GENERICWS_THREAD_SYNCHRONIZATION";
	
	/**
	 * <p>
	 * <b>Error:</b> I/O Exception on a server's stream.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The connection to the server that the GenericWS VTI was reading the stream of has been cut. 
	 * <br/><br/>
	 * <b>Action:</b> Restart service?
	 * <p>
	 * FIXME 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_LOST_CONNECTION = "DSWRAPPER_GENERICWS_LOST_CONNECTION";
	
	/**
	 * <p>
	 * <b>Error:</b> ParserConfigurationException happening when 
	 * creating a parser scanning data.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * There has been an exception when generating the parser 
	 * because of the configuration given in the code. 
	 * <br/><br/>
	 * <b>Action:</b> Warn IBM in order to bring out this error. 
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <!li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_GENERICWS_PARSER_ERROR = "DSWRAPPER_GENERICWS_PARSER_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> The connection call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The connection call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * match the location of the mongoDB process and that the process is started
	 * and network-accessible.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_CONNECTION_ERROR = "DSWRAPPER_MONGODB_CONNECTION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * specify an existing, valid database in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_DATABASE_CONN_ERROR = "DSWRAPPER_MONGODB_DATABASE_CONN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * match the user name and password in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_AUTHENTICATION_ERROR = "DSWRAPPER_MONGODB_AUTHENTICATION_ERROR";


	/**
	 * <p>
	 * <b>Error:</b> The call to the mongoDB process to get a collection failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The call to the mongoDB process to get a collection failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * identify a mongoDB collection that is valid.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */	public static final String DSWRAPPER_MONGODB_COLLECTION_ACCESS_ERROR = "DSWRAPPER_MONGODB_COLLECTION_ACCESS_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * match the user name and password in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_META_DATA_ERROR = "DSWRAPPER_MONGODB_META_DATA_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The call to resolve metadata for the qualifier failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The call to resolve metadata for the qualifier failed
	 * <br/><br/>
	 * <b>Action:</b> Check the code in the area of resultset metadata
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_QUALIFIER_META_DATA_ERROR = "DSWRAPPER_MONGODB_QUALIFIER_META_DATA_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The mapping of data from the mongoDB to derby types is trying
	 * to map fields of incompatible types..
	 * <br/><br/>
	 * <b>Reason:</b>
	 * Possible mis-configured data source properties
	 * <br/><br/>
	 * <b>Action:</b> Check the configured mapping between Logical Table and DataSource fields.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR = "DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The mapping of data from the mongoDB to derby types is trying
	 * to map fields of incompatible types..
	 * <br/><br/>
	 * <b>Reason:</b>
	 * Possible mis-configured data source properties
	 * <br/><br/>
	 * <b>Action:</b> Check the configured mapping between Logical Table and DataSource fields.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_VALUE_CONVERSION_ERROR = "DSWRAPPER_MONGODB_VALUE_CONVERSION_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The call to access a derby database qualifier failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The call to access a derby database qualifier failed.
	 * <br/><br/>
	 * <b>Action:</b> Check the code in the area of resultset metadata
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_QUALIFIER_ACCESS_ERROR = "DSWRAPPER_MONGODB_QUALIFIER_ACCESS_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> an attempt is made to execute a mongo query, but we have no connection to mongo
	 * <br/><br/>
	 * <b>Reason:</b>
	 * an attempt is made to execute a mongo query, but we have no connection to mongo
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * specify an existing, valid database in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_NOT_CONNECTED = "DSWRAPPER_MONGODB_NOT_CONNECTED";
	
	/**
	 * <p>
	 * <b>Error:</b> an attempt is made to execute a mongo query which fails.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * an attempt is made to execute a mongo query which fails.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * specify an existing, valid database in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_RESULTSET_NOT_CONNECTED = "DSWRAPPER_RESULTSET_NOT_CONNECTED";

	/**
	 * Not in use - reserved for future use.
	 * <p>
	 * <b>Error:</b>
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_ROW_GET_ERROR = "DSWRAPPER_ROW_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to close Stream.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot close the InputStream for ICAREST.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_STREAM_CLOSE_ERROR_IO = "DSWRAPPER_STREAM_CLOSE_ERROR_IO";

	// db2j/SpatialQuery.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to close GaianChildVTI underlying resultSet.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot close the ResultSet for the child VTI used in a SpatialQuery.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_CHILD_RESULTSET_CLOSE_ERROR = "DSWRAPPER_CHILD_RESULTSET_CLOSE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to recycle JDBC connection associated to GaianChildVTI underlying resultSet.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot close the JDBC connection for the child VTI used in a SpatialQuery.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_JDBC_CONN_RECYCLE_ERROR = "DSWRAPPER_JDBC_CONN_RECYCLE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to fetch row.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB is unable to fetch the row from the cache table.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String DSWRAPPER_ROW_FETCH_SPATIAL_ERROR = "DSWRAPPER_ROW_FETCH_SPATIAL_ERROR";

	// db2j/GaianConfig.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to get rows for configuration request.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB is unable to get the rows forming part of the ResultSet for the configuration request issued.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_GET_ROWS_FOR_CONFIG_ERROR = "ENGINE_GET_ROWS_FOR_CONFIG_ERROR";

	// gaiandb/DatabaseConnector.java
	/**
	 * <p>
	 * <b>Error:</b> Caught InterruptedException whilst waiting for a Database Connection.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB caught an exception and will try again later to connect.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public static final String ENGINE_CONN_WAIT_INTERRUPTED_ERROR = "ENGINE_CONN_WAIT_INTERRUPTED_ERROR";

	// gaiandb/DataSourcesManager.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to close GaianChildVTI.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to close the child VTI process. It will try again later.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_CHILD_CLOSE_ERROR = "ENGINE_CHILD_CLOSE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to re-initialise or close VTI.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB tried to re-use a VTI, but couldn't. A new VTI will be used instead.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_REUSE_VTI_ERROR = "ENGINE_REUSE_VTI_ERROR";

	// gaiandb/GaianChildRSWrapper.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to convert a value from the ResultSet physical column type to a logical table type.
	 * <br/><br/>
	 * <b>Reason:</b> The database table that GaianDB was attempting to create a logical table for, is used vendor specific types that GaianDB cannot convert.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation for information on supported types and their sizes. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_CONVERT_VALUE_ERROR = "ENGINE_CONVERT_VALUE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to close InputStream.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to close an InputStream, it will try again later.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_CLOSE_STREAM_ERROR = "ENGINE_CLOSE_STREAM_ERROR";

	// gaiandb/GaianDBConfig.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to check if resource properties file needs to be reloaded.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB fails to reload the resource properties file. 
	 * <br/><br/>
	 * <b>Action:</b> Check that the GaianDB configuration file hasn't been corrupted. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_CHECK_PROPS_ERROR = "CONFIG_CHECK_PROPS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve Gaian Node host name.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot find the host name for the local machine.
	 * <br/><br/>
	 * <b>Action:</b> Check your Network properties are set correctly, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_GET_NODE_HOSTNAME_ERROR = "CONFIG_GET_NODE_HOSTNAME_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to persist configuration updates.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to save the updates to its configuration file.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_PERSIST_UPDATE_ERROR = "CONFIG_PERSIST_UPDATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot find or instantiate SQLQueryFilter.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot load the class specified as the SQLQueryFilter implementation in the configuration file.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file settings for SQLQueryFiler, that it is pointing to an existing class file. Make sure that the class file has been added to the CLASSPATH in the GaianDB launch script. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_SQL_QUERY_FILTER_ERROR = "CONFIG_SQL_QUERY_FILTER_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot find or instantiate SQLResultFilter.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot load the class specified as the SQLResultFilter implementation in the configuration file.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file settings for SQLResultFilter, that it is pointing to an existing class file. Make sure that the class file has been added to the CLASSPATH in the GaianDB launch script. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_SQL_RESULT_FILTER_ERROR = "CONFIG_SQL_RESULT_FILTER_ERROR";

	// gaiandb/GaianDBConfigProcedures.java
	/**
	 * <p>
	 * <b>Error:</b> Failed to create Gaian Connection to a data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB failed to connection to a data source. Either the data source is not accessible or the configuration settings are incorrects.
	 * <br/><br/>
	 * <b>Action:</b> Check the data source is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_CONNECTION_CREATE_ERROR = "CONFIG_CONNECTION_CREATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot remove Gaian Connection to a data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot remove a connection it holds to a data source. It will retry later.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_CONNECTION_REMOVE_ERROR = "CONFIG_CONNECTION_REMOVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to load a data source for a VTI.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to load a data source provided.
	 * <br/><br/>
	 * <b>Action:</b> Check the data source is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_SET_VTI_ERROR = "CONFIG_DS_SET_VTI_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to create a data source connection to a database table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to connect to a data source specified as a database table.
	 * <br/><br/>
	 * <b>Action:</b> Check the database table is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_SET_RDB_ERROR = "CONFIG_DS_SET_RDB_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to remove a data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a problem while removing a data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_REMOVE_ERROR = "CONFIG_DS_REMOVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to create a data source for a file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a logical table based on a file.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_CREATE_DS_ERROR = "CONFIG_LT_CREATE_DS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to create a data source for a spreadsheet file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a logical table based on an spreadsheet file.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_CREATE_DS_EXCEL_ERROR = "CONFIG_LT_CREATE_DS_EXCEL_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to create a data source for a database table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a logical table based on a database table.
	 * <br/><br/>
	 * <b>Action:</b> Check the database table is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_CREATE_DS_RDB_ERROR = "CONFIG_LT_CREATE_DS_RDB_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to prepare a logical table based on a file data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to prepare a logical table based on the file.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_FILE_IMPORT_ERROR = "CONFIG_LT_FILE_IMPORT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Error while importing from the data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to import the data source from the file.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_FILE_IMPORT_ERROR_SQL = "CONFIG_LT_FILE_IMPORT_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Failed to create a logical table for a spreadsheet file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a logical table for the spreadsheet file.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_CREATE_EXCEL_ERROR = "CONFIG_LT_CREATE_EXCEL_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to create a logical table for a database table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a logical table for a database table.
	 * <br/><br/>
	 * <b>Action:</b> Check the table is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_CREATE_RDB_ERROR = "CONFIG_LT_CREATE_RDB_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to set/register new user.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to set or register a new user.
	 * <br/><br/>
	 * <b>Action:</b> Check the user is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_USER_SET_ERROR = "CONFIG_SET_USER_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to remove user.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to remove the user.
	 * <br/><br/>
	 * <b>Action:</b> Check the user is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_USER_REMOVE_ERROR = "CONFIG_USER_REMOVE_ERROR";

	// gaiandb/GaianDBUtilityProcedures.java
	/**
	 * <p>
	 * <b>Error:</b> Failed to deploy file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB couldn't deploy the file specified.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String UTILITY_DEPLOY_FILE_ERROR = "UTILITY_DEPLOY_FILE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> An exception occurred during the extraction for a ripple deploy.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encounted a problem while performing the ripple deploy.
	 * <br/><br/>
	 * <b>Action:</b> Check the file is specified correctly, and that the file is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String UTILITY_RIPPLE_EXTRACT_ERROR = "UTILITY_RIPPLE_EXTRACT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> An exception occurred while loading Memory info through ManagementFactory.getMemoryMXBean()/.getMemoryPoolMXBeans()
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB could not access/process some Memory management libraries - possibly if these are not included in the JRE.
	 * <br/><br/>
	 * <b>Action:</b> Check the Java platform and whether it is linked to a full JRE. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String UTILITY_MEMORYMXBEAM_ERROR = "UTILITY_MEMORYMXBEAM_ERROR";
	
	// gaiandb/GaianNode.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve Derby Stored Procedure
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to lookup the stored procedure in the Derby catalog (sys.sysaliases).
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Stop GaianDB and try again. 
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NODE_UNABLE_TO_RESOLVE_PROCEDURE_ERROR = "NODE_UNABLE_TO_RESOLVE_PROCEDURE_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to read EntityAssociations properties.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to read the EntityAssociations properties relating to a VTI.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NODE_LOAD_ENTITY_ERROR = "NODE_LOAD_ENTITY_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception caught initialising the MQTT Message Storer.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to initialise the MQTT Message Storer using the properties provided, the node failed to start.
	 * <br/><br/>
	 * <b>Action:</b> Verify that the MQTT Message Storer's properties are specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NODE_MQTT_CONSTRUCT_ERROR = "NODE_MQTT_CONSTRUCT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to close old VTIWrapper during node startup.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to close a VTI during its startup. The node failed to start.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Stop GaianDB and try again. 
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NODE_START_CLOSE_OLD_VTI_ERROR_SQL = "NODE_START_CLOSE_OLD_VTI_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Exception whilst unsubscribing to topic and disconnecting from MQTT and database.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered and exception while starting. The node failed to start.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. 
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NODE_UNSUBSCRIBE_ERROR = "NODE_UNSUBSCRIBE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception in Watchdog loop.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception in its watchdog loop. The node failed to start.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NODE_WATCHDOG_LOOP_ERROR = "NODE_WATCHDOG_LOOP_ERROR";

	// gaiandb/GaianNodeSeeker.java
	/**
	 * <p>
	 * <b>Error:</b> Could not find the driver for this connection.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to connect using the driver specified for this connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_NODE_ADD_ERROR = "DISCOVERY_NODE_ADD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to create or register user database.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create and/or obtain a connection to a user database node which it is assigned to manage.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. It may be that another node is already connected to this user database.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_USER_DB_CONNECT_ERROR = "DISCOVERY_USER_DB_CONNECT_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Discovery Loop Failure.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while discovering other nodes, it will retry again.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_LOOP_ERROR = "DISCOVERY_LOOP_ERROR";

	// gaiandb/GaianResult.java
	/**
	 * <p>
	 * <b>Error:</b> Query execution failure against data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to execute a query against a data source.
	 * <br/><br/>
	 * <b>Action:</b> Check that the data source is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_DS_EXEC_QUERY_ERROR = "RESULT_DS_EXEC_QUERY_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to get integer value for row count from a Gaian Node result.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB node was unable to get an integer as the result for a row count query.
	 * <br/><br/>
	 * <b>Action:</b> Check that the data source connection details are correct, and that the table specified is not corrupt. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_EXPLAIN_ERROR = "RESULT_EXPLAIN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception while setting column values, aborting data source fetch.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while processing column values in a filtered query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_FILTER_ERROR = "RESULT_FILTER_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception while setting column values, aborting data source fetch.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while processing column values in a filtered batch.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_FILTERED_ERROR = "RESULT_GET_FILTERED_ERROR";

	// gaiandb/GaianResultSetMetaData.java
	/**
	 * <p>
	 * <b>Error:</b> Exception caught whilst cloning meta data.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while cloning meta data from a result set.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_CLONE_ERROR = "RESULT_CLONE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Column display size is too big.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot set a column to the size specified in the data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_COL_DISPLAY_SIZE_ERROR = "RESULT_GET_COL_DISPLAY_SIZE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Column name is too big.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot set the column name to the size specified in the data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_COL_NAME_ERROR = "RESULT_GET_COL_NAME_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The number of exposed columns is too big.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot set the number of exposed columns to the size specified in the data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_COL_NAME_EXPOSED_ERROR = "RESULT_GET_COL_NAME_EXPOSED_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The column does not have a type associated with it.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot find a type for this column in the data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_COL_TYPE_ERROR = "RESULT_GET_COL_TYPE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The precision is out of range.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot handle this precision.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation for information on supported types and their sizes. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_PRECISION_ERROR = "RESULT_GET_PRECISION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The scale is out of range.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot handle this scale.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation for information on supported types and their sizes. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_GET_SCALE_ERROR = "RESULT_GET_SCALE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Could not match up column definitions.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot match the result set with the column definition for this query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_MATCHUP_ERROR = "RESULT_MATCHUP_ERROR";

	// gaiandb/InMemoryRows.java
	/**
	 * <p>
	 * <b>Error:</b> Error referencing a logical column which does not exist in physical table. Null ResultSet will be returned for this node.
	 * <br/><br/>
	 * <b>Reason:</b> There is a mismatch between the logical table and the physical table, this might only happen to be on this node.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String RESULT_LOGICAL_COLUMN_REF_ERROR = "RESULT_LOGICAL_COLUMN_REF_ERROR";

	// gaiandb/RowFilter.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to apply qualifiers on index, ignoring index.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot apply the qualifiers from the query.
	 * <br/><br/>
	 * <b>Action:</b> Review the query and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_APPLY_QUALIFIERS_ERROR = "ENGINE_APPLY_QUALIFIERS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to build an appropriate WHERE clause based on query qualifiers.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to process the WHERE qualifiers from the query.
	 * <br/><br/>
	 * <b>Action:</b> Review the query and try again. Check the associated exception in the GaianDB log.
	 * < p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_WHERE_CLAUSE_ERROR = "ENGINE_WHERE_CLAUSE_ERROR:  Exception building WHERE clause: ";

	// gaiandb/VTIBasic.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to process the VTI.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while executing a query in the VTI.
	 * <br/><br/>
	 * <b>Action:</b> Review the query and try again. Review your VTI implementation and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_PROCESS_ERROR = "ENGINE_PROCESS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve VTI's pluralized instances.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception whist invoking a PluralizableVTI method (e.g. getting it's instance IDs) or trying to close it after.
	 * <br/><br/>
	 * <b>Action:</b> Review the VTI class implementation and configuration for the data source wrapper that references it. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_RESOLVING_PLURALIZED_VTI_ATTRIBUTES = "ENGINE_RESOLVING_PLURALIZED_VTI_ATTRIBUTES";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to process VTIBasic as it does not implement GaianVTIChild.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while executing a query in the VTI.
	 * <br/><br/>
	 * <b>Action:</b> Review the query and try again. Review your VTI implementation and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_PROCESS_IMPLEMENTATION_ERROR = "ENGINE_PROCESS_IMPLEMENTATION_ERROR";

	// gaiandb/VTIWrapper.java
	/**
	 * <p>
	 * <b>Error:</b> Failed to load rows in memory.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to load the result set rows in memory.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String ENGINE_ROWS_LOAD_ERROR = "ENGINE_ROWS_LOAD_ERROR";

	
	// gaiandb/apps/dashboard/QueryTab.java
//	/**
//	 * <p>
//	 * <b>Error:</b>
//	 * <br/><br/>
//	 * <b>Reason:</b>
//	 * <br/><br/>
//	 * <br/><br/>
//	 * <b>Action:</b> Check the associated exception in the GaianDB log.
//	 * <p>
//	 * Related Links:
//	 * <ul>
//	 * <li><a href="../../../../../Readme.html">Readme</a>
//	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
//	 * Troubleshooting</a>
//	 * </ul>
//	 * 
//	 **/
//	public final static String DASHBOARD_DOC_OPEN_ERROR = "DASHBOARD_DOC_OPEN_ERROR";
//	
//	/**
//	 * <p>
//	 * <b>Error:</b>
//	 * <br/><br/>
//	 * <b>Reason:</b>
//	 * <br/><br/>
//	 * <br/><br/>
//	 * <b>Action:</b> Check the associated exception in the GaianDB log.
//	 * <p>
//	 * Related Links:
//	 * <ul>
//	 * <li><a href="../../../../../Readme.html">Readme</a>
//	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
//	 * Troubleshooting</a>
//	 * </ul>
//	 * 
//	 **/
//	public final static String DASHBOARD_DOC_PATH_ERROR = "DASHBOARD_DOC_PATH_ERROR";
	
	
	// gaiandb/app/MetricMonitor.java
	/**
	 * <p>
	 * <b>Error:</b> Could not insert metrics into the logical table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to write to the logical table specified for metrics.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String MMON_METRICS_INSERT_ERROR_SQL = "MMON_METRICS_INSERT_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Could not create the physical table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a physical table for metrics.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String MMON_STATEMENT_CREATE_ERROR_SQL = "MMON_STATEMENT_CREATE_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Could not create the physical table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a physical table for metrics.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String MMON_STATEMENT_EXECUTE_ERROR_SQL = "MMON_STATEMENT_EXECUTE_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Could not prepare the MetricMonitor insert statement.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to prepare a database statement for the MetricMonitor.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String MMON_STATEMENT_PREPARE_ERROR_SQL = "MMON_STATEMENT_PREPARE_ERROR_SQL";

	// gaiandb/jdbc/discoveryClient/DiscoveryDriver.java
	/**
	 * <p>
	 * <b>Error:</b> Failed to register Discovery Driver.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB failed to register the Discovery Driver using the java.sql.DriverManager API.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_DRIVER_REGISTER_ERROR = "DISCOVERY_DRIVER_REGISTER_ERROR";

	// gaiandb/jdbc/discoveryClient/GaianConnectionSeeker.java
	/**
	 * <p>
	 * <b>Error:</b> Could not get connection to node.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to connect to the node specified.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_CONNECTION_ERROR = "DISCOVERY_CONNECTION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Could not load the Derby JDBC driver.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to load the Derby JDBC driver from its CLASSPATH.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB launch script to verify that the derby jars are on the CLASSPATH for the node and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_DERBY_JDBC_DRIVER_LOAD_ERROR = "DISCOVERY_DERBY_JDBC_DRIVER_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Error sending discovery requests.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB failed to send the discovery request over multicast.
	 * <br/><br/>
	 * <b>Action:</b> Check your network settings for multicast, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_REQUEST_SEND_ERROR = "DISCOVERY_REQUEST_SEND_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Error setting the discovery client's configuration file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB discovery client failed to set the configuration file specified in the URL.
	 * <br/><br/>
	 * <b>Action:</b> Check that the file exists and is correctly formatted.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_CLIENT_CONFIG_FILE_ERROR = "DISCOVERY_CLIENT_CONFIG_FILE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Could not set the JDBC client driver configuration file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB failed to set the configuration file property for the JDBC client driver.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_JDBC_SET_CONFIG_ERROR = "DISCOVERY_JDBC_SET_CONFIG_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Exception closing Network Socket.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while closing the multicast socket.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String DISCOVERY_NETWORK_SOCKET_CLOSE_ERROR = "DISCOVERY_NETWORK_SOCKET_CLOSE_ERROR";

	// gaiandb/lite/LiteDriver.java
	/**
	 * <p>
	 * <b>Error:</b> Failed to register Discovery Driver.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB failed to register the Discovery Driver using the java.sql.DriverManager API.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NETDRIVER_DRIVER_REGISTER_ERROR = "NETDRIVER_DRIVER_REGISTER_ERROR";

	// gaiandb/searchapis/SearchSIAPI.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve Omnifind EE document search.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to resolve Omnifind EE document search.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String SIAPI_EE_DOC_SEARCH_RESOLVE_ERROR = "SIAPI_EE_DOC_SEARCH_RESOLVE_ERROR";

	// gaiandb/updriver/client/ClientListener.java
	/**
	 * <p>
	 * <b>Error:</b> ClientListener run() failed.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error in the client listener thread for a network driver.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NETDRIVER_CLIENT_LISTENER_ERROR = "NETDRIVER_CLIENT_LISTENER_ERROR";

	// gaiandb/updriver/client/UDPDriver.java
	/**
	 * <p>
	 * <b>Error:</b> Failed to register Discovery Driver.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB failed to register the Discovery Driver using the java.sql.DriverManager API.
	 * <br/><br/>
	 * <b>Action:</b> Check that your GaianDB installation is not corrupt and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NETDRIVER_CLIENT_DRIVER_REGISTER_ERROR = "NETDRIVER_CLIENT_DRIVER_REGISTER_ERROR";

	// gaiandb/updriver/server/RunnableWorker.java
	/**
	 * <p>
	 * <b>Error:</b> RunnableWorker Interrupted.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception that interrupted the RunnableWorker thread for a network driver.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NETDRIVER_SERVER_INTERRUPTED_ERROR = "NETDRIVER_SERVER_INTERRUPTED_ERROR";

	// gaiandb/updriver/server/ServerListener.java
	/**
	 * <p>
	 * <b>Error:</b> ServerListener run() failed.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error in the server listener thread for a network driver.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String NETDRIVER_RUN_ERROR = "NETDRIVER_RUN_ERROR";

	// gaiandb/tools/MQTTMessageStorer.java
	/**
	 * <p>
	 * <b>Error:</b> Exception checking or reloading registry.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while refreshing the configuration for the MQTT Message Storer.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related Links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String MQTT_REFRESH_CONFIG_ERROR = "MQTT_REFRESH_CONFIG_ERROR";

	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------
	// logError() calls
	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------

	// db2j/AbstractVTI.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to cache rows.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to cache the rows, it will not try further.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_CACHE_ROWS_ERROR = "DSWRAPPER_CACHE_ROWS_ERROR";

	// db2j/DocumentFinder.java
	/**
	 * <p>
	 * <b>Error:</b> OmnifindRestPort VTI property must be set in gaiandb_config.properties.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the VTI's properties in its configuration file, it'll abort the query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_OMNIFINDRESTPORT_NOT_SET = "DSWRAPPER_OMNIFINDRESTPORT_NOT_SET";

	/**
	 * <p>
	 * <b>Error:</b> Could not query Omnifind Enterprise.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to connect and query Omnifind Enterprise, this could be due to missing dependencies (siapi.jar & esapi.jar) on the CLASSPATH.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB launch script to verify that the Omnifind jars are on the CLASSPATH for the node and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_OMNIFIND_VERIFY_ERROR = "DSWRAPPER_OMNIFIND_VERIFY_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> No rows to fetch, did not execute - resultRows is null.
	 * <br/><br/>
	 * <b>Reason:</b> There were no more rows to fetch for this query, either the query has finished or the result set was empty.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_NO_ROWS = "DSWRAPPER_NO_ROWS";

	/**
	 * <p>
	 * <b>Error:</b> Unable to read integer value from maxResults property.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to read the maxResults property for Omnifind.
	 * <br/><br/>
	 * <b>Action:</b> Check the Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_MAX_RESULTS_PROP_ERROR = "DSWRAPPER_MAX_RESULTS_PROP_ERROR";

	// db2j/EntityAssociations.java
	/**
	 * Not in use - reserved for future use.
	 * <p>
	 * <b>Error:</b>
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_ENTITYMATRIXJOINER_GET_ERROR = "DSWRAPPER_ENTITYMATRIXJOINER_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> No loaded entity associations.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to locate entity associations for this query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_NO_ENTITIY_ASSOC = "DSWRAPPER_NO_ENTITIY_ASSOC";

	// db2j/FileImport
	/**
	 * <p>
	 * <b>Error:</b> Error while importing file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to import the file specified.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_FILE_IMPORT_NEXT_ERROR_SQL = "DSWRAPPER_FILE_IMPORT_NEXT_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Error while importing file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to import the file specified. Possible structure issue with the file, e.g. missing final record delimiter at EOF
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_FILE_IMPORT_POSSIBLE_STRUCTURE_ERROR = "DSWRAPPER_FILE_IMPORT_POSSIBLE_STRUCTURE_ERROR";

	// db2j/GaianQuery.java
	/**
	 * <p>
	 * <b>Error:</b> Exception whilst getting subquery meta-data via datasource.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception in a subquery. The query will be aborted.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_SUBQUERY_ERROR = "ENGINE_SUBQUERY_ERROR";

	// db2j/GaianTable.java
	/**
	 * <p>
	 * <b>Error:</b> Queried logical table is not defined.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to query a logical table as it has not been defined on this node. They query will be aborted.
	 * <br/><br/>
	 * <b>Action:</b> Define the logical table, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_LT_UNDEFINED = "ENGINE_LT_UNDEFINED";

	
	/**
	 * <p>
	 * <b>Error:</b> Derby error.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error from Derby.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DATA_VALUE_DESCRIPTER_ERROR = "ENGINE_DATA_VALUE_DESCRIPTER_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Queried logical table is already loaded or didn't load properly.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while loading a logical table. The query will be aborted.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_LT_LOAD_ERROR = "ENGINE_LT_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to process argument.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to process the query arguments.
	 * <br/><br/>
	 * <b>Action:</b> Review the query and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_GT_SETUP_ARGS_ERROR = "ENGINE_GT_SETUP_ARGS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Undefined logical table - Building empty meta data object.
	 * <br/><br/>
	 * <b>Reason:</b> The logical table specified in the query is undefined.
	 * <br/><br/>
	 * <b>Action:</b> Define the logical table and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_LT_RSMD_UNDEFINED = "ENGINE_LT_RSMD_UNDEFINED";

	/**
	 * <p>
	 * <b>Error:</b> Local and propagated definitions for the logical table have non matching types.
	 * <br/><br/>
	 * <b>Reason:</b> The logical table definition does not match definitions on other nodes.
	 * <br/><br/>
	 * <b>Action:</b> Review the logical table definition for this node and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DEFS_NON_MATCHING_TYPES = "ENGINE_DEFS_NON_MATCHING_TYPES";

	/**
	 * <p>
	 * <b>Error:</b> GaianTable is already closed.
	 * <br/><br/>
	 * <b>Reason:</b> The query cannot proceed as the GaianTable does not exist anymore.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_FAST_PATH_QUERY_GT_CLOSED = "ENGINE_FAST_PATH_QUERY_GT_CLOSED";

	/**
	 * <p>
	 * <b>Error:</b> Unable to compute credentials block from GaianDB properties.
	 * <br/><br/>
	 * <b>Reason:</b> The credentials block from the GaianDB properties file is corrupt.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB credentials properties are specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_COMPUTE_CREDENTIALS_ERROR = "ENGINE_COMPUTE_CREDENTIALS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Pruning indexes for data sources of logical table as they failed to load properly.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB is performing a cleanup on failed indexes for logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_PRUNING_INDEXES = "ENGINE_PRUNING_INDEXES";

	/**
	 * <p>
	 * <b>Error:</b> Policy plugin returned an invalid type to GaianDB for an operation invoked using method: 
	 * public final Object executeOperation( int opID, Object... args ).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot interpret the result of this plugin operation, so the plugin is ignored and the query will proceed.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_POLICY_PLUGIN_INVALID_OPERATION_RETURN_TYPE = "ENGINE_POLICY_PLUGIN_INVALID_OPERATION_RETURN_TYPE";
	
//	/**
//	 * <p>
//	 * <b>Error:</b> Policy plugin was unable to authenticate user on this node.
//	 * <br/><br/>
//	 * <b>Reason:</b> GaianDB cannot authenticate the user for this policy plugin. The query will be aborted.
//	 * <br/><br/>
//	 * <b>Action:</b> Check the associated exception in the GaianDB log.
//	 * <p>
//	 * Related links:
//	 * <ul>
//	 * <li><a href="../../../../../Readme.html">Readme</a></li>
//	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
//	 * Troubleshooting</a></li>
//	 * </ul>
//	 **/
//	public static final String ENGINE_AUTHENTICATION_ERROR = "ENGINE_AUTHENTICATION_ERROR"; // previously used in GaianTable, line 1030

	/**
	 * <p>
	 * <b>Error:</b> Unable to get a Unique ID.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to get a unique id for this query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_UNIQUE_QUERY_ID_ERROR = "ENGINE_UNIQUE_QUERY_ID_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unexpected explain path value.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to process the path value for the explain query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_EXPLAIN_PATH_VALUE_ERROR = "ENGINE_EXPLAIN_PATH_VALUE_ERROR";

	// db2j/GExcel
	/**
	 * <p>
	 * <b>Error:</b> Spreadsheet cell type cannot be evaluated. This causes the column type to default to a String type (VARCHAR).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB's GExcel VTI was unable to evaluate the cell or it's type using the POI library API.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log: This should point to the specific row and column of the cell.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_GEXCEL_CELL_TYPE_EVALUATION_FAILURE = "DSWRAPPER_GEXCEL_CELL_TYPE_EVALUATION_FAILURE";
	
	/**
	 * <p>
	 * <b>Error:</b> Spreadsheet cell could not be mapped into the logical table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to map the spreadsheet file into a logical table because of a logical type problem.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_GEXCEL_MAP_LT_ERROR = "DSWRAPPER_GEXCEL_MAP_LT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to close the spreadsheet data source wrapper.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while closing the spreadsheet data source wrapper.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_GEXCEL_CLOSE_ERROR = "DSWRAPPER_GEXCEL_CLOSE_ERROR";
	
	// db2j/ICAREST.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to find document URI in an href link of search results.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the document requested.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_DOC_URI_NOT_FOUND = "DSWRAPPER_DOC_URI_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> Unable to find document URI in an href link of search results.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the URI for a referenced document in the search results.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_URI_ARG_NOT_FOUND = "DSWRAPPER_URI_ARG_NOT_FOUND";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to set the value of a logical table field using the value in the physical data source
	 * <br/><br/>
	 * <b>Reason:</b> Possibly the data from the physical data source is incompatible with the logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the expected type and value in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_VALUE_CONVERSION_ERROR = "DSWRAPPER_VALUE_CONVERSION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to find 'collection' argument in href link.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the 'collection' argument in the search results.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_COLLECTION_ARG_NOT_FOUND = "DSWRAPPER_COLLECTION_ARG_NOT_FOUND";
	
	// db2j/SpatialQuery.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to execute Geo Spatial Query.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to execute a Geo Spatial Query on the data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the Spatial Query properties are specified correctly in the GaianDB configuration file. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_SPATIAL_QUERY_EXEC_ERROR = "DSWRAPPER_SPATIAL_QUERY_EXEC_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> No rows to fetch, did not execute - resultRows is null.
	 * <br/><br/>
	 * <b>Reason:</b> There were no more rows to fetch for this query, either the query has finished or the result set was empty.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_SPATIAL_NO_ROWS = "DSWRAPPER_SPATIAL_NO_ROWS";
	
	/**
	 * <p>
	 * <b>Error:</b> The raw Spatial Query has been restricted to a maximum number of results.
	 * <br/><br/>
	 * <b>Reason:</b> The result of the query is being restricted.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file for the Spatial Query setup properties. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DSWRAPPER_SPATIAL_QUERY_PARTIAL_RESULT = "DSWRAPPER_SPATIAL_QUERY_PARTIAL_RESULT";

	// db2j/VTI60.java
	/**
	 * <p>
	 * <b>Error:</b> VTI method not implemented in given class.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB exception used for debugging VTI.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_VTI60_EXCEPTION = "ENGINE_VTI60_EXCEPTION";

	// gaiandb/DatabaseConnectionsChecker.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to poll active jdbc connection of data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a problem while polling connection to a data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DS_CONN_POLL_ERROR = "ENGINE_DS_CONN_POLL_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Maintenance check failed for gaian connection.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a problem while doing maintaining check on a connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_CONN_MAINTENANCE_CHECK_ERROR = "ENGINE_CONN_MAINTENANCE_CHECK_ERROR";

	// gaiandb/DatabaseConnector.java
	/**
	 * <p>
	 * <b>Error:</b> Failed JDBC Connection attempt.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB is reporting an error connecting to a data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Possible causes include database unavailability, 
	 * incorrect user/password and/or insufficient database access rights (e.g. if derby.database.defaultConnectionMode=noAccess in derby.properties).
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_JDBC_CONN_ATTEMPT_ERROR = "ENGINE_JDBC_CONN_ATTEMPT_ERROR";

	// gaiandb/DataSourcesManager.java
//	/**
//	 * <p>
//	 * <b>Error:</b> Unable to create/reload views for logical tables.
//	 * <br/><br/>
//	 * <b>Reason:</b> GaianDB was unable to reload views for logical tables, possibly because other views depend on them.
//	 * <br/><br/>
//	 * <b>Action:</b> Check the associated exception in the GaianDB log.
//	 * <p>
//	 * Related links:
//	 * <ul>
//	 * <li><a href="../../../../../Readme.html">Readme</a></li>
//	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
//	 * Troubleshooting</a></li>
//	 * </ul>
//	 **/
//	public static final String ENGINE_LT_VIEWS_UPDATE_ERROR = "ENGINE_LT_VIEWS_UPDATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to refresh logical table data source list following discovery of a new Gaian Node.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to get a logical table name. It will abort the load operation.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_LT_REFRESH_ERROR = "ENGINE_LT_REFRESH_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to get JDBC connection details of Gaian Connection (skipped).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to connection to the Data Source in order to load it.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DS_CLEAN_ERROR = "ENGINE_DS_CLEAN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to close() JDBC Connection (aborting pool purge).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to close a JDBC Connection while returning the connection to the pool.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_JDBC_CONN_CLOSE_ERROR = "ENGINE_JDBC_CONN_CLOSE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Could not load table meta data so cannot reload table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to reload the table meta data after a change to the logical table definition was made.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_LT_META_DATA_LOAD_ERROR = "ENGINE_LT_META_DATA_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to unload connection pool for a disconnected gaian connection.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to unload/close the JDBC connection pool for the disconnected gaian connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Gaian connections may now intermittently fail, in which case you must restart the node.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_GAIAN_CONN_UNLOAD_ERROR = "ENGINE_GAIAN_CONN_UNLOAD_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to load gaian connection data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to load the data source for this new connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_CONN_DS_LOAD_ERROR = "ENGINE_CONN_DS_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to load data source.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to reload the data source for this connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DS_LOAD_ERROR = "ENGINE_DS_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot load dynamic data source (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to load the data source for the dynamic logical table. Dynamic nodes are needed for subqueries. A dynamic logical table is not defined for the current node being queried, the node is acting as a Gateway and there are no local sources to be queried.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DS_DYNAMIC_LOAD_ERROR = "ENGINE_DS_DYNAMIC_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Suppressed Exception closing vti.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while closing the vti. GaianDB will continue to process requests as normal.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_VTI_CLOSE_ERROR = "ENGINE_VTI_CLOSE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Asynchronous DB connection attempt failed.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to acquire an asynchronous connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DB_CONN_ASYNC_ERROR = "ENGINE_DB_CONN_ASYNC_ERROR";

	// gaiandb/GaianChildRSWrapper.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to set dvdr[0] with updateCount (resultSet was null) - returning no rows.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while fetching the next row of data for this data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_ROW_NEXT_FETCH_ERROR = "ENGINE_ROW_NEXT_FETCH_ERROR";

	// gaiandb/GaianDBConfig.java
	/**
	 * <p>
	 * <b>Error:</b> Invalid Int property value.
	 * <br/><br/>
	 * <b>Reason:</b> The configuration property does not match the expected value of: Integer.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file, correct the error and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_PROP_INT_VALUE_INVALID = "CONFIG_PROP_INT_VALUE_INVALID";

	/**
	 * <p>
	 * <b>Error:</b> Invalid Long property value.
	 * <br/><br/>
	 * <b>Reason:</b> The configuration property does not match the expected value of: Long.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file, correct the error and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_PROP_LONG_VALUE_INVALID = "CONFIG_PROP_LONG_VALUE_INVALID";
	
	/**
	 * <p>
	 * <b>Error:</b> Invalid Boolean property value.
	 * <br/><br/>
	 * <b>Reason:</b> The configuration property does not match the expected value of: Boolean.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file, correct the error and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_PROP_BOOLEAN_VALUE_INVALID = "CONFIG_PROP_BOOLEAN_VALUE_INVALID";

	/**
	 * <p>
	 * <b>Error:</b> Unable to reload user properties.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unabled to reload its configuration file.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file, correct the error and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_USER_PROPS_RELOAD_ERROR = "CONFIG_USER_PROPS_RELOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Only 1 unique column index may be specified - ignoring index definition.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered multiple columns defined as index for this table. 
	 * <br/><br/>
	 * <b>Action:</b> Correct the table definition and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_INDEX_DEF_IGNORE = "CONFIG_INDEX_DEF_IGNORE";

	/**
	 * <p>
	 * <b>Error:</b> Unrecognised physical column name for indexing.
	 * <br/><br/>
	 * <b>Reason:</b> No index was defined for this table.
	 * <br/><br/>
	 * <b>Action:</b> Correct the table definition and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_COLUMN_NOT_RECOGNISED = "CONFIG_COLUMN_NOT_RECOGNISED";

	/**
	 * <p>
	 * <b>Error:</b> Incorrect index definition (ignored), should be: INMEMORY INDEX ON <col_name>.
	 * <br/><br/>
	 * <b>Reason:</b> The column index wasn't specified correctly.
	 * <br/><br/>
	 * <b>Action:</b> Correct the table definition and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_INDEX_DEF_ERROR = "CONFIG_INDEX_DEF_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to get JDBC connection details of LT data source (skipped).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to get the JDBC connection details for the logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_DS_GET_CONN_DETAILS_ERROR = "CONFIG_LT_DS_GET_CONN_DETAILS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to parse/load column mappings for data source end-point constants - used e.g. for early predicate filtering (skipped).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to resolve which columns are constants - probably due to a syntax error in config
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Verify the syntax of your endpoint cols mapping definition in config.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_INVALID_ENDPOINT_COL_MAPPING_SYNTAX = "CONFIG_INVALID_ENDPOINT_COL_MAPPING_SYNTAX";

	/**
	 * <p>
	 * <b>Error:</b> Unable to interpret the syntax for option 'PLURALIZED' of a file data source wrapper (reverting to default wilcard (*) scheme).
	 * <br/><br/>
	 * <b>Reason:</b> The syntax around the PLURALIZED keyword appears to be incorrect. Syntax should be: PLURALIZED [USING WILDCARD|REGEX]
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log. Specify "PLURALIZED USING REGEX" for your file path mask to be interpreted as a regular expression.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_INVALID_FILE_OPTION_PLURALIZED_SYNTAX = "CONFIG_INVALID_FILE_OPTION_PLURALIZED_SYNTAX";
	
	/**
	 * <p>
	 * <b>Error:</b> Invalid property value: MSGBROKER_PORT must be a number.
	 * <br/><br/>
	 * <b>Reason:</b> The value for the property MSGBROKER_PORT was not specified as a number.
	 * <br/><br/>
	 * <b>Action:</b> Fix the GaianDB configuration file's property and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_BROKER_PORT_GET_ERROR = "CONFIG_BROKER_PORT_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Invalid property value: MSGSTORER_ROWEXPIRY_HOURS must be a number.
	 * <br/><br/>
	 * <b>Reason:</b> The valur for the property MSGSTORER_ROWEXPIRY_HOURS was not specified as a number.
	 * <br/><br/>
	 * <b>Action:</b> Fix the GaianDB configuration file's property and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_ROW_EXPIRY_GET_ERROR = "CONFIG_ROW_EXPIRY_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot (un)scramble pwd using key.
	 * <br/><br/>
	 * <b>Reason:</b> This usually happens when no password has been specified.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_KEY_UNSCRAMBLE_ERROR = "CONFIG_KEY_UNSCRAMBLE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Ignoring incorrectly defined field definitions.
	 * <br/><br/>
	 * <b>Reason:</b> This usually happens when the credentials haven't been specified.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_CREDENTIALS_DEF_ERROR = "CONFIG_CREDENTIALS_DEF_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Concurrent config file update occurred
	 * <br/><br/>
	 * <b>Reason:</b> The config file was updated and not reloaded (e.g. by the watchdog) before this API was called.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_FILE_CONCURRENT_UPDATE = "CONFIG_FILE_CONCURRENT_UPDATE";
	

	// gaiandb/GaianDBConfigProcedures.java
	/**
	 * <p>
	 * <b>Error:</b> File not found (will be created).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the file specified as data source, it will be created.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_FILE_NOT_FOUND = "CONFIG_DS_FILE_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> No control file found - (default basic csv properties will be used). 
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to locate the control file for this data source. The control file must be '&lt;datafilename&gt;.properties' or 'FileImportDefaults.properties' in the same folder.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_CONTROL_FILE_NOT_FOUND = "CONFIG_DS_CONTROL_FILE_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> Detected null data source in set of data sources for LT.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB detected that the data source(s) for this logical table are NULL.
	 * <br/><br/>
	 * <b>Action:</b> Check the logical table definition, fix it and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_DS_NULL = "CONFIG_LT_DS_NULL";

	/**
	 * <p>
	 * <b>Error:</b> Cannot set logical table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a new logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the logical table definition. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_SET_LT_ERROR = "CONFIG_SET_LT_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Cannot set constants for logical table as it is not defined.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to set constants for a logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the logical table definition, fix it and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_SET_CONSTANTS_ERROR = "CONFIG_LT_SET_CONSTANTS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Logical Table does not exist (nothing removed).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the logical table specified for deletion, it will not do anything.
	 * <br/><br/>
	 * <b>Action:</b> Check the logical table is specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_REMOVE_ERROR = "CONFIG_LT_REMOVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to find Logical Table at Node.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find a logical table at a specified node.
	 * <br/><br/>
	 * <b>Action:</b> Check the logical table and node are specified correctly and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_SET_FOR_NODE_NULL_LT = "CONFIG_LT_SET_FOR_NODE_NULL_LT";

	/**
	 * <p>
	 * <b>Error:</b> From "setltfornode('&lt;ltName&gt;', '&lt;nodeID&gt;')": The remote table column &lt;columnName&gt; was a constant column with value &lt;value&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> The command wasn't issued properly.
	 * <br/><br/>
	 * <b>Action:</b> To replicate this locally, use API: "call setltconstants('&lt;ltName&gt;' , '&lt;ltconstants*&gt;')". Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_RT_COLUMN_CONSTANT_VALUE = "CONFIG_RT_COLUMN_CONSTANT_VALUE";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve name as a Connection ID &lt;connId&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find an existing connection ID, it will create a new one.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_SET_RDBT_ERROR = "CONFIG_LT_SET_RDBT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> File not found (will be created).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the file specified as data source, it will be creat
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_SET_FILE_NOT_FOUND = "CONFIG_LT_SET_FILE_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> No control file found  - (default basic csv properties will be used).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to locate the control file for this data source. The control file must be '&lt;datafilename&gt;.properties' or 'FileImportDefaults.properties' in the same folder.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_LT_SET_CONTROL_FILE_NOT_FOUND = "CONFIG_LT_SET_CONTROL_FILE_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> RDB Connection does not exist (nothing removed).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the RDB Connection specified for removal, it will not do anything.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_RDB_CONN_REMOVE_NOT_FOUND = "CONFIG_RDB_CONN_REMOVE_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> Cannot remove active connection &lt;connectionID&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to remove a connection as it is still active.
	 * <br/><br/>
	 * <b>Action:</b> Remove dependant data sources first, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_RDB_CONN_REMOVE_ACTIVE = "CONFIG_RDB_CONN_REMOVE_ACTIVE";

	/**
	 * <p>
	 * <b>Error:</b> No fileName was specified.
	 * <br/><br/>
	 * <b>Reason:</b> No file was specified when issuing the call setDsExcel command.
	 * <br/><br/>
	 * <b>Action:</b> Specify a file in the command and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_EXCEL_FILE_NOT_SPECIFIED = "CONFIG_DS_EXCEL_FILE_NOT_SPECIFIED";

	/**
	 * <p>
	 * <b>Error:</b> File not found.
	 * <br/><br/>
	 * <b>Reason:</b> The file specified in the setDsExcel command cannot be found.
	 * <br/><br/>
	 * <b>Action:</b> Verify the file path is correct and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_DS_EXCEL_FILE_NOT_FOUND = "CONFIG_DS_EXCEL_FILE_NOT_FOUND";
	

	// gaiandb/GaianDBProcedureUtils.java
	/**
	 * <p>
	 * <b>Error:</b> SQL API command &ltcall&gt; is not allowed. 
	 * <br/><br/>
	 * <b>Reason:</b> The property ALLOW_SQL_API_CONFIGURATION is not set in the GaianDB configuration.
	 * <br/><br/>
	 * <b>Action:</b> To allow configuration management via the SQL API, set this property in the GaianDB configuration file ALLOW_SQL_API_CONFIGURATION=TRUE. To prevent others from updating your configuration, set a different 'GAIAN_NODE_USR' property and its associated password in derby.properties. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_SQL_API_UNAUTHORISED = "CONFIG_SQL_API_UNAUTHORISED";


	// gaiandb/GaianDBUtilityProcedures.java
	/**
	 * <p>
	 * <b>Error:</b> Thread interrupted.
	 * <br/><br/>
	 * <b>Reason:</b> This is an internal message, part of the GaianDB task scheduling.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_JSLEEP_INTERRUPTED = "CONFIG_JSLEEP_INTERRUPTED";

	/**
	 * <p>
	 * <b>Error:</b> Unable to extract zipped blob for &lt'fromPath&gt; from node &lt;fromNode&gt; (empty result).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to unzip a blob.
	 * <br/><br/>
	 * <b>Action:</b> Check the blob data's integrity, fix it if needed, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_BLOB_EXTRACT_ERROR = "CONFIG_BLOB_EXTRACT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to propagate xripple() for &lt;file&gt; originating or received from &lt;node&gt;. Stopping propagation.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a problem while propagating a file.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB nodes are up and running, and that the file is not corrupted, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_XRIPPLE_PROPAGATE_ERROR = "CONFIG_XRIPPLE_PROPAGATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to deploy file to &lt;toPath&gt; at node &lt;toNode&gt; (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot deploy the file to a node.
	 * <br/><br/>
	 * <b>Action:</b> Check the file and the node's integrity, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a>
	 * <li><a href="../../../../../Readme.html#contents175">Configuration</a>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a>
	 * </ul>
	 * 
	 **/
	public final static String CONFIG_FILE_DEPLOY_ERROR = "CONFIG_FILE_DEPLOY_ERROR";

	// gaiandb/GaianNode.java
	/**
	 * <p>
	 * <b>Error:</b> GaianNode terminating due to Exception.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to start, usually due to the fact a node is already running with the same database.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_START_EXCEPTION_ERROR = "NODE_START_EXCEPTION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> GaianNode terminating due to Exception.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to start, usually due to the fact a node is already running with the same database.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_ID_EXCEPTION_ERROR = "NODE_ID_EXCEPTION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve process name or pid (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> The name or process id for this node cannot be found.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_PID_RESOLVE_ERROR = "NODE_PID_RESOLVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to update logical table views.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to reload the logical table views.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file is not corrupt, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_LT_VIEW_UPDATE_ERROR = "NODE_LT_VIEW_UPDATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to get threadMXBean - will not be able to compute CPU utilisation (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to get a handle to its thread MBean, and will not be able to report performance data.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_THREADMXBEAM_ERROR = "NODE_THREADMXBEAM_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> An exception occurred while querying Memory info through ManagementFactory.getMemoryMXBean()/.getMemoryPoolMXBeans()
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB could not access/process some Memory management libraries - possibly if these are not included in the JRE.
	 * <br/><br/>
	 * <b>Action:</b> Check the Java platform and whether it is linked to a full JRE. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_MEMORYMXBEAM_ERROR = "NODE_MEMORYMXBEAM_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Unable to compute node CPU.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to compute the node's CPU.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_CPU_COMPUTE_ERROR = "NODE_CPU_COMPUTE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> GaianDB was unable to resolve install path.
	 * <br/><br/>
	 * <b>Reason:</b> Failed to find the location of the GaianDB bytecode (e.g. GAIANDB.jar file); 
	 * or unable to resolve relative install path one level above "lib/" folder.
	 * This can happen legitimately when running GaianDB within an OSGi framework.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated warning in the GaianDB log.
	 * Ignore this warning if running GaianDB from within an OSGi framework.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_UNRESOLVED_INSTALL_PATH = "NODE_UNRESOLVED_INSTALL_PATH";
	
	/**
	 * <p>
	 * <b>Error:</b> Load of EntityAssociations, Failed: File does not exist.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to open the Entity Associations input file.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_ENTITY_ASSOC_LOAD_FILE_NOT_FOUND = "NODE_ENTITY_ASSOC_LOAD_FILE_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> Failed to setup logical table views.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to reload the logical table views from its configuration file.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file's integrity and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NODE_LT_VIEWS_SETUP_ERROR = "NODE_LT_VIEWS_SETUP_ERROR";

	// gaiandb/GaianNodeSeeker.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to apply specified MULTICAST_INTERFACES (using default &lt;defaultInterface&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to resolve the multicast interfaces.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file. Check your network settings and firewall, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_MULTICAST_INTERFACES_ERROR = "DISCOVERY_MULTICAST_INTERFACES_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to validate discovery IP speficied (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to validate the discovery IP it was given.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_IP_VALIDATE_ERROR = "DISCOVERY_IP_VALIDATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Discovery Loop Failure: possible lack of network connectivity (re-trying...).
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the network connectivity for the GaianDB node.
	 * <br/><br/>
	 * <b>Action:</b> Check the network settings and firewall, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_LOOP_ATTEMPT_ERROR = "DISCOVERY_LOOP_ATTEMPT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to locally re-broadcast ACK discovery message destined to another node on localhost.
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the network connectivity for the GaianDB node.
	 * <br/><br/>
	 * <b>Action:</b> Check whether the local loopback interface appears at the top of the log file and has a broadcast IP.
	 * You may need to adjust settings in files /etc/hosts and /etc/resolv.conf.
	 * Also check the general network settings (ifconfig/ipconfig) and firewall, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_LOCALHOST_REBROADCASTING_ERROR = "DISCOVERY_LOCALHOST_REBROADCASTING_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> Failed to reciprocate connection (already exists).
	 * <br/><br/>
	 * <b>Reason:</b> The GaianDB node was discovered by another node, and tried to connect back, but the connection to that node already exists.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_CONN_RECIPROCATE_ERROR = "DISCOVERY_CONN_RECIPROCATE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Connection to node already exists (but is neither inbound nor outbound).
	 * <br/><br/>
	 * <b>Reason:</b> The GaianDB node has an existing connection to another node, but it is neither an inbound or outbound connection.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_CONN_EXISTS = "DISCOVERY_CONN_EXISTS";

	/**
	 * <p>
	 * <b>Error:</b> Exception resolving gateway (ignoring).
	 * <br/><br/>
	 * <b>Reason:</b> The GaianDB node property for discovery gateway does not specify a reachable host.
	 * <br/><br/>
	 * <b>Action:</b> Check network connectivity to the gateway and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_GATEWAY_RESOLVE_ERROR = "DISCOVERY_GATEWAY_RESOLVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve local IP address to gateway node.
	 * <br/><br/>
	 * <b>Reason:</b> The GaianDB was unable to resolve the specified IP address for the gateway node.
	 * <br/><br/>
	 * <b>Action:</b> Check the IP address is correct, the network connectivity to the gateway and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_GATEWAY_IP_RESOLVE_ERROR = "DISCOVERY_GATEWAY_IP_RESOLVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve local network interfaces (using empty set).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to resolve local network interfaces for its own host.
	 * <br/><br/>
	 * <b>Action:</b> Check the host network settings and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_IP_LOCAL_RESOLVE_ERROR = "DISCOVERY_IP_LOCAL_RESOLVE_ERROR";

	// gaiandb/GaianNodeSeeker.java
	/**
	 * <p>
	 * <b>Error:</b> Invalid provider values for precision and scale &lt;columnPrecisions&gt;, &lt;columnScales&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> The number/decimal found in the column is too big, it will be truncated. Derby requirements are: precision >= scale >= 0; using defaults (5,0).
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String RESULT_PROVIDER_VALUES_INVALID = "RESULT_PROVIDER_VALUES_INVALID";

//	/**
//	 * <p>
//	 * <b>Error:</b> Missing type length definition for column &lt;columnName&gt;: &lt;typeName&gt;.
//	 * <br/><br/>
//	 * <b>Reason:</b> GaianDB encountered a database which needs to have a length specified, but did not find one.
//	 * <br/><br/>
//	 * <b>Action:</b> Check the associated exception in the GaianDB log.
//	 * <p>
//	 * Related links:
//	 * <ul>
//	 * <li><a href="../../../../../Readme.html">Readme</a></li>
//	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
//	 * Troubleshooting</a></li>
//	 * </ul>
//	 **/
//	public static final String RESULT_COLUMN_LENGTH_DEF_MISSING = "RESULT_COLUMN_LENGTH_DEF_MISSING";

	/**
	 * <p>
	 * <b>Error:</b> Unsupported JDBC type: &lt;type&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a JDBC type it does not support.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation on supported JDBC types for each supported database providers. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String RESULT_JDBC_TYPE_UNSUPPORTED = "RESULT_JDBC_TYPE_UNSUPPORTED";

	/**
	 * <p>
	 * <b>Error:</b> Invalid length in column specification for column &lt;colName&gt;: &lt;typeName&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an invalid length for the type of column.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation on supported JDBC types for each supported database providers. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String RESULT_COLUMN_LENGTH_INVALID = "RESULT_COLUMN_LENGTH_INVALID";

	/**
	 * <p>
	 * <b>Error:</b> Unsupported JDBC type: &lt;type&gt;. Known mapping(s): &lt;typeName&gt; (&l;tprovider&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a JDBC type it does not support.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation on supported JDBC types for each supported database providers. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String RESULT_LOOKUP_JDBC_TYPE_UNSUPPORTED = "RESULT_LOOKUP_JDBC_TYPE_UNSUPPORTED";

	// gaiandb/RowsFilter.java
	/**
	 * <p>
	 * <b>Error:</b> Could not get Orderable value for special column (e.g. query id or propagation count) from Qualifiers.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while reading the query qualifiers.
	 * <br/><br/>
	 * <b>Action:</b> Check the query is valid, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_QUALIFIERS_SPECIAL_COLUMN_ERROR = "ENGINE_QUALIFIERS_SPECIAL_COLUMN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Invalid operator detected (not one of the Orderable interface).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while reading the query qualifiers. There are unknown operators present.
	 * <br/><br/>
	 * <b>Action:</b> Check the query is valid, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_OPERATOR_INVALID = "ENGINE_OPERATOR_INVALID";

	/**
	 * <p>
	 * <b>Error:</b> Could not get Orderable value from Qualifier.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while reading the query qualifiers. There are unknown operators present.
	 * <br/><br/>
	 * <b>Action:</b> Check the query is valid, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_QUALIFIER_ORDERABLE_ERROR = "ENGINE_QUALIFIER_ORDERABLE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot build DVD: Unsupported JDBC type: &lt;jdbcType&gt;for RDB Provider &lt;provider&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a JDBC type it does not support.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation on supported JDBC types for each supported database providers. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DVD_BUILD_JDBC_TYPE_UNSUPPORTED = "ENGINE_DVD_BUILD_JDBC_TYPE_UNSUPPORTED";

	// gaiandb/SecurityManager.java
	/**
	 * <p>
	 * <b>Error:</b> Could not extract user credentials from DataValueDescriptor.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB could not verify the user credentials for this logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_CREDENTIALS_VERIFY_ERROR = "ENGINE_CREDENTIALS_VERIFY_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Could not create GDB_USERS table.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB could not create its internal table GDB_USERS.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_USER_TABLE_INIT_ERROR = "ENGINE_USER_TABLE_INIT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to extract user fields - no entry found for user.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to extract the user information.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_USER_FIELDS_GET_ERROR = "ENGINE_USER_FIELDS_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Error case detected: more than one credentials entry was found for user.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered multiple credentials for the user.
	 * <br/><br/>
	 * <b>Action:</b> Check the credentials, and remove duplicate. Only one entry is allowed. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_USER_CREDENTIALS_ERROR = "ENGINE_USER_CREDENTIALS_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> User not found on local server.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to authenticate the user as it is not registered locally.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_USER_NOT_FOUND = "ENGINE_USER_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> Setting password (currently blank) from incoming query for user.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB has received a request to change the currently blank password for the user.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_USER_PASSWORD_BLANK = "ENGINE_USER_PASSWORD_BLANK";

	/**
	 * <p>
	 * <b>Error:</b> Incorrect password entered for user.
	 * <br/><br/>
	 * <b>Reason:</b> The new password supplied for the user is incorrect, GaianDB will not alter the current password.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_USER_PASSWORD_INCORRECT = "ENGINE_USER_PASSWORD_INCORRECT";

	// gaiandb/VTIFile.java
	/**
	 * <p>
	 * <b>Error:</b> Error while importing file.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while importing a file as a data source.
	 * <br/><br/>
	 * <b>Action:</b> Check the file exists and is not corrupt, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_FILE_IMPORT_INIT_ERROR_SQL = "ENGINE_FILE_IMPORT_INIT_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Error while importing.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an SQL exception while processing an file with SQL or Gaian Queries in it.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_FILE_IMPORT_ERROR_SQL = "ENGINE_FILE_IMPORT_ERROR_SQL";

	// gaiandb/VTIRDBResult.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to PREPARE statement - (empty result for this data source).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a prepare statement for a data source.
	 * <br/><br/>
	 * <b>Action:</b> 'call listrdbc()' to identify the data source, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_STATEMENT_PREPARE_ERROR_SQL = "ENGINE_STATEMENT_PREPARE_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Unreachable Gateway: (can slow down queries) - to remove, run: call gdisconnect(&lt;connectionIdd&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> A gateway node is unreachable.
	 * <br/><br/>
	 * <b>Action:</b> Try removing and adding the gateway again, check the network settings, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_GATEWAY_UNREACHABLE = "ENGINE_GATEWAY_UNREACHABLE";

	/**
	 * <p>
	 * <b>Error:</b> Gaian connection is not registered as either a defined or discovered connection.
	 * <br/><br/>
	 * <b>Reason:</b> A gaian connection id is not defined so cannot be looked up.
	 * <br/><br/>
	 * <b>Action:</b> If connections appear unstable, disconnect the node or recycle it. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_GAIAN_CONN_NOT_REGISTERED = "ENGINE_GAIAN_CONN_NOT_REGISTERED";
	
	/**
	 * <p>
	 * <b>Error:</b> Unreachable RDBMS in Data Source: (can slow down queries) - to remove, run: call removeds(&lt;dsName&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> A data source for a logical table is unreachable.
	 * <br/><br/>
	 * <b>Action:</b> Try removing and adding the data source again, check the network settings, then try again.  Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DS_RDBMS_UNDREACHABLE = "ENGINE_DS_RDBMS_UNDREACHABLE";

	/**
	 * <p>
	 * <b>Error:</b> Statement execution failed due to lost JDBC connection (returning null).
	 * <br/><br/>
	 * <b>Reason:</b> The connection is invalid.
	 * <br/><br/>
	 * <b>Action:</b> Check the network settings, and the remote database, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_STATEMENT_EXEC_JDBC_CONN_ERROR = "ENGINE_STATEMENT_EXEC_JDBC_CONN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to EXECUTE statement (returning null result).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to execute a statement.
	 * <br/><br/>
	 * <b>Action:</b> Check the data source, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_STATEMENT_EXEC_ERROR = "ENGINE_STATEMENT_EXEC_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unreachable Gateway: (can slow down queries) - to remove, run: call gdisconnect(&lt;connectionIdd&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> A gateway node is unreachable.
	 * <br/><br/>
	 * <b>Action:</b> Try removing and adding the gateway again, check the network settings, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_GATEWAY_UNREACHABLE_CONN_LOST = "ENGINE_GATEWAY_UNREACHABLE_CONN_LOST";

	/**
	 * <p>
	 * <b>Error:</b> Unreachable RDBMS in Data Source: (can slow down queries) - to remove, run: call removeds(&lt;dsName&gt;).
	 * <br/><br/>
	 * <b>Reason:</b> A data source for a logical table is unreachable.
	 * <br/><br/>
	 * <b>Action:</b> Try removing and adding the data source again, check the network settings, then try again.  Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DS_RDBMS_UNDREACHABLE_CONN_LOST = "ENGINE_DS_RDBMS_UNDREACHABLE_CONN_LOST";

	/**
	 * <p>
	 * <b>Error:</b> JDBC Execution time was not recorded for a ResultSet (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the execution time for this ResultSet.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_JDBC_EXEC_TIME_UNRECORDED = "ENGINE_JDBC_EXEC_TIME_UNRECORDED";

	/**
	 * <p>@deprecated
	 * <b>Error:</b> Unsupported VTI class.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to access the VTI class.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_VTI_CLASS_UNSUPPORTED = "ENGINE_VTI_CLASS_UNSUPPORTED";

	/**
	 * <p>@deprecated
	 * <b>Error:</b> Class not found.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to access the VTI class.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_VTI_CLASS_NOT_FOUND = "ENGINE_VTI_CLASS_NOT_FOUND";

	/**
	 * <p>
	 * <b>Error:</b> Cannot load DBMS rows in memory for remote GaianNode - ignoring INMEMORY option.
	 * <br/><br/>
	 * <b>Reason:</b> INMEMORY is not a valid option for Gaian data sources.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DBMS_ROWS_LOAD_ERROR = "ENGINE_DBMS_ROWS_LOAD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Cannot load DBMS rows in memory for subqueries - ignoring INMEMORY option.
	 * <br/><br/>
	 * <b>Reason:</b> INMEMORY is not a valid option for Gaian data sources.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_DBMS_ROWS_LOAD_SUBQ_ERROR = "ENGINE_DBMS_ROWS_LOAD_SUBQ_ERROR";

	// gaiandb/VTIWrapper.java
	/**
	 * <p>
	 * <b>Error:</b>  Failed to get all column types (ignoring).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to get all the colum types for this logical table.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_COLUMN_TYPES_GET_ERROR = "ENGINE_COLUMN_TYPES_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Physical source column contains null values - skipping this column for indexing.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a null values in a column.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_PHYSICAL_SRC_COLUMN_VALUES_NULL = "ENGINE_PHYSICAL_SRC_COLUMN_VALUES_NULL";

	/**
	 * <p>
	 * <b>Error:</b> Physical source column cannot be converted to logical table column type (skipping this column for indexing).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to convert a column to a logical table type.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_PHYSICAL_SRC_COLUMN_CONVERT_ERROR = "ENGINE_PHYSICAL_SRC_COLUMN_CONVERT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to build InMemoryRows because of the type of physical column id.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a Column type it does not support.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation on supported JDBC types for each supported database providers. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String ENGINE_ROWS_LOAD_COLUMN_ID_ERROR = "ENGINE_ROWS_LOAD_COLUMN_ID_ERROR";

	// gaiandb/jdbc/discoveryClient/GaianConnectionSeeker.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to apply specified MULTICAST_INTERFACES (using default).
	 * <br/><br/>
	 * <b>Reason:</b> The specified MULTICAST_INTERFACES is incorrect.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_MULTICAST_INTERFACES_APPLY_ERROR = "DISCOVERY_MULTICAST_INTERFACES_APPLY_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve local network interfaces (using empty set).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to resolve local network interfaces for its own host.
	 * <br/><br/>
	 * <b>Action:</b> Check the host network settings and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_NETWORK_INTERFACES_RESOLVE_ERROR = "DISCOVERY_NETWORK_INTERFACES_RESOLVE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Failed to open Discovery Socket: possible lack of network connectivity (re-trying...).
	 * <br/><br/>
	 * <b>Reason:</b> There is a problem with the network connectivity for the GaianDB node.
	 * <br/><br/>
	 * <b>Action:</b> Check the network settings and firewall, then try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_NETWORK_SOCKET_OPEN_ERROR = "DISCOVERY_NETWORK_SOCKET_OPEN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to validate IP speficied.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to validate the discovery IP it was given.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String DISCOVERY_CONN_IP_VALIDATE_ERROR = "DISCOVERY_CONN_IP_VALIDATE_ERROR";

	// gaiandb/lite/LiteConnection
	/**
	 * <p>
	 * <b>Error:</b> Unable to extract predicates (delegating query).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to extract predicates in the query received.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_PREDICATES_EXTRACT_ERROR = "NETDRIVER_PREDICATES_EXTRACT_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to execute subquery (delegated).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to execute the subquery.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_SUBQUERY_EXEC_ERROR = "NETDRIVER_SUBQUERY_EXEC_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unresolved queried column name for GaianTable.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a column in a query which it cannot resolve.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_COLUMN_UNRESOLVED_FOR_GT = "NETDRIVER_COLUMN_UNRESOLVED_FOR_GT";

	// gaiandb/lite/LitePreparedStatement
	/**
	 * <p>
	 * <b>Error:</b> Invalid number of arguments detected for DELEGATE_SQL_PROC(&lt;sql&gt;) - ignoring call.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB received an invalid number of arguments for the procedure.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_ARGS_NUMBER_INVALID = "NETDRIVER_ARGS_NUMBER_INVALID";

	/**
	 * <p>
	 * <b>Error:</b> Erroneous request for SQL delegation: distance to server node should be greater than 0 - returning null for query.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB received erroneous information in a query.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_ERRONEOUS_REQUEST = "NETDRIVER_ERRONEOUS_REQUEST";

	/**
	 * <p>
	 * <b>Error:</b> Unable to delegate SQL.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to delegate a query to its nearest node.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_SQL_DELEGATE_ERROR_SQL = "NETDRIVER_SQL_DELEGATE_ERROR_SQL";

	/**
	 * <p>
	 * <b>Error:</b> Unable to delegate SQL: Unknown node path.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to delegate a query to its nearest node.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_SQL_DELEGATE_NODE_ID_NULL = "NETDRIVER_SQL_DELEGATE_NODE_ID_NULL";

	// gaiandb/searchapis/SearchSIAPI.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to resolve Omnifind Federator for app &lt;applicationName&gt;, &lt;appID&gt; - returning no results.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the application in Omnifind.
	 * <br/><br/>
	 * <b>Action:</b> Check collection name and id match, and check security for search application. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String SIAPI_OMNIFIND_RESOLVE_ERROR = "SIAPI_OMNIFIND_RESOLVE_ERROR";

	// gaiandb/tools/MQTTMessageStorer
	/**
	 * <p>
	 * <b>Error:</b> Cannot connect to Microbroker.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot connect to Microbroker, it will retry periodically in background.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_MICROBROKER_CONN_ATTEMPT_FAILED = "MQTT_MICROBROKER_CONN_ATTEMPT_FAILED";

	/**
	 * <p>
	 * <b>Error:</b> A valid MICROBROKER_HOST definition must be specified in the config file to start the Message Storer.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the MICROBROKER_HOST property.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_MICROBROKER_HOST_DEF_UNSPECIFIED = "MQTT_MICROBROKER_HOST_DEF_UNSPECIFIED";

	/**
	 * <p>
	 * <b>Error:</b> A valid MICROBROKER_PORT definition must be specified in the config file to start the Message Storer.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to find the MICROBROKER_PORT property.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB configuration file, and try again. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_MICROBROKER_PORT_DEF_UNSPECIFIED = "MQTT_MICROBROKER_PORT_DEF_UNSPECIFIED";

	/**
	 * <p>
	 * <b>Error:</b> Cannot re-connect to Microbroker.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB cannot connect to Microbroker, it will retry periodically in background.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_MICROBROKER_RECONN_ATTEMPT_FAILED = "MQTT_MICROBROKER_RECONN_ATTEMPT_FAILED";

	/**
	 * <p>
	 * <b>Error:</b> Unable to parse timestamp.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to parse the timestamp value from MQTT.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_TIMESTAMP_PARSE_ERROR = "MQTT_TIMESTAMP_PARSE_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to get DB columns data from message (skipping it).
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to parse the column data from MQTT.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_DB_COLUMNS_GET_ERROR = "MQTT_DB_COLUMNS_GET_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Routine garbage collect: Unable to delete expired rows.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an error while clearing up expired rows.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String MQTT_ROWS_EXPIRED_DELETE_ERROR = "MQTT_ROWS_EXPIRED_DELETE_ERROR";

	// gaiandb/updriver/client/UDPConnection.java
	/**
	 * <p>
	 * <b>Error:</b> A message longer than the current datagram size has been sent.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB received a message it cannot process as it's too big.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_MESSAGE_TOO_LONG = "NETDRIVER_MESSAGE_TOO_LONG";

	/**
	 * <p>
	 * <b>Error:</b> Throwing away message containing unwanted queryID, sequenceNumber and type.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB's network driver tries to return a message from the map having the expected queryID, sequenceNumber and type before the timeout exceeds.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_MESSAGE_CONTAINS_UNWANTED_TYPES = "NETDRIVER_MESSAGE_CONTAINS_UNWANTED_TYPES";


	/**
	 * <p>
	 * <b>Error:</b> Unimplemented method.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB's network driver doesn't support this method.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_UNIMPLEMENTED_METHOD = "NETDRIVER_UNIMPLEMENTED_METHOD";

	// gaiandb/udpdriver/client/ResultSetMetaData.java
	/**
	 * <p>
	 * <b>Error:</b> Unsupported JDBC type: &lt;type&gt;.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered a JDBC type it does not support.
	 * <br/><br/>
	 * <b>Action:</b> Check the GaianDB documentation on supported JDBC types for each supported database providers. Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_COLUMN_LOOKUP_JDBC_TYPE_UNSUPPORTED = "NETDRIVER_COLUMN_LOOKUP_JDBC_TYPE_UNSUPPORTED";
	
	// gaiandb/udpdriver/server/ClientState.java:
	/**
	 * <p>
	 * <b>Error:</b> Detected duplicate client message (ignored).
	 * <br/><br/>
	 * <b>Reason:</b> A message with a sequence number identical to a previous message received has been found.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_CLIENT_MESSAGE_DUPLICATE = "NETDRIVER_CLIENT_MESSAGE_DUPLICATE";

	// gaiandb/udpdriver/server/RunnableWorker.java
	/**
	 * <p>
	 * <b>Error:</b> Unable to lookup client query state on server.
	 * <br/><br/>
	 * <b>Reason:</b> This node may have been recycled (ignoring client request)
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_CLIENT_QUERY_STATE_LOOKUP_ERROR = "NETDRIVER_CLIENT_QUERY_STATE_LOOKUP_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Unable to lookup client query state on server.
	 * <br/><br/>
	 * <b>Reason:</b> This node may have been recycled (ignoring client request).
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_MESSAGE_RESPONSE_BUILD_ERROR = "NETDRIVER_MESSAGE_RESPONSE_BUILD_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> A message longer than the current datagram size has been sent.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB received a message it cannot process as it's too big.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String NETDRIVER_MESSAGE_LONGER_THAN_DATAGRAM = "NETDRIVER_MESSAGE_LONGER_THAN_DATAGRAM";

	
	// QueryTab
	/**
	 * <p>
	 * <b>Error:</b> A client statement was executed on the server, but received an error.
	 * <br/><br/>
	 * <b>Reason:</b> The error code could not be determined. 
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/
	public static final String CLIENT_STMT_CLOSE_ERROR = "CLIENT_STMT_CLOSE_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> A client statement was executed on the server, but received an error.
	 * <br/><br/>
	 * <b>Reason:</b> The error code could not be determined. 
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 **/
	public static final String CLIENT_STMT_EXEC_RETURNED_ERROR = "CLIENT_STMT_EXEC_RETURNED_ERROR";
	
	/**
	 * Not in use - reserved for future use.
	 * <p>
	 * <b>Error:</b>
	 * <br/><br/>
	 * <b>Reason:</b>
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/
	public static final String CLIENT_DOC_LOOKUP_ERROR = "CLIENT_DOC_LOOKUP_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> Could not create a secure context.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to create a secure context using the credentials provided.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/
	public static final String SECURITY_NO_CONTEXT = "SECURITY_NO_CONTEXT";
	
	/**
	 * <p>
	 * <b>Error:</b> Could not find a valid kerberos token.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB was unable to locate a valid kerberos token.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/	
	public static final String SECURITY_INVALID_TOKEN = "SECURITY_INVALID_TOKEN";
	
	/**
	 * <p>
	 * <b>Error:</b> Could not get the secure token.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while processing a security token.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/		
	public static final String SECURITY_TOKEN_IO_EXCEPTION = "SECURITY_TOKEN_IO_EXCEPTION";
	
	/**
	 * <p>
	 * <b>Error:</b> Could not get the secure token.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while processing a security token.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/	
	public static final String SECURITY_TOKEN_CLASS_NOT_FOUND = "SECURITY_TOKEN_CLASS_NOT_FOUND";
	
	/**
	 * <p>
	 * <b>Error:</b> Could not get a valid secure token.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while processing a security token.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/	
	public static final String SECURITY_FOUND_VALID_TOKEN = "SECURITY_FOUND_VALID_TOKEN";
	
	/**
	 * <p>
	 * <b>Error:</b> Could not connect to the database with credentials.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while connecting to the database with credentials.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/	
	public static final String DBCONNECTOR_CANNOT_CONNECT = "DBCONNECTOR_CANNOT_CONNECT";
	
	/**
	 * <p>
	 * <b>Error:</b> Could not find the session ID for this token.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB encountered an exception while looking for a session ID.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/	
	public static final String DBCONNECTOR_CANNOT_FIND_SESSION_ID = "DBCONNECTOR_CANNOT_FIND_SESSION_ID";
	
	/**
	 * <p>
	 * <b>Error:</b> The program is shutting down.
	 * <br/><br/>
	 * <b>Reason:</b> GaianDB has shut down.
	 * <br/><br/>
	 * <b>Action:</b> Check the associated exception in the GaianDB log.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../../Readme.html">Readme</a></li>
	 * <li><a href="../../../../../Readme.html#contents359">FAQ &
	 * Troubleshooting</a></li>
	 * </ul>
	 *
	 **/	
	public static final String DBCONNECTOR_SHUTDOWN_MESSAGE = "DBCONNECTOR_SHUTDOWN_MESSAGE";
}


