/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;



import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianChildRSWrapper;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilterX;

/**
 * @author DavidVyvyan
 */
public class SpatialQuery extends AbstractVTI { //VTI60 implements VTICosting, IFastPath, GaianChildVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "SpatialQuery", 20 );
	
	private static final String DB2 = "DB2";
	private static final String ORACLE = "ORACLE";
	
	private static final String FUNCTION_WITHIN = "within";
	private static final String FUNCTION_DISTANCE = "distance";

	private static final String PROP_SOURCE = "SOURCE";
	private static final String PROP_DISTANCE_UNIT = "DISTANCE.UNIT";

	private static final String PROP_DB2_SQL_WITHIN_REF = DB2 + ".SQL.WITHIN.REF";
	private static final String PROP_DB2_SQL_DISTANCE_REF = DB2 + ".SQL.DISTANCE.REF";
	
	private static final String PROP_DB2_SQL_WITHIN_GML = DB2 + ".SQL.WITHIN.GML";
	private static final String PROP_DB2_SQL_DISTANCE_GML = DB2 + ".SQL.DISTANCE.GML";
	
	private String geoArgs = null; // An array holding the geoFunction as first arg, followed by any applicable arguments
	
	private String[] jdbcSources = null;
	
	private String sqlQuery = null;
	
	// Vector of Integers
//	private Vector<GaianChildVTI> results = null;
	private GaianChildVTI resultRows = null;
	private ResultSet underlyingResultSet = null;
	
	private int numFetches = 0;
	
	
	
	// search sring -> latest row count
	private static Map<String, Long> estimatedRowCounts =
		Collections.synchronizedMap( new CachedHashMap<String, Long>( GaianTable.QRY_METADATA_CACHE_SIZE ) );

	private final int rowsBatchSize;
	private final int fetchBufferSize;
	private final BlockingDeque<DataValueDescriptor[][]> fetchBuffer;
	private final DataValueDescriptor[] resultRowTemplate;
	
	private DataValueDescriptor[][] currentResultBatch;
	private int currentResultBatchIndex = 0;
	
	private SQLResultFilter sqlResultFilter;
	private SQLResultFilterX sqlResultFilterX;
	
	private int maxSourceRows = -1;
	
	private boolean queryRunning = false;
		
	public Hashtable<String, String> getDefaultVTIProperties() {
		
		if ( null == defaultVTIProperties ) {
		
			Hashtable<String, String> props = super.getDefaultVTIProperties();
			
			props.put(PROP_SCHEMA, "GEOREF VARCHAR(256), DNUM BIGINT, CACHEID INT");
			props.put(PROP_CACHE_EXPIRES, "60");
			
			props.put(PROP_CACHE_PKEY, "GEOREF, DNUM, CACHEID");
			
			props.put(PROP_DISTANCE_UNIT, "KILOMETER");
					
			props.put(PROP_DB2_SQL_WITHIN_GML,
					"select ref, dnum from esadmin.geo_table " +
					"where db2gse.st_within(geo, db2gse.st_geometry( cast ('$0' as clob(2g)), db2gse.st_srsid(geo)) )=1"
			);
			
			props.put(PROP_DB2_SQL_DISTANCE_GML,
					"select ref, dnum from esadmin.geo_table " +
					"where db2gse.st_distance(geo, db2gse.st_geometry( cast('$0' as clob(2g)), db2gse.st_srsid(geo)), '$2')<$1"
			);
			
			props.put(PROP_DB2_SQL_WITHIN_REF,
					"select g1.ref, g1.dnum from esadmin.geo_table g1, esadmin.geo_table g2 " +
					"where db2gse.st_within(g1.geo, g2.geo)=1 and g2.ref='$0'"
			);
			
			props.put(PROP_DB2_SQL_DISTANCE_REF,
					"select g1.ref, g1.dnum from esadmin.geo_table g1, esadmin.geo_table g2 " +
					"where db2gse.st_distance(g1.geo, g2.geo, '$2')<$1 and g2.ref='$0'"
			);
			
			defaultVTIProperties = props;
		}
		
		return defaultVTIProperties;
	}
	
	/**
	 * @param searchString
	 * @throws Exception
	 */
	public SpatialQuery(String geoArgs) throws Exception {
		super( geoArgs );
		this.geoArgs = geoArgs;
		logger.logImportant("Entered SpatialQuery(geoArgs), function: '" + getPrefix() + "', args: " + replacements);
		
		// Get the batch size to fetch from the db and filter rows in - from the gaian config
		rowsBatchSize = GaianDBConfig.getRowsBatchSize();
		
		// Create bucket to fill with results for derby to fetch from (a fetch buffer)
		// Add 1 to the buffer sizes to allow for the poison pill batch.
		// This ensures offer() always succeeds and means we don't need to block with put()..
		fetchBufferSize = GaianDBConfig.getFetchBufferSize();
		fetchBuffer = new LinkedBlockingDeque<DataValueDescriptor[][]>( fetchBufferSize + 1 );
		
		//Get the DVDR[] template for result rows
		resultRowTemplate = getMetaData().getRowTemplate();
		
		
		if ( FUNCTION_DISTANCE.equals(getPrefix()) ) {
			if ( 2 > replacements.size() )
				throw new Exception("Invalid argument: '" + geoArgs + "' - Expecting location and distance values for prefix function " + getPrefix());
			
			// Check if distance unit is specified - this would be one of: select unit_name from DB2GSE.ST_UNITS_OF_MEASURE
			if ( 3 > replacements.size() )
				replacements.add( getVTIProperty( PROP_DISTANCE_UNIT ) );
			
		} else if ( FUNCTION_WITHIN.equals(getPrefix()) ) {
			if ( 1 > replacements.size() )
				throw new Exception("Invalid argument: '" + geoArgs + "' - Expecting named location for prefix function " + getPrefix());
		} else
			throw new Exception("Unrecognised prefix function: " + getPrefix());
		
		//Load the SQL result filter - if there is one
		sqlResultFilter = GaianDBConfig.getSQLResultFilter();
		
		//If there's a filter and it's a ...FilterX - assign vars appropriately
		if(sqlResultFilter != null && sqlResultFilter instanceof SQLResultFilterX) {
			
			sqlResultFilterX = (SQLResultFilterX)sqlResultFilter;
			sqlResultFilter = null;
			
			//Also, ask the policy for the max source rows to return
			maxSourceRows = sqlResultFilterX.setDataSourceWrapper(vtiClassName);
		}
	}

	public SpatialQuery(String geoArgs, String cid) throws Exception {
		this( geoArgs );
		jdbcSources = new String[] { cid };
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setArgs(java.lang.String[])
	 */
	public void setArgs(String[] geoArgs) {
//		this.geoArgs = geoArgs;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#executeAsFastPath()
	 */
	public boolean executeAsFastPath() {
		
		logger.logInfo("Entered executeAsFastPath()");
		
		if(queryRunning) {
			logger.logImportant("The query is already running - no need to re-execute.");
		}
		else {			
			//Kick off the query worker thread
			new Thread(new Runnable(){
				
				@Override
				public void run() {
					logger.logInfo("Query worker thread started");
					runQuery();
				}
			}, "SpatialQuery" ).start();
		}
		
		return true; // never return false - derby calls executeQuery() if you do
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#nextRow(org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		
		/*
		 * While we don't have a batch from the buffer
		 */
		while(currentResultBatch == null || currentResultBatchIndex >= currentResultBatch.length) {
			
			try {
				currentResultBatch = fetchBuffer.takeFirst();
				currentResultBatchIndex = 0;
				
				//If we get an empty batch AND the query is no longer running AND the fetch buffer is now empty - then we've reached the end of results
				if(currentResultBatch.length == 0 && !queryRunning && fetchBuffer.isEmpty()) {
					return IFastPath.SCAN_COMPLETED;
				}
			} catch (InterruptedException e) {
				logger.logException( GDBMessages.ENGINE_NEXT_ROW_ERROR, "Caught Exception in nextRow() (returning SCAN_COMPLETED): ", e );
				return IFastPath.SCAN_COMPLETED;
			}
		}
		
		/*
		 * At this point we should have the next non-empty batch, copy the next element in it into given dvdr.
		 */
		DataValueDescriptor[] currentResult = currentResultBatch[currentResultBatchIndex];
		
		System.arraycopy(currentResult, 0, dvdr, 0, currentResult.length);
		
		currentResultBatchIndex++;

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

	public void close() {
		logger.logInfo("SpatialQuery.close()");
		reinitialise();
	}

	public boolean reinitialise() {
		if ( null != resultRows ) {

			// Recycle the JDBC connections by putting each back into the appropriate GaianDB connection pool
			for ( String cid : jdbcSources )
				try {
					DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getRDBConnectionDetailsAsString(cid) )
					.push( underlyingResultSet.getStatement().getConnection() );
				} catch (Exception e) {
					logger.logException(GDBMessages.DSWRAPPER_JDBC_CONN_RECYCLE_ERROR, "Unable to recycle JDBC connection associated to GaianChildVTI underlying resultSet", e);
				}

			try { resultRows.close(); }
			catch ( Exception e ) {
				logger.logException(GDBMessages.DSWRAPPER_CHILD_RESULTSET_CLOSE_ERROR, "Unable to close GaianChildVTI underlying resultSet", e);
			}
			
			resultRows = null;
		}
		numFetches = 0;
		
		return true;
	}
	
	public boolean isBeforeFirst() {
		return 0 == numFetches;
	}

//	/**
//	 * Return the number of columns in the user-specified table.
//	 *
//	 * @exception SQLException	Thrown if there is an error getting the
//	 *							metadata.
//	 */
//	public int getColumnCount() throws SQLException {
//
//		logger.logInfo("!!!!!!!!!!!!SpatialQuery.getColumnCount()!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//		return rsmd.getColumnCount(); //DataSourcesManager.getLogicalTableRSMD( logicalTable ).getColumnCount();
//	}
	

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedRowCount(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
		
		Long l = estimatedRowCounts.get( geoArgs ); //Arrays.asList(geoArgs).toString() );
		double val = null == l ? 1 : l.doubleValue();
		
		logger.logInfo("getEstimatedRowCount() returning: " + val);
		return val;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedCostPerInstantiation(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException {
		int rc = 0;
		logger.logInfo("getEstimatedCostPerInstantiation() returning: " + rc);
		return rc;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#supportsMultipleInstantiations(org.apache.derby.vti.VTIEnvironment)
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException {

		boolean rc = false;
		
		logger.logInfo("supportsMultipleInstantiations() returning: " + rc);
		return rc;
	}

	public int getRowCount() throws Exception {
		return resultRows.getRowCount();
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

	/**
	 * No-op -
	 * No need to set qualifiers - the search string acts as a filter instead.
	 */
	@Override
	public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException {}

	public void runQuery() {		
		
		queryRunning = true;
					
		//Reset the number of fetches performed for this query
		numFetches = 0;

		/*
		 * Figure out the query we need to execute.
		 * If this is cached, then no need to do anything.
		 * Else fire the query off to the DB.
		 */
		try {
			
			if ( null == jdbcSources ) jdbcSources = Util.splitByCommas( getVTIProperty( PROP_SOURCE ) );

			if ( null == jdbcSources || 1 > jdbcSources.length ) {
				throw new Exception("No data sources found");
			}
			else {
				String src = jdbcSources[0];
				String cdetails = GaianDBConfig.getRDBConnectionDetailsAsString(src);
				String srcDriver = GaianDBConfig.getConnectionTokens(cdetails)[0];
				String geoDatabase = srcDriver.equals("com.ibm.db2.jcc.DB2Driver") ? DB2 : 
					srcDriver.equals("oracle.jdbc.OracleDriver") ? ORACLE : null;
				String geoLanguage = "SQL";
				String geoFunction = getPrefix().toUpperCase();
				String geoDatatype = replacements.get(0).startsWith("<gml") ? "GML" : "REF";

				// Lookup the query to execute - get the appropriate query property, e.g. "DB2.SQL.WITHIN.GML", and substitute its positional parms for VTI constructor arguments
				sqlQuery = getVTIPropertyWithReplacements( geoDatabase + "." + geoLanguage + "." + geoFunction + "." + geoDatatype );

				//If maxSourceRows is specified, then add on to the query
				if(-1 < maxSourceRows && maxSourceRows != Integer.MAX_VALUE) {

					if(geoDatabase.equals(DB2)) {
						sqlQuery += " FETCH FIRST " + maxSourceRows + " ROWS ONLY";
					}
					else if(geoDatabase.equals(ORACLE)) {
						sqlQuery += " WHERE ROWNUM <= " + maxSourceRows;
					}
				}

				
				if ( isCached( "CACHEID="+sqlQuery.hashCode() ) ) {
					logger.logImportant("Data is cached - no need to run Spatial Query");

				}
				else {
					logger.logImportant("Executing Spatial Query: " + sqlQuery);

					underlyingResultSet = DataSourcesManager.getPooledJDBCConnection( cdetails,
							DataSourcesManager.getSourceHandlesPool( cdetails ) ).createStatement(
									ResultSet.TYPE_SCROLL_INSENSITIVE,
									ResultSet.CONCUR_READ_ONLY).executeQuery(sqlQuery);

					resultRows = new GaianChildRSWrapper( underlyingResultSet );

					logger.logImportant("Spatial Ref query executed");

					/*
					 * If max source rows specified - determine if limited, and if so, then warn.
					 */
					if(-1 < maxSourceRows && maxSourceRows != Integer.MAX_VALUE) {
						underlyingResultSet.beforeFirst();

						int count = 0;

						while(underlyingResultSet.next()) {
							count++;
						}

						underlyingResultSet.beforeFirst();

						if(-1 < maxSourceRows && count == maxSourceRows) {
							logger.logWarning(GDBMessages.DSWRAPPER_SPATIAL_QUERY_PARTIAL_RESULT, "The raw Spatial Query has been restricted to a maximum of " + maxSourceRows + " results.");
						}
					}
				}
			}

		} catch (Exception e) {
			logger.logWarning(GDBMessages.DSWRAPPER_SPATIAL_QUERY_EXEC_ERROR, "Unable to execute Geo Spatial Query (db sources:"+Arrays.asList(jdbcSources)+"), cause:" + e);
			queryRunning = false;
		}

		/*
		 * Loop until we're finished querying (and possibly filtering)
		 */
		while(queryRunning) {

			/*
			 * Create a reusable array to fill with batches of results to work with.
			 */
			DataValueDescriptor[][] resultBatch = new DataValueDescriptor[rowsBatchSize][];
		
			for (int i=0; i < resultBatch.length; i++) {
				//Create a new 'row'
				DataValueDescriptor[] nextRow = new DataValueDescriptor[resultRowTemplate.length];
				
				//Fill the new row with empty copies of every DataValueDescriptor type in the rowTemplate
				for ( int j=0; j < resultRowTemplate.length; j++ ) {
					nextRow[j] = resultRowTemplate[j].getNewNull();
				}
				
				//Place the new holder row into the result batch
				resultBatch[i] = nextRow;
			}
			
			int resultsInThisBatch = 0;
			
			try {
				if(isCached()) {

					// While more results from cache && we've not hit the batch limit
					while( resultsInThisBatch < rowsBatchSize && (nextRowFromCache(resultBatch[resultsInThisBatch])) != SCAN_COMPLETED) {
						resultsInThisBatch++;
					}
				}
				else {				
					if ( null == resultRows ) {
						if ( numFetches == 0 )
							logger.logWarning(GDBMessages.DSWRAPPER_SPATIAL_NO_ROWS, "No rows to fetch, "+this.getClass().getSimpleName()+" did not execute - resultRows is null");
					}
					else {

						// While more results && we've not hit the batch limit
						while(resultsInThisBatch < rowsBatchSize && (resultRows.fetchNextRow(resultBatch[resultsInThisBatch]))) {

							//Set CACHEID
							resultBatch[resultsInThisBatch][2].setValue( sqlQuery.hashCode() );

							//Cache the row
							cacheRow(resultBatch[resultsInThisBatch]);
							numFetches++;

							resultsInThisBatch++;
						}
					}
				}
			} catch (Exception e) {
				logger.logException(GDBMessages.DSWRAPPER_ROW_FETCH_SPATIAL_ERROR, "Unable to fetch row", e);
			}
			
			//Don't bother filtering if no results
			if(resultsInThisBatch != 0) {
				
				//If not a full batch - reduce the batch size to pass to the filter
				//Note: this should only happen at the tail end of the query - so no need to worry about re-expanding
				if(resultsInThisBatch < rowsBatchSize) {

					//Create temp reduced batch
					DataValueDescriptor[][] reducedBatch = new DataValueDescriptor[resultsInThisBatch][];

					//Copy just the filled rows into the reduced batch
					System.arraycopy(resultBatch, 0, reducedBatch, 0, resultsInThisBatch);

					//Re-assign the resultBatch to the reduced version
					resultBatch = reducedBatch;

					logger.logDetail("Batched Filtering: Reduced final filtering batch to size: " + resultBatch.length);
				}

				//If batch filtering available
				if(sqlResultFilterX != null) {
					//Note: use geoArgs (the args passed into the VTI) as the datasourceid
					DataValueDescriptor[][] rb = sqlResultFilterX.filterRowsBatch(this.geoArgs, resultBatch);
					if ( null != rb ) resultBatch = rb;
				}
				//Else if single filtering available
				else if(sqlResultFilter != null) {
					
					//Create temp batch representing the records the user is allowed - this has max size resultBatch.length
					//Note: records are only added to this (and hence the index is only incremented) when a user is allowed to see them
					DataValueDescriptor[][] allowedBatch = new DataValueDescriptor[resultsInThisBatch][];
					int allowedBatchIndex = 0;
					
					for(int i = 0; i < resultBatch.length; i++) {
						if(sqlResultFilter.filterRow(resultBatch[i])) {
							allowedBatch[allowedBatchIndex] = resultBatch[i];
							allowedBatchIndex++;
						}
					}
					
					//Make resultBatch (which gets reported) a reduced copy of the allowed batch 
					resultBatch = Arrays.copyOf(allowedBatch, allowedBatchIndex);
				}
				//Else no filtering
				
				//Put result batch onto fetchBuffer for derby
				fetchBuffer.offerLast(resultBatch);
			}
			
			//If the batch was not filled, then we've hit the end of the results - query finished
			if(resultsInThisBatch < rowsBatchSize) {
				queryRunning = false;
			}
		}
		
		//If not cached query - then store estimated row count (if increased) 
		if(!isCached()) {
			String key = geoArgs;

			Long previousCount = estimatedRowCounts.get( key );
			if ( null == previousCount || numFetches > previousCount.longValue() ) {
				estimatedRowCounts.put( key, new Long( numFetches ) );
			}
		}

		numFetches = 0;
		
		//Put an empty batch on the end of the fetchBuffer to indicate end of results (in conjunction with queryRunning being false at this point)
		//(in case nextRow is still blocking on take)
		fetchBuffer.offerLast(new DataValueDescriptor[0][]);
	}
}
