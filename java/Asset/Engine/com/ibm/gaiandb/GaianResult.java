/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.vti.IFastPath;

import com.ibm.db2j.GaianQuery;
import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.policyframework.SQLQueryElements;
import com.ibm.gaiandb.policyframework.SQLQueryFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilterX;

/**
 * @author DavidVyvyan
 */

//public class GaianResult implements Runnable {
public class GaianResult {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "GaianResult", 25 );

	public static final String DS_EXECUTOR_THREAD_PREFIX = "GaianDataSourceExecutor-";
	
	private static ExecutorService resultExecutor = null, resultExecutorDash = null;
	
	static void purgeThreadPools() {
		if ( null != resultExecutor ) resultExecutor.shutdown(); resultExecutor = null;
		if ( null != resultExecutorDash ) resultExecutorDash.shutdown(); resultExecutorDash = null;
	}

	private static long EXEC_TIMEOUT_MICROS;

	private static final int POLL_FACTOR_MICROS = 1; // 1000 times less than the EXEC TIMEOUT, expressed in MICROS.
//	private static final int POLL_FACTOR_NANOS = 500;
	
	private int FETCH_BUFFER_SIZE; // Default (100) is determined in GaianDBConfig.java
	private BlockingQueue<DataValueDescriptor[][]> resultRowsBuffer = null;
	private BlockingQueue<DataValueDescriptor[][]> recycledRowsBuffer = null;
	private int numBufferedRowsBuilt;
	
	private int ROWS_BATCH_SIZE;
	private DataValueDescriptor[][] rowsBatch = new DataValueDescriptor[0][];
	private int rowsBatchPos; // start at pos 0 for an array of length 0 - will cause new batch to be fetched

	private boolean isScanCompleted;
	private boolean isQuiesceQuery; // tells child exec-threads to abort fetching (this may not mean the outer query is aborted - e.g. re-fetcher from cache)
	
    // The number of columns that this node exposes for this query
	// This does not include the NULL columns as they are not defined (so not exposed) by this node...
	// ...they will be left as NULL.
	private int exposedColumnCount;
	
	private ConcurrentMap<String,VTIWrapper> dataSourceWrappers = null;
	private int numExecThreads, numRemainingExecThreads;
	AtomicInteger numExecThreadsHavingCompletedExecution = new AtomicInteger(0);
	
	private long rowNumber;
	
	private long queryStartTime, queryFetchStartTime, queryExecTime, queryTime, fetchTime, queryTimeIncludingRefetch;

	// Set of data sources that are currently in their execute phase - so we can verify if they are hanging
	// Note - Operations on executingDataSources are already locked with synchronized blocks - so no need for
	// a Collections.synchronizedSet( new HashSet() );
	private Set<VTIWrapper> executingDataSourceWrappers = new HashSet<VTIWrapper>();
	
	private boolean isActiveConnectionsChecked;
	
	private SQLWarning warnings; // Never seen this being used. If derby called getWarnings() it would be.
	
	private long explainRowCount;
	
	private final GaianTable gaianStatementNode;
	private final String gaianStatementNodeClassName;
	
	private boolean isExplain;
	private boolean isLogPerfOn;
	private int maxSourceRows;

	private static long totalWorkloadSoFar;
	private long dsTimes;
	private int estimatedRecordSize;
	
	private DataValueDescriptor[] dvdrTemplate;
	private int[] physicalProjectedColumns, allProjectedColumns, fullProjectionZeroBased;
	private int fullProjectionSize;
	
	private SQLQueryFilter sqlQueryFilter;
	private SQLResultFilter sqlResultFilter;
	private SQLResultFilterX sqlResultFilterX;
		
	private Qualifier[][] gaianStatementQualifiers;
	
	private final ArrayList<Long> pollTimes = new ArrayList<Long>(10000);
	
	public final boolean checkCancel( String queryID ) { return gaianStatementNode.checkCancel(queryID); }
	
	public final boolean cancelOnTimeout() {
		Integer timeout = (Integer) gaianStatementNode.getQueryDetails().get(GaianTable.QRY_TIMEOUT);
		return null == timeout ? false : gaianStatementNode.cancelOnTimeout( (long) (queryStartTime+timeout) );
	}
	
	public final String getQueryID() { return gaianStatementNode.getQueryID(); }
	public final int getQueryDepth() { return gaianStatementNode.getQueryPropagationCount(); }
	public final String getWID() { return (String) gaianStatementNode.getQueryDetails().get(GaianTable.QRY_WID); }
	public final String getQueryHash() { return (String) gaianStatementNode.getQueryDetails().get(GaianTable.QRY_HASH); }
	public final String getLTName() { return (String) gaianStatementNode.getLogicalTableName(true); }
	
	/**
	 * Create a GaianResult which kicks off as many Threads as there are nodeDefNames, each of which processes
	 * the SQL query determined by the qualifiers and projectedColumns for the vti associated to the child node 
	 * in the gaiandb_config.properties file associated with the GaianNode which kicked off this execution originally.
	 * 
	 * Note vtis[] must contain at least one vti / child node.
	 * 
	 * @param vtis
	 * @param node
	 * @throws Exception
	 */
	public GaianResult( GaianTable node, VTIWrapper[] dsWrappers ) throws Exception {
		gaianStatementNode = node;
		gaianStatementNodeClassName = (gaianStatementNode instanceof GaianQuery ? GaianQuery.class : GaianTable.class).getName();
		
		if ( null == resultExecutor )
			resultExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
		        private final AtomicInteger threadNumber = new AtomicInteger(1);
		        public Thread newThread(Runnable r) { return new Thread(r, DS_EXECUTOR_THREAD_PREFIX+threadNumber.getAndIncrement()); }
			});
		
		if ( null == resultExecutorDash )
			resultExecutorDash = Executors.newCachedThreadPool(new ThreadFactory() {
		        private final AtomicInteger threadNumber = new AtomicInteger(1);
		        public Thread newThread(Runnable r) { return new Thread(r, Logger.LOG_EXCLUDE + DS_EXECUTOR_THREAD_PREFIX+threadNumber.getAndIncrement()); }
			});	
		
		reExecute( dsWrappers );
	}
	
	@SuppressWarnings("unchecked")
	public void reExecute( VTIWrapper[] dsWrappers ) throws Exception {
		
		// "Official" Execution start time
		queryStartTime = System.currentTimeMillis();
		queryExecTime = 0; queryFetchStartTime = 0; queryTime = -1; fetchTime = -1; queryTimeIncludingRefetch = -1;
		
		int oldFetchBufferSize = FETCH_BUFFER_SIZE;
		
		ROWS_BATCH_SIZE = GaianDBConfig.getRowsBatchSize(); // Rows per batch fetched from data source - Also determines batch size for policy filtering
		FETCH_BUFFER_SIZE = GaianDBConfig.getFetchBufferSize(); // Size of record batches queue
		logDerbyThreadInfo( "Using fetch buffer size " + FETCH_BUFFER_SIZE );
		
		EXEC_TIMEOUT_MICROS = 1000*GaianDBConfig.getExecTimeoutMillis();
		
		numBufferedRowsBuilt = 0;
		rowsBatchPos = 0;
		rowNumber = 0;
		explainRowCount = 0;
		warnings = null;
		
		isActiveConnectionsChecked = false;
		isScanCompleted = false;
		isQuiesceQuery = false;
		
		if ( oldFetchBufferSize != FETCH_BUFFER_SIZE ) {
			if ( null != resultRowsBuffer ) resultRowsBuffer.clear();
			if ( null != recycledRowsBuffer ) recycledRowsBuffer.clear();
			resultRowsBuffer = recycledRowsBuffer = null;
		}

		// Add 1 to the buffer sizes to allow for the poison pill batch.
		// This ensures offer() always succeeds and means we don't need to block with put()..
		if ( null == resultRowsBuffer ) resultRowsBuffer = new ArrayBlockingQueue<DataValueDescriptor[][]>( FETCH_BUFFER_SIZE+1 );//new PriorityBlockingQueue();
		if ( null == recycledRowsBuffer ) recycledRowsBuffer = new ArrayBlockingQueue<DataValueDescriptor[][]>( FETCH_BUFFER_SIZE+1 );
		
//		long count = 0;
//		for ( Iterator<String> iter = dvdPools.keySet().iterator(); iter.hasNext(); )
//			{ String t = iter.next(); long card = dvdPools.get( iter.next() ).size();
//			System.out.println("Pooled dvds of type " + t + ": " + card); count += card; }
//		System.out.println("Pooled dvds: " + count);
		
//		LinkedBlockingQueue lbq = null;
//		lbq.poll();
//		ConcurrentLinkedQueue clq = null;
//		clq.isEmpty()offer()poll();
		
		// Need to be specific about class name invocation - for cases where subclasses invoke us, e.g. LiteGaianStatement
		gaianStatementQualifiers = gaianStatementNode.getQualifiers();

		allProjectedColumns = gaianStatementNode.getProjectedColumns();
		fullProjectionSize = allProjectedColumns.length;
		fullProjectionZeroBased = new int[fullProjectionSize];
		for ( int i=0; i<fullProjectionSize; i++ )
			fullProjectionZeroBased[i] = allProjectedColumns[i]-1;
		
		physicalProjectedColumns = gaianStatementNode.getPhysicalProjectedColumns();
		sqlQueryFilter = gaianStatementNode.getSQLQueryFilter();
		
		SQLResultFilter srf = gaianStatementNode.getResultFilter();
		if ( srf instanceof SQLResultFilterX ) { sqlResultFilter = null; sqlResultFilterX = (SQLResultFilterX) srf; }
		else { sqlResultFilter = srf; sqlResultFilterX = null; }
		
		GaianResultSetMetaData ltrsmd = (GaianResultSetMetaData) gaianStatementNode.getTableMetaData();
		dvdrTemplate = ltrsmd.getRowTemplate();
//		tempRow = new DataValueDescriptor[ dvdrTemplate.length ];
		
		exposedColumnCount = ltrsmd.getExposedColumnCount();

		// qryDetails are mostly used for GaianNode Data Sources to propagate qryid and steps, but may also be 
		// useful for local queries to discriminate between updates run locally or pushed around from a remote client
		ConcurrentMap<String,Object> qryDetails = gaianStatementNode.getQueryDetails();
		isExplain = qryDetails.containsKey( GaianTable.QRY_IS_EXPLAIN );
		Integer msr = (Integer) qryDetails.get( GaianTable.QRY_MAX_SOURCE_ROWS );
		maxSourceRows = null == msr ? -1 : msr.intValue();
		qryDetails.put( GaianTable.QRY_ID, gaianStatementNode.getQueryID() );
//		 Increment propagation depth here
		qryDetails.put( GaianTable.QRY_STEPS, new Integer( gaianStatementNode.getQueryPropagationCount() ) );
		if ( null != gaianStatementNode.getEncodedCredentials() )
			qryDetails.put( GaianTable.QRY_CREDENTIALS, gaianStatementNode.getEncodedCredentials() );
//		qryDetails.put( GaianTable.QRY_FWDER, GaianDBConfig.getGaianNodeID() );
		
		isLogPerfOn = gaianStatementNode.isLogPerfOn();
		
		executingDataSourceWrappers.clear();
		executingDataSourceWrappers.addAll( Arrays.asList(dsWrappers) ); // the set of vtis to check up on mid way through in case they hang
		executingDataSourceWrappers.remove(null); // just in case
		
		// Kick off a query executing thread for each node...
		numExecThreads = 0;
		numExecThreadsHavingCompletedExecution.set(0);
		numRemainingExecThreads = 0;
		
		if ( null != dataSourceWrappers ) dataSourceWrappers.clear();
		else dataSourceWrappers = new ConcurrentHashMap<String, VTIWrapper>(dsWrappers.length*4-1);
		
		String partialThreadName = " queryID=" + gaianStatementNode.getQueryID() + " steps=" + gaianStatementNode.getQueryPropagationCount();
		
		totalWorkloadSoFar = 0; dsTimes = 0; estimatedRecordSize = 0;
		
		// First calculate how many threads will be required to access all end-points of all data source wrappers.
		
		int[] numThreadsForEachNode = new int[dsWrappers.length];
		
		for ( int i=0; i<dsWrappers.length; i++ ) {
			VTIWrapper dsWrapper = dsWrappers[i];
			if ( null != dsWrapper ) {
				Stack<String> pluralizedInstancesForThisNode =
					(Stack<String>) gaianStatementNode.getQueryDetails().get( GaianTable.PLURALIZED_INSTANCES_PREFIX_TAG + dsWrapper.getNodeDefName() );
				
				numThreadsForEachNode[i] = null==pluralizedInstancesForThisNode ? 1 : pluralizedInstancesForThisNode.size();
				numExecThreads += numThreadsForEachNode[i];
				
				logDerbyThreadInfo("Added thread count for dsWrapper: " + dsWrapper + ": " + numThreadsForEachNode[i] + ", runningTotal: " + numExecThreads);
			}
		}

		numRemainingExecThreads = numExecThreads;
		
		// STOP HERE IF THE RESULT HAS BEEN CLOSED DUE TO A SHUTDOWN!
		if ( isQuiesceQuery ) {
			logger.logThreadAlways( "Query execution aborted (possibly due to shutdown request) - No exec threads started - isQuiesceQuery = " + isQuiesceQuery );
			return;
		}
		
		// Now launch a thread for each end-point (there will be multiple ones per dsWrapper if it is "PLURALIZED").
		
		for ( int i=0; i<dsWrappers.length; i++ ) {
			VTIWrapper dsWrapper = dsWrappers[i];
			if ( null != dsWrapper ) {
				String vtiName = dsWrapper.getNodeDefName();
				
				dataSourceWrappers.put(vtiName, dsWrapper);
				
				while ( 0 < numThreadsForEachNode[i]-- )
					( gaianStatementNode.isSystemQuery() ? resultExecutorDash : resultExecutor ).execute( new GaianSubResult(this, vtiName + partialThreadName) );
					// new Thread(this, vtiName + partialThreadName) // expensive Thread instantiation
			}
		}
		
		logDerbyThreadInfo( "Kicked off all " + numExecThreads + " exec threads, exec complete" );
	}
	
	private GaianChildVTI executeQueryAgainstDataSource( VTIWrapper dsWrapper, String dsInstanceID ) {
		
		GaianChildVTI result = null;
		
		try {
			if ( dsWrapper.isGaianNode() ) {

				// branch node				
				String logicalTable = Util.escapeSingleQuotes( gaianStatementNode.getLogicalTableName(false) );
				
				logger.logThreadInfo( "Setting arguments for query to Gaian Node" );
				String newArgs = "('" +
					logicalTable + "', '" +
					gaianStatementNode.getTableArguments() + "', '" +
					( gaianStatementNode instanceof GaianQuery ?
							((GaianQuery) gaianStatementNode).getQueryArguments() + "', '" : "" ) +
					gaianStatementNode.getTableDefinition() + "', '" +
					GaianDBConfig.getGaianNodeID() + "')"; // "', '" +
//					gaianStatementNode.getQueryID() + "', " +
//					(gaianStatementNode.getQueryPropagationCount()+1) + ")";
				
				Qualifier[][] qualifiers = gaianStatementQualifiers;
				int[] projectedColumns = allProjectedColumns;
				
				if ( null != sqlQueryFilter ) {
					SQLQueryElements queryElmts = new SQLQueryElements(qualifiers, projectedColumns);
					if ( !sqlQueryFilter.applyPropagatedSQLFilter(gaianStatementNode.getQueryID(), GaianDBConfig.getGaianNodeID(dsWrapper.getNodeDefName()), queryElmts) ) {
						logDerbyThreadInfo("Propagated query cancelled by SQLQueryFilter policy in: " + sqlQueryFilter.getClass().getName());
						return null;
					}
					
					qualifiers = queryElmts.getQualifiers();
					projectedColumns = queryElmts.getProjectedColumns();
				}
				
				// Do not propagate the query to this gaian node if it sent it to us previously.
				if ( gaianStatementNode.wasQueryAlreadyReceivedFrom( GaianDBConfig.getGaianNodeID(dsWrapper.getNodeDefName()) ) ) {
					logger.logThreadInfo( "Query was already received from this node, so no need to send it there" );
					result = null;
				} else {
				
					logger.logThreadInfo( "About to execute dataSource query" );
					// A branch (Gaian) node currently has to return an RDB Result, as it connects to another Derby
					result = ((VTIRDBResult) dsWrapper).execute(
							gaianStatementNode.getQueryDetails(),
							qualifiers, //gaianStatementQualifiers,
							projectedColumns, //allProjectedColumns, // can never use 'select *' because table definitions may vary across nodes
							"NEW " + gaianStatementNodeClassName + newArgs + " " + gaianStatementNode.getTableAlias()); // + dsWrapper );
					logger.logThreadInfo( "Finished execution of dataSource query" );
				}
			
			} else {
				
				Qualifier[][] pQualifiers = gaianStatementNode.getPhysicalQualifiers();
				int[] pProjectedColumns = physicalProjectedColumns;
				
				if ( null != sqlQueryFilter ) {
					// Invoke SQL policy plugin to alter qualifiers and projected columns *for this execution thread* 
					SQLQueryElements queryElmts = new SQLQueryElements(pQualifiers, pProjectedColumns);
					if ( !sqlQueryFilter.applyDataSourceSQLFilter(gaianStatementNode.getQueryID(), dsWrapper.getSourceDescription( dsInstanceID ), queryElmts) ) {
						logDerbyThreadInfo("Propagated query cancelled by SQLQueryFilter policy in: " + sqlQueryFilter.getClass().getName());
						return null;
					}
					
					pQualifiers = queryElmts.getQualifiers();
					pProjectedColumns = queryElmts.getProjectedColumns();
				}
				
				if ( dsWrapper.isSubQuery() ) {
					
					String subQuery = gaianStatementNode.getLogicalTableName(false);
					
					logger.logImportant("SUBQUERY IS: " + subQuery);
					
					String splitColumnRangeSQL = ((GaianQuery) gaianStatementNode).getSplitColumnRangeSQL();
					
					// We have to cater for the case where we are doing a 'select *' on a subquery
					// where 1 or more columns are not named, e.g. select * ( select count(*) from GT ) T
					// We cannot get appropriate column names to select from the result of the sub select.
					// e.g. select "1" from ( select count(*) from GT ) T does not work.
					// We cannot either make it a 'select *' in cases where the subselect is a 'select *'
					// because the table def on some nodes may have more columns than on others
					
					// The solution is to use 'select *' when the subquery is not itself a 'select *'
					// When the subquery is itself a 'select *', we can just use the named physical columns outside

//					System.out.println("isSelectStar: " + gaianStatementNode.isSelectStar() );

					String first7chars = 7 < subQuery.length() ? subQuery.substring(0, 7).toLowerCase().replaceAll("\\s", " ") : null;
					boolean isNotSelect = null != first7chars && !first7chars.startsWith("select"); // select could be followed by \\s or a double quote (")
					
					// If this is not a select OR if there are no projected columns and no qualifiers; then there's no need to wrap the subquery.
					// Note - we pass the qualifiers in in case this is a procedure call()
					if ( isNotSelect || gaianStatementNode.isSelectStar() && (null==pQualifiers || 0==pQualifiers.length) && null==splitColumnRangeSQL )
						result = ((VTIRDBResult) dsWrapper).execute( gaianStatementNode.getQueryDetails(), pQualifiers, null, subQuery );
					else {
						// Build an actual sub-query
						boolean useSelectStarAroundSubQuery = false;
						
						if ( gaianStatementNode.isSelectStar() ) {
							
//							String trimmedSubQuery = subQuery.trim(); // already trimmed in GaianTable constructor
							String SELECT_TOKEN = "SELECT";
							int selectTokenLength = SELECT_TOKEN.length();
							
							if ( selectTokenLength < subQuery.length() ) {
								// include the next char after the 'select' to ensure it has a space delimiter
								String expectedSelectToken = subQuery.substring(0, selectTokenLength+1).trim();
								String remainingQueryTxt = subQuery.substring(selectTokenLength+1).trim();
								
								useSelectStarAroundSubQuery = 
									expectedSelectToken.equalsIgnoreCase( SELECT_TOKEN ) && ! remainingQueryTxt.startsWith("*");
							}
						}
						
		//				int starIndex = subQuery.indexOf('*');
		//				boolean useSelectStarAroundSubQuery = gaianStatementNode.isSelectStar() && -1 == starIndex ? true :
		//					false == subQuery.substring(0, starIndex).trim().toUpperCase().equals("SELECT");
						
						result = ((VTIRDBResult) dsWrapper).execute(
							gaianStatementNode.getQueryDetails(),
							pQualifiers, // gaianStatementNode.getPhysicalQualifiers(),
							useSelectStarAroundSubQuery ? null : pProjectedColumns, //physicalProjectedColumns,
							"(" + subQuery + ") SUBQ" + (null==splitColumnRangeSQL ? "" : " where " + splitColumnRangeSQL) );
					}
				
				} else {
				
					// leaf node
					if ( false == dsWrapper.isPluralized() ) dsInstanceID = null; // just to make sure
					
					result = dsWrapper.execute(
							gaianStatementNode.getQueryDetails(),
							pQualifiers, //gaianStatementNode.getPhysicalQualifiers(),
							pProjectedColumns, //physicalProjectedColumns
							dsInstanceID
							); // can never use 'select *' as it applies to logical cols not physical.
				}
			}
			
		} catch (Exception ex) {
			
//			Throwable t = e;
//			while ( null != t.getCause() ) t = t.getCause();
//			String msg = t.getMessage();
			
			// This exception below (with the undefined data source message) is not relevant anymore - we don't trap anything as
			// propagated queries always contain a definition so there should always be meta data, and therefore a defined data source.
//			int index = null==msg ? -1 : msg.lastIndexOf(GaianTable.UNDEFINED_DATA_SOURCE);
//			if ( -1 != index ) logger.logThreadInfo( "Remote node aborted execution, cause: " + msg.substring(index) );
//			else {
			String eString = Util.getExceptionAsString(ex);
			logger.logThreadException( GDBMessages.RESULT_DS_EXEC_QUERY_ERROR, "Query exec failure against Data Source " + dsWrapper + ": " + eString, ex );
			
			setWarning( "WARNING for node: " + dsWrapper + ": " + eString );
//			}
		}
		
		if ( null == result ) {
			logger.logThreadInfo( "Returning null Result for this child node" );
		}
		return result;
	}
	
	private static Map<String, LinkedBlockingQueue<DataValueDescriptor>> dvdPools =
		new ConcurrentHashMap<String, LinkedBlockingQueue<DataValueDescriptor>>();
	
	private LinkedBlockingQueue<DataValueDescriptor> getDVDPool( String typeName ) {
		LinkedBlockingQueue<DataValueDescriptor> dvdPool = dvdPools.get( typeName );
		if ( null == dvdPool ) dvdPools.put(typeName, dvdPool = new LinkedBlockingQueue<DataValueDescriptor>(1000)); // 1000 * 10 common types =~ 10k pooled dvds
		return dvdPool;
	}
	
	private DataValueDescriptor getPooledDVDOrNewNull( DataValueDescriptor dvdToClone ) {
		DataValueDescriptor pooledDVD = getDVDPool( dvdToClone.getTypeName() ).poll();
		return null != pooledDVD ? pooledDVD : dvdToClone.getNewNull();
	}
	
	private void clearAndrecycleBufferedCells( BlockingQueue<DataValueDescriptor[][]> buffer ) {
		for ( Iterator<DataValueDescriptor[][]> batchIterator = buffer.iterator(); batchIterator.hasNext(); ) {
			DataValueDescriptor[][] batch = batchIterator.next();
			for ( DataValueDescriptor[] row : batch )
				for ( DataValueDescriptor cell : row ) {
					cell.setToNull();
					getDVDPool( cell.getTypeName() ).offer( cell );
				}
			batchIterator.remove(); // clear the buffer progressively
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@SuppressWarnings("unchecked")
	public void run(String tName) {
		
		VTIWrapper dsWrapper = (VTIWrapper) dataSourceWrappers.get( tName.substring(0, tName.indexOf(' ')) );
		
//		Code below contains v. expensive Thread.setName() (takes 2ms in a 12ms query), so replaced with Hashtable lookup above
//		vtis[ Integer.parseInt( Thread.currentThread().getName() ) ];
//		Thread.currentThread().setName( vti + " queryID=" + gaianStatementNode.getQueryID() + 
//				" steps=" + gaianStatementNode.getQueryPropagationCount() );
		
		String dsInstanceID = dsWrapper.isPluralized()
			?	((Stack<String>) gaianStatementNode.getQueryDetails().get( GaianTable.PLURALIZED_INSTANCES_PREFIX_TAG + dsWrapper.getNodeDefName() )).pop()
			:	null;
		
		logger.logThreadInfo( "run(): Starting to process child data source wrapper for: "
				+ dsWrapper.getNodeDefName() + ", dsInstanceID: " + dsInstanceID );
		
		long execTime = ( Logger.LOG_NONE < Logger.logLevel ? System.currentTimeMillis() : 0 );
		if ( Logger.LOG_LESS < Logger.logLevel ) logger.logThreadInfo( "Start exec time:"  + execTime );
		GaianChildVTI nodeRows = executeQueryAgainstDataSource( dsWrapper, dsInstanceID );
				
		long timeAfterExec = Logger.LOG_NONE < Logger.logLevel ? System.currentTimeMillis() : 0;
		
		if ( numExecThreadsHavingCompletedExecution.incrementAndGet() == numExecThreads )
			queryExecTime = ( 0 == timeAfterExec ? System.currentTimeMillis() : timeAfterExec ) - queryStartTime;
		
		if ( Logger.LOG_NONE < Logger.logLevel ) {
			execTime = timeAfterExec - execTime;
			logger.logThreadInfo( dsWrapper + " End exec time:"  + timeAfterExec );
		}
		
		int dataSourceExplainCount = -1;
		
		if ( null == nodeRows ) {
			// Null result returned: Query failed. Skip this result
			logDerbyThreadImportant(dsWrapper + ": Data source returned null result, ignored");
			
		} else if ( isExplain && !dsWrapper.isGaianNode() ) {
			try {
				dataSourceExplainCount = cycleThroughBackEndVTIRows( nodeRows );
			} catch (Exception e) {
				logDerbyThreadException(dsWrapper + " Unable to get explainRowCount for this Data Source, cause: ", e);
			}
		} else {
			
//			logDerbyThreadInfo("Getting rows for dataSource: " + dataSource);
			
			DataValueDescriptor[][] nextBatch = null;
			
			DataValueDescriptor lastGaianNodeCountDVD = null;
			
			// Determine max rows to extract for this data source. Both maxSourceRows and maxRowsDeterminedByPolicy might affect this. 
			// A value of -1 for either of these setting means there is no limit.
			int maxRowsDeterminedByPolicy = gaianStatementNode.getPolicyOnMaxDataSourceRows(dsWrapper.getNodeDefName(), dsInstanceID);
			int sourceRowsCountdown = dsWrapper.isGaianNode() ? -1 : 0>maxSourceRows || 0>maxRowsDeterminedByPolicy ?
				Math.max(maxSourceRows, maxRowsDeterminedByPolicy) : Math.min(maxSourceRows, maxRowsDeterminedByPolicy);
			boolean isSourceRowsLimitSet = !dsWrapper.isGaianNode() && -1 < sourceRowsCountdown;
			
			int lastBatchSize = ROWS_BATCH_SIZE;
			
			// Policy batch filtering state variables
			DataValueDescriptor[][] filteredBatch = null;
			int numRemainingFromLastFilteredBatch = 0;
			
			// Create a temporary batch of standard size which the policy batch filter will be applied to
			DataValueDescriptor[][] filteringBatch = null;
			if ( null != sqlResultFilterX ) {
				filteringBatch = new DataValueDescriptor[ROWS_BATCH_SIZE][];
				int colcount = dvdrTemplate.length;
				for (int n=0; n<ROWS_BATCH_SIZE; n++) {
					DataValueDescriptor[] nextRow = new DataValueDescriptor[colcount];
					for ( int i=0; i<colcount; i++ )
						nextRow[i] = getPooledDVDOrNewNull( dvdrTemplate[i] );
//						nextRow[i] = dvdrTemplate[i].getNewNull();
					filteringBatch[n] = nextRow;
				}
			}

			boolean isRowsRemaining = true;
			
			while ( isRowsRemaining ) {
				
				if ( isQuiesceQuery || isSourceRowsLimitSet && 1 > sourceRowsCountdown ) break;
								
				// Try polling for any row batches recycled by the Derby fetching thread.
				if ( null == (nextBatch = recycledRowsBuffer.poll()) ) {

					// No recycled batches.. build a new one if we haven't built enough to fill the buffer yet
//						if ( resultRowsBuffer.size() < FETCH_BUFFER_SIZE - numExecThreads ) { // this wouldnt work - consumer may have altered recycled rows
					if ( FETCH_BUFFER_SIZE > numBufferedRowsBuilt ) {
						synchronized ( this ) {
							if ( FETCH_BUFFER_SIZE <= numBufferedRowsBuilt++ ) continue;
						}
						
						nextBatch = new DataValueDescriptor[ROWS_BATCH_SIZE][];
						int colcount = dvdrTemplate.length;
						for (int n=0; n<ROWS_BATCH_SIZE; n++) {
							DataValueDescriptor[] nextRow = new DataValueDescriptor[colcount];
							for ( int i=0; i<colcount; i++ )
								nextRow[i] = getPooledDVDOrNewNull( dvdrTemplate[i] );
//								nextRow[i] = dvdrTemplate[i].getNewNull();
							nextBatch[n] = nextRow;
						}
						
					} else
						// Otherwise, wait for the fetcher thread to recycle a row
						try {
							nextBatch = recycledRowsBuffer.take(); // this is the blocking point for producers when the consumer is being slow
						} catch (InterruptedException e) {
							logDerbyThreadWarning(dsWrapper + " Unable to take() recycled row, aborting fetch for this Data Source");
							break;
						}
				}
				
				try {
					// If a max number of rows per source was specified and this batch exceeds it, reduce the batch size
					if ( isSourceRowsLimitSet ) {
//						System.out.println("Reducing nextBatch due to max limit on source rows for " + dsWrapper + ", " + dsInstanceID);
						sourceRowsCountdown -= nextBatch.length;
						if ( 0 > sourceRowsCountdown ) {
							// This batch size will retrieve more rows than was specified (maxSourceRows)
							// Reduce the batch size: substract the number of records that shouldn't be retrieved (i.e. sourceRowsCountdown)
							int reducedSize = sourceRowsCountdown + nextBatch.length;
							if ( 0 > reducedSize ) reducedSize = 0; // defensive programming
							DataValueDescriptor[][] reducedBatch = new DataValueDescriptor[reducedSize][];
							System.arraycopy(nextBatch, 0, reducedBatch, 0, reducedSize);
							nextBatch = reducedBatch;
							sourceRowsCountdown = 0;
						}
					}
					
					if ( null == sqlResultFilterX ) {
						// Records will be filtered one by one... (not by batch)
						
						// Try to get the first record for the batch - if this fails then we have a clean break.
						if ( false == nextFastPathRow(dsWrapper, dsInstanceID, nodeRows, nextBatch[0]) ) {
							if ( ROWS_BATCH_SIZE == nextBatch.length ) recycledRowsBuffer.offer( nextBatch ); // recycle this batch for use by another exec thread
							break; // clean break on this batch.. break while loop directly
						}
						
						// Get all remaining available records
						for (int n=1; n<nextBatch.length; n++) {
							if ( false == nextFastPathRow(dsWrapper, dsInstanceID, nodeRows, nextBatch[n]) ) {
								// No more rows - batch cannot be completed - remember last batch size and stop fetching
								lastBatchSize = n;
								break;
							}
						}
					} else {
						// Filter the records by batch, until we have a complete filtered batch to pass on.. or until the records run out
						
						logger.logThreadDetail("Batch Filtering: !!! START:: Adding numRemainingFromLastFilteredBatch: " + numRemainingFromLastFilteredBatch );
						
						// Get remaining filtered records from a previous iteration first
						if ( 0 < numRemainingFromLastFilteredBatch )
							for ( int offset=filteredBatch.length - numRemainingFromLastFilteredBatch, j=0; j<numRemainingFromLastFilteredBatch; j++ )
								for ( int k=0; k<dvdrTemplate.length; k++ )
									try {
										nextBatch[j][k].setValue( filteredBatch[offset+j][k] );
									} catch (Exception e) {
										logger.logException(GDBMessages.RESULT_GET_FILTERED_ERROR, "Unexpected Exception while setting dvd column value from remaining filteredBatch rows (aborting ds fetch): ", e);
										isRowsRemaining = false;
									}
							
//							System.arraycopy(	filteredBatch, filteredBatch.length - numRemainingFromLastFilteredBatch, 
//												nextBatch, 0, numRemainingFromLastFilteredBatch);
						
						int numStillRequiredForNextBatch = ROWS_BATCH_SIZE - numRemainingFromLastFilteredBatch;
						numRemainingFromLastFilteredBatch = 0;
						
						// Note that numStillRequiredForNextBatch *has to be* > 0 at this stage,
						// because the number remaining from the last iteration had to be smaller than a full batch..
						
						// Now try to get/filter new records until we have filled the nextBatch or until there are none remaining
						// Also stop if filteringBatch has been reduced (indicates no more rows)...
						while ( isRowsRemaining && ROWS_BATCH_SIZE == filteringBatch.length ) {

							logger.logThreadDetail("Batched Filtering: numStillRequiredForNextBatch is now: " + numStillRequiredForNextBatch );
//							logger.logDetail("Batched Filtering: Before retrieval, nextBatch starts with: " + nextBatch[0][0]);
							
							// Get records for a filtering batch
							int n;
							for ( n=0; n<filteringBatch.length; n++ ) {
								if ( false == nextFastPathRow2(dsWrapper, dsInstanceID, nodeRows, filteringBatch[n]) ) {
									// No more rows - Filtering batch cannot be completed
									// but don't set isRowsRemaining to false, because we might still have a surplus of rows compared with what is required in nextBatch.
									if ( 0 < n ) {
										// Reduce filtering batch itself
										DataValueDescriptor[][] reducedBatch = new DataValueDescriptor[n][];
										System.arraycopy(filteringBatch, 0, reducedBatch, 0, n);
										filteringBatch = reducedBatch;
										logger.logThreadDetail("Batched Filtering: Reduced final filtering batch to size: " + filteringBatch.length);
									}
									break;
								}
							}

							logger.logThreadDetail("Batched Filtering: Num records retrieved for filtering: " + n);
							logger.logThreadDetail("Batched Filtering: After retrieval, nextBatch starts with: " + nextBatch[0][0]);
							
							if ( 1 >  n ) {
								isRowsRemaining = false; // causes *outer* while loop to end after this batch
								break; // No more records to apply filtering to
							}
							
							// Apply the policy filter.. note the returned batch can be smaller than the input batch (although the number of cols remains the same)
							filteredBatch = sqlResultFilterX.filterRowsBatch( dsWrapper.getNodeDefName(), filteringBatch );
							if ( null == filteredBatch ) filteredBatch = filteringBatch;

							logger.logThreadDetail("Batched Filtering: Applied batched filtering.. num records left: " + filteredBatch.length);
							
							if ( 0 == filteredBatch.length ) continue; // All records were excluded by the policy filter - fetch/filter another batch

							logger.logThreadDetail("Batched Filtering: Starting row id: " + filteredBatch[0][0]);
							
							int numFilteredRecordsToAdd = Math.min( filteredBatch.length, numStillRequiredForNextBatch );
							int offset = ROWS_BATCH_SIZE - numStillRequiredForNextBatch; // find offset past records already set
							for ( int i=0; i<numFilteredRecordsToAdd; i++ )
								for ( int k=0; k<dvdrTemplate.length; k++ )
									try {
										nextBatch[offset+i][k].setValue( filteredBatch[i][k] );
									} catch (Exception e) {
										logger.logThreadException( GDBMessages.RESULT_FILTER_ERROR,
												"Unexpected Exception while setting dvd column value nextBatch["+(i+offset)+"]["+k+"] (size "+nextBatch.length
												+") from filteredBatch["+i+"]["+k+"] (size "+filteredBatch.length+") (aborting ds fetch): ", e);
										isRowsRemaining = false;
									}
							
//							System.arraycopy( filteredBatch, 0, tempBatchForSwapping, 0, numFilteredRecordsToAdd );
//							System.arraycopy( nextBatch, ROWS_BATCH_SIZE - numStillRequiredForNextBatch, filteredBatch, 0, numFilteredRecordsToAdd );
//							System.arraycopy( tempBatchForSwapping, 0, nextBatch, ROWS_BATCH_SIZE - numStillRequiredForNextBatch, numFilteredRecordsToAdd );
							
							numStillRequiredForNextBatch -= numFilteredRecordsToAdd;

							if ( 0 == numStillRequiredForNextBatch ) {
								// nextBatch is now full - but remember what is left in the filteredBatch for the next iteration...
								numRemainingFromLastFilteredBatch = filteredBatch.length - numFilteredRecordsToAdd;
								logger.logThreadDetail("Batched Filtering: nextBatch complete, with Num Filtered Remaining: " + numRemainingFromLastFilteredBatch);
								break;
							}
						}

						lastBatchSize = ROWS_BATCH_SIZE - numStillRequiredForNextBatch;
						
						logger.logThreadDetail("Batched Filtering: Last nextBatch size: " + lastBatchSize);
						
						// If the records ran out and/or the filtering excluded all rows, then exit the wider fetching loop for this data source
						if ( 0 == lastBatchSize ) {
							if ( ROWS_BATCH_SIZE == nextBatch.length ) recycledRowsBuffer.offer( nextBatch ); // recycle this batch for use by another exec thread
							break; // No filtered rows at all for the batch - clean break - break outer while loop
						}
					}
					
					// Reduce batch and mark end of fetching if required
					if ( lastBatchSize < nextBatch.length ) {
						// Record scan completed half way through a batch - reduce the batch size and get ready to complete fetch loop
						DataValueDescriptor[][] reducedBatch = new DataValueDescriptor[lastBatchSize][];
						System.arraycopy(nextBatch, 0, reducedBatch, 0, lastBatchSize);
						nextBatch = reducedBatch;
						isRowsRemaining = false; // causes while loop to end after this batch
					}					
					
					if ( isExplain )
						// We are processing a Gaian Node data source in explain mode, which means the last row returned contains
						// a count that applies to the current cumulative count.
						// Set this value for every last row of every batch that goes by as it is no more expensive than doing a comparison and
						// constantly holding on to the previous row... alternatively we need to make nextFastPathRow return false
						// when the last row is returned (rather than after it has been returned)
						lastGaianNodeCountDVD = nextBatch[ nextBatch.length-1 ][ exposedColumnCount-1 ]; //resultSet.getInt( GaianDBConfig.EXPLAIN_COUNT_COLUMN_INDEX );
					
				} catch (SQLException e) {
					logDerbyThreadException(dsWrapper + " Unable to fetch next row, aborting fetch for this Data Source, cause: ", e);
					break;
				}
				
				resultRowsBuffer.offer( nextBatch ); // No need to block
//				try {
//					System.out.println("Enqeued " + nextBatch[0][0].getString());
//				} catch (StandardException e) {
//					e.printStackTrace();
//				}
			} // end while ( isRowsRemaining )
						
			// For explain queries, if the processed node was a Gaian Node then lastGaianNodeCountDVD will be set and it
			// represents the total count of its rows. We add this to our own count.
			if ( isExplain && null != lastGaianNodeCountDVD ) // will be null if there were no rows (due to a predicate filtering them out)
				try {
					dataSourceExplainCount = lastGaianNodeCountDVD.getInt();
				} catch (StandardException e) {
					logger.logException( GDBMessages.RESULT_EXPLAIN_ERROR, dsWrapper + " Explain exception: Unable to get int value for row count from Gaian Node result: ", e );
				}
		}
			
//		int numRemainingExecThreads;
		synchronized( executingDataSourceWrappers ) {
			executingDataSourceWrappers.remove(dsWrapper);
//			numRemainingExecThreads = executingDataSourceWrappers.size();
			numRemainingExecThreads--;
			if ( -1 < dataSourceExplainCount ) {
				explainRowCount += dataSourceExplainCount;
				logDerbyThreadInfo( dsWrapper + " Increased explain row count from data source result, current count = " + explainRowCount);
			}

			dsTimes += System.currentTimeMillis() - queryStartTime;
		}
		
//		logDerbyThreadInfo(vti + ": numBufferedRowsBuilt: " + numBufferedRowsBuilt + ", num rows in result buffer: " + resultRowsBuffer.size());
		
		if ( 0 == numRemainingExecThreads ) {
//			try {
			// There is an extra slot free in the results buffer at creation time for the poison pill
				resultRowsBuffer.offer( new DataValueDescriptor[0][] );
				logDerbyThreadInfo( dsWrapper + " Put poison pill on resultRowsBuffer as there are no more executing threads");
//			} catch (InterruptedException e) {
//				logger.logException("Interrupted while putting final termination row on resultRowsBuffer queue: ", e);
//			}
		}
		
		// Data extraction complete for this data source
		
		logDerbyThreadInfo( dsWrapper + ": NO MORE ROWS IN THIS RESULT SET - recyling dataSource");
		
		if ( Logger.LOG_NONE < Logger.logLevel ) {
			
			long timeNow = System.currentTimeMillis();
//			logDerbyThreadImportant("currentNodeFetchEndTime=" + timeNow);
//			VTIWrapper currentVTI = (VTIWrapper) originatingVTIs.get( resultSet );
			
			long jdbcExecTime = dsWrapper instanceof VTIRDBResult && null != nodeRows ? ((VTIRDBResult) dsWrapper).removeRDBExecTime(nodeRows) : -1;
			
			logDerbyThreadImportant(dsWrapper + " nextRow(): Processed New Data Source, (" + 
					numRemainingExecThreads + "/" + numExecThreads + " remaining), NumRows fetched so far: " + rowNumber +
					"\n\t>>Performance metrics: " +
					"Exec time: " + execTime + "ms" +  
					", Fetch time: " + (timeNow - timeAfterExec) + "ms" + 
					( -1 != jdbcExecTime ? ", External RDB Exec time: " + jdbcExecTime + "ms" : ""));
//	    			execCount + " out of " + nodeDefNames.length + " nodes processed");
		}
		
		try { if ( null != nodeRows ) dsWrapper.recycleOrCloseResultWrapper( nodeRows ); }
		catch (Exception e1) { logDerbyThreadException(dsWrapper + " Could not recycle Data Source (ignored), cause: ", e1); }
		
//		System.out.println("Data source " + (numExecThreads-numRemainingExecThreads) + (dataSource.isGaianNode()?"(G)":"(L)") + 
//				": ds query time: " + dsTime + ", thread cpu time: " + cpuTime/1000000 +
//				(numRemainingExecThreads==0 ? ", final totals:: ds: " + dsTimes + ", cpu: " + (((double)cpuTimes)/1000000) : "") );
	}
	
	public int getCumulativeDataSourceTimes() {
		synchronized(executingDataSourceWrappers) {
			long previousWorkload = totalWorkloadSoFar;
			long timeSinceStart = System.currentTimeMillis()-queryStartTime;
//			System.out.println("dsTimes " + dsTimes + ", numExecThreads " + executingVTIs.size() + ", timeSinceStart " + timeSinceStart );
			totalWorkloadSoFar = dsTimes + executingDataSourceWrappers.size() * timeSinceStart;
//			System.out.println("prev wl = " + previousWorkload + ", tot = " + totalWorkloadSoFar + ", delta = " + (totalWorkloadSoFar-previousWorkload));
			return (int) (totalWorkloadSoFar-previousWorkload);
		}
	}
	
	public int getFinalDataSourceTime() {
		return (int) dsTimes;
	}

	public final int getEstimatedRecordSize() {
		return estimatedRecordSize;
	}
	
	public final long getQueryTimeIncludingRefetch() { return queryTimeIncludingRefetch; }
	public final long getQueryTime() { return queryTime; }
	public final long getFetchTime() { return fetchTime; }
	public final long getQueryStartTime() { return queryStartTime; }
	
	public final boolean isExecuting() { return numExecThreadsHavingCompletedExecution.intValue() < numExecThreads; }
	public final boolean isAwaitingRefetch() { return gaianStatementNode.isAwaitingReFetch(); }
	
	private int cycleThroughBackEndVTIRows( GaianChildVTI dataSourceHandle ) throws Exception {
		
		int countValue = 0;
		
		if ( dataSourceHandle instanceof GaianChildRSWrapper ) {
			// A count(*) should have been issued to obtain the resulting ResultSet
			DataValueDescriptor[] dvdr = { new SQLInteger() };
			dataSourceHandle.fetchNextRow( dvdr );
			 countValue = dvdr[0].getInt();
			if ( dataSourceHandle.fetchNextRow( dvdr ) )
				throw new SQLException( "Explain mode problem: Result from a count(*) query against an RDB back-end has more than 1 row" );
			
		} else 
			// Result from custom VTI call - e.g. FileImport or InMemoryRows
			countValue = dataSourceHandle.getRowCount();
		
		return countValue;
	}
	
	private boolean nextFastPathRow( VTIWrapper dsWrapper, String dsInstanceID, GaianChildVTI nodeRows, DataValueDescriptor[] dvdr ) throws SQLException  {
		
		if ( null == sqlResultFilter )
			return nextFastPathRow2( dsWrapper, dsInstanceID, nodeRows, dvdr );
		
		while ( true ) {
			if ( false == nextFastPathRow2( dsWrapper, dsInstanceID, nodeRows, dvdr ) ) return false;
			// Apply row filtering at the local data source level, not on rows returned by other nodes.
			if ( !dsWrapper.isGaianNode() && !sqlResultFilter.filterRow(dvdr) ) continue; // skip this row
			return true;
		}
	}
	
	/**
	 * Get a row for a certain Data Source VTIWrapper node
	 * 
	 * @param dvdr
	 * @return true if we got a row, false if there are none left
	 * @throws SQLException
	 */
	private boolean nextFastPathRow2( VTIWrapper dsWrapper, String dsInstanceID, GaianChildVTI nodeRows, DataValueDescriptor[] dvdr ) throws SQLException {
		
		boolean logDetailForFirstRecordOnly = Logger.LOG_ALL == Logger.logLevel && nodeRows.isBeforeFirst();
		if ( logDetailForFirstRecordOnly ) {
			logDerbyThreadInfo(dsWrapper + " first record input dvdr: " + Arrays.asList(dvdr));
			logDerbyThreadInfo(dsWrapper + " first record rwtemplate: " + Arrays.asList(dvdrTemplate) );
			String[] colTypeNames = new String[dvdr.length];
			for (int i=0; i<dvdr.length; i++) colTypeNames[i] = dvdr[i].getTypeName();
			logDerbyThreadInfo(dsWrapper + " first record type Names: " + Arrays.asList( colTypeNames ));
		}
		
		// Set the physical columns
		try {
//			logDerbyThreadInfo("Getting row for dataSource: " + dataSource);
			if ( false == nodeRows.fetchNextRow(dvdr) ) return false;
			
			if ( logDetailForFirstRecordOnly ) logDerbyThreadInfo(dsWrapper + " first record out dvdr1: " + Arrays.asList(dvdr));
			
		} catch (Exception e) {
			logDerbyThreadException(dsWrapper + " nextFastPathRow2(): Failed to obtain a row from IFastPath result (truncated), cause: ", e);
			return false;
		}
		
//		logDerbyThreadInfo("nextFastPathRow2(): Setting leaf node cols: " + !currentNodeResult.getOriginatingVTI().isGaianNode());
		
		// We have a row
		// Set the special columns for leaf nodes now.
		if ( !dsWrapper.isGaianNode() ) {
			
			// If we get here we cannot be in 'explain' mode - so the exposed columns can only include
			// physical cols, constant cols and provenance cols -
			// Remember columns beyond the exposed ones are NULL columns - used to fool Derby into thinking they exist when actualy they haven't
			// been defined for the Logical Table on this node (yet). This allows partial results to be retrieved.

//			int[] physicalProjectedColumns = gaianStatementNode.getPhysicalProjectedColumns();
//			int[] allProjectedColumns = gaianStatementNode.getProjectedColumns();
//			
//			DataValueDescriptor[] dvdrTemplate =
//				((GaianResultSetMetaData) gaianStatementNode.getMetaData()).getRowTemplate();
			
			// Overwrite any cells that were intended to be constants (this is an option for custom VTI wrappers which are "PLURALIZED")
			if ( null != dsInstanceID ) gaianStatementNode.setConstantEndpointColumnValues( dvdr, dsWrapper, dsInstanceID );
			
			for (int i=physicalProjectedColumns.length; i<allProjectedColumns.length; i++) {
				
				int colID = allProjectedColumns[i]-1;
				
				// Do not go beyond the number of exposed columns, where the rest are NULL columns (not defined for this LT yet)
				// 15/04/09 -> No need for this check here ... null cols are already factored out of pcols in GaianTable ... 
//				if ( colID >= exposedColumnCount ) break;
				
				try {
					DataValueDescriptor dvdt = dvdrTemplate[colID];
					
					if ( dvdt.isNull() ) {
						// This is a constant leaf column - there only exists one at the moment which is GDB_LEAF, so set it.
						dvdr[colID].setValue( dsWrapper.getSourceDescription( dsInstanceID ) );
					} else
						// this is either a constant for the node or for the logical table
						dvdr[colID].setValue( dvdt );
					
				} catch (Exception e) {
					logDerbyThreadException(
							"nextFastPathRow(): Unable to set special column from DVD[] Template " + Arrays.asList(dvdrTemplate) +
							" into table result row (size "+ dvdr.length +"), column: projectedColumn["+i+"]-1 = " + colID + ", cause: ", e);
					return false;
				}
			}
		}

		if ( logDetailForFirstRecordOnly ) logDerbyThreadInfo(dsWrapper + " first record out dvdr2: " + Arrays.asList(dvdr));
		
		// We got a row
		return true;
	}

//	/**
//	 * get the metadata from this query.
//	 *
// 	 * @exception SQLException on unexpected JDBC error
//	 */
//    public ResultSetMetaData getMetaData() throws SQLException {	
//		
//		return nodeRows.getMetaData();
//	}

	/**
	 * Closes the GaianResult. All thread resources and structures are cleared down
	 * 
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() throws SQLException {

		logDerbyThreadInfo("Entering GaianResult.close()");
		
		if ( !isScanCompleted ) {
			isScanCompleted = true;
			long timeNow = System.currentTimeMillis();
			queryTime = timeNow - queryStartTime;
			fetchTime = timeNow - queryFetchStartTime;
			if ( !gaianStatementNode.isAwaitingReFetch() ) queryTimeIncludingRefetch = queryTime;
			
		} else if ( -1 == queryTimeIncludingRefetch && !gaianStatementNode.isAwaitingReFetch() )
			queryTimeIncludingRefetch = System.currentTimeMillis() - queryStartTime;
		
		// Stop all threads that are fetching rows from the data sources and populating the resultRowsBufer.
		// All of them will be recycled at the end of execution as per normal.
		// The last of these will write a poison pill to the buffer.
		numBufferedRowsBuilt = 0; // we are about to clear the recyled rows, so make sure we don't wait for one.
		isQuiesceQuery = true;

		// Release the execution threads blocked on the blocking buffers by clearing the buffers. (these should never be null really)
		if ( null != resultRowsBuffer ) clearAndrecycleBufferedCells( resultRowsBuffer ); // resultRowsBuffer.clear();
		if ( null != recycledRowsBuffer ) clearAndrecycleBufferedCells( recycledRowsBuffer ); // recycledRowsBuffer.clear();
		executingDataSourceWrappers.clear(); // just to be sure
//		if ( null != resultRowsBuffer ) resultRowsBuffer.clear();
//		if ( null != recycledRowsBuffer ) recycledRowsBuffer.clear();
		
		// rowsBatch and dataSources should not be tampered with while the threads flush themselves out.
		// In any case they have little impact in that they are small and don't hold threads back.
//		if ( null != rowsBatch ) {
//		for ( int i=0; i < rowsBatch.length; i++ ) {
//			DataValueDescriptor[] dvdr = rowsBatch[i];
//			for ( int j=0; j < dvdr.length; j++ ) dvdr[j] = null;
//			rowsBatch[i] = null;
//		}
//		rowsBatch = null;
//		}
//		if ( null != dataSources ) dataSources.clear();
	}
	
	private void logDerbyThreadInfo( String s ) {
		if ( Logger.LOG_MORE <= Logger.logLevel ) logInfo(s, Logger.LOG_MORE);
	}
	
	private void logDerbyThreadImportant( String s ) {
		if ( Logger.LOG_LESS <= Logger.logLevel ) logInfo(s, Logger.LOG_LESS);
	}
	
	private void logDerbyThreadWarning( String s ) {
		logInfo(s, Logger.LOG_NONE);
	}

	private void logDerbyThreadException( String s, Exception e ) {
		logInfo(s + Util.getExceptionAsString(e), Logger.LOG_NONE);
	}
	
	private void logInfo( String s, int threshold ) {
//		String ctx = (null==currentNodeResult?gaianStatementNode.getLogicalTable(true):currentNodeResult.getOriginatingVTI())
		String ctx = (gaianStatementNode.isSystemQuery() ? Logger.LOG_EXCLUDE : "")
					+ gaianStatementNode.getLogicalTableName(true)
					+ " queryID=" + gaianStatementNode.getQueryID() + 
					" steps=" + gaianStatementNode.getQueryPropagationCount();
		logger.log( ctx + " " + s, threshold );
	}
	
	/**
	 * Deprecated - now GaianResult is only really used through the IFastPath interface directly
	 */
	public boolean next() throws SQLException {
		
		setWarning("Warning added in next() opty!!!!!!!!!!");
		return false;
//		return IFastPath.GOT_ROW == nextRow(null) ? true : false;
	}
		
	public long getExplainRowCount() {
		return explainRowCount;
	}
	
	public long getRowCount() {
		return rowNumber;
	}
	
	public ArrayList<Long> getPollTimes() {
		return pollTimes;
	}
	
	public boolean isScanCompleted() { return isScanCompleted; }
	
	/**
	 * Get the next row using the IFastPath interface, i.e. get columns straight into a DataValueDescriptor[] object.
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int nextRow( DataValueDescriptor[] dvdr ) throws Exception {
		
		if ( isScanCompleted ) return IFastPath.SCAN_COMPLETED;
		
		// Fetch the next row.
		// Finish fetching from the current data source before moving on to the next one having completed query execution.
//		long periodicFetchTime = -1;
		
		if ( Logger.LOG_NONE < Logger.logLevel ) {
			if ( 0 == rowNumber ) {
				queryFetchStartTime = System.currentTimeMillis();
//				logDerbyThreadImportant("queryFetchStartTime=" + queryFetchStartTime);
			}
			
//			System.out.println("Fetched " + rowNumber + " rows");
						
			if ( rowNumber % GaianTable.LOG_FETCH_BATCH_SIZE == 0 ) {
				logDerbyThreadInfo("nextRow(): Fetching new batch of " + 
						GaianTable.LOG_FETCH_BATCH_SIZE + " rows from : " + rowNumber + 
						". Row Buffer sizes: Results " + resultRowsBuffer.size() + "/" + FETCH_BUFFER_SIZE + 
						", Recycled " + recycledRowsBuffer.size() + "/" + FETCH_BUFFER_SIZE);
//				periodicFetchTime = System.nanoTime();
			}
		}

		if ( rowsBatch.length <= rowsBatchPos ) {
		
			try {
				// Recycle the old batch
//				if ( 0 != rowsBatch.length ) recycledRowsBuffer.offer(rowsBatch);
				if ( ROWS_BATCH_SIZE == rowsBatch.length ) recycledRowsBuffer.offer(rowsBatch);
				
				// Get a new batch of rows
				long polls = 0;
				
				while ( true ) {

//					nextRow = isLogPerfOn ? resultRowsBuffer.poll( POLL_FACTOR_NANOS, TimeUnit.NANOSECONDS ) :
					rowsBatch = isLogPerfOn ? resultRowsBuffer.poll( POLL_FACTOR_MICROS, TimeUnit.MICROSECONDS ) :
//						isActiveConnectionsChecked ? resultRowsBuffer.take() :
						resultRowsBuffer.poll( EXEC_TIMEOUT_MICROS/1000, TimeUnit.MILLISECONDS );
							
//					ArrayBlockingQueue q = null;
//					q.poll()timeout, unit)

					if ( null != rowsBatch ) {
						
						if ( isLogPerfOn ) pollTimes.add( polls );
//						if ( -1 != periodicFetchTime ) logger.logInfo("1-off Fetch Time (ns): " + (System.nanoTime() - periodicFetchTime) ); }
						
						if ( 0 == rowsBatch.length ) {

							close();
							if ( Logger.LOG_NONE < Logger.logLevel ) {
								logDerbyThreadImportant( "Fetch complete, rowCount = " + rowNumber + "\n\t>>OVERALL QUERY TIME: " + queryTime + 
										"ms (ALL EXECS: " + queryExecTime + "ms, ALL FETCHES: " + fetchTime + "ms)\n");
//								System.out.println("OVERALL QUERY TIME: " + queryTime);
//								chainOurWarningsToResultSetWarnings(); // Attach any warnings to the last ResultSet
//								currentChildNodeName = null;
//								logDerbyThreadInfo("nextRow() - Exiting: SCAN_COMPLETED");
							}
							return IFastPath.SCAN_COMPLETED;
						}
						
//						try {
//							System.out.println("Deqeued " + rowsBatch[0][0].getString());
//						} catch (StandardException e) {
//							e.printStackTrace();
//						}
						
						// Got a new rows batch - set position to 1st row in batch
						rowsBatchPos = 0;
						
						if ( 0 == rowNumber ) for ( DataValueDescriptor dvd : rowsBatch[0] ) estimatedRecordSize += dvd.estimateMemoryUsage();
						
						break;
					}
					
//					if ( (!isLogPerfOn || polls++*POLL_FACTOR_NANOS > 1000*EXEC_THREAD_TIMEOUT_MICROS) &&
					if ( (!isLogPerfOn || polls++*POLL_FACTOR_MICROS > EXEC_TIMEOUT_MICROS) && //EXEC_THREAD_TIMEOUT_MICROS) &&
							!isActiveConnectionsChecked ) {
						
						if ( !GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianNode.isNetworkDriverGDB() ) {
							// Can't afford to wait for long running queries.. the remote calling UDP client is waiting for us...
							// ToDo: In future our server should send messages periodically to say a response is coming and tell the remote client to keep waiting.
							logDerbyThreadInfo("Exec Thread Timeout: Temporarily disabling potentially hanging Data Sources...");
							
							for ( VTIWrapper dataSource : executingDataSourceWrappers )
								if ( dataSource instanceof VTIRDBResult ) {
									((VTIRDBResult) dataSource).temporarilyDisable(1000); // invalidate data source
//									DataSourcesManager.clearSourceHandlesStackPool( dataSource.sourceID ); // close all associated jdbc connections
								}
						}

						int numExecThreadsRemaining = numExecThreads - numExecThreadsHavingCompletedExecution.get();
						logDerbyThreadInfo("Exec Thread Timeout: Long running query detected - "
								+ " checking for hanging data sources amongst ~" + numExecThreadsRemaining + '/' + numExecThreads + " remaining ones: "
								+ executingDataSourceWrappers );
						
						// Register executing sources even for GDB UDP driver... so that lost connections can be detected.
						// Note that the hanging data source check is only done for nodes that are RDBMS connections to other GaianDB nodes. 
						DatabaseConnectionsChecker.rootOutHangingDataSources( executingDataSourceWrappers, this );

						if ( GaianNode.isNetworkDriverGDB() ) {
							// Clear all executing data sources.. don't allow long running queries for now..
							executingDataSourceWrappers.clear();
							resultRowsBuffer.offer( new DataValueDescriptor[0][] ); // Indicates end of result
							logger.logInfo("Put poison pill on rowResultsBuffer after dismissing executing data sources");
						}
						
						// Item 30947 - commented out to repeat polling the remote gdbs throughout lifetime of long-running query (in case multiple cascading nodes fall-over)
						// Changes were made elsewhere to support this as well
						// isActiveConnectionsChecked = true;
					}
				}
				
			} catch ( Exception e ) {
				logDerbyThreadException( "Exception in nextRow() (returning SCAN_COMPLETED): ", e );
				close();
				return IFastPath.SCAN_COMPLETED;
			}
		}
		
		// Get the next row in the batch
		DataValueDescriptor[] nextRow = rowsBatch[rowsBatchPos++];
		

//		int[] colSizes = new int[ fullProjectionSize ];
//		int total = 0;
		
		// Copy the next row from the batch into Derby's dvdr row.
		for ( int i=0; i<fullProjectionSize; i++ ) {
//			colSizes[i] = dvdr[i].estimateMemoryUsage(); total += colSizes[i];
			int cid = fullProjectionZeroBased[i];
//			try {
			dvdr[cid] = nextRow[cid];
//			} catch ( ArrayIndexOutOfBoundsException e ) {
//				logger.logInfo("AIOBE: dvdr size " + dvdr.length + ", nextRow size " + nextRow.length + 
//						", index: " + cid + ", full proj zero based: " + Util.intArrayAsString(fullProjectionZeroBased));
//				throw(e);
//			}
		}
		rowNumber++;
		
//		System.out.println("row size: " + total + ", cols: " + Util.intArrayAsString(colSizes));
		return IFastPath.GOT_ROW;
	}
	
	void endResults() { resultRowsBuffer.offer( new DataValueDescriptor[0][] ); } // Indicates end of result;
	void reEnableCheckingOfHangingQueries() { isActiveConnectionsChecked = false; }
		
	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public SQLWarning getWarnings() throws SQLException {
    	
//    	warnings.setNextWarning( resultSet.getWarnings() );
    	logger.logThreadInfo("Derby is Getting Warnings... opty");
        return warnings;
    }

    private void setWarning( String warning ) {
    	if ( null == warnings ) {
    		warnings = new SQLWarning( warning );
    	} else {
    		warnings.setNextWarning( new SQLWarning(warning) );
    	}

    	logger.logThreadInfo("Entered setWarning()... setting GaianTable warnings");
    	gaianStatementNode.setWarnings( warnings );
    }
    
//    private void chainOurWarningsToResultSetWarnings() throws SQLException {
//    	if ( null != warnings && null != resultSet && null != resultSet.getWarnings() ) {
//    		logger.logThreadInfo("Chaining Warnings to last ResultSet... " + resultSet.getWarnings());
//    		resultSet.getWarnings().setNextWarning( warnings );
//    	}
//    }
//
//    public void clearWarnings() throws SQLException {
//        nodeResult.clearWarnings();
//    }
}
