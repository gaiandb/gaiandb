/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;

import com.ibm.db2j.FileImport;
import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.DataSourcesManager.RDBProvider;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * A VTIWrapper for JDBC data sources.
 * 
 * @author DavidVyvyan
 */
public class VTIRDBResult extends VTIWrapper {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "VTIRDBResult", 30 );
	
//	private static final int PREPARED_STMTS_CACHE_SIZE = 100;
//	private final Map cachedPreparedStatements = Collections.synchronizedMap( new PreparedStatementsCache(PREPARED_STMTS_CACHE_SIZE) );
//	private final Map reversePreparedStatementsSQLLookup = new Hashtable();
	
	private static final int PREPARED_STMTS_CACHE_SIZE_PER_CONNECTION = 100;
	
//	private static final int PREPARED_STMTS_CACHE_SIZE = 100;
	
	// Connection -> <SQL,PreparedStatement>
	// Structure used to find already prepared statements on a connection taken from the pool.
	private static final ConcurrentMap<Connection, Map<String, PreparedStatement>> preparedStatementsOfConnections =
		new ConcurrentHashMap<Connection, Map<String, PreparedStatement>>();
	
	public static final void clearPreparedStatementsCacheForConnection( Connection c ) throws SQLException {
		Map<String, PreparedStatement> pstmts = preparedStatementsOfConnections.remove(c);
		if ( null != pstmts ) {
			for ( Statement s : pstmts.values() ) s.close();
			pstmts.clear();
		}
	}

	// Time at which to re-enable the vti data source if it was disabled on account of the connection being lost
	private long reenablementTime = 0;
	private static final int DISABLEMENT_PERIOD_MS = 5000; // TODO: This value should maybe be linked to the connection checker heartbeat ?
	
//	// PreparedStatement -> SQL
//	private final Map preparedStatementsSQL = 
//		Collections.synchronizedMap( new PreparedStatementsCache(PREPARED_STMTS_CACHE_SIZE) );
//	
//	// SQL -> Stack of PreparedStatement objects
//	private final ConcurrentMap<String,Stack> cachedPreparedStatements = new ConcurrentHashMap();
//	
//	private final class PreparedStatementsCache extends CachedHashMap {
//		private PreparedStatementsCache( int cacheSize ) {
//			super( cacheSize );
//		}
//		
//		protected boolean removeEldestEntry(Map.Entry eldest) {
//			
//			if ( size() <= cacheSize ) return false;
//			
//			PreparedStatement pstmt = (PreparedStatement) eldest.getKey();
////			PreparedStatement pstmt = (PreparedStatement) eldest.getValue();
//			
//			String sql = (String) eldest.getValue();
//			
//			Stack pstmts = (Stack) cachedPreparedStatements.get(sql);
//			if ( null != pstmts ) {
//				if ( pstmts.remove(pstmt) ) {
//					try { pstmt.close(); } // Only do this close() here as we know the pstmt is not currently in use.
//					catch (SQLException e) {} // Ignore exception - i.e. already closed..
//				}
//				if ( pstmts.isEmpty() ) {
//					logger.logInfo("REMOVING CACHED PREPARED STMTS ENTRY");
//					cachedPreparedStatements.remove(sql);
//				}
//			}
//			return true;
//		}
//	}
	
//	private static final String[] TRIGEVENTS = { "DELETE", "UPADTE", "INSERT" };
	
	// GaianChildVTI (GaianChildRSWrapper) -> Long exec time
	private static final ConcurrentMap<GaianChildVTI, Long> rdbExecTimes = new ConcurrentHashMap<GaianChildVTI, Long>();
	private final String connectionDetails;
	private final RDBProvider rdbmsProvider;
	
	private final boolean isLocalDerbyDataSource;
	
	private String physicalTable = null;
	
	// Shorthand table expressions (i.e. not wrapped in sub-query), example: LTN_TABLE=PTABLE1,PTABLE2 WHERE PTABLE1.A=TABLE2.B
	// This cannot be used if there are column name clashes, e.g. if PTABLE1 and PTABLE2 have some matching column names.
	private int shorthandTableXprWhereClauseIndex = -1;
	
	private boolean isGaianNode = false;
	private boolean isSubQuery = false;
//	private boolean isTableNeedsRefresh = false; // Not currently used - would be used to trigger a refresh for in memory tables...
	
	// List used to track down the connection attached to a hanging statement.
	// We need a synchronized List due to concurrent threads - hence Vector rather than ArrayList
	// A Stack allows faster removal time than Vector for the fast queries because it searches from the first element in the list.
	private Stack<Connection> activeConnections = new Stack<Connection>();
	
	public VTIRDBResult( String connectionDetails, String nodeDefName, GaianResultSetMetaData logicalTableRSMD ) throws Exception {
		
		super( connectionDetails, nodeDefName );
		this.connectionDetails = connectionDetails;
		this.rdbmsProvider = RDBProvider.fromGaianConnectionID(connectionDetails);
		
		logger.logInfo( nodeDefName + " Building new VTIRDBResult based on: " +
				connectionDetails.substring( 0, connectionDetails.lastIndexOf("'") ) + ", RDBMS provider is " + rdbmsProvider ); // omit password from logs
		
		String[] props = GaianDBConfig.getConnectionTokens( connectionDetails );
		this.isLocalDerbyDataSource = props[1].startsWith(
				props[1].startsWith("jdbc:derby://") ? "jdbc:derby://localhost:" + GaianDBConfig.getDerbyServerListenerPort() : "jdbc:derby:");
		
		reinitialise( logicalTableRSMD );
	}
	
	public boolean isGaianNode() { return isGaianNode; }
	public boolean isSubQuery() { return isSubQuery; }

	@Override
	public String[] getPluralizedInstances() { return null; }

	@Override
	public DataValueDescriptor[] getPluralizedInstanceConstants(String dsInstanceID) { return null; }
	
//	public ResultSet execute( Qualifier[][] qualifiers, int[] projectedColumns ) throws Exception {
//		return execute( null, qualifiers, projectedColumns );
//	}
//	private static final ConcurrentHashMap<String, Object> noArguments = new ConcurrentHashMap<String, Object>(0);
//	private static final int resultPrefetchSize = 20;
//	
//	public GaianChildVTI execute( Qualifier[][] qualifiers, int[] projectedColumns, String table ) throws Exception {
//		return execute( noArguments, qualifiers, projectedColumns, table );
//	}
	
	public GaianChildVTI execute( ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns ) throws Exception {
		return execute( arguments, qualifiers, projectedColumns, physicalTable );
	}
		
	protected GaianChildVTI execute( ConcurrentMap<String, Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns,
			String tableExpression ) throws Exception {
		
		GaianChildVTI result = null;
		if ( null == tableExpression ) tableExpression = physicalTable;
		
		// Take a local snapshot of this value in case it changes mid-execution.
		boolean isInMemRows = isRowsInMemory;
		
		logger.logThreadInfo(nodeDefName + " Entered VTIDBResult.execute");
		
		// Stack of objects that to access the back end data source.
		Stack<Object> sourceHandles = null;
		
		if ( isInMemRows ) {
			// InMemRows is set: DEAL WITH IN MEMORY ROWS CASE:

			sourceHandles = DataSourcesManager.getSourceHandlesPool( connectionDetails, true );
			
			// No automated in-mem reload at present as we can't know when the underlying rdb table has changed.
			// However rows will have been reloaded if option INMEMORY has just been set in the config file for this node.
			
//				if ( isTableNeedsRefresh ) {
//					// Trigger was set off - or the rows were unloaded
//					loadInMemory();
//					isTableNeedsRefresh = false;
//				}
					
			synchronized( sourceHandles ) {
				if ( !sourceHandles.empty() ) result = (InMemoryRows) sourceHandles.pop();
			}
			
			if ( null == result ) {
				// No more instances of the InMemoryRows Class available
				result = new InMemoryRows();
				logger.logThreadInfo( "Created a new InMemoryRows() instance as the Stack Pool was empty");
				
			} else {				
				logger.logThreadInfo( "Extracted an existing InMemoryRows() instance from the Stack Pool");
			}
						
			// set rows and indexes
			((InMemoryRows) result).setRowsAndIndexes( inMemoryRows, inMemoryRowsIndexes );
			
			((InMemoryRows) result).setExtractConditions( qualifiers, projectedColumns, 
					safeExecNodeState.getColumnsMapping( (int[]) arguments.get(GaianTable.QRY_INCOMING_COLUMNS_MAPPING) ) );
			
			if ( Logger.LOG_LESS <= Logger.logLevel ) {
			    String prefix = Logger.sdf.format(new Date(System.currentTimeMillis())) + " ---------------> OBTAINED rows from: ";
				logger.logThreadImportant( nodeDefName + " OBTAINED ROWS USING:\n\n" + prefix + "InMemoryRows('" + getSourceDescription(null) + "')\n" );
			}

			
			return result;
		}
		
		// Execution of a query against an rdbms source relies on us having obtained col types from the source:
		// For the Physical table directly: so that appropriate casts can be done when the logical types don't match.
		// For the InMemorRows: so that column mappings can be made against the physical cols.
		
		sourceHandles = DataSourcesManager.getSourceHandlesPool( connectionDetails ); // Use jdbc source handles
		Connection conn = null;
		Statement pstmt = null;
		
		try {
			
//			System.out.println("nodedef: " + nodeDefName + ", reenablement time: " + reenablementTime);
			
			if ( reenablementTime != 0 ) {
				if ( reenablementTime > System.currentTimeMillis() ) return null;
				reenablementTime = 0;
			}

			long t = 0;

			// See if we already have a retained resultSet (only the case when it HAD to be retrieved to get its meta-data)
			
			// *** WE WILL HAVE A RETAINED RESULT ON EACH INVOCATION OF A GAIANQUERY THAT WRAPS A CALL TO A STORED PROCEDURE ***
			// FOR REPEATED INVOCATIONS, WE STILL HAVE A NEW RETAINED RESULT EACH TIME - BECAUSE THE SHAPE OF A PROCEDURE RESULT CAN CHANGE
			
			ResultSet resultSet = logicalTableRSMD.getRetainedResultSet( nodeDefName );
			Connection parentCalledProcedureConnection = logicalTableRSMD.getParentCalledProcedureConnection( nodeDefName );
			int updateCount = logicalTableRSMD.getRetainedUpdateCount( nodeDefName );
			
			logger.logThreadInfo(nodeDefName+" has a Retained resultSet/updateCount ? " + (null != resultSet) + "/" + (-2 < updateCount) );
			
			// Check if we have a retained result
			if ( null == resultSet && -1 > updateCount ) {
				// No retained result - execute the query against the data source 
				
				String sql = null;
				boolean isSubqCall = false;
				
				if ( isSubQuery ) {
					// This block of code analyzes pass-through sub-queries to:
					// 		1. Determine if it is a stored procedure call - for later
					//		2. Abort it if it might perform a write and if propagated writes are disallowed
					//		3. Determine if the sub-query has nothing surrounding it, to by-pass later code setting selected cols or outer where-clause.
					
					String first7chars = 7 < tableExpression.length() ? tableExpression.substring(0, 7).toLowerCase() : null;
					
					if ( null != first7chars ) {
						
						// replace other whitespace (\\s) or double quotes (") with a space
						first7chars = first7chars.replaceAll("\\s", " ").replaceAll("\"", " ");
						isSubqCall = first7chars.startsWith("call ") || first7chars.startsWith("exec");
						
						// Check if this is a straight sub-query - i.e. that it is not wrapped with brackets, e.g "(select * from t) SUBQ"
						// Note "select" or "update" tokens may be followed by a space ( ) or a double quote (")
						
						// If the expression is not bracketed then we don't need to wrap it ourselves (see GaianResult calling code)...
						if ( ! first7chars.startsWith("(") ) {
							
							// If this is anything other than a 'select' and was propagated (steps>0) and propagated writes are disallowed, do not execute.
							if ( !"select ".equals(first7chars) && !first7chars.startsWith("with ") && !first7chars.startsWith("values") && !first7chars.equals("xquery ") &&
									0 < ((Integer)arguments.get(GaianTable.QRY_STEPS)).intValue() && !GaianDBConfig.isAllowedPropagatedWrites() ) {
								logger.logImportant("Propagated write operation '" + first7chars.substring(0, first7chars.indexOf(' ')) +
										"' is not allowed. To enable it add this line to " +
										GaianDBConfig.getConfigFileName() + ": " + GaianDBConfig.ALLOW_PROPAGATED_WRITES + "=TRUE");
								return null;
							}
							
							sql = tableExpression; // This is a non-bracketed sub-query - so there are no applicable external projected cols or qualifiers
						}
					}
				}
				
				//	Just count the rows for back end db queries in explain mode
				logger.logThreadInfo(nodeDefName + " Args are: " + arguments.keySet());
//						+ " -> " + Arrays.asList( (String[]) arguments.keySet().toArray(new String[0]) ) );
				
				String credentials = (String) arguments.get(GaianTable.QRY_CREDENTIALS); // only applicable for a propagation query to a gaian node

				if ( null == sql ) {
					// If we get here, we are not dealing with a plain sub-query (i.e. non-bracketed). Therefore, we may have projected cols and predicates to process.
					
					int[] pColTypes = safeExecNodeState.getPhysicalColTypes();
					logger.logThreadInfo("Existing colTypes: " + Util.intArrayAsString(pColTypes));
					
					if ( null == pColTypes && !isGaianNode ) { // && !isSubQuery ) { // NOTE! sub-queries need mappings for inner query resolved, e.g. select * from ( select * from abc ) t
						conn = getConnection( sourceHandles );
//						if ( null == conn && 0 != reenablementTime ) {
//							logger.logThreadInfo( nodeDefName + " JDBC Connection is not available (yet) - returning null for this data source" );
//							return null; // No connection as connection is not available (yet or not at all)
//						}
						
						final String bracketedSubQueryOrRawTableNameWithOptionalWhereClause = -1 == shorthandTableXprWhereClauseIndex ? tableExpression :
							tableExpression.substring(0, shorthandTableXprWhereClauseIndex);
						
						// crucial synchronous initialisation step:
						pColTypes = safeExecNodeState.getPhysicalColTypes(conn, bracketedSubQueryOrRawTableNameWithOptionalWhereClause);
					}
					
					final String[] pColNames = safeExecNodeState.getColNames();
					
					int exposedColsCount = ((Integer)arguments.get(GaianTable.QRY_EXPOSED_COLUMNS_COUNT)).intValue();
					
					String allProjectedColNames = !isGaianNode && arguments.containsKey(GaianTable.QRY_IS_EXPLAIN) ? "count(*)" :
						getColumnNamesAsCSV( pColNames, pColTypes, projectedColumns, exposedColsCount );
					
					// DRV - 16/07/2012 - Don't do any explicit cast operations for MySQL as it is loosely typed. MySQL will do casting implicitly.
					// MySQL does not support casting to REAL, FLOAT, DOUBLE or NUMERIC (-> causes MySQL ERROR 1064). Only casts to DECIMAL work.
					// MySQL is actually loosely typed compared to other RDBMS providers.
					// Note that a RSMD resulting from a query on a MySQL table having several different column types returns columns all having column type = 1.
					// This means that our logic (in RowsFilter) would do a cast even when physical and logical types were defined as being the same (!).
					String sqlWhereClause =
						isGaianNode && arguments.containsKey(GaianTable.QRY_APPLICABLE_ORIGINAL_PREDICATES) ?
						(String) arguments.get(GaianTable.QRY_APPLICABLE_ORIGINAL_PREDICATES) :
						RowsFilter.reconstructSQLWhereClause( qualifiers,
							safeExecNodeState.getLogicalTableRSMD(), pColNames, rdbmsProvider,
							isGaianNode || isSubQuery || rdbmsProvider == RDBProvider.MySQL ? null : pColTypes ); //safeExecNodeState.getColTypes( statement, tableExpression ) );

					qualifiers = null; // Qualifiers don't need to be applied again
					
					if ( 0 < sqlWhereClause.length() )
						sqlWhereClause = ( -1 == shorthandTableXprWhereClauseIndex ? " WHERE (" : " AND (" ) + sqlWhereClause + ")";
					
					if ( isGaianNode ) {
						sqlWhereClause += ( 0 < sqlWhereClause.length() ? " AND " : " WHERE " ) +
							GaianDBConfig.GDB_QRYID + "=? AND " + GaianDBConfig.GDB_QRYSTEPS + "=?" + // AND " + GaianDBConfig.GDB_QRYFWDER + "=?" ;
							( null == credentials ? "" : " AND " + GaianDBConfig.GDB_CREDENTIALS + "=?" );
					
						if ( arguments.containsKey(GaianTable.QRY_IS_GAIAN_QUERY) && ! tableExpression.endsWith(")") )
							// Add column aliases to GaianQuery aliases... this allows us to select anonymous cols from a sub-query
							// DO NOT DO THIS for GaianTable because 1. col names always match so no need to, and 2. the column names may be in different positions
							// (we'd have to compute and use pColNames for GaianNode to change this)
							tableExpression += '(' + logicalTableRSMD.getColumnNamesIncludingNullOnes(exposedColsCount, true) + ')';
					
					} else if ( isSubQuery && false == "1".equals(allProjectedColNames) )
						// Add this to cater for non-ordinary column identifiers, e.g. "1"
						tableExpression += '(' + Util.stringArrayAsCSV(safeExecNodeState.getPhysicalColumnNames(), rdbmsProvider) + ')';
					
//					System.out.println("tableExpression: " + tableExpression + ", whereClause: " + sqlWhereClause);
					
					sql = "select " + allProjectedColNames + " from " + tableExpression + sqlWhereClause;
				}
				
				String sqlOrderBy = isGaianNode ? "" : (String) arguments.get(GaianTable.QRY_ORDER_BY_CLAUSE);
				if ( null == sqlOrderBy ) sqlOrderBy = "";
				if ( 0 < sqlOrderBy.length() ) sqlOrderBy = " " + sqlOrderBy;
				
				sql += sqlOrderBy;
				
				Integer timeout = (Integer) arguments.get( GaianTable.QRY_TIMEOUT );
				
				// Only push gaiandb hint information for queries destined to 1) other gaiandb nodes or 2) the local derby(/gaiandb).
				// Note it would be incorrect to use ( isGaianNode || isSubQuery ) because sub-queries may target other RDBMS which may not accept the same hint syntax
				// Also, queries may be generated for sources of logical tables against the local derby (not just sub-queries) - and these may themselves have logical table references.
				if ( isGaianNode || isLocalDerbyDataSource ) {
				
					sql += " -- "+GaianTable.GDB_HASH+"="+arguments.get(GaianTable.QRY_HASH); // there should *always* be a QRY_HASH
					// Note GDB_CREDENTIALS is read via a positional parameter if isGaianNode is true (i.e. for propagated queries - see also GaianTable.java near line 985)
					if ( false == isGaianNode && null != credentials ) sql += " "+SecurityManager.CREDENTIALS_LABEL+"="+credentials;
					if ( arguments.containsKey(GaianTable.QRY_WID) ) sql += " "+GaianTable.GDB_WID+"="+arguments.get(GaianTable.QRY_WID);
					if ( null != timeout ) sql += " "+GaianTable.GDB_TIMEOUT+"="+timeout;
				}
				
				if ( null == conn ) conn = getConnection( sourceHandles );
//				if ( null == conn && 0 != reenablementTime ) {
//					logger.logThreadInfo( nodeDefName + " JDBC Connection is not available (yet) - returning null for data source" );
//					return null; // No connection as connection is not available (yet or not at all)
//				}
				
				// Keep track of ordered active  connections in order to find any potential hanging ones later on..
				// The hanging one will be the oldest.
				activeConnections.push( conn );
				logger.logThreadInfo(nodeDefName + " Active connection added: " + conn);
				
				Map<String, PreparedStatement> preparedStatements = preparedStatementsOfConnections.get( conn );
				if ( null == preparedStatements ) {
					// No prepared statements for this connection at all yet! -
					// Create a CachedHashMap for them which starts closing them if it gets too big...
					preparedStatements = new CachedHashMap<String, PreparedStatement>(PREPARED_STMTS_CACHE_SIZE_PER_CONNECTION) {
						private static final long serialVersionUID = 1L;

						protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
							final boolean isSizeSurpassed = super.removeEldestEntry(eldest);
							if ( isSizeSurpassed ) {
								PreparedStatement pstmt = eldest.getValue();
								if ( null != pstmt ) try { pstmt.close(); } catch ( SQLException e ) {}
							}
							return isSizeSurpassed;
						};
					};
					preparedStatementsOfConnections.put( conn, preparedStatements );
				}
				
				// NOTE: Unresolved issue:
				// DROP statements cannot run concurrently with any other statement. This throws a SQLException.
				// The fundamental problem is that Derby closes its resources lazily. We see that through lazy calls to GaianTable.close().
				// This comes up as an intermittent test failure with Test_workedExamplesTwo.java
				
				pstmt = preparedStatements.get(sql);
				boolean isPrepared = null != pstmt;
				
				logger.logThreadInfo("Number of PreparedStatements for Connection: " + preparedStatements.size() + ", SQL is prepared? " + isPrepared);
				
				if ( isSubQuery )
					// Preparing complex sub-queries having nested joins requires derby to cache sub-result "conglomerates".
					// This is seemingly not implemented by derby for vtis (yet) - as we hit "conglomerate does not exist" errors.
					// Therefore we don't prepare sub-queries... we just execute them explicitly each time.
					pstmt = conn.createStatement();
				else if ( !isPrepared ) {
			    	String prefix = Logger.sdf.format(new Date(System.currentTimeMillis())) + " ---------------> PREPARING against " + nodeDefName + ": ";
					logger.logThreadImportant( nodeDefName + " PREPARING:\n\n" + prefix + sql + "\n" );
					pstmt = conn.prepareStatement(sql);
					preparedStatements.put(sql, ((PreparedStatement) pstmt));
				}
				
				if ( isGaianNode ) {					
					((PreparedStatement) pstmt).setString( 1, (String) arguments.get(GaianTable.QRY_ID));
					((PreparedStatement) pstmt).setInt( 2, ((Integer) arguments.get(GaianTable.QRY_STEPS)).intValue()+1 );
					if ( null != credentials ) ((PreparedStatement) pstmt).setString( 3, credentials );
//					pstmt.setString( 3, (String) arguments.get(GaianTable.QRY_FWDER));	
				}
				
				// logger.logThreadDetail("Statement: " + statement + ", Connection: " + (null==statement?null:statement.getConnection()));
				// logger.logThreadDetail("Connection is closed: " + (null==statement?true:statement.getConnection().isClosed()));
				
				if ( null == timeout ) {
					if ( !GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && isGaianNode && GaianNode.isNetworkDriverGDB() ) {
						// Early hack For UDP - Reduce timeout depending on depth, so top level client doesn't timeout before partial results get back
						int depth = ((Integer) arguments.get(GaianTable.QRY_STEPS)).intValue();
						timeout = Math.max( 1000, GaianDBConfig.getNetworkDriverGDBUDPTimeout()-depth*1000 )/1000;
						pstmt.setQueryTimeout( timeout ); // Use UDP network driver timeout
					}
//					logger.logThreadInfo("Setting JDBC Statement execution timeout to Default:" + 10);
				} else {
					logger.logThreadInfo(nodeDefName + " Setting JDBC Statement execution timeout to " + timeout);
					pstmt.setQueryTimeout( timeout.intValue() );
				}
				
				//statement.setFetchSize(10000);
				
				if ( Logger.LOG_NONE < Logger.logLevel ) {
			    	String prefix = Logger.sdf.format(new Date(System.currentTimeMillis())) + " ---------------> EXECUTING against " + nodeDefName + ": ";					
					logger.logThreadImportant( nodeDefName + " EXECUTING against connection:\n\n" + prefix + sql +
							(isGaianNode ? " ; qryid: " + arguments.get(GaianTable.QRY_ID) + 
									", steps: " + (((Integer) arguments.get(GaianTable.QRY_STEPS)).intValue()+1) : "") + "\n" );
//									+ ", fwder: " + arguments.get(GaianTable.QRY_FWDER) + "\n" );
					t = System.currentTimeMillis();
				}
				
				if ( isSubQuery )
					pstmt.execute(sql);
				else
					((PreparedStatement) pstmt).execute(); // can be any query - not just a select
				
				if ( ! (pstmt instanceof GaianChildVTI) ) {
					resultSet = pstmt.getResultSet();
					
					if ( null == resultSet ) {

						// isSubQuery must also be true.. because we only run CRUD or CALLs in sub-queries
						updateCount = pstmt.getUpdateCount();
						logger.logThreadInfo("Closing pstmt and Recycling JDBC Connection early because result is an Update Count");
						
						// This must be a sub-query (as CRUD and CALLs only occur in them) - so pstmt will not be cached - so close it now to free client+server resources
						try { pstmt.close(); } catch ( SQLException e ) {}
						
						if ( false == recycleSourceHandleToPool( conn ) )
							try { conn.close(); } catch ( SQLException e ) {} // pool is probably maxed out - close resource.
					}
				}
				
				// Record the parent connection for recycling (later), because the result is based off a separate connection spawned in the procedure...
				if ( isSubqCall ) {
					// this MUST NOT be recycled now... because a wrapping GaianQuery may re-use the connection before we've fetched rows.. this causes a locking condition..
					parentCalledProcedureConnection = conn;
					logger.logThreadInfo("Recorded parent Connection for executed nested stored procedure - to be recycled later");
				}
			}

			if ( ! (pstmt instanceof GaianChildVTI) ) {
				if ( null == resultSet ) {
					// This was one of insert/update/delete/call/create/drop
					// The outer connection should be recycled already - whether the update count was obtained here or was in a retained result from a SP (resolved in DataSourcesManager).
					result = new GaianChildRSWrapper( updateCount );
				} else
					// Note most queries generate a resultSet which is linked directly to the parent connection to be recycled -
					// The exception to this rule is for resultSets that are derived from a child connection created inside a stored procedure.
					// In that latter case, we need to remember the parent connection used to execute the SP and recycle that one at the end.
					result = new GaianChildRSWrapper( resultSet, parentCalledProcedureConnection ); // either a select or a call() returning a resultSet
			} else
				result = (GaianChildVTI) pstmt; // Already implements our interface - no need to wrap
				
			if ( Logger.LOG_NONE < Logger.logLevel )
				rdbExecTimes.put( result, new Long( System.currentTimeMillis() - t ) );

			// Low value feature to skip maintenance for certain connections
//			if ( isGaianNode ) DatabaseConnectionsChecker.excludeConnectionFromNextMaintenanceCycle(
//					nodeDefName.substring(nodeDefName.lastIndexOf('_')+1));
			
			if ( null != qualifiers )
				logger.logInfo(nodeDefName + " setExtractConditions() setting qualifiers to: " +
						RowsFilter.reconstructSQLWhereClause(qualifiers));
			
			result.setExtractConditions( qualifiers, projectedColumns, isGaianNode ? null :
					safeExecNodeState.getColumnsMapping( (int[]) arguments.get(GaianTable.QRY_INCOMING_COLUMNS_MAPPING) ) );
			
			return result;
			
		} catch ( SQLException e ) {
			
			// Low value feature to skip maintenance for certain connections
//			String eDigest = Util.getAllExceptionCauses(e);
//			if ( -1 != eDigest.indexOf(GaianTable.REVERSE_CONNECTION_NOT_ESTABLISHED_ERROR) ) {
//				logger.logWarning(nodeDefName + " Query Failure (dropping gaian connection): " + eDigest);
//				lostConnection();
//				return null;
//			}
			
			if ( null == pstmt ) {

//				e.printStackTrace();
				final String iex = Util.getGaiandbInvocationTargetException(e);
				logger.logThreadWarning( GDBMessages.ENGINE_STATEMENT_PREPARE_ERROR_SQL, nodeDefName + " Unable to PREPARE statement - (empty result for this data source): " + 
						e.getMessage() + (null==iex?"":" Root cause: "+iex) + getRdbmsProviderSpecificInfo(e) +
						" - 'call listrdbc()' to identify the data source");

				// Changed condition below... If the connection is null this may just be because we cant obtain one quickly enough -
				// Don't drop the entire gaian connection as a result of this.
//				if ( null == conn || conn.isClosed() ) lostConnection();
				
				if ( null == conn ) {
					// We can't obtain a connection fast enough - disable this rdbms node temporarily to avoid continuous re-tries
					if ( isGaianNode ) {
						String cid = nodeDefName.substring( nodeDefName.lastIndexOf('_') + 1 );
						if ( GaianDBConfig.isDiscoveredConnection(cid) ) return null;
						if ( GaianDBConfig.isDefinedConnection(cid) )
							logger.logThreadWarning(GDBMessages.ENGINE_GATEWAY_UNREACHABLE, nodeDefName + " Unreachable Gateway: " + cid + 
								" (can slow down queries) - to remove, run: call gdisconnect('" + cid + "')");
						else 
							logger.logThreadWarning(GDBMessages.ENGINE_GAIAN_CONN_NOT_REGISTERED, nodeDefName + " Undefined connection id: " + cid + 
									". Disabled data source and associated left over connection were erroneously used (purge could be in process)");
					} else
						logger.logThreadWarning(GDBMessages.ENGINE_DS_RDBMS_UNDREACHABLE, nodeDefName + " Unreachable RDBMS in Data Source: " + nodeDefName + 
								" (can slow down queries) - to remove, run: call removeds('" + nodeDefName + "')");

					// Disable temporarily to save resources
					temporarilyDisable();
					
				} else {
//					if ( null != conn ) {
					if ( conn.isClosed() ) lostConnection();
					else {
						// Connection is still active and Statement is ok - recycle it and return null
						if ( false == recycleSourceHandleToPool( conn ) ) {
							// We didn't recycle this connection (due to pool being maxed out), so close it - 
							// Ignore exceptions (e.g. if the connection reference has already gone)
							try { conn.close(); }
							catch ( SQLException e1 ) {}
						}
					}
				}
				return null;
				
			} else {
				
				// Statement EXECUTION failed - we need to determine whether the connection has been lost...
				// Assume connection is invalid unless proven otherwise
				// Note: statement.getConnection() throws an exception if the connection has been lost
				boolean isConnectionInvalid = true;					
				try { isConnectionInvalid = pstmt.getConnection().isClosed(); }
				catch ( SQLException e1 ) {
					logger.logThreadDetail("Unable to check connection validity with pstmt.getConnection().isClosed() (ignored): " + e1);
					if ( -1 < e1.getMessage().indexOf("Method not supported") ) isConnectionInvalid = false; // we don't know if connection is ok..
				}
				
				if ( isConnectionInvalid ) {
					logger.logThreadWarning(GDBMessages.ENGINE_STATEMENT_EXEC_JDBC_CONN_ERROR, nodeDefName + " Statement execution failed due to lost jdbc connection (returning null): " + e);
					lostConnection();
					return null;
				}
				
				final String iex = Util.getGaiandbInvocationTargetException(e);
				logger.logThreadWarning( GDBMessages.ENGINE_STATEMENT_EXEC_ERROR, nodeDefName + " Unable to EXECUTE statement (returning null result): " + 
						Util.getStackTraceDigest(e) + (null==iex?"":" Root cause: "+iex) );
				
				// Connection is still active and pstmt must be closed (as it failed) - recycle connection and return null
				if ( false == recycleSourceHandleToPool( conn ) ) {
					// We didn't recycle this connection (due to pool being maxed out), so close it - 
					// Ignore exceptions (e.g. if the connection reference has already gone)
					try { conn.close(); }
					catch ( SQLException e1 ) {}
				}
				
				return null;
			}
		} finally {
			if ( !isInMemRows && null != pstmt ) {
				activeConnections.remove( conn ); // note we can't pop in case of concurrent threads!!
				logger.logThreadInfo(nodeDefName + " Active connection removed: " + conn);
			}
		}
	}
	
	private static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
	
	private String getRdbmsProviderSpecificInfo( SQLException e ) {
		if ( null != connectionDetails && connectionDetails.startsWith( DB2_DRIVER ) )
			return Util.getDB2Msg(e, false);
		return "";
	}
	
//	private ResultSet execute( Statement stmt, String sql ) throws SQLException {
//		// For executions against other GaianNodes, use a watchdog to ensure the execution doesn't hang due to network problems...
////		return isGaianNode ? new StatementExecutor(false).execute(stmt, sql) : stmt.executeQuery( sql );
//		return stmt.executeQuery( sql );
//	}
	
	public void lostConnection() {
		
		if ( isGaianNode ) {
			// Let the config know that the connection is lost - if it is a system discovered connection it will be removed 
			// from config and config will be reloaded, thus purging stale connections and vtis.
			// If it is not a system discovered connection, then just clear the stack pool (closing statements/connections within).
			// VTIs based on the connection id will still try to obtain connections for it after the re-enablement time
			String cid = nodeDefName.substring( nodeDefName.lastIndexOf('_') + 1 );
			if ( GaianDBConfig.isDiscoveredConnection(cid) ) {
				GaianNodeSeeker.lostDiscoveredConnection(cid);
				return;
			}
			logger.logThreadWarning(GDBMessages.ENGINE_GATEWAY_UNREACHABLE_CONN_LOST, nodeDefName + " Unreachable gateway: " + cid + 
					" (can slow down queries) - To disable, run: call gdisconnect('" + cid + "')");
		} else
			logger.logThreadWarning(GDBMessages.ENGINE_DS_RDBMS_UNDREACHABLE_CONN_LOST, nodeDefName + " Unreachable RDBMS in data source: " + nodeDefName + 
					" (can slow down queries) - To disable, run: call removeds('" + nodeDefName + "')");
		
		DataSourcesManager.clearSourceHandlesStackPool( connectionDetails );
		temporarilyDisable();
	}
	
	private void temporarilyDisable() {
		temporarilyDisable(DISABLEMENT_PERIOD_MS);
	}
	
	public void temporarilyDisable( int millis ) {
		reenablementTime = System.currentTimeMillis() + millis;
	}
	
	public void reEnableNow() {
		reenablementTime = 0;
	}
	
//	public Connection getConnectionOfLongestRunningStatement() {
//		// We are looking for a potentially broken and hanging connection, but it may be that the connections pool contains 
//		// connections that have been established after the one that was broken... Therefore we target the longest running statement,
//		// knowing that if that one is still active (i.e. a long running query), then all subsequent ones should be active as well.
//		logger.logDetail(nodeDefName + " Concurrent running statements: " + activeConnections.size());
//		try { return (Connection) activeConnections.lastElement(); }
//		catch ( Exception e ) { return null; }
//	}
	
	public Connection getConnectionFromApplicablePool() throws SQLException {
		return getConnection( DataSourcesManager.getSourceHandlesPool( connectionDetails ) );
	}
	
	public void returnConnectionToApplicablePool(Connection c) throws SQLException {
		DataSourcesManager.getSourceHandlesPool( connectionDetails ).push(c);
	}
	
	private Connection getConnection( Stack<Object> sourceHandles ) throws SQLException {
		
		Connection connection = null;
		
		synchronized( sourceHandles ) {
			if ( !sourceHandles.empty() ) connection = (Connection) sourceHandles.pop();
		}
		
		if ( null != connection ) {
			
			logger.logThreadInfo( "Extracted an existing DB Handle from the Stack Pool" );
			
			if ( sourceHandles.empty() ) {
				logger.logThreadInfo( "Getting another one asynchronously as pool is now empty..." );
				DataSourcesManager.getRDBHandleInSeparateThread( connectionDetails, sourceHandles );
			}
			
			try {
				// Check if connection was closed by JDBC for whatever reason and explicitly throw an SQLException if it was.
				if ( connection.isClosed() ) {
					throw new SQLException("Connection closed");
				}
			} catch ( SQLException e ) {
				logger.logInfo(e.getMessage());
				lostConnection();
				throw e;
			}
		
		} else {
			// The pool is originally primed so this code should only be reached when query
			// throughput exceeds the rate at which connections can be created.
			// However - even if we can't get a connection fast enough here, this does not
			// mean the source is unavailable and MUST DEFINITELY NOT trigger a lostConnection()
			// to be sent to the GaianNodeSeeker...
			
			// Don't try to get another connection if the node is disabled temporarily
//			if ( 0 == reenablementTime || reenablementTime < System.currentTimeMillis() ) {
//				reenablementTime = 0;
				logger.logThreadInfo( "Ran out of connections! Trying to get 2 (1 + 1 spare)..." );
				connection = DataSourcesManager.getRDBHandleQuickly( connectionDetails, sourceHandles );
				// And get another one as a spare
				DataSourcesManager.getRDBHandleInSeparateThread( connectionDetails, sourceHandles );
//			}
		}
		
		return connection;
	}
	
	/**
	 * TODO
	 * This method should call out directly to DatabaseConnector.getConnectionWithinTimeoutOrToPoolAsynchronously()
	 * Need to replace the getConnection() code in this class to get pooled Connection objects using the method: getPooledSourceHandle(), which calls this one.
	 */
	@Override
	protected Object getNewSourceHandleWithinTimeoutOrToSourcesPoolAsynchronously() throws Exception {
		return null; // currently not used - we just get connections from the pool using getConnection()
	}
	
	/**
	 * Gets a comma separated String of column name mappings that would be inserted into the select
	 * section of an SQL statement, e.g: "physicalcol1, physicalcol2, ..."
	 * Each column name will either have an explicit mapping to a different physical column name, or it
	 * will have no mapping, in which case the logical column name is used.
	 * The column name may also have lower-case or special characters, in which case it will be wrapped in RDBMS-appropriate delimiters.
	 * 
	 * @param pColNames: Physical column names for each logical column index
	 * @param pColTypes: Physical column types
	 * @param projectedColumns: Array of 1-based indexes of logical table columns involved in the query
	 * @param exposedColCount: 
	 * @return String of column names appropriate for the SELECT list of the SQL query.
	 */
	private String getColumnNamesAsCSV(
			final String[] pColNames, final int[] pColTypes, final int[] projectedColumns, final int exposedColCount ) {

		if ( null == projectedColumns )
			// Only applicable for queries on subqueries which are NOT themselves 'select *'
			return "*";
		
		int colCount = projectedColumns.length;
		// Deal with the count(*) query case - Derby expects us to return an empty row for each record that matches the query
		// By returning "1" here we will pull out a minimal row for every record... 
		// [Note: in future it might be better to translate this to a count(*) instead and return true in GaianChildRSWrapper.nextRow()
		// the number of times given by the result of the count (but this means changing all data sources i.e. FileImport and 
		// InMemoryRows to also cycle through rows differently for a count(*))... remember this must be different for an explain 
		// query where we just use the value of the count(*) as a direct result to be added to the aggregated count.]
		if ( 0 == colCount ) return "1";

		logger.logThreadInfo("Getting columns list to query, columns count = " + colCount + ", pColNames = " + Arrays.asList(pColNames) );		

//		System.out.println("colCount: " + colCount + ", pcolnames: " + Arrays.asList(pColNames));
		
		GaianResultSetMetaData ltrsmd = safeExecNodeState.getLogicalTableRSMD();
		
		StringBuffer csvColsForSelect = new StringBuffer();
		for ( int i=0; i<colCount; i++ ) {

			int colIndex = projectedColumns[i]-1;
			String columnName;
			
			if ( colIndex < pColNames.length ) {
				if ( safeExecNodeState.isColumnMissingInPhysicalSource(colIndex) )
					continue; // skip this column - we wont pull anything out of the db for it. We will replace with null when fetching.
				
				// The column is physical and exists in the physical table, so colNames will hold the correct mapped name.
				columnName = pColNames[ colIndex ]; // Column was mapped to a non-null value, so select this column.
			} else
				// Otherwise, for other logical cols (constant or hidden cols), just use the name in the logical table's meta data.
				columnName = ltrsmd.getColumnName( colIndex+1, exposedColCount );
			
//			String columnName = colIndex < pColNames.length ?
//					pColNames[ colIndex ] : ltrsmd.getColumnName( colIndex+1, exposedColCount );
			
//    		logger.logInfo("Got column name " + columnName + " for colindex " + colIndex + ", isColumnMapped = "
//    				+ safeExecNodeState.isColumnMappingExplicitlyDefinedInConfig(colIndex));
			
			String colXpr = null;
			
			if ( !isGaianNode && !isSubQuery && safeExecNodeState.isColumnMappingExplicitlyDefinedInConfig(colIndex) )
				colXpr = columnName;
			else {
				colXpr = GaianResultSetMetaData.wrapColumnNameForQueryingIfNotAnOrdinaryIdentifier(columnName, rdbmsProvider);
				if (!isGaianNode && null != pColTypes) {
					String colXprNormalised = RowsFilter.applyProviderNormalisationTransformIfNecessary(colXpr, pColTypes[i], rdbmsProvider);
					if ( false == colXpr.equals(colXprNormalised) ) colXpr = colXprNormalised + " " + colXpr; // Use alias to re-assign original col name
				}
			}
			
			csvColsForSelect.append( (0==i?"":", ") + colXpr );
		}
		return csvColsForSelect.toString();
	}
		
	public void tableNeedsRefresh() {
//		isTableNeedsRefresh = true;
	}
	
	GaianChildVTI getAllRows() throws SQLException {
//		return DataSourcesManager.getRDBHandle( connectionDetails ).executeQuery( "select * from " + table );
		logger.logThreadInfo( nodeDefName + " Getting all rows from table: " + physicalTable );

		Stack<Object> sourceHandles = DataSourcesManager.getSourceHandlesPool( connectionDetails );

		Connection c = null;
		
		// Repeat some code from getStatement() as we don't want to impose a time limit on getting it.
		synchronized( sourceHandles ) {
			if ( !sourceHandles.isEmpty() ) {
				c = (Connection) sourceHandles.pop();
				try { if ( c.isClosed() ) throw new SQLException("Connection closed"); }
				catch ( SQLException e ) { lostConnection(); throw e; }
			} else {
				logger.logThreadInfo( nodeDefName + " getAllRows() requesting new RDBMS connection for: " + connectionDetails );
				c = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getConnection();
			}
		}

		return new GaianChildRSWrapper( c.createStatement().executeQuery( "select * from " + physicalTable ) );
	}
	
	public long removeRDBExecTime( GaianChildVTI nodeRows ) {
		Long execTime = (Long)rdbExecTimes.remove( nodeRows );
		if ( null==execTime ) {
			if ( !isRowsInMemory )
				logger.logWarning( GDBMessages.ENGINE_JDBC_EXEC_TIME_UNRECORDED,  nodeDefName + " JDBC Execution time was not recorded for a ResultSet (ignored)" );
			return -1;
		}
		return execTime.longValue();
	}
	
	public boolean isBasedOn( String s ) {
		return s.equals( connectionDetails );
	}
	
	public String getSourceDescription( String dsInstanceID ) {
		if ( null == dsInstanceID ) dsInstanceID = physicalTable;
		return GaianDBConfig.getConnectionTokens(connectionDetails)[1] + ( null==dsInstanceID ? "" : "::" + dsInstanceID );
	}

	public void recycleOrCloseResultWrapper( GaianChildVTI rows ) throws Exception {
		
		logger.logThreadInfo("Entered recycleOrCloseResultWrapper(), rows instanceof InMemoryRows? " + (rows instanceof InMemoryRows));
		
		if ( rows instanceof InMemoryRows ) {
			rows.reinitialise();
			recycleSourceHandleToPool( rows );
			return;
		}
		
		logger.logThreadInfo("rows instanceof IFastPath? " + (rows instanceof IFastPath));
		
		// The distinction between IFastPath and GaianChildVTI is subtle.
		// An IFastPath is an executable statement with easy row access, but its rows are tightly coupled with the statement.
		// A GaianChildVTI is a wrapper for an external data source result - which is easily accessible. The external result may need releasing.
		if ( rows instanceof IFastPath ) { // true for example if rows are a LightPreparedStatement
			recycleSourceHandleToPool( ((Statement) rows).getConnection() );
			return;
		}
		
		Statement s = null;

		logger.logThreadInfo("Checking Statement for cleanup"); // + (s = ((GaianChildRSWrapper) rows).getStatementForCleanup()));

		// Statement will be null if an update count was derived on a callable statement.
		try { if ( null == ( s = ((GaianChildRSWrapper) rows).getStatementForCleanup() ) ) return; }
		catch ( SQLException e ) { return; }
		
		Connection c = s.getConnection();
		rows.close(); // don't close the statement as well as it may be a prepared statement we want to re-use..
		
		if ( c.isClosed() ) {
			logger.logThreadInfo("Connection is closed (it was possibly spawned in a procedure call) - nothing to recycle");
			return;
		}
		
		logger.logThreadInfo("Recycling JDBC Connection associated with ResultSet");
		
		// If this VTIRDBResult is a sub-query, then it's statements will not be cached - so clear this one now to clear down client+server resources
		if ( isSubQuery )
			try { s.close(); logger.logThreadInfo("Closed sub-query statement (because they are not reused)"); }
			catch ( SQLException e ) { logger.logThreadInfo("Unable to close() sub-query statement (ignored), cause: " + e); }
		if ( false == recycleSourceHandleToPool( c ) )
			// We didn't recycle this connection (due to pool being maxed out), so close it - 
			// Ignore exceptions (e.g. if the connection reference has already gone)			
			try { c.close(); logger.logThreadInfo("Closed connection (could not recycle as connection pool was full"); }
			catch ( SQLException e ) { logger.logThreadInfo("Unable to close() connection after finding pool was maxed out (ignored), cause: " + e); }
	}
		

//	public void recycleResult( GaianChildVTI rows ) throws SQLException {
//		
//		boolean isInMemoryRows = rows instanceof InMemoryRows;
//		// Statement will be null if an update count was derived on a callable statement.
//		try { 
//			if ( !isInMemoryRows && !(rows instanceof IFastPath) && null == ((GaianChildRSWrapper) rows).getStatement() )
//				return;
//		} catch ( SQLException e ) { return; }
//		
//		// Get the wrapping resource handle which we will want to recycle after closing the rows.
//		Object rsrc = isInMemoryRows ? (Object) rows : //(Object) ((GaianChildRSWrapper) rows).getStatement().getConnection();
//			(Object) ( (Statement) ( rows instanceof IFastPath ? rows : ((GaianChildRSWrapper) rows).getStatement() ) ).getConnection();
//
//		rows.close();		
//		
//		if ( rsrc instanceof Connection ) {
//			if ( ((Connection) rsrc).isClosed() ) {
//				logger.logThreadInfo("Connection is closed (it was possibly spawned in a procedure call) - nothing to recycle");
//				return;
//			}
//			logger.logThreadInfo("Recycling JDBC Connection associated with ResultSet");
//		}
//					
//		if ( false == genericRecycleResult( rsrc ) && ! isInMemoryRows && !(rows instanceof IFastPath) )
//			// We didn't recycle this connection (due to pool being maxed out), so close it - 
//			// Ignore exceptions (e.g. if the connection reference has already gone)
//			
//			try { Statement s = ((GaianChildRSWrapper) rows).getStatement(); Connection c = s.getConnection(); s.close(); c.close(); }
//			catch ( SQLException e ) {}
//		
//		// Make this transient node available for re-use
////		if ( isSubQuery || nodeDefName.startsWith( DataSourcesManager.GATEWAY_PREFIX ) )
////			DataSourcesManager.recycleTransientNode( nodeDefName, this );
//	}
	
    // Pattern used to guard against WHERE expressions which are nested in GaianQuery VTIs.
	// Limitation: this will match when we wouldn't want it to: ' ) T where (g>'s') group by g
	// So we have to disallow special end clauses in the docs: GROUP|ORDER|FETCH|FOR|OFFSET|WITH
	private static final Pattern patternEndVTIExpression = Pattern.compile("(?i)'\\s*\\)\\s*(?:AS\\s+)?[A-Z]\\w*(?:\\s+.*)?");
	
	public void customReinitialise() throws SQLException {
		
		isGaianNode = false;
		isSubQuery = nodeDefName.startsWith( DataSourcesManager.SUBQUERY_PREFIX );
		activeConnections.clear();
		logger.logThreadDetail(nodeDefName + " Active connections cleared");
		
		String ptDef = GaianDBConfig.getNodeDefRDBMSTable( nodeDefName );
		String vtiClassName = GaianDBConfig.getNodeDefVTI( nodeDefName );
		
		if ( null != ptDef && !isSubQuery ) {
			
			ptDef = ptDef.trim();
			
//			ptDef = ptDef.toUpperCase(); //Sybase is case-sensitive, so leave the table definition as-is.
			
			// Search for WHERE clause in physical table definition
			if ( !ptDef.startsWith("(") ) { // don't look to protect a where-clause if the table expression is defined as a subquery against the physical db
				shorthandTableXprWhereClauseIndex = ptDef.toUpperCase().lastIndexOf(" WHERE ");
				if ( -1 != shorthandTableXprWhereClauseIndex ) {
					// Guard against situations of having a WHERE clause being wrongly detected from inside a sub-query...
					int gqIndex = ptDef.toUpperCase().indexOf("COM.IBM.DB2J.GAIANQUERY");
					int lastQuoteIndex = ptDef.lastIndexOf('\'');
					if ( -1 != gqIndex && gqIndex < shorthandTableXprWhereClauseIndex && lastQuoteIndex > shorthandTableXprWhereClauseIndex &&
						patternEndVTIExpression.matcher( ptDef.substring(lastQuoteIndex) ).matches() )
						shorthandTableXprWhereClauseIndex = -1; // do not try to extract this as a WHERE clause later

					if ( -1 != shorthandTableXprWhereClauseIndex ) {
						StringBuilder buf = new StringBuilder( ptDef );
						buf.insert(shorthandTableXprWhereClauseIndex+7, '(');
						buf.append(')');
						ptDef = buf.toString();
					}
				}
			} else
				shorthandTableXprWhereClauseIndex = -1;
			
			logger.logInfo( nodeDefName + " table def = " + ptDef ); // + ", vticlass def = " + vtiClassName );
			
			// If the table definition has just changed and this source is in memory, then it needs 
			// reloading now - so clear it out so it can be reloaded
			// Note that if isRowsInMemory is false, it would mean the setting has been cleared, 
			// so we'll clear the in-memory rows below anyway.
			if ( !ptDef.equals(physicalTable) && isRowsInMemory && null != inMemoryRows ) {
				logger.logInfo(nodeDefName + " Clearing in-memory rows and indexes for reload as table def has changed" );
				clearInMemoryRowsAndIndexes();
				isRowsInMemory = true; // set the flag again to indicate the rows need reloading
			}
		}
		
		physicalTable = ptDef;
				
		if ( !isSubQuery && null == physicalTable ) {
			
			if ( null == vtiClassName ) {
				// No table or vti definitions. This must be a query to another Derby node.
				// We can't define table yet as we don't know the query id or propagation count
				isGaianNode = true;
				
			} else {
				
				// Deprecated method of accessing VTIs...
				// This is a VTI - accessed through a DB connection
				try {
					if ( Class.forName(vtiClassName).getName().equals( FileImport.class.getName() ) ) {
						
						String vtiArgs = GaianDBConfig.getVTIArguments(nodeDefName);
						if ( null == vtiArgs ) //|| 0 == vtiArgs.length )
							throw new SQLException("Undefined ARGS property for Node");
						physicalTable = "NEW " + vtiClassName  + "('" + vtiArgs + "') AS " + nodeDefName;
					} else {
						logger.logWarning( GDBMessages.ENGINE_VTI_CLASS_UNSUPPORTED, nodeDefName + " Unsupported VTI class: " + vtiClassName );
					}
				} catch ( ClassNotFoundException e ) {
					logger.logWarning( GDBMessages.ENGINE_VTI_CLASS_NOT_FOUND, nodeDefName + " Class not found: " + vtiClassName );
				}
			}
		}
		
		if ( !isGaianNode ) // inMemory option is not applicable to gaian data sources and we want to avoid logging anyway as nodes can connect/disconnect often
			logger.logThreadInfo( nodeDefName + " inMemoryRows is null: " + (null==inMemoryRows) + ", isNodeInMemoryOptionSet: " + isRowsInMemory );
		
		if ( null == inMemoryRows && isRowsInMemory ) {
			
			if ( isGaianNode ) {
				logger.logWarning( GDBMessages.ENGINE_DBMS_ROWS_LOAD_ERROR, "Cannot load DBMS rows in memory for remote GaianNode - ignoring INMEMORY option" );
				isRowsInMemory = false;
				
//			} else if ( !isLocalDerbyTable ) {
//				logger.logWarning( "Cannot load DBMS rows in memory for non Derby database - ignoring INMEMORY option" );
//				isRowsInMemory = false;

			} else if ( isSubQuery ) {
				logger.logWarning( GDBMessages.ENGINE_DBMS_ROWS_LOAD_SUBQ_ERROR, "Cannot load DBMS rows in memory for subqueries - ignoring INMEMORY option" );
				isRowsInMemory = false;
			
			} else {
				
				loadRowsInMemoryAsynchronously();
//				try {
//					loadInMemory();
//				} catch (Exception e) {
//					logger.logWarning( "Unable to load rows in memory: " + e );
//					isRowsInMemory = false;
//					clearInMemoryRowsAndIndexes();
//				}
				
//				// Create triggers
//				Statement stmt = DataSourcesManager.getRDBStatement( connectionDetails );
//				for (int i=0; i<TRIGEVENTS.length; i++) {
//					
//					String trig = TRIGEVENTS[i];
//					String triggerSQL = 
//						"CREATE TRIGGER " + table.toUpperCase() + "_" + trig + " AFTER " + trig + " ON " + table +
//						" FOR EACH STATEMENT MODE DB2SQL CALL GAIANDB_TABLE_CHANGED('" + nodeDefName + "')";
//					
//					logger.logInfo( nodeDefName + " Creating trigger using SQL: " + triggerSQL);					
//					stmt.execute( triggerSQL );
//					logger.logInfo( nodeDefName + " Created trigger: " + table.toUpperCase() + "_" + trig );
//				}
			}
			
		} else if ( !isRowsInMemory && null != inMemoryRows ) {
			
			logger.logInfo( nodeDefName + " Clearing in-memory rows and indexes" );
			clearInMemoryRowsAndIndexes();
			
//			// Drop triggers
//			if ( !isGaianNode && isLocalDerbyTable ) {
//				
//				Statement stmt = DataSourcesManager.getRDBStatement( connectionDetails );
//				for (int i=0; i<TRIGEVENTS.length; i++) {
//					
//					String trig = TRIGEVENTS[i];
//					String triggerSQL = "DROP TRIGGER " + table.toUpperCase() + "_" + trig;
//					logger.logInfo( nodeDefName + " Dropping trigger using SQL: " + triggerSQL);
//					stmt.execute( triggerSQL );
//					logger.logInfo( nodeDefName + " Dropped trigger: " + table.toUpperCase() + "_" + trig );
//				}
//			}
		}

		Stack<Object> sourceHandles = DataSourcesManager.getSourceHandlesPool( connectionDetails, isRowsInMemory );
		
		if ( isRowsInMemory ) {

			if ( sourceHandles.empty() ) {
				sourceHandles.push( new InMemoryRows() );
				logger.logInfo( nodeDefName + " Created initial InMemory rows for Stack Pool");
			} else
				logger.logInfo( nodeDefName + " Initial InMemory rows already exist in Stack Pool");
		}
//		else {
//			// Refresh column mappings, because in-memory rows being loaded asynchronously will rely on them once loaded
//			// and they may not have been set in the safeExecNodeState as this is done in this thread when returning from this method.
		// --->> This is now done as part of setColTypes() for InMemoryRows of RDBMS sources as well as non-InMemory ones.
		// --->> We shdnt be checking the physical data source and refreshing col mappings with it here because the data source 
		// --->> may only become available way after initialisation...
//			Statement s = null;
//			
//			// Repeat some code from getStatement() as we don't want to impose a time limit on getting it.
//			synchronized( sourceHandles ) {
//				if ( !sourceHandles.isEmpty() ) {
//					s = (Statement) sourceHandles.pop();
//					try { if ( s.getConnection().isClosed() ) throw new SQLException("Connection closed"); }
//					catch ( SQLException e ) { lostConnection(); throw e; }
//				} else
//					s = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getStatement();
//			}
//		physicalTableRSMD = 
//			s.getConnection().prepareStatement( "select * from " + table ).getMetaData();
//			
//			if ( sourceHandles.empty() ) {
//				// try to get a statement now, but nevermind if we can't
//				try {
//					
//					sourceHandles.push( getStatement( sourceHandles ) );
//					logger.logInfo( nodeDefName + " Created initial JDBC Connection" );
//				} catch (SQLException e) {
//					logger.logWarning( nodeDefName + " Currently unable to obtain initial Connection: " + e.getMessage());
//				}
//			} else
//				logger.logInfo( nodeDefName + " Initial JDBC Connection already obtained in Stack");
//		}
	}
}
