/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.IQualifyable;
import org.apache.derby.vti.Pushable;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianDBConfigProcedures;
import com.ibm.gaiandb.GaianDBProcedureUtils;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.GaianNodeSeeker;
import com.ibm.gaiandb.GaianResult;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.SecurityManager;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.VTIBasic;
import com.ibm.gaiandb.VTIWrapper;
import com.ibm.gaiandb.apps.SecurityClientAgent;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.policyframework.SQLQueryElements;
import com.ibm.gaiandb.policyframework.SQLQueryFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilterX;

/**
 * @author DavidVyvyan
 */
public class GaianTable extends AbstractVTI implements VTICosting, IQualifyable, Pushable, IFastPath {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "GaianTable", 20 );
	
	// Define some error message strings.
	public static final String IEX_PREFIX  = "***************";
	private static final String IEX_UNDEFINED_LOGICAL_TABLE = IEX_PREFIX + " UNDEFINED LOGICAL TABLE: ";
	private static final String IEX_LOGICAL_TABLE_CONFIG_LOAD_ERROR = IEX_PREFIX + " CONFIG LOAD ERROR FOR LOGICAL TABLE: ";
	private static final String IEX_DISALLOWED_NODE = IEX_PREFIX + " ACCESS CONFIG FOR CLUSTER MEMBERSHIP OR PERMITTED/DENIED HOSTS DISALLOWS QUERIES FROM NODEID: ";
	
	protected final String logicalTableName;
	protected String tableArguments;
	protected String tableDefinition;
	protected String ltSignature;
	
	// This determines whether SQLBlob columns are "zipped", and so need to be unzipped
	// as part of the nextRow operation.
	boolean unzipLobs = false;
	
	// Logical table structure as presented to Derby. Note this is fixed for any given query instantiation of GaianTable().
	// Hence, for propagated queries, there may be as many combinations of this as there are variations of existance 
	// and ordering of its columns...
	// i.e. If nodes A and B propagate table defs: "a int, b char" and "b char, a int" in 2 separate queries to this node,
	// then there will be 2 different logicalTableRSMDs presented to Derby.
	// Likewise, if the logical table definition is modified, its changes will be picked up dynamically in that we map
	// the new columns to the columns that are expected by derby. Note that the names and types of columns expected 
	// by derby are constant for a given propagated query because the query has the expected definition within itself.
	protected GaianResultSetMetaData logicalTableRSMD = null;
	
	protected VTIWrapper[] allLogicalTableVTIs = null, dsWrappers = null;
	
	private GaianResult gaianResult = null;

	// Cache holds 1000 entries before query data is overwritten - corresponds to ~200KB of memory
	static final int QRY_METADATA_CACHE_SIZE = 1000;
	
	// Any unique query can only be executed on one logical table (a join is considered to be multiple diff. unique queries).
	// Therefore, we map a queryID to the min number of steps to reach this node.
	// queryID -> min propagation count to this node
	private static final Map<String, Integer> minPropagationCounts = Collections.synchronizedMap( new CachedHashMap<String, Integer>(QRY_METADATA_CACHE_SIZE) );
	private static final Map<String, Set<String>> querySenders = Collections.synchronizedMap( new CachedHashMap<String, Set<String>>(QRY_METADATA_CACHE_SIZE) );
	private Integer minPropagationCount = null;
	
	// Variables used for fetching rows... (incl 'explain' state variables)
	private boolean scanWasCompleted = false;
	private long reFetchIteration = 0;
	
	// Latest load time for 'this' GaianTable instance of its logicaltableRSMD and its list of data sources.
	// This is checked every time a query is re-executed against it.
	private long latestInstanceConfigLoadTime = -1;
	
	// This variable is only false the first time this Statement has its executeQuery method called.
	// A same instance of this Statement will be called multiple times for example when Derby is executing a JOIN
	private boolean isStatementInitialised = false;
	private boolean isNodeMeetsAccessRestrictions = true;
	private boolean isMetaDataReady = false;
	
	// Row-caching variables - applied for nested joins of VTIs.
	// e.g. GaianQuery( GaianTable, GaianTable ); or select from v (with view v = GaianTable, GaianTable)
	public static final int LOG_FETCH_BATCH_SIZE = 100000;
	private static final int LOG_FETCH_ITERATION_BATCH_SIZE = 1000;
	private long reFetchedRowIndex = 0;
	private ArrayList<DataValueDescriptor[]> cachedResultRows = null;
	// Note this is needed as there is no get() operation on ArrayList that takes a 'long' as argument.
	private Iterator<DataValueDescriptor[]> cachedResultRowsIterator = null;
	
	// Private VTI nested arguments
	private static final String LT_ARG_MAX_DEPTH = "maxDepth";
	private static final String LT_ARG_WITH_PROVENANCE = "with_provenance";
	private static final String LT_ARG_EXPLAIN = "explain";
	
	// Keys to queryDetails collection which needs to be accessible outside this class
	
	// Keys that may be used in VTI table arguments
	public static final String QRY_TIMEOUT = "timeout"; // seconds
	public static final String QRY_HASH = "queryHash"; // original query hash code
	public static final String QRY_WID = "wid"; // workload id
	public static final String QRY_MAX_SOURCE_ROWS = "maxSourceRows";
	public static final String QRY_PATH = "queryPath";
	public static final String ORIGINATING_CLUSTER_IDS = "origClusters";
	
	// Keys used internally
	public static final String QRY_ID = "QRY_ID";
	public static final String QRY_STEPS = "QRY_STEPS";
	public static final String QRY_CREDENTIALS = "QRY_CREDENTIALS";
	public static final String QRY_ORDER_BY_CLAUSE = "ORDER_BY_CLAUSE";
	public static final String QRY_IS_EXPLAIN = "IS_EXPLAIN";
	public static final String QRY_EXPOSED_COLUMNS_COUNT = "EXPOSED_COLUMNS_COUNT";
	public static final String QRY_INCOMING_COLUMNS_MAPPING = "QRY_INCOMING_COLUMNS_MAPPING";
	public static final String QRY_APPLICABLE_ORIGINAL_PREDICATES = "QRY_APPLICABLE_ORIGINAL_PREDICATES";
	
	public static final String QRY_IS_GAIAN_QUERY = "QRY_IS_GAIAN_QUERY";
	
	public static final String PLURALIZED_INSTANCES_PREFIX_TAG = "PLURALIZED_INSTANCES_";
	
	// queryDetails stores the attribute data for the query. 
	// The key is a string representing the name of the attribute and the value is the attribute itself.
	private final ConcurrentMap<String, Object> queryDetails = new ConcurrentHashMap<String, Object>();
	public ConcurrentMap<String, Object> getQueryDetails() { return queryDetails; }
	
	// policyOnMaxDataSourceRows stores the maximum number of rows that should be fetched from each of the
	// query's datasources. 
	// The key is an ID representing the datasource and the value is the maximum number of rows that should be fetched.
	private ConcurrentMap<String, Integer> policyOnMaxDataSourceRows = new ConcurrentHashMap<String, Integer>();
	public int getPolicyOnMaxDataSourceRows( String dsWrapperID, String dsInstanceID ) {
		if ( !policyOnMaxDataSourceRows.containsKey(dsWrapperID + ':' + dsInstanceID) ) return -1; // no policy, so unlimited
		return policyOnMaxDataSourceRows.get(dsWrapperID + ':' + dsInstanceID);
	}
	
	// table + where clause -> latest row count
	private static Map<String, Long> estimatedRowCounts = 
		Collections.synchronizedMap( new CachedHashMap<String, Long>( QRY_METADATA_CACHE_SIZE ) );
		
	// The host:port/db of the node that forwarded the query (from which we received the query).
	// This is null at the node where the query was issued originally.
	protected String forwardingNode = null;
	protected boolean isPropagatedQuery = false;
	
	private String tableAlias = "GQ";
	
	private String originalSQL = null;
	private String queryID = null;
	private int queryPropagationCount = -1;
	private String credentialsStringBlock = null;

	private String derbyContextCurrentUser = null;
	
	// Currently holds user, affiliation and clearance. Ultimately it would be better to have a HashMap with 
	// configurable taxonomy types for user related categories (e..g role etc). These would all be held in a digitally signed certificate.
//	private String[] authenticatedUserFields = null; // DRV - 22/10/2011 - Commented out - Authentication and user fields should now get resolved in the policy plugin
	
	private boolean isMaxDepthArgSet = false;
	private int maxPropagation = -1;
	private Qualifier[][] qualifiers = null;
	private Qualifier[][] physicalQualifiers = null;
	private Qualifier[][] explainFullQualifiers = null;
	
	private int[] projectedColumns = null;
	private int[] physicalProjectedColumns = null;
	private boolean isSelectStar = true;
	
	// Include provenance and explain columns in computed ResultSet -
	// The user may choose or not to select them and apply predicates to them
	protected boolean withProvenance = false;
	protected boolean isExplain = false, isExplainDS = false;
	private char explainPath = DOT_EXPLAIN_UNSET_PATH;
	private BufferedWriter dotFileBW = null;
	private StringBuffer dotGraphText = null;
	private int numLocalDataSourcesInExplainDsMode = 0;
	
	private boolean isLogPerfOn = false;
	private ArrayList<Long> fetchTimes = new ArrayList<Long>(10000);
	private static Object perfFileLock = new Object();
	
	public boolean isLogPerfOn() {
		return isLogPerfOn;
	}
	
	private static final Set<String> GDB_SYSTEM_QUERIES = new HashSet<String>( Arrays.asList( new String[] {
	// Query used to get node name
	"SELECT gdbx_to_node local_node FROM new com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T WHERE gdbx_depth = 0",
	
	// Topology query + forwarded equivalent
	"SELECT DISTINCT           gdbx_from_node source,           gdbx_to_node target      FROM gdb_ltnull_x WHERE gdbx_depth > 0  ORDER BY source, target",
	"SELECT DISTINCT           gdbx_from_node source,           gdbx_to_node target      FROM new com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T WHERE gdbx_depth > 0  ORDER BY source, target",	
	"select GDBX_FROM_NODE, GDBX_TO_NODE, GDBX_DEPTH from NEW com.ibm.db2j.GaianTable('GDB_LTNULL', 'explain', 'CNULL CHAR(1)', '", //L3R3844:6416') GQ WHERE (GDBX_DEPTH>0) AND GDB_QRYID=? AND GDB_QRYSTEPS=?",
	
	// 2 Metrics queries + forwarded equivalents
	"SELECT gdb_node,           jSecs(CURRENT_TIMESTAMP) - age received_timestamp,           name,           CAST(value AS INT) value     FROM new com.ibm.db2j.GaianQuery(		'SELECT name, jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) age, value FROM gdb_local_metrics 		UNION SELECT ''Data Throughput'' name, 0 age, cast(GDB_THROUGHPUT()/1000 as char(20)) value from sysibm.sysdummy1 		UNION SELECT ''Query Activity'' name, 0 age, cast(GDB_QRY_ACTIVITY() as char(20)) value from sysibm.sysdummy1 		UNION SELECT ''Node CPU'' name, 0 age, cast(GDB_NODE_CPU() as char(3)) value from sysibm.sysdummy1 		UNION SELECT ''JVM Used Memory'' name, 0 age, cast(JMEMORYPERCENT() as char(3)) value from sysibm.sysdummy1', 		'with_provenance') Q    WHERE name IN ('Data Throughput', 'Query Activity', 'Node CPU', 'JVM Used Memory', 'CPU Usage', 'Used Memory', 'Total Memory', 'Disk I/O', 'Network I/O', 'Battery Power', 'Temperature')      AND age < ? ORDER BY gdb_node, received_timestamp, name",
	"SELECT gdb_node,           max( jSecs(CURRENT_TIMESTAMP) - age ) received_timestamp,           name,           max( CAST(value AS INT) ) value     FROM new com.ibm.db2j.GaianQuery(		'SELECT name, jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) age, value FROM gdb_local_metrics 		UNION SELECT ''Data Throughput'' name, 0 age, cast(GDB_THROUGHPUT()/1000 as char(20)) value from sysibm.sysdummy1 		UNION SELECT ''Query Activity'' name, 0 age, cast(GDB_QRY_ACTIVITY() as char(20)) value from sysibm.sysdummy1 		UNION SELECT ''Node CPU'' name, 0 age, cast(GDB_NODE_CPU() as char(3)) value from sysibm.sysdummy1 		UNION SELECT ''JVM Used Memory'' name, 0 age, cast(JMEMORYPERCENT() as char(3)) value from sysibm.sysdummy1', 		'with_provenance') Q    WHERE name IN ('", //Data Throughput', 'Query Activity', 'Node CPU', 'JVM Used Memory', 'CPU Usage', 'Used Memory', 'Total Memory', 'Disk I/O', 'Network I/O', 'Battery Power', 'Temperature')      AND age < ? GROUP BY gdb_node, name",
	
	"select NAME, AGE, VALUE, GDB_NODE from NEW com.ibm.db2j.GaianQuery('SELECT name, jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) age, value FROM gdb_local_metrics 		UNION SELECT ''Data Throughput'' name, 0 age, cast(GDB_THROUGHPUT()/1000 as char(20)) value from sysibm.sysdummy1 		UNION SELECT ''Query Activity'' name, 0 age, cast(GDB_QRY_ACTIVITY() as char(20)) value from sysibm.sysdummy1 		UNION SELECT ''Node CPU'' name, 0 age, cast(GDB_NODE_CPU() as char(3)) value from sysibm.sysdummy1 		UNION SELECT ''JVM Used Memory'' name, 0 age, cast(JMEMORYPERCENT() as char(3)) value from sysibm.sysdummy1', 'with_provenance', '', 'NAME VARCHAR(32), AGE BIGINT, VALUE VARCHAR(255)', '", //L3R3844:6415') GQ WHERE (AGE<5) AND GDB_QRYID=? AND GDB_QRYSTEPS=?",
	
	// logTail query + forwarded equivalent + sub-query
	"select GDB_NODE, line, log from new com.ibm.db2j.GaianQuery('select * from ( select row_number() over () line, column1 log from new com.ibm.db2j.GaianTable(''GDB_LTLOG'', ''maxDepth=0'') GT ) SQ', 'with_provenance, order by line desc fetch first", // 100 rows only') GQ  order by gdb_node, line"
	"select LINE, LOG, GDB_NODE from NEW com.ibm.db2j.GaianQuery('select * from ( select row_number() over () line, column1 log from new com.ibm.db2j.GaianTable(''GDB_LTLOG'', ''maxDepth=0'') GT ) SQ', 'with_provenance, order by line desc fetch first", // 100 rows only', '', 'LINE BIGINT, LOG VARCHAR(255)', '", //L3R3844:6416') GQ WHERE GDB_QRYID=? AND GDB_QRYSTEPS=?"
	"select LINE, LOG from (select * from ( select row_number() over () line, column1 log from new com.ibm.db2j.GaianTable('GDB_LTLOG', 'maxDepth=0') GT ) SQ) SUBQ order by line desc fetch first", // 100 rows only",
	
	// GaianConfig getWarnings query
	"select gdb_node, tstamp, warning from new com.ibm.db2j.GaianQuery('select * from new com.ibm.db2j.GaianConfig(''USERWARNING'') GC', 'with_provenance, maxDepth=0') GQ"
	}));

	private boolean isSystemQuery = false; // Is this GaianTable executing a GaianDB System Query ?
	public boolean isSystemQuery() { return isSystemQuery; }
	private boolean testIsSystemQuery( String sql ) {
		for ( String s : GDB_SYSTEM_QUERIES ) { if ( sql.startsWith(s) ) { isSystemQuery = true; break; } }
		return isSystemQuery;
	}
	
	private final static Set<GaianResult> gResults = new HashSet<GaianResult>();
	public static Set<GaianResult> getGresults() { return gResults; }

	private static int queryActivity = 0, dataThroughput = 0;
	
	public static int getDataThroughput() {
		synchronized ( gResults ) {
			int dt = dataThroughput;
			dataThroughput = 0;
			for ( GaianResult gr : gResults )
				if (gr.getQueryTime() == -1){
					// The query is in the process of execution, queryTime is set to the actual elapsed time
					// when the query completes.
					dt += gr.getRowCount() * gr.getEstimatedRecordSize();
				}
			return dt;
		}
	}

	public static int getQueryActivity() {
		synchronized ( gResults ) {
			int qa = queryActivity;
			queryActivity = 0;
			for ( GaianResult gr : gResults )
				qa += gr.getCumulativeDataSourceTimes();
			return qa;
		}
	}
	
	private void startQuery() {
		synchronized ( gResults ) { gResults.add(gaianResult); }
	}
	
	private void endQuery() {
		synchronized ( gResults ) { gResults.remove(gaianResult); }
	}
	
	private void updateQueriesStats() {
		if ( null == gaianResult ) return;
		synchronized ( gResults ) {
			queryActivity += gaianResult.getFinalDataSourceTime();
			dataThroughput += gaianResult.getRowCount() * gaianResult.getEstimatedRecordSize();
		}
	}
	
	// Column-level passthrough.
	// (Note that table-level passthrough, amounting to gateway functionality, works using propagated meta-data.)
//	private boolean passThrough = false;

	private SQLQueryFilter sqlQueryFilter = null;
	private SQLResultFilter sqlResultFilter = null;
	
	public SQLQueryFilter getSQLQueryFilter() {
		return sqlQueryFilter;
	}
	
	public SQLResultFilter getResultFilter() {
		return sqlResultFilter;
	}
	
	public String getForwardingNode() {
		return forwardingNode;
	}
	
	public String getTableAlias() {
		return tableAlias;
	}
	
	// Note: can not usually use the "select *" directly (without mapping columns).
	// The reason is that back end source columns can vary, and so can logical table definitions on other nodes, and so can indeed
	// columns defined for back end sources referenced within subqueries
	// The exception is for queries on subqueries that are NOT themselves a 'select *'
	// e.g. select * from ( select a, b from GT )
	public boolean isSelectStar() {
		return isSelectStar;
	}
	
	public String getLogicalTableName( boolean shortenForLogging ) {
		if ( shortenForLogging && this instanceof GaianQuery )
			return GaianDBConfig.SUBQUERY_LT;
		return logicalTableName;
	}
	
	public String getTableArguments() {
		if ( null==tableArguments ) return "";
		return tableArguments;
	}
	
	public String getTableDefinition() {
		
		if ( null==tableDefinition ) {
			tableDefinition = logicalTableRSMD.getColumnsDefinitionExcludingHiddenOnes();
//			
//			HashSet cols = RowsFilter.getColumnIDsUsedInQualifiers( qualifiers );
//			int[] allProjectedCols = getProjectedColumns();
//			for ( int i=0; i<allProjectedCols.length; i++ )
//				cols.add( new Integer( allProjectedCols[i]-1 ) ); // 1-based
//			colsDefinition = logicalTableRSMD.getDefinitionForVisibleColumnsIn( cols );
		}
		return tableDefinition;
	}
	
//	private StringBuffer sqlWhereClause = null;
//	private int		columnCount = 0;
	private boolean closed = false;
	
	/**
	 * The method is a Table Function implementation hook - most of the conversion from Table Function to the VTI interface is provided by the superclass VTI60.
	 * 
	 * All that is needed beyond the VTI60 code to make a VTI work as a Table Function, is:
	 * 	- To provide a method like this one for every constructor required - only 1 is permitted per table function name for now...(until varargs are introduced in 10.10)
	 * 	- To register the table function with Derby - using a "CREATE FUNCTION ... RETURNS TABLE ..." SQL statement.
	 * 
	 *  Table function names for the different GaianTable constructors are based on the table shape they return.
	 *  No suffix is used for the basic table and it only returns the physical columns.
	 *  Suffix '_' is used for the most complex constructor (4 arguments) and returns all the physical and hidden columns.
	 *  e.g. for logical table LT0:
	 *  	LT0  => example invocation: SELECT * FROM TABLE ( LT0('LT0') ) T
	 *  	LT0_ => example invocation: SELECT gdb_node FROM TABLE ( LT0_('LT0', 'with_provenance', '', null) ) T
	 * 
	 * @param ltable
	 * @return
	 * @throws Exception
	 */
	public static GaianTable queryGaianTable(final String ltable) throws Exception {
		return queryGaianTable(ltable, null, null, null);
	}
	
	public static GaianTable queryGaianTable(
			final String ltable, final String ltargs, final String ltdef, final String fwdingNode) throws Exception {
		logger.logInfo("TABLE FUNCTION queryGaianTable(), LT = " + ltable + ", args: " + ltargs + ", def = " + ltdef + 
				", fwdingNode: " + fwdingNode);
		
		GaianTable gt = new GaianTable(ltable, ltargs, ltdef, fwdingNode);
		
    	try {
	    	ContextManager contextMgr = ((EmbedConnection) GaianDBProcedureUtils.getDefaultDerbyConnection()).getContextManager();
	    	LanguageConnectionContext languageContext = (LanguageConnectionContext) contextMgr.getContext("LanguageConnectionContext");
	    	StatementContext derbyStatementContext = languageContext.getStatementContext();
	    	gt.originalSQL = derbyStatementContext.getStatementText();
			gt.derbyContextCurrentUser = derbyStatementContext.getSQLSessionContext().getCurrentUser();
			
//	    	System.out.println("Context info: session user id: " + languageContext.getSessionUserId()
//			+ ", idname: " + languageContext.getIdName() + ", stmt id name: " + languageContext.getStatementContext().getIdName()
//			+ ", current user: " + languageContext.getStatementContext().getSQLSessionContext().getCurrentUser()
//			+ ", Last Query Tree: " + languageContext.getLastQueryTree());
			
    	} catch ( Exception e ) { logger.logInfo("Unable to resolve Original SQL + Current User from connection context, cause: " + e); }

		return gt;
	}
	
	public GaianTable() throws Exception {
		logInfo("GaianTable() empty constructor - extension of: " + super.getClass().getName());
		logicalTableName = null;
	}
	
	public GaianTable(final String logicalTableName) throws Exception {
		this( logicalTableName, null, null, null);
	}
	
	public GaianTable(final String logicalTableName, final String tableArguments) throws Exception {		
		this( logicalTableName, tableArguments, null, null);
	}

	public GaianTable(final String logicalTableName, final String tableArguments, final String tableDefinition ) throws Exception {
		this( logicalTableName, tableArguments, tableDefinition, null);
	}
	
	public GaianTable(String logicalTableName, final String tableArguments, final String tableDefinition, final String forwardingNode) throws Exception {

		this.forwardingNode = null == forwardingNode || 1 > forwardingNode.trim().length() ? null : forwardingNode.trim();
		isPropagatedQuery = null != forwardingNode;
		
    	// Check access restrictions based on forwardingNode
    	checkAccessRestrictions();
		
		boolean isSubQuery = this instanceof GaianQuery;
		
		logicalTableName = logicalTableName.trim();
		this.logicalTableName = isSubQuery ? logicalTableName : logicalTableName.toUpperCase();
		this.tableArguments = null == tableArguments || 1 > tableArguments.trim().length() ? null : tableArguments.trim();
		this.tableDefinition = null == tableDefinition || 1 > tableDefinition.trim().length() ? null : tableDefinition.trim();
		
		ltSignature = this.logicalTableName + ( this.tableArguments + this.tableDefinition + this.forwardingNode ).replaceAll("\\s", " ");
		
		isSystemQuery = Arrays.asList(Util.splitByCommas(this.tableArguments)).contains(Logger.LOG_EXCLUDE);
		
//		System.out.println("lt: " + logicalTable + ", tabDef: " + tableDefinition + ", resolvedtabDef: " + this.tableDefinition);
		
		if ( null != tableArguments ) {
			this.withProvenance = -1 != tableArguments.toLowerCase().indexOf(LT_ARG_WITH_PROVENANCE);
			this.isExplain = -1 != tableArguments.toLowerCase().indexOf(LT_ARG_EXPLAIN);
			this.isExplainDS = -1 != tableArguments.toLowerCase().indexOf(LT_ARG_EXPLAIN+"ds");
		}
//		this.passThrough = null != tableArguments && -1 != tableArguments.indexOf("passthrough");
//		this.queryID = queryID;
		
//		queryPropagationCount = steps.intValue();
//		maxPropagation = maxSteps.intValue();
		
		// The explain GAIAN_PATH column comes after the provenance cols, so they must be included if it is itself.
		if ( this.isExplain ) this.withProvenance = true;
		
		String ltName = getLogicalTableName(true);

		if ( !DataSourcesManager.isLogicalTableViewsLoading(logicalTableName) ) // don't log if we're reloading views
		logInfo("GaianTable() Constructor:\n========================================================================================================================================================================\n"+
				"New " + this.getClass().getSimpleName() + " VTI Call, table = " + ltName + 
				", explain = " + isExplain + ", withProvenance = " + withProvenance //+ ", passThrough = " + passThrough
				);

		if ( null == tableDefinition && !isSubQuery ) {
			if ( null == GaianDBConfig.getLogicalTableVisibleColumns(this.logicalTableName) ) {
				logger.logWarning(GDBMessages.ENGINE_LT_UNDEFINED, "Queried Logical Table is not defined: " + ltName + " - aborting query");
				throw new Exception(GDBMessages.ENGINE_LT_UNDEFINED + ":" + IEX_UNDEFINED_LOGICAL_TABLE + ltName);
			}
			if ( ! DataSourcesManager.isLogicalTableLoaded(this.logicalTableName) ) {
				logger.logWarning(GDBMessages.ENGINE_LT_LOAD_ERROR, "Queried Logical Table is loading or didn't load properly (see load trace): " + ltName + " - aborting query");
				throw new Exception(GDBMessages.ENGINE_LT_LOAD_ERROR + ":" + IEX_LOGICAL_TABLE_CONFIG_LOAD_ERROR + ltName);
			}
		}
	}
	
	private void setupGaianTableArguments() {
		
		if ( null != tableArguments && 0 < tableArguments.length() ) {
			StringBuffer forwardTableArguments = new StringBuffer(tableArguments);
			// deltaoffset tracks the number of chars by which the table arguments string has changed in the forwardTableArguments
			int deltaoffset = 0;
			
			// If this is a subquery call, only process arguments listed beyond the sourcelist arg 
			// and its colon delimiter
//			int colonsIndex = tableArguments.indexOf("::");
			String[] options = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrSingleQuotes(tableArguments, ','); //.substring(colonsIndex+1) );
			for (int i=0; i<options.length; i++) {
				final String option = options[i];
				final int valIndex = option.indexOf('=')+1;
				final String parmName = 0 < valIndex ? option.substring(0, valIndex-1).trim() : option;
				final String value = 0 < valIndex ? option.substring(valIndex).trim() : null;
				try {
					if ( parmName.equalsIgnoreCase( LT_ARG_MAX_DEPTH ) ) {
						isMaxDepthArgSet = true;
						maxPropagation = new Integer( value );
					}
					else if ( parmName.equalsIgnoreCase( QRY_MAX_SOURCE_ROWS ) )
						queryDetails.put( QRY_MAX_SOURCE_ROWS, new Integer( value ) );
					else if ( parmName.equalsIgnoreCase( QRY_PATH ) ) {
						queryDetails.put( QRY_PATH, value );
						int offset = tableArguments.indexOf(',', tableArguments.indexOf(QRY_PATH));
						if ( -1 == offset ) offset = tableArguments.length();
						String thisNodeString = " "+GaianDBConfig.getGaianNodeID();
						forwardTableArguments.insert(offset+deltaoffset, thisNodeString);
						deltaoffset += thisNodeString.length();
					}
					else if ( !isExplain && option.toLowerCase().startsWith("order by") )
						// Note the pushed "order by" clause is not applicable to an 'explain' because the query is translated into a count(*)
						queryDetails.put( QRY_ORDER_BY_CLAUSE, option );
					else if ( option.toLowerCase().startsWith(LT_ARG_EXPLAIN)) {
						queryDetails.put( QRY_IS_EXPLAIN, "EXPLAIN_ENABLED" );
						String[] elmts = option.split(" ");
						logInfo("Explain option with elements: " + Arrays.asList( elmts ));
						if ( 3 == elmts.length && "in".equalsIgnoreCase(elmts[1]) ) {
							// remove the 'in file' clause from the arguments - as we don't want this to propagate to other nodes
							int startOffset = tableArguments.indexOf(LT_ARG_EXPLAIN)+7;
							int endOffset = tableArguments.indexOf(',', startOffset);
							if ( -1 == endOffset ) endOffset = tableArguments.length();
							forwardTableArguments.delete( startOffset+deltaoffset, endOffset+deltaoffset );
							deltaoffset += endOffset - startOffset;
							
							// Prepare a buffered writer for the file we want to write to
							// Note this overwrites the file for every new 'explain in' query
							try {
								dotFileBW = new BufferedWriter( new FileWriter( elmts[2] ));
								dotGraphText = new StringBuffer();
							} catch (IOException e) {
								logger.logException(GDBMessages.ENGINE_EXPLAIN_FILE_OPEN_ERROR, "Unable to open explain file for writing: ", e);
							}
						}
					}
					else {
						if ( null != value ) {
							// Validate key and extract assigned value
							String key = parmName.toUpperCase();
							logger.logThreadInfo("Getting param for key: " + key);
							if ( key.endsWith( VTIBasic.EXEC_ARG_CUSTOM_VTI_ARGS ) ) { queryDetails.put( key, value ); continue; }
							else if ( key.equalsIgnoreCase( ORIGINATING_CLUSTER_IDS ) )
								{ queryDetails.put( ORIGINATING_CLUSTER_IDS, 2 > value.length() ? "" : /* remove wrapping brackets => */ value.substring(1, value.length()-1) ); continue; }
							throw new SQLException("Unrecognised table argument key: " + key);
						}
						// Check remaining possible unary arguments
						if ( !option.equals(LT_ARG_EXPLAIN) && !option.equals(LT_ARG_WITH_PROVENANCE) && !option.equals(Logger.LOG_EXCLUDE) )
							throw new SQLException("Unrecognised unary table argument: " + option);
					}
				} catch ( Exception e ) {
					logger.logWarning( GDBMessages.ENGINE_GT_SETUP_ARGS_ERROR, "Unable to process argument " + option + ", cause: " + e );
				}
			}
			
			tableArguments = forwardTableArguments.toString();
		
		} else
			// No arguments - make tableArguments null
			tableArguments = null;
		
		if ( null == forwardingNode ) {
			// We are on the originating node
			
			final String originatingClusterIDs = GaianDBConfig.getAccessClusters();
			if ( null != originatingClusterIDs ) queryDetails.put( ORIGINATING_CLUSTER_IDS, originatingClusterIDs ); // needed for policy
			
			final String gdbNodeID = GaianDBConfig.getGaianNodeID();
			// note 'queryDetails' should have QRY_PATH as null when the path has not started...
			
			// Initiate the query path
			tableArguments = (null==tableArguments?"":tableArguments+",") +
				QRY_PATH+"="+gdbNodeID + (null!=originatingClusterIDs ? ","+ORIGINATING_CLUSTER_IDS+"=("+originatingClusterIDs+")" : "");
			
			// Also fwd whether we want to log this query on nodes it is propagated to
			if ( isSystemQuery() ) tableArguments += ","+Logger.LOG_EXCLUDE;
			
			// Query hash (now passed in comment instead so we are sure to propagate it on in all branches of joins of sub-queries too...)
//			tableArguments += ","+QRY_HASH+"="+queryHash;
			// Add GDB_WID if there is one
//			if ( queryDetails.containsKey(QRY_WID) ) tableArguments += ","+QRY_WID+"="+queryDetails.get(QRY_WID);
		}
	}
	
//	private static Statement localDerbyStatement = null;
//	private static void generateLocalDerbyStatement() {
//		
//		boolean isLocalConnectionInvalid = true;
//		
//		if ( null != localDerbyStatement )
//			try { isLocalConnectionInvalid = localDerbyStatement.getConnection().isClosed(); }
//			catch ( SQLException e1 ) {}
//		
//		if ( isLocalConnectionInvalid ) {
//			String cdetails = GaianDBConfig.getLocalDerbyConnectionID();
//			Stack<Object> connectionPool = DataSourcesManager.getSourceHandlesPool( cdetails, false );
//			try {
//				localDerbyStatement = DataSourcesManager.getJDBCConnection( cdetails, connectionPool ).createStatement();
//			} catch (SQLException e) {
//				logger.logThreadWarning("Cannot generate static statement against local supporting Derby DB");
//				localDerbyStatement = null;
//			}
//		}
//	}
	
	private void setupGaianDataSources( boolean isMetaDataLookupOnly ) throws SQLException {
		
		boolean isViewsLoadedForThisLT = !DataSourcesManager.isLogicalTableViewsLoading(logicalTableName);
		
		if ( isViewsLoadedForThisLT ) logInfo("Resolving logical table");
		
		try {
//			boolean configChanged = false;
			synchronized ( DataSourcesManager.class ) {
				
				// Refresh resources
//				if ( true == ( configChanged = GaianDBConfig.refreshRegistryIfNecessary() ) ) {
				if ( GaianDBConfig.refreshRegistryIfNecessary() )
					DataSourcesManager.refresh();

				setupMetaDataAndDataSourcesArray( isMetaDataLookupOnly ); // this is a no-op for subquery meta data lookup on the query's originating node
				latestInstanceConfigLoadTime = DataSourcesManager.getLatestGlobalConfigLoadTime();
//				dsWrappers = allLogicalTableVTIs;
				// take a cloned copy of the md so we have our own thread safe version for this query (and can include/exclude columns freely)
				if ( null != logicalTableRSMD )
					logicalTableRSMD = (GaianResultSetMetaData) logicalTableRSMD.clone();
				
				if ( !isMaxDepthArgSet ) maxPropagation = GaianDBConfig.getMaxPropagation();
				isLogPerfOn = GaianDBConfig.isLogPerformanceOn();
				
				if ( false == isMetaDataLookupOnly ) {
	    			sqlQueryFilter = GaianDBConfig.getSQLQueryFilter();
	    			sqlResultFilter = GaianDBConfig.getSQLResultFilter();
				}
    			
//				if ( false == isMetaDataLookupOnly ) {
//					System.out.println("Provisioning new policy objects");
//					// Note they may exist already on a first repetition of a query - however they should be reset to null after a close().
//	    			sqlQueryFilter = null == sqlQueryFilter ? GaianDBConfig.getSQLQueryFilter() : sqlQueryFilter;
//	    			sqlResultFilter = null == sqlResultFilter ? GaianDBConfig.getSQLResultFilter() : sqlResultFilter;
//				}
			}
			
			// Drop views using a Derby embedded driver connection (to avoid deadlock)
//			if ( configChanged ) {
//				generateLocalDerbyStatement();
//				DataSourcesManager.dropOldLogicalTableViews( localDerbyStatement );
//			}
			
			// refresh views if config has changed - but only if this is a qry exec, as we dont want view compilation
			// to cause a potential secondary reload of the views..
			// (this causes a hanging condition when config is updated during query execution..)
			// The meta-data for a view is obtained from the LT structure - no need for a secondary lookup
			// If this is actually a query on the view then it can't be a meta-data lookup as the meta data lookup is only
			// done when the view is created.., so it must be a 
//			if ( true == configChanged && !isMetaDataLookupOnly )
//				DataSourcesManager.checkUpdateLogicalTableViews(); // must be outside of synchronized block to avoid deadlock
			
		} catch ( Exception e ) {
			logger.logException( GDBMessages.ENGINE_REGISTRY_REFRESH_ERROR, getContext() + "Exception refreshing registry and dsWrappers: ", e );
		}
		
		// We should have a logical table definition - as otherwise an exception would have been raised in the GaianTable constructor.
		// However there is still a possible race condition whereby the config file was saved after the contructor
		// call, which removed the logical table we are dealing with... - create an empty meta data object to handle this case here
		if ( null == logicalTableRSMD ) {
//			logInfo("logicalTableRSMD " + logicalTableRSMD + ", isMetaDataLookupOnly " + isMetaDataLookupOnly + ", dsWrappers: " + dsWrappers);
			logger.logWarning(GDBMessages.ENGINE_LT_RSMD_UNDEFINED, "Undefined Logical Table: " + getLogicalTableName(true) + " - Building empty meta data object");
			try { logicalTableRSMD = new GaianResultSetMetaData(); }
			catch ( Exception e ) { logger.logException(GDBMessages.ENGINE_METADATA_CONSTRUCT_ERROR, "Unable to construct empty GaianResultSetMetaData (aborting query)", e); return; }
		}
		
//		if ( !withProvenance )
//			logicalTableRSMD.excludeTailColumns( GaianDBConfig.PROVENANCE_COLS.length );
		
		// If the query was propagated from another node, then include all the extra columns from the other node's definition
		// as well as the queryid and propcount (these 2 will be removed before we actually start query execution).
		if ( isPropagatedQuery ) 
			logicalTableRSMD.includeNullColumns();
		
		if ( !isExplain )
			logicalTableRSMD.excludeTailColumns( withProvenance ? GaianDBConfig.EXPLAIN_COLS.length
					: GaianDBConfig.EXPLAIN_COLS.length + GaianDBConfig.PROVENANCE_COLS.length );
		
		// Now that we know the number of exposed columns (incl or excl the prov/explain ones), pass the number of 
		// exposed columns through as a query argument as it won't be updated in the VTIWrapper's base meta data object.
		queryDetails.put( QRY_EXPOSED_COLUMNS_COUNT, new Integer(logicalTableRSMD.getExposedColumnCount()) );
		
		if ( isMetaDataLookupOnly ) {
			if ( isViewsLoadedForThisLT ) // don't log if we're reloading views
			logInfo( "Got new meta data for LT def, numCols: " + logicalTableRSMD.getColumnCount() +
				", numExposedCols: " + logicalTableRSMD.getExposedColumnCount() );
		} else
			logInfo("Retrieved node definitions: " + Arrays.asList( allLogicalTableVTIs ));
		
//		latestInstanceConfigLoadTime = latestGlobalConfigLoadTime;
		
		// Show in logs the schema that is returned to Derby (incl all queried cols)
		if ( isViewsLoadedForThisLT ) // don't log if we're reloading views
		logInfo("MetaData " + (isMetaDataLookupOnly?"to be returned":"for rows passed")+ " to Derby: ["
				+ logicalTableRSMD.getColumnsDefinitionForExposedColumns() + "]");
		isMetaDataReady = true;
	}
	
	protected void setupMetaDataAndDataSourcesArray( boolean isMetaDataLookupOnly ) throws Exception {

		if ( !DataSourcesManager.isLogicalTableViewsLoading(logicalTableName) ) // don't log if we're reloading views
		logInfo("Establishing logical table definition and set of data sources" );
		
		String configuredTableDefinition = GaianDBConfig.getLogicalTableVisibleColumns(logicalTableName);
		GaianResultSetMetaData baseLogicalTableRSMD = DataSourcesManager.getLogicalTableRSMD(logicalTableName);
		
		if ( null == tableDefinition ) {
		
			// This is the node where the query originates
			// configuredTableDefinition cannot be null as this is checked for in the GaianTable constructor

			// Note that the table definition is set here... but perhaps it wd be best to not set it at all
			// and instead work out which are the columns involved in the query and only pass a definition for them
			// in the the qry forwarded between nodes... code is already written for this.. see:
			//		- RowsFilter.getColumnIDsUsedInQualifiers( qualifiers )
			//		- GaianResultSetMetaData.getDefinitionForVisibleColumnsIn( colIDs )
			// However it seems best to be strict in saying that 2 nodes should not have conflicting definitions
			// for a table and that if they do then these tables should not be trusted to be semantically the same, 
			// even for columns that match.
			
			tableDefinition = configuredTableDefinition;
			
//			// Exclude qryid and propcount columns (and other cols not defined on this node) as they may have been added
//			// when qries on this logical table propagated through from other nodes previously. 
//			baseLogicalTableRSMD.excludeNullColumns();
			
		} else if ( null == configuredTableDefinition ||
				!baseLogicalTableRSMD.matchupWithTableDefinition( tableDefinition, true ) ) {
			
			// Check if it was the case that the definitions didn't match...
			
			if ( null != configuredTableDefinition ) {
				// We have a propagated definition which does not match the locally defined one for the queried logical table.
				// This means that matching column names don't have the same type defs (for type, width, precision or scale). 
				// The matching ensures deterministic behaviour by avoiding potential differences due to casting or truncation.
				// A slight amount of flexibility is lost in that a subset of nodes exposing a portion of a logical table
				// cannot decide to expose more precise data using wider types for some of the columns.
				// Also this doesn't allow for column renaming.
				// On the other hand, it does allow for column re-positioning and for missing or extra columns.
				
				logger.logWarning( GDBMessages.ENGINE_DEFS_NON_MATCHING_TYPES, getContext() + " Local and propagated definitions for " + logicalTableName + " have non matching types: '" +
						configuredTableDefinition + "' != '" + tableDefinition + "'" );
				logInfo("Local table definition ignored - this node will act as a gateway only");
			}
			
			logInfo( "Node acting as Gateway" );
			
			// This is a gateway node - derive meta data and construct dynamic nodes
			
			// The following is a straight lookup if the meta data was cached.
			logicalTableRSMD = DataSourcesManager.deriveMetaDataFromTableDef( tableDefinition, logicalTableName, withProvenance+""+isExplain );			
			
//			logicalTableRSMD.includeNullColumns();
			
			if ( !isMetaDataLookupOnly )
				allLogicalTableVTIs = DataSourcesManager.constructDynamicDataSources(
						DataSourcesManager.GATEWAY_PREFIX + "_" + logicalTableName, logicalTableRSMD, null );

			return;
		}
//		else
//			baseLogicalTableRSMD.includeNullColumns(); // Match-up succeeded, include extra cols from the fwded def + qryid and propcount cols
		
		// Both table defs exist and their columns were matched up... and unless this is a new incoming query, all matched up cols are visible 
		// in the base meta data.
		// As we are in a synchronized block, we can safely clone the meta data for this query, and it will be independant of other queries' changes.
		// This has no impact on VTIWrappers.
				
		// There was no propagated logical table definition, or it matches the locally defined one.
		// Some of the variables like numExposedColumns may be different for this VTI call (e.g. if 'with_provenance' was specified),
		// so take a cloned copy of the meta data to have an independant version.
		// Note for each new GaianTable() there is a new cloned logicalTableRSMD. This gets disposed of when the GaianTable() instance is garbage collected...
		logicalTableRSMD = baseLogicalTableRSMD; //(GaianResultSetMetaData) baseLogicalTableRSMD.clone(); //DataSourcesManager.getLogicalTableRSMDClone( logicalTable );
				
//		if ( !tableDefinition.equals( configuredTableDefinition ) )
//			logicalTableRSMD.addMissingColumnsAsNulls( tableDefinition );
		
		allLogicalTableVTIs = DataSourcesManager.getDataSources( logicalTableName );
		
//		DataSourcesManager.incrementGaianTablesUsingVTIArray( allLogicalTableVTIs );
	}
	
	private static AtomicLong cacheTableIndex = new AtomicLong(0);
	
    /**
     *  Provide the metadata for the query against the given table. Cloudscape
	 *  calls this method when it compiles the SQL-J statement that uses this
	 *  VTI class.
     *
     *   @return the result set metadata for the query
	 * 
     *   @exception SQLException thrown by JDBC calls
     */
    public GaianResultSetMetaData getMetaData() throws SQLException {
    	
		if (closed) throw new SQLException("getMetaData() failed - GaianTable already closed");
		
		// Only do this the first time getMetaData is called
		// Note isMetaDataReady is set on a new GaianQuery invocation, but it is not "ready" until inclusion of 
		// explain/provenance and null columns has been decided.
		if ( !isMetaDataReady ) setupGaianDataSources(true);
		
		if ( null == grsmd ) {
			grsmd = createMetaData();
		}
		
		return logicalTableRSMD;
    }
    
	/**
	 * 
	 */
	private GaianResultSetMetaData createMetaData() {
		// assign a unique AbstractVTI prefix name for every schema of columns we need to cache - so we create cache tables for each too.
		String tableDef = logicalTableRSMD.getColumnsDefinitionForExposedColumns(); //ExcludingHiddenOnesAndConstantValues();
		
		// Always assign a unique prefix for every query (determining the persistent cache table name - only required in expensive joins)
		setPrefix( cacheTableIndex.incrementAndGet()+"" );
//			setPrefix( new Long( (long) tableDef.hashCode() + Integer.MAX_VALUE ).toString() ); // previous solution - with a shared cache table per result schema
		
		getDefaultVTIProperties().put( PROP_SCHEMA, tableDef + ", CACHEID BIGINT" );
		return logicalTableRSMD;
	}
    
    public GaianResultSetMetaData getTableMetaData() throws SQLException { return getMetaData(); } // Overridden in LiteGaianStatement
    
    public boolean wasQueryAlreadyReceivedFrom( String nodeid ) {
    	if ( !isPropagatedQuery ) return false;
    	
    	synchronized( querySenders ) {
    		return querySenders.get( queryID ).contains( nodeid );
    	}
    }
    
	/**
	 * Returns a ResultSet (an EITResult) to Cloudscape. Cloudscape calls this\
	 * method when it executes the SQL-J statement that uses this VTI class.
	 * 
	 * Instantiate a user-defined ResultSet (EITResult) that is a wrapper for the
	 * DBMS's result set.
	 *
	 * We need a wrapper because we need to be able to explictly
	 * commit the connection at the close
	 * of each result set. .
	 *
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 * @throws StandardException
	 */
	@SuppressWarnings("unchecked")
	private GaianResult executeFastPathQuery() throws SQLException {
		
		if (closed) {
			logger.logWarning( GDBMessages.ENGINE_FAST_PATH_QUERY_GT_CLOSED, getContext() + "executeFastPathQuery(): GaianTable is already closed");
			return null;
		}
		
		if ( !isNodeMeetsAccessRestrictions ) return null; //throw new SQLException("Node Access Restricted for: " + forwardingNode);
		
		boolean isRepeatedExec = isStatementInitialised; //latestInstanceConfigLoadTime > 0;
//		if ( !isRepeatedExec ) setupGaianTableArguments();
		
		if ( false == isStatementInitialised ) {
			
//		if ( latestInstanceConfigLoadTime < DataSourcesManager.getLatestGlobalConfigLoadTime() ) {
			// Logical table def and/or data sources need setting (or updating) 
			
			isStatementInitialised = true;
			logInfo( /*(isRepeatedExec ? "Re-" : "" ) + */"Initialising query: Getting table def and data sources");
			logger.logDetail("Original SQL: " + originalSQL); // Printed later on already
			
			setupGaianTableArguments();
			setupGaianDataSources(false);
			
			// Eliminate the qryid and propcount (and all NULL columns that only exist on other nodes for this table def) from the logical 
			// table meta data as we don't need these anymore (and don't want them as projected cols) now that Derby got the (fake) meta data from us.
			// When rows are later fetched, the DataValueDescriptors in these columns will just remain NULL.
			logicalTableRSMD.excludeNullColumns();
		}
		
		// Do this for ALL invocations, even query re-executions - so the plugin always has a choice to abort the query early.
		if ( null == sqlResultFilter ) sqlResultFilter = GaianDBConfig.getSQLResultFilter();
		if ( null != sqlResultFilter ) {
			if ( false == sqlResultFilter.setLogicalTable(getLogicalTableName(false), logicalTableRSMD) ) {
				logInfo("Policy plugin rejects query against this logical table expression (returning null): " + getLogicalTableName(false));
				return null;
			}
			if ( false == sqlResultFilter.setForwardingNode(forwardingNode) ) {
				logInfo("Policy plugin rejects query from this forwarding node (returning null): " + forwardingNode);
				return null;
			}

			if ( sqlResultFilter instanceof SQLResultFilterX ) {
				
				String[] boolOpIDs = new String[] {
						SQLResultFilterX.OP_ID_SET_FORWARDING_PATH_RETURN_IS_QUERY_ALLOWED,
						SQLResultFilterX.OP_ID_SET_ACCESS_CLUSTERS_RETURN_IS_QUERY_ALLOWED,
						SQLResultFilterX.OP_ID_SET_AUTHENTICATED_DERBY_USER_RETURN_IS_QUERY_ALLOWED,
				};
				
				Object[] boolOpArgs = new Object[] {
						Arrays.asList( Util.splitByTrimmedDelimiter((String)queryDetails.get(QRY_PATH), ' ') ),
						Arrays.asList( Util.splitByCommas((String)queryDetails.get(ORIGINATING_CLUSTER_IDS)) ),
						derbyContextCurrentUser,
				};
				
				for ( int i=0; i<boolOpIDs.length; i++ ) {
					
					String opID = boolOpIDs[i]; Object args = boolOpArgs[i];
					
					try {
						Boolean isQueryAllowed = (Boolean) ((SQLResultFilterX) sqlResultFilter).executeOperation( opID, args );
						
						if ( null == isQueryAllowed ) {
							logInfo( "Policy Plugin returned null for opID: " + opID + " - query will proceed");

						} else if ( false == isQueryAllowed.booleanValue() ) {
							logInfo("Policy plugin rejects query (returning null) for opID: " + opID + ", args: " + args);
							return null;
						}
					} catch ( ClassCastException e ) {
						logger.logWarning( GDBMessages.ENGINE_POLICY_PLUGIN_INVALID_OPERATION_RETURN_TYPE,
							"Policy Plugin returned invalid return type for opID: " + opID + " - expected Boolean");
					}
				}
			}
		}
    	
		if ( ! isPropagatedQuery ) {
			
			int commentIndex = null == originalSQL ? -1 : originalSQL.lastIndexOf("--");
			String marker = SecurityManager.CREDENTIALS_LABEL + "=";
			int credArgIndex = -1 == commentIndex ? -1 : originalSQL.indexOf(marker, commentIndex);
			
			if ( -1 != credArgIndex ) {
				// The credentials were supplied by the client, and are already encrypted
				logInfo("Got credentials string block from sql hint");
				credentialsStringBlock = originalSQL.substring(credArgIndex + marker.length());
				originalSQL = originalSQL.substring(0, credArgIndex);
			} else {
				// Get credentials from config file - this would imply user info is held at the node level (rather than client app user)
				// Not supported anymore
				
				if ( !GaianNode.IS_SECURITY_EXCLUDED_FROM_RELEASE ) {
				
					SecurityClientAgent securityAgent = GaianNode.getSecurityClientAgent();		
					if ( securityAgent.isSecurityCredentialsSpecified() ) {
						securityAgent.refreshPublicKeysFromServers();
						try {
							credentialsStringBlock = securityAgent.getEncryptedCredentialsValueInBase64(originalSQL);
						} catch (Exception e) {
							logger.logWarning(GDBMessages.ENGINE_COMPUTE_CREDENTIALS_ERROR, "Unable to compute credentials block from GaianDB properties (ignored): " + e);
						}
					}
				}
			}
		
		// If this is a propagated query (i.e. forwardingNode is set), get the query id and propagation count out of the predicates now
		} else { // if ( isPropagatedQuery ) {
			
			// If this is a lite node, then the query id, propagation count and credentials string will be set on this GaianTable from the udp driver.
			if ( ! GaianNode.isLite() ) {
				// The first 2 null columns contain the query id and prop count. The qualifiers will have these 2 columns indexed 
				// relative to the 'real' columns exposed for this GaianTable() instance, which may or may not include the 
				// provenance/explain columns.
	    		int arrayIndexOfFirstNullColumn = logicalTableRSMD.getExposedColumnCount(); // relative to numbering from 0
				int[] targettedColumnIndices = { arrayIndexOfFirstNullColumn+1, arrayIndexOfFirstNullColumn+2, arrayIndexOfFirstNullColumn+3 };
				DataValueDescriptor[] orderableValuesOut = new DataValueDescriptor[3];
	//			logInfo("Factoring out queryid and propagation count, column indices: " + Util.intArrayAsString(targettedColumnIndices));
				qualifiers = RowsFilter.factorOutColumnPredicatesCollectingOrderables( qualifiers, targettedColumnIndices, orderableValuesOut );
				physicalQualifiers = qualifiers;
				try {
					queryID = orderableValuesOut[0].getString();
					queryPropagationCount = orderableValuesOut[1].getInt();
					if ( null != orderableValuesOut[2] )
						credentialsStringBlock = orderableValuesOut[2].getString();
	//				joinedQueryID.queryID = queryID;
				} catch ( StandardException e ) {
					String errmsg = "Could not get String or int designating query id or propagation count from DataValueDescriptor: " + e;
					logger.logThreadWarning(GDBMessages.ENGINE_DATA_VALUE_DESCRIPTER_ERROR, "DERBY ERROR: " + errmsg);
					throw new SQLException( errmsg );
				}
			}
			
			logInfo("Propagated queryID: " + queryID + ", queryPropagationCount: " + queryPropagationCount);
			
			synchronized( querySenders ) {
				Set<String> s = querySenders.get( queryID );
				if ( null == s ) {
					s = new HashSet<String>();
					querySenders.put(queryID, s);
				}
//				s.add( forwardingNode );
				String[] visitedNodes = Util.splitByTrimmedDelimiter((String)queryDetails.get(QRY_PATH), ' ');
				logInfo("Recording visited nodes to avoid crosstalk: " + Arrays.asList(visitedNodes));
				for ( String node : visitedNodes ) s.add(node);
			}
		}
		
		// DRV - 22/10/2011 - Don't authenticate in GaianDB engine anymore... we just pass a byte array to the plugin further down..
//		// Try to authenticate user on this node from credentials block if there is one.
//		if ( null != credentialsStringBlock )
//			authenticatedUserFields = SecurityManager.verifyCredentials( credentialsStringBlock );
//		
//		logInfo("Authenticated User Fields: " +
//				(null == authenticatedUserFields ? null : Arrays.asList(authenticatedUserFields)) );
		
		
		if ( null != sqlResultFilter ) {
			
			// DRV - 22/10/2011 - Factored out use of authenticatedUserFields - User should be authenticated in plugin
//			// authenticatedUserFields can be null - this will be authorised if no blocking policies are active
//			if ( !sqlResultFilter.setAuthenticatedUserCredentials( authenticatedUserFields ) ) {
//				logger.logWarning("Unauthorised access to node/table by user (exiting): " + 
//						(null==authenticatedUserFields?null:authenticatedUserFields[0]) );
			
			if ( !sqlResultFilter.setUserCredentials( credentialsStringBlock ) ) {
				logger.logAlways("Policy plugin setUserCredentials() failed - check plugin diags (exiting query)");
				return null;
			}
		}
		
		
//		logInfo("Checking for loops, applying node qualifiers and setting explain cols");
		
//		// Get credentials column value
//		int arrayIndexOfFirstNullColumn = logicalTableRSMD.getExposedColumnCount();
//		int[] targettedColumns = { arrayIndexOfFirstNullColumn+3 };
//		DataValueDescriptor[] orderableValues = new DataValueDescriptor[1];
////		logInfo("Factoring out queryid and propagation count, column indices: " + Util.intArrayAsString(targettedColumnIndices));
//		qualifiers = RowsFilter.factorOutColumnPredicatesCollectingOrderables( qualifiers, targettedColumns, orderableValues );
				
		// Establish new query id or check current minimum propagation for this query
		// If loop detected, set dummy meta data for meta data call only
		// When query execution takes place, don't set meta data - just return immediately.
		
		establishQueryIdAndFindMinPropagation(); //isMetaDataLookupOnly );
		
//		logInfo("Query id has minPropagation " + minPropagationCount + ", current queryPropagation is " + queryPropagationCount);
				
    	if ( isRepeatedExec ) {
    		reFetchIteration = 0; // Rows from a previous run on this GaianTable instance are not to be re-fetched 
    		
    		try {    			
//    	    	!! only do this if the md has been changed - i.e. if config has been reloaded...
    			long latestGlobalConfigLoadTime = DataSourcesManager.getLatestGlobalConfigLoadTime();

				logInfo("Re-executing query, logical table needs reloading: " + (latestInstanceConfigLoadTime < latestGlobalConfigLoadTime) );
    			if ( latestInstanceConfigLoadTime < latestGlobalConfigLoadTime ) {

    				// Derby still expects the RSMD to be ltrsmdExpectedByDerby - so we might have to map new RSMD columns into it for 
    				// repeated executions for the lifetime of this GaianTable.
	    			GaianResultSetMetaData ltrsmdExpectedByDerby = logicalTableRSMD;
	    			
    				synchronized( DataSourcesManager.class ) {
	    				// Reload anything that may have changed: lt meta data, data sources, max prop, sql and row filters
    					setupMetaDataAndDataSourcesArray(false);
						if ( !isMaxDepthArgSet ) maxPropagation = GaianDBConfig.getMaxPropagation();
						isLogPerfOn = GaianDBConfig.isLogPerformanceOn();
//						if ( false == isLogPerfOn && null != perfFileBW ) { perfFileBW.close(); perfFileBW = null; }
		    			if ( null == sqlQueryFilter ) sqlQueryFilter = GaianDBConfig.getSQLQueryFilter();
		    			if ( null == sqlResultFilter ) sqlResultFilter = GaianDBConfig.getSQLResultFilter();
    				}
    				
					if ( ltrsmdExpectedByDerby != logicalTableRSMD ) {
						// Logical table rsmd
						int[] newColsMapping = logicalTableRSMD.derivePhysicalColumnsMapping(ltrsmdExpectedByDerby);
						logInfo("Derived logical columns mapping for changed def: " + Util.intArrayAsString(newColsMapping));
						queryDetails.put( QRY_INCOMING_COLUMNS_MAPPING, newColsMapping );
						logicalTableRSMD = ltrsmdExpectedByDerby;
					}
					logInfo("Retrieved new set of data sources: " + Arrays.asList( allLogicalTableVTIs ));
					
					latestInstanceConfigLoadTime = latestGlobalConfigLoadTime;
	    		}
				
			} catch (Exception e) {
				logger.logException( GDBMessages.ENGINE_CONFIG_RELOAD_ERROR, getContext() + " Cannot reload config: ", e );
			}
    	}
    	
    	String prefix = Logger.sdf.format(new Date(System.currentTimeMillis())) + " -------> ";
		
    	if ( 0 == queryPropagationCount ) {
			
			if ( Logger.LOG_LESS <= Logger.logLevel )
				logger.logImportant( getContext() + "executeFastPathQuery():" + Logger.HORIZONTAL_RULE +
					"\n" + prefix + "NEW INCOMING" + (isRepeatedExec?" RE-EXECUTED":"") + " SQL QUERY: " + originalSQL + "\n\n");
    	} else {
    		
    		// If the query has been here before *AND* ( there is no depth limit *OR* the query previously got here in fewer steps )
    		if ( null != minPropagationCount && ( 0 > maxPropagation || queryPropagationCount >= minPropagationCount.intValue() ) ) {
				// Loop detected - and we got there in no fewer steps than the quickest route...
				explainPath = DOT_EXPLAIN_LONGER_PATH;
    			if ( Logger.LOG_LESS <= Logger.logLevel )
    				logger.logImportant( getContext() + 
	        			"executeFastPathQuery():\n" +
    					"----------------------------------------------------------------------------" +
    					"\n" + prefix + "REJECTING LOOPED SQL QUERY FROM " + forwardingNode + " (MIN STEPS=" + 
						minPropagationCount + "): " + originalSQL + "\n" +
    					"----------------------------------------------------------------------------" +
    					"\n" );
	        	return null;
    		}
    		if ( Logger.LOG_LESS <= Logger.logLevel )
    			if ( isRepeatedExec )
    				logger.logImportant( getContext() + 
    					"executeFastPathQuery():\n" +
//    					"_____________________________________________________________________________________________________________________________________________________________________________" +
    					"-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------" +
    					"\n" + prefix + "RE-PROCESSING FORWARDED SQL QUERY FROM " + forwardingNode + ": " + originalSQL + "\n" );
    			else
    				logger.logImportant( getContext() + 
    	        		"executeFastPathQuery():\n" +
//    					"_____________________________________________________________________________________________________________________________________________________________________________" +
    					"-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------" +
    					"\n" + prefix + "PROCESSING FORWARDED SQL QUERY FROM " + forwardingNode + ": " + originalSQL + "\n" ); // "+(isStatementInitialised?"RE-":"")+"
    	}
    	
    	// Query has not looped - prepare to process it -
    	
    	dsWrappers = allLogicalTableVTIs;
    	
		if ( null != dsWrappers ) {
			
			// Prune off badly loaded data sources - see DataSourcesManager.reloadLogicalTable
			List<Integer> indexesToPrune = new ArrayList<Integer>();
			for ( int i=0; i<dsWrappers.length; i++ ) if ( null == dsWrappers[i] ) indexesToPrune.add(i);
			
			if ( !indexesToPrune.isEmpty() ) {
				logger.logWarning(GDBMessages.ENGINE_PRUNING_INDEXES, "Pruning " + indexesToPrune.size() + " data sources of logical table " +
						getLogicalTableName(true) + " as they failed to load properly (see earlier logs)");
				List<VTIWrapper> prunedResult = new ArrayList<VTIWrapper>( Arrays.asList(dsWrappers) );
				for ( int i : indexesToPrune ) prunedResult.remove(i);
				dsWrappers = prunedResult.toArray( new VTIWrapper[0] );
			}
			
			if ( null != forwardingNode ) {
				
				// First check if this node *is* the forwarding node and prune local sources if it is.
				// This is a special case where the aplication does not want to query local sources.
				if ( forwardingNode.equals(GaianDBConfig.getGaianNodeID()) ) {
					logInfo("Application specified forwarding node as local node, so prunning local data sources");
					pruneLeafNodes();
				}
				
				// Prune off any instance of the forwarding node from the set of dsWrappers to execute the query against.	
				ArrayList<VTIWrapper> prunedResult = new ArrayList<VTIWrapper>();
				for ( VTIWrapper dsWrapper : dsWrappers ) {
					
					// Skip this node if it is the one that sent this query...
					String nodeDefName = dsWrapper.getNodeDefName();
					if ( dsWrapper.isGaianNode() &&
							GaianDBConfig.getGaianNodeID(nodeDefName).equals(forwardingNode) ) {
						// Low value feature to skip maintenance for certain connections
//						DatabaseConnectionsChecker.excludeConnectionFromNextMaintenanceCycle(
//								nodeDefName.substring(nodeDefName.lastIndexOf('_')+1));
						continue;
					}
					prunedResult.add( dsWrapper );
				}
				dsWrappers = (VTIWrapper[]) prunedResult.toArray( new VTIWrapper[0] );
			}
			
			// Now prune nodes as appropriate if the query has already visited this node (in more steps)
			// or reached the max allowed depth.
			testPropagationDepthAndPruneNodesAccordingly();
		}
		
		if ( null == dsWrappers || 0 == dsWrappers.length ) {
			logInfo("No nodes to execute query on after initialisation, returning empty ResultSet");
			dsWrappers = null; return null;
		}
		
		// If a list of projected cols was specified, derive which of the projected cols actually reference the physical data source columns
		// Do this for every query execution as it may change due to qualifier columns being variable and implicated in projected cols...
		setupProjectedColumns();
		
		if ( null != sqlQueryFilter ) {
			SQLQueryElements queryElmts = new SQLQueryElements(qualifiers, projectedColumns);
			if ( !sqlQueryFilter.applyIncomingSQLFilter(queryID, logicalTableName, logicalTableRSMD, originalSQL, queryElmts) ) {
				logInfo("Query cancelled by SQLQueryFilter policy in: " + sqlQueryFilter.getClass().getName());
				return null;
			}
			
			qualifiers = queryElmts.getQualifiers();
			projectedColumns = queryElmts.getProjectedColumns();
		}
		
		setupPhysicalProjectedColumns();
		
		// Fill in explain constants for this node to test any qualifiers on them up front - except on the count as
		// we don't know it yet
		if ( isExplain ) {
			
	    	// Remove predicates on column GDBX_COUNT as these must not be pushed through the network
	    	if ( !isPropagatedQuery ) {
	    		explainFullQualifiers = qualifiers;
	    		int arrayIndexOfExplainRowCountColumn = logicalTableRSMD.getExposedColumnCount() - 1;
	    		qualifiers = RowsFilter.factorOutColumnPredicates( qualifiers, arrayIndexOfExplainRowCountColumn );
	    		physicalQualifiers = qualifiers;
	    	}
			
			logInfo("Setting template explain columns to test predicates on them");
			try {
				logicalTableRSMD.setExplainTemplateColumns(
						null==forwardingNode ? "<QUERY>" : forwardingNode,
						GaianDBConfig.getGaianNodeID(),
						queryPropagationCount, explainPath );

				logInfo("DVD row template with added EXPLAIN cols: " + Arrays.asList( logicalTableRSMD.getRowTemplate() ) );

			} catch (StandardException e) {
				logger.logException( GDBMessages.ENGINE_EXPLAIN_COLUMN_SET_ERROR, "Unable to set explain column: ", e );
			}
		}
		
		// Massage the qualifiers a little, converting all the orderable constants into the LT column types
		// of the columns they are compared against.
    	RowsFilter.morphQualifierOrderablesIntoLTTypes( qualifiers, logicalTableRSMD );
    	
    	if ( /*!passThrough &&*/ !testNodeQualifiers() ) {
			logInfo("Branch qualifiers disqualified query on this node, pruning leaf nodes and propagating...");
			pruneLeafNodes();
//			dsWrappers = null; return null;
		}
		
		if ( Logger.LOG_LESS < Logger.logLevel ) {
//			GaianResultSetMetaData ltrsmd = DataSourcesManager.getLogicalTableRSMD( logicalTable );
			logInfo("Physical Qualifiers after branch prune: " + RowsFilter.reconstructSQLWhereClause(physicalQualifiers, logicalTableRSMD));
		}
		
		for ( VTIWrapper dsWrapper : dsWrappers )
			if ( dsWrapper.isPluralized() ) {
				
				final String dsWrapperID = dsWrapper.getNodeDefName();
				Stack<String> pluralizedInstances = new Stack<String>();
				String[] dsInstances = dsWrapper.getPluralizedInstances();
//				logInfo("Pluralized instances for this query for " + dsWrapperID + ": " + Arrays.asList(dsInstances));
				if ( null != dsInstances ) pluralizedInstances.addAll( Arrays.asList( dsInstances ) );
				queryDetails.put(PLURALIZED_INSTANCES_PREFIX_TAG + dsWrapperID, pluralizedInstances );
				
				// Initialise optional endpoint constants - needs to be done before testLeafQualifiers()
				if ( dsWrapper.supportsEndpointConstants() && null != dsInstances && 0 < dsInstances.length ) {
				
					// map values are 1-based
					int[] endpointConstantColMappings = GaianDBConfig.getDataSourceWrapperPluralizedEndpointConstantColMappings( dsWrapperID );
					if ( null != endpointConstantColMappings && 0 < endpointConstantColMappings.length ) {
						if ( null == endpointConstantsMappingToLTCols ) endpointConstantsMappingToLTCols = new HashMap<String, int[]>();
						endpointConstantsMappingToLTCols.put( dsWrapperID, endpointConstantColMappings );
						logInfo("Resolved logical table columns map for end-point constants: " + Util.intArrayAsString(endpointConstantColMappings));
						
						// Set other endpoint constants as identified by the DS wrapper.
						if ( null == endpointConstantsPerInstance ) endpointConstantsPerInstance = new HashMap<String, DataValueDescriptor[]>();
						for ( String dsInstanceID : dsInstances )
							endpointConstantsPerInstance.put( dsWrapperID + dsInstanceID, dsWrapper.getPluralizedInstanceConstants( dsInstanceID ) );
					}
				}
			}
		
		if ( 0 < dsWrappers.length ) {
		
			// If leaf qualifiers remove all nodes, return null ResultSet.
			if ( !testLeafQualifiers() ) {
				logInfo("Leaf qualifiers for query disqualified all sources on this node, returning null");
				dsWrappers = null; return null;
			}
			
			if ( Logger.LOG_LESS < Logger.logLevel ) {
//				GaianResultSetMetaData ltrsmd = DataSourcesManager.getLogicalTableRSMD( logicalTable );
				logInfo("Physical Qualifiers after leaf prune: " + RowsFilter.reconstructSQLWhereClause(physicalQualifiers, logicalTableRSMD));
			}
		}
		
		// Apply policy on which data sources may be queried, and determine the max number of rows to extract from each.
		if ( null != sqlResultFilter && 0 < dsWrappers.length ) {
			ArrayList<VTIWrapper> prunedResult = new ArrayList<VTIWrapper>();
			int maxRowsToExtract;
			for ( VTIWrapper dsWrapper : dsWrappers ) {
				
				String dsNodeName = dsWrapper.getNodeDefName();
				String[] dsInstanceIDs = false == dsWrapper.isPluralized() ? new String[] {null} :
						((Stack<String>) queryDetails.get(PLURALIZED_INSTANCES_PREFIX_TAG + dsNodeName)).toArray( new String[0] );
				
				boolean isAtLeastOneDSInstanceAllowed = false;
				
				for ( String dsInstanceID : dsInstanceIDs ) {
					
					String dsDescription = dsWrapper.getSourceDescription( dsInstanceID );
					
					maxRowsToExtract = sqlResultFilter instanceof SQLResultFilterX ?
							((SQLResultFilterX) sqlResultFilter).nextQueriedDataSource(dsNodeName, dsDescription, dsWrapper.getColumnsMappingCurrent()) :
							sqlResultFilter.nextQueriedDataSource(dsDescription, dsWrapper.getColumnsMappingCurrent());
					
					if ( 0 == maxRowsToExtract ) {
						logInfo("Policy excludes data source " + dsNodeName + ": " + dsDescription);
						continue;
					}
					isAtLeastOneDSInstanceAllowed = true;
					logInfo("Policy data source extraction limit for " + dsNodeName + ": " + dsDescription + ": " +
							(0>maxRowsToExtract ? "unlimited" : maxRowsToExtract + " rows") );
					policyOnMaxDataSourceRows.put(dsNodeName + ':' + dsInstanceID, maxRowsToExtract);
				}
				if ( isAtLeastOneDSInstanceAllowed ) prunedResult.add(dsWrapper);
			}
			dsWrappers = prunedResult.toArray( new VTIWrapper[0] );
		}
		
		if ( null == dsWrappers || 0 == dsWrappers.length ) {
			logInfo("No nodes to execute query on, returning empty ResultSet");
			return null;
		}
		
		if ( isExplainDS ) {
			// eliminate (and count) remaining local data sources (leaf nodes) as we just want the count of them
			numLocalDataSourcesInExplainDsMode = pruneLeafNodes();
		}
		
		logInfo("Remaining nodes after pruning: " + Arrays.asList( dsWrappers ));
		
		// We now know we have some data sources to run the qry against
		if ( null != sqlResultFilter ) sqlResultFilter.setQueriedColumns(getProjectedColumns());
		
		try {
			if ( null == gaianResult ) gaianResult = new GaianResult( this, dsWrappers );
			else gaianResult.reExecute( dsWrappers );
		} catch (Exception e) {
			logger.logException(GDBMessages.ENGINE_GAIAN_RESULT_ERROR, getContext() + "Exception in GaianResult initialisation: ", e);
		}
		return gaianResult;
	}

	/**
	 * Explicitly closes this PreparedStatement class. (Note: Cloudscape
	 * calls this method only after compiling a SELECT statement that uses
	 * this class.)<p>
	 *
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() throws SQLException {

		logInfo("Entering GaianTable.close()" + (closed?" - already closed":""));
		if ( closed ) return;
		
//		if (closed) {
//	        throw new SQLException("close - GaianTable already closed");
//			logger.logWarning( getContext() + " close - GaianTable already closed" );
//		}
		
		super.close();
		clearResultAndInMemoryCache();
		if ( null != endpointConstantsMappingToLTCols ) { endpointConstantsMappingToLTCols.clear(); endpointConstantsMappingToLTCols = null; }
		if ( null != endpointConstantsPerInstance ) { endpointConstantsPerInstance.clear(); endpointConstantsPerInstance = null; }
		endQuery();
		gaianResult = null;
		if ( 0 != cacheTableIndex.longValue() && isCached() ) dropCacheTableAndDeleteExpiryEntry();
//		if ( 0 != cacheTableIndex.longValue() ) dropCacheTableAndDeleteExpiryEntry();		

		closed = true;
		
//		logInfo("Exiting GaianTable.close()");
	}
	
	public boolean isBeforeFirst() {
		return null != gaianResult && 0 == gaianResult.getRowCount();
	}
	
	private void clearResultAndInMemoryCache() throws SQLException {
		
		logInfo("Entered clearResultAndInMemoryCache(), gaianResult isNull? " + (null==gaianResult) +
				(1<reFetchIteration ? ". Final completed Re-Fetch iterations: " + reFetchIteration : "") );
		closeCacheStatementsAndReleaseConnections();
		
		// Only called on the executing GaianTable (not the compile time one)
		if ( null != gaianResult ) gaianResult.close();
		
		if ( null != cachedResultRows ) {
			int size = cachedResultRows.size();
			logInfo("Clearing cachedResultRows: " + size);
			cachedResultRows.clear();
			cachedResultRows.trimToSize();
			cachedResultRows = null;
//			GaianNode.notifyArrayElementsCleared(size); // No need to do this for 100 rows in memory - System.gc() can cause negative effects
		}
		
		// Invalidate cache + re-initialise entirely - this allows us to know we are passed the RE-FETCH state
		isPersistentCacheInitialised = false;
		reFetchIteration = 0;
	}

	/**
	 * Return the number of columns in the user-specified table.
	 *
	 * @exception SQLException	Thrown if there is an error getting the
	 *							metadata.
	 */
	public int getColumnCount() throws SQLException {
		
		logInfo("!!!!!!!!!!!!GaianTable.getColumnCount()!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		if (closed) {
	        throw new SQLException("getColumnCount() failed - GaianTable already closed");
		}

//		if (columnCount == 0) {
//			DataSourcesManager.getLogicalTableRSMD( logicalTable );
//		}
		return logicalTableRSMD.getColumnCount(); //DataSourcesManager.getLogicalTableRSMD( logicalTable ).getColumnCount();
	}
	
	
	private String getContext() {
		return (isSystemQuery?Logger.LOG_EXCLUDE:"")+(null==logicalTableName?"-":getLogicalTableName(true)) +
			" queryID="+queryID + " steps=" + queryPropagationCount + " ";
	}
	
	protected void logInfo( String s ) {
		if ( Logger.LOG_MORE <= Logger.logLevel ) {
			logger.logInfo( getContext() + s ); //, Logger.LOG_MORE );
		}
	}
	
	/**
	 * This method is called when this Gaian node has already been visited by a query, but this
	 * this time we got there in fewer steps.
	 * 
	 * In this case we only propagate the query to the next nodes, without running it on local
	 * sources... This will allow the query to reach MAX_PROPAGATION_COUNT steps down every gaian
	 * node connection of the network.
	 * @throws SQLException
	 */	
	private int pruneLeafNodes() { return pruneNodesOrLeaves( false ); }
	private int pruneGaianNodes() { return pruneNodesOrLeaves( true ); }
	private int pruneNodesOrLeaves( boolean nodesOtherwiseLeaves ) {
		
		// Check if there is at least one type of data source (node or leaf) that we don't want.
		// If there is, then allocate a new prunedResult array and only put the data source types we want in it.
		int numPruned = 0;
		for (int j=0; j<dsWrappers.length; j++)
			if ( !nodesOtherwiseLeaves ^ dsWrappers[j].isGaianNode() ) {
				ArrayList<VTIWrapper> prunedResult = new ArrayList<VTIWrapper>();
				for (int i=0; i<dsWrappers.length; i++)
					if ( nodesOtherwiseLeaves ^ dsWrappers[i].isGaianNode() )
						prunedResult.add( dsWrappers[i] );
					else
						numPruned++;
				
				dsWrappers = prunedResult.toArray( new VTIWrapper[0] );
				break;
			}
		return numPruned;
	}
	
	/**
	 * Setup a query id for an incoming query if this is a new query, and record min steps as 0 to avoid loopbacks.
	 * 
	 * If this is not a new query, find out if there is a min count of steps by which is arrived at this node previously.
	 * 
	 * If there isn't one, or if the current number of steps is lower than that number then update that number to the current
	 * number of steps, which is the new lowest number used for reaching this node.
	 * 
	 * @param isMetaDataLookupOnly
	 */
	private void establishQueryIdAndFindMinPropagation() { // boolean isMetaDataLookupOnly ) {
		
		// Note this method is only called at exec time
		if ( null == queryID ) {
//			// No need for a query ID at compile time... only when we actually execute the query.
//			if ( isMetaDataLookupOnly ) return;
			
			// This is the first node to receive the query. Create a new ID and sequence number
			logInfo("Creating a new queryID, and associated propagationCount=0 for this Query" );
			queryID = GaianDBConfig.generateUniqueQueryID();
			if ( null == queryID )
				logger.logWarning( GDBMessages.ENGINE_UNIQUE_QUERY_ID_ERROR, getContext() + " Unable to getUniqueQueryID" );
			queryPropagationCount = 0;
			
			minPropagationCounts.put( queryID, new Integer(0) );
			return;
		}
		
		// We are dealing with a forwarded query.. check the number of steps and update on first invocation
		synchronized( queryID.intern() ) {
			
			// Get the min number of steps taken by this query to get to this Gaiandb/Derby instance node.
			minPropagationCount = (Integer) minPropagationCounts.get( queryID );
			
			// Now we must check/update the min steps count.
			// Note - we can't update the steps count as soon as meta-data lookup time because the execute invocation 
			// cannot differentiate between its own associated meta-data lookup and someone else's (i.e. coming fom another route)
//			if ( !isMetaDataLookupOnly )
				// Record minimum steps to get to this node - this allows us to deal with loops
				if ( null == minPropagationCount || queryPropagationCount < minPropagationCount.intValue() )
		    		minPropagationCounts.put( queryID, new Integer( queryPropagationCount ) );
		}
	}
	
	/**
	 * If this is a new query, assigns a new query id and number of steps to it.
	 * If not, this method checks whether this node was reached before, meaning a loop 
	 * is detected.
	 * 
	 * This method should not be called if the node has just been reached in more steps 
	 * than previously, as it means the loop is a long path and the calling code should have 
	 * returned a null ResultSet already.
	 * 
	 * If the node has just been reached in fewer steps that previously, the query is allowed
	 * to propagate, but not to be executed locally.
	 * 
	 * If the query has reached maxPropagation and has not visited this node before, then
	 * the query is executed against local sources, but not propagated further.
	 *  
	 * @throws Exception
	 */
	private void testPropagationDepthAndPruneNodesAccordingly() {
		
		// Prevent loops from occuring
		if ( null == minPropagationCount ) {
			
			explainPath = DOT_EXPLAIN_FIRST_PATH;
									
			if ( -1 < maxPropagation && queryPropagationCount >= maxPropagation ) {

				logInfo("Propagation count >= max allowed by network: " + 
						queryPropagationCount + ">=" + maxPropagation + 
						", so cannot propagate query further");
				logInfo("Prunning Gaian nodes from sources to query");
				// We have reached the last node that we should recurse from, so prune off the recursive links.
				pruneGaianNodes();
			}
			
		} else {
			
			// This node has been visited before by this query
			logInfo("Loop detected: This node has already been visited by this query");
			
			if ( queryPropagationCount < minPropagationCount.intValue() ) {
				
				explainPath = DOT_EXPLAIN_SHORTER_PATH;
				
				logInfo("Propagation count < previous one: " + queryPropagationCount + " < " + minPropagationCount);
				logInfo("The query should be propagated but not executed locally");
				// Allow propagation of the query as we have got to this node in less steps than
				// anyone else. However, prune off the local statements as these are or have already been executed.
//				minPropagationCount = new Integer( queryPropagationCount ); //minPropagationCount.intValue() + 1 );
				logInfo("Prunning local back-end and VTI sources from sources to query");
				pruneLeafNodes();
				
//			} else {
//				// Loop detected - and we got there in no fewer steps than the quickest route...
//				if ( explain ) explainPath = DOT_EXPLAIN_LONGER_PATH;
//				logInfo("Propagation count >= previous one: " + queryPropagationCount + " >= " + minPropagationCount);
//				logInfo("Returning null ResultSet...");
//				return false;
			}
		}
	}
	
	/**
	 * Verifies that qualifiers involving special columns that apply to the whole Logical Table are met.
	 * If they are not met, then return false so the query will not be executed against local sources on this node.
     * Otherwise, just remove these qualifiers from the main qualifiers[][] structure.
	 *
	 * @return false if the special constant columns for this Logical Table cause the qualifiers' condition 
	 * to fail, otherwise true, and the constant columns conditions will be factored out of the physicalQualifiers' 
	 * structure.
	 * @throws SQLException
	 */
	private boolean testNodeQualifiers() throws SQLException {
		
		// Check if there are any qualifiers at all
		if ( null == qualifiers ) return true;
		
		try {
			physicalQualifiers = RowsFilter.testAndPruneQualifiers( logicalTableRSMD.getRowTemplate(), qualifiers, true );
		} catch (Exception e) {
			logger.logException( GDBMessages.ENGINE_NODE_QUALIFIERS_TEST_ERROR, getContext(), e );
			physicalQualifiers = null;
		}
		
		// Check if qualifiers were not met, if so return false
		if ( null == physicalQualifiers ) return false;
		
		if ( 0 == physicalQualifiers.length )
			physicalQualifiers = null; // No more qualifiers, as they have all been tested now
		
		return true;
	}
	
	/**
	 * Prunes off nodes that do not satisfy leaf-level qualifiers.
	 * e.g. GAIAN_LEAF is a column tested by a leaf (or physical) node qualifier as its value is different
	 * for each child-node. So if a condition in the qualifiers disqualifies a node then we should
	 * prune it off before querying rows from it...
	 * 
	 * @return false if no child-nodes satisfy leaf-level qualifiers, otherwise true, and the
	 * leaf column conditions will be factored out of the physicalQualifiers' structure. Nodes that don't
	 * satify leaf-qualifiers are pruned from the set of nodes to run the query against.
	 */
	@SuppressWarnings("unchecked")
	private boolean testLeafQualifiers() {
		
		if ( null == physicalQualifiers || !withProvenance ) return true; // No qualifiers to test against.
		
		final DataValueDescriptor[] rowTemplate = logicalTableRSMD.getRowTemplate();
		final int exposedColCount = logicalTableRSMD.getExposedColumnCount();
				
//		GaianResultSetMetaData ltrsmd = DataSourcesManager.getLogicalTableRSMD( logicalTable );
		ArrayList<VTIWrapper> prunedResult = new ArrayList<VTIWrapper>();
		Qualifier[][] qs = null;
		Qualifier[][] resultingQualifiers = physicalQualifiers;
		
		// Find the index of the GDB_LEAF column, default => not in the template
		int arrayIndexOfGdbLeafColumn = -1;
		// Do not go beyond the number of exposed columns, where the rest are NULL columns (not defined for this LT yet)
		for ( int i = logicalTableRSMD.getPhysicalColumnCount()-1; i < exposedColCount; i++ )
			if ( GaianDBConfig.GDB_LEAF.equalsIgnoreCase( logicalTableRSMD.getColumnName(i+1) ) ) { arrayIndexOfGdbLeafColumn = i; break; }
		
		for ( VTIWrapper dsWrapper : dsWrappers ) {
			
			String dsWrapperID = dsWrapper.getNodeDefName();
			
			// Note - removed passThrough condition as  leaf qualifiers should never prevent
			// a query from being propagated to connected Gaian Nodes (these do not have leaf constants to test by definition against anyway)
			if ( /*passThrough && */ dsWrapper.isGaianNode() ) {
				// Do not test this node - we pass the query through branch nodes regardless of qualifiers.
				prunedResult.add( dsWrapper );
				continue;
			}
			
			List<String> dsInstanceIDs = false == dsWrapper.isPluralized() ? Arrays.asList( new String[] {null} ) :
				(Stack<String>) queryDetails.get(PLURALIZED_INSTANCES_PREFIX_TAG + dsWrapperID);
			
			logInfo("Testing leaf qualifiers for source: " + dsWrapperID + ", instances: " + dsInstanceIDs );
			
			for ( Iterator<String> iter = dsInstanceIDs.iterator(); iter.hasNext(); ) {
				
				String dsInstanceID = iter.next();
				
				// Set the leaf column value
				if ( -1 != arrayIndexOfGdbLeafColumn )
					try { rowTemplate[ arrayIndexOfGdbLeafColumn ].setValue( dsWrapper.getSourceDescription( dsInstanceID ) ); }
					catch (StandardException e) {
						logger.logException( GDBMessages.ENGINE_LEAF_QUALIFIERS_TEST_ERROR, getContext() + ": Error setting GDB_LEAF", e );
						return false;
					}
				
				// Set other endpoint constants as identified by the DS wrapper.
				if ( null != dsInstanceID )
					setConstantEndpointColumnValues( rowTemplate, dsWrapper, dsInstanceID );
				
				// Test leaf qualifiers, now set for this node in dvdr
				try {
					// Prune off qualifiers which apply to the constant leaf columns
					// Only prune if no child node has been found to pass the qualifiers yet
					// Note that leaf qualifiers must be included until all node pruning has been completed - so we still use the full 'physicalQualifiers'
					qs = RowsFilter.testAndPruneQualifiers( rowTemplate, physicalQualifiers, prunedResult.isEmpty() );
					
				} catch (Exception e) { logger.logException( GDBMessages.ENGINE_QUALIFIERS_PRUNE_ERROR, getContext(), e ); return false; }
				
				logInfo("Tested leaf qualifiers for source instance: " + dsInstanceID + ", passed? " + (null!=qs) );
				
				// Check if qualifiers were not met, if so prune data source instance
				if ( null == qs ) { iter.remove(); continue; }
				
				if ( false == prunedResult.contains( dsWrapper ) ) {
				
					// Qualifiers were met, so at least one data source instance passes them - so we have a final pruned qualifiers structure
					// Record the pruned qualifiers - only do this once (when the prunedResult is still empty)
					if ( prunedResult.isEmpty() ) {
						// ...modify the physical qualifiers now, as we know there will be a valid data source instance to keep and run the query against
						if ( 0 == qs.length )	resultingQualifiers = null;
						else					resultingQualifiers = qs;
					}
					
					// Keep this node - as qualifiers contain conditions that still need testing against its physical columns.
					prunedResult.add( dsWrapper );
				}
			}
		}
		
		// Now we can update the actual end qualifiers.
		physicalQualifiers = resultingQualifiers;
		
		// Unset the leaf column values
		// Do not go beyond the number of exposed columns, where the rest are NULL columns (not defined for this LT yet)
		for (int j=logicalTableRSMD.getPhysicalColumnCount()-1; j<exposedColCount; j++)
			if ( GaianDBConfig.GDB_LEAF.equalsIgnoreCase( logicalTableRSMD.getColumnName(j+1) ) ) {
				rowTemplate[j].restoreToNull();
				break;
			}
		
		dsWrappers = (VTIWrapper[]) prunedResult.toArray( new VTIWrapper[0] );

		return 0 < dsWrappers.length;
	}
	
	
	private Map<String, int[]> endpointConstantsMappingToLTCols = null; // dsWrapper id => int[] of LT indexes where constants should go (lt indices are 1-based)
	private Map<String, DataValueDescriptor[]> endpointConstantsPerInstance = null; // dsWrapper id + dsInstance id => DVD[] of constant endpoint values
	
	public void setConstantEndpointColumnValues( final DataValueDescriptor[] dvdr, final VTIWrapper dsWrapper, final String dsInstanceID ) {
		
		// Only check possible 'null' for endpointConstantsPerInstance value. If this is NOT null, then the mapping value cannot be null...
		if ( false == dsWrapper.supportsEndpointConstants() || null == endpointConstantsPerInstance ) return;
		
		final String dsWrapperID = dsWrapper.getNodeDefName();
		final DataValueDescriptor[] endpointConstants = endpointConstantsPerInstance.get( dsWrapperID + dsInstanceID );
		if ( null == endpointConstants ) return;
		
		final int[] endpointConstantsMappingsForThisDS = endpointConstantsMappingToLTCols.get( dsWrapperID );
		
		logger.logThreadDetail("Setting endpoint constants for " + dsWrapper + ": " + Arrays.asList(endpointConstants) +
				" with mapping to lt cols: " + Util.intArrayAsString(endpointConstantsMappingsForThisDS));
		
		final int ltPhysicalColCount = logicalTableRSMD.getPhysicalColumnCount();
		for ( int i=0; i<endpointConstants.length && i<endpointConstantsMappingsForThisDS.length; i++ ) {
			int ltColIdx = endpointConstantsMappingsForThisDS[i] - 1; // map values are 1-based
			try { if ( -1 < ltColIdx && ltPhysicalColCount > ltColIdx ) dvdr[ ltColIdx ].setValue( endpointConstants[i] ); }
			catch (StandardException e) {
				logger.logException( GDBMessages.ENGINE_LEAF_QUALIFIERS_TEST_ERROR,
						getContext() + ": Error setting rowTemplate for " + getLogicalTableName(true) + ", ltColIdx: " + (ltColIdx+1) +
						", from end-point constant column index: " + (i+1) + " (skipped)", e );
			}
		}
	}

	public String getQueryID() {
		return queryID;
	}
	
	public int getQueryPropagationCount() {
		return queryPropagationCount;
	}
	
	public String getEncodedCredentials() {
		return credentialsStringBlock;
	}
	
	public int[] getProjectedColumns() {
//    	logInfo("getProjectedColumns returning " + Util.intArrayAsString(projectedColumns));		
		return projectedColumns;
	}
	
	public int[] getPhysicalProjectedColumns() throws SQLException {
//    	logInfo("getPhysicalProjectedColumns returning " + Util.intArrayAsString(physicalProjectedColumns));
		return physicalProjectedColumns;
	}
	
	public Qualifier[][] getPhysicalQualifiers() {
		return physicalQualifiers;
	}
	public Qualifier[][] getQualifiers() {
		return qualifiers;
	}

	private SQLWarning warnings = null; // new SQLWarning("Dummy GAIANDB Warning"); // NOTE: This dummy warning breaks queries against Table Functions!! - use null!
	public void setWarnings( SQLWarning warnings ) { this.warnings = warnings; }
	@Override public SQLWarning getWarnings() throws SQLException { return warnings; }
    
    private void checkAccessRestrictions() throws SQLException {
    	
		if ( null != forwardingNode && 0 != GaianNode.getPID() ) { // pid tells us if node was initialised properly - to avoid config lookups when running ztests (shallow parsing regex tests)
			
//			logInfo("Checking access restrictions for " + forwardingNode + ", from: " + Util.getStackTraceDigest(4, -1));
			
			if ( false == ( isNodeMeetsAccessRestrictions = GaianNodeSeeker.isNodeMeetsAccessRestrictions(forwardingNode) ) ) {
				String errmsg = GDBMessages.ENGINE_DISALLOWED_NODE_ERROR + ":" + IEX_DISALLOWED_NODE + forwardingNode;
				logInfo("Rejecting query from " + forwardingNode + ": " + errmsg);
				// Throw a Derby EmbedSQLException (a straight SQLException is not compatible)
//				throw PublicAPI.wrapStandardException( StandardException.newException(errmsg) );

//				At the moment the only way to actually make Derby send an exception back to the
//				calling node so it stops trying to execute queries against us and clears out its connection
//				to us is to throw a SQLException rather which is incompatible with its expected EmbedSQLException 
//				from setQualifiers or pushProjection or executeAsFastPath.
				close(); // must close() as this may be a re-executed GaianTable with cached data, and we are throwing an exception right after (so derby will never call close() itself).
				throw new SQLException( errmsg );
			}
			
//			// Low value feature to skip maintenance for certain connections
////		if ( GaianNodeSeeker.isMasterNodeAndReverseConnectionIsMissing(forwardingNode) )
////			throw new Exception(REVERSE_CONNECTION_NOT_ESTABLISHED_ERROR);
		}
    }
        
    private static final Pattern fromPattern =     Pattern.compile("(?is)['\"\\s]FROM[\"\\s]"); // there may be a single or double quote before the 'for' token, and a double one after.
    private static final Pattern wherePattern =    Pattern.compile("(?is)[\"\\s]WHERE['\"\\s]"); // there may be a single or double quote AFTER the 'where' token, and a double one BEFORE. 
//    private static final Pattern vtiStartPattern = Pattern.compile("(?is)NEW[\\s]+COM.IBM.DB2J.GAIAN.*");
    
    // Note flags expression (?is) means: CASE_INSENSITIVE and DOTALL, i.e. ignore case and match new line characters with dots ('.')
    private static final Pattern vtiPattern = Pattern.compile("(?is)NEW[\\s]+COM\\.IBM\\.DB2J\\.GAIAN.*\\)[\"\\s]*[\\w]+[\"\\s]*"); // match start and end of vti expression

    // Ignoring case and new line chars, match anything followed by: a single quote, optional whitespace, a closing bracket, optional whitepace, optionally the "AS" token and whitespace,
    // an alias composed of a letter then any number of word characters, and then either optional whitespace and a comma ',' or whitespace and the token 'JOIN' followed 
    // by whitespace and any chars after that.
    private static final Pattern vtiEndJoinedPattern = Pattern.compile("(?is).*'[\\s]*\\)[\\s]*(?:AS[\\s]+)?[a-zA-Z][\\w]*(?:[\\s]*,|[\\s]+JOIN[\\s]).*");

    // Ignoring case, match any optional lt view suffix and an optional closing double quote ("), followed optionally by an expression 
    // containing optionally whitespace and the "AS" token and then whitespace and an alias composed of a letter or double quote (") then 
    // any number of word characters then optionally a double quote (").
    private static final Pattern logicalTableEndViewPattern =
    	Pattern.compile("(?is)(?:_0|_1|_p|_x|_xf)?(?:\")?(?:(?:[\\s]+AS)?[\\s]+[\"a-zA-Z][\\w]*(?:\")?)?");
       
    // Ignoring case and new lines, match a recorded quote, closing bracket or space char (one of which will replace the entire matched expression at the end), followed 
    // by any number of space chars and then a non-recorded ending token (i.e. GROUP|ORDER...), and finally at least one white space char and anything after that.
    private static final Pattern endWherePattern = Pattern.compile("(?is)(['\"\\)\\s])[\\s]*(?:GROUP|ORDER|FETCH|FOR|OFFSET|WITH)[\\s]+.*");
    private static final Pattern oneOrMoreWhiteSpaceOrAClosingBracketPattern = Pattern.compile("[\\s]+|\\)");
    
    private boolean isPossibleInnerTableOfJoin = false;
    
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IQualifyable#setQualifiers(org.apache.derby.vti.VTIEnvironment, org.apache.derby.iapi.store.access.Qualifier[][])
	 */
	public void setQualifiers(VTIEnvironment vtiEnvironment, Qualifier[][] qualifiersPassedInByDerby) throws SQLException {
		reinitForReExecutions();
    	
		// Use this "official" value if it was not derived via Table Function invocation
		if ( null == originalSQL ) originalSQL = vtiEnvironment.getOriginalSQL().trim();
		
    	// Detect if this query is potentially against the INNER table of a JOIN - by looking at the stack trace...
    	
		// USEFUL HACK: Use call stack information to determine if Derby is calling us in the context of a JOIN
		// This will tell us whether we need to cache returned records...
		String callStackInfo = Util.getStackTraceDigest(-1, -1); // (4, -1)
//		System.out.println("Stack trace for " + originalSQL + ":\n" + Util.getStackTraceDigest(-1, -1) );

		if ( -1 != callStackInfo.indexOf( " JoinResultSet.openRight:" ) || // .startsWith( "JoinResultSet.openRight:" )
		     -1 != callStackInfo.indexOf( " OnceResultSet.openCore:" ) ) { // a OnceResultSet is a single value for a SELECT column value - ok to cache
			if ( -1 != callStackInfo.indexOf( " OnceResultSet.openCore:" ) )
				 logInfo("Detected a OnceResultSet in SQL query (result will be cached): " + callStackInfo);
			else logInfo("Call stack indicates that LT is accessed as inner table of join (result will be cached): " + callStackInfo);
			
			isPossibleInnerTableOfJoin = true;
		}
    	
    	// Record internal state for test purposes...
    	if ( GaianNode.isInTestMode() && originalSQL.matches(".* -- testcache[0-9]+$") )
    		GaianDBConfigProcedures.internalDiags.put(originalSQL.substring(originalSQL.lastIndexOf(' ')+1), cacheTableIndex.longValue()+"");
    	
    	testIsSystemQuery(originalSQL);
    	
//    	logInfo("\n\nORIGNAL SQL: " + originalSQL + "\n");

    	// Take a deep copy of the passed in Qualifier[][] structure - in case of re-execution with different predicates.
		try { this.qualifiers = RowsFilter.getQualifiersDeepCopyWithColumnsMapped( qualifiersPassedInByDerby, null ); }
		catch ( Exception e ) { logger.logException( GDBMessages.ENGINE_QUALIFIERS_SETUP_ERROR, "Unable to copy Derby Qualifier[][] structure", e ); }

		this.physicalQualifiers = this.qualifiers;
		
		logInfo("Qualifiers: " + RowsFilter.reconstructSQLWhereClause( this.qualifiers, logicalTableRSMD ));
		
//		joinedQueryID = (QueryID) vtiEnvironment.getSharedState("queryID");
//		if ( null == joinedQueryID ) {
//			joinedQueryID = new QueryID();
//			vtiEnvironment.setSharedState("queryID", joinedQueryID );
//		}
//		else
//			logInfo("Shared Join Query ID: " + joinedQueryID.queryID);
    	
    	
		// Also extract the predicates from the original SQL if this is not a JOIN and there is just one FROM and one WHERE token.
		// This allows predicates containing functions to be pushed around to the nodes hosting the sources (even if not quite to the sources themselves)
		// Only get these predicates if there is only one FROM token and one WHERE token and if this is a query against a 
    	// plain GaianXX VTI (with no join against another table) or a query against one of the managed logical table views. Also retain any alias used.
    	
		if ( !GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianNode.isNetworkDriverGDB() )
			return; // Don't try and do any clever predicate push downs over the UDP driver... (as it may be talking to a LiteDriver on the server side which handles simple predicates) 
		
    	String[] sqls1 = fromPattern.split( originalSQL ), sqls2 = null;
    	if ( 2 == sqls1.length && 2 == ( sqls2 = wherePattern.split( sqls1[1] ) ).length ) {

    		String fromSQL = sqls2[0];
    		String sqlAfterWhereToken = sqls2[1];
    		
    		// We need to put any potential single or double quotes back into the FROM and WHERE expressions: so that we can detect an alias and "where expression" properly.
    		int indexOfCharAfterWhereToken = originalSQL.lastIndexOf( sqlAfterWhereToken ) - 1; // the delimiter char that we removed (a white space or quote)
    		char c = originalSQL.charAt( indexOfCharAfterWhereToken );
    		if ( '\'' == c || '"' == c ) sqlAfterWhereToken = c + sqlAfterWhereToken;
    		c = originalSQL.charAt( indexOfCharAfterWhereToken - "WHERE".length() - 1 ); // the delimiter char before the where token that we removed
    		if ( '"' == c ) fromSQL = fromSQL + c; // note this char cannot be a single quote. It must be a double quote or whitespace
    		
    		fromSQL = fromSQL.trim();
    		sqlAfterWhereToken = sqlAfterWhereToken.trim();
    		
    		int ltNameLen = logicalTableName.length();
    		
    		if ( (vtiPattern.matcher(fromSQL).matches() &&	!vtiEndJoinedPattern.matcher(fromSQL).matches()) || // avoid joins after VTI instantiation and alias
    				fromSQL.length() >= ltNameLen &&
    				fromSQL.substring(0, ltNameLen).toUpperCase().equals(logicalTableName) && // if not a VTI instance, only accept specific managed views for the logical table
    				logicalTableEndViewPattern.matcher( fromSQL.substring(ltNameLen) ).matches() ) {
//    				0 > fromSQL.indexOf(',') && !joinPattern.matcher(fromSQL).matches() ) { // avoid all possible joins if it is not a VTI call
    			
    			String wherePredicates = endWherePattern.matcher(sqlAfterWhereToken).replaceFirst("$1").trim();
    			
        		int commentIndex = wherePredicates.indexOf("--");        		
        		boolean isCommentNotSpecifiedOrIsDefinitelyOutsideAString = -1 == commentIndex || -1 == wherePredicates.indexOf('\'', commentIndex);
        		
        		if ( isCommentNotSpecifiedOrIsDefinitelyOutsideAString ) {
        			// Strip off the comment if we have one
        			if ( -1 != commentIndex ) wherePredicates = wherePredicates.substring(0, commentIndex);
        			
	    			// Remove queryid and querysteps predicates if this is a forwarded query
	            	if ( null!=forwardingNode ) {
	            		if ( '(' == wherePredicates.charAt(0) ) {
		            		// We need to remove ANDed QRYID and QRYSTEPS, meaning we also take out the brackets surrounding the predicates we keep
		            		int endIndex = wherePredicates.lastIndexOf( ")",
		            				wherePredicates.lastIndexOf( GaianDBConfig.GDB_QRYID ) ); // remove closing bracket and QRYID and what follows
		            		if ( 0 < endIndex )
		            			wherePredicates = wherePredicates.substring(1, endIndex).trim();
	            		} else
	            			wherePredicates = ""; // no predicates other than the QRYID/QRYSTEPS
	            	}
	            	
	            	if ( 0 > wherePredicates.indexOf('?') ) {
	           	
		        		if ( 0 < wherePredicates.length() ) {
		        			// See if an alias is being used (which may be referenced in the predicates) - use this as alias to the propagated VTI call
		        			String[] sqls4 = oneOrMoreWhiteSpaceOrAClosingBracketPattern.split( fromSQL );
		        			if ( 1 < sqls4.length ) tableAlias = sqls4[ sqls4.length-1 ];
		        			
		        			logInfo("Original SQL is not a JOIN and has one FROM and WHERE token and no positional parms, EXTRACTED PREDICATES: " + wherePredicates + 
		        					", table alias: " + tableAlias);
		        			queryDetails.put(QRY_APPLICABLE_ORIGINAL_PREDICATES, wherePredicates);
		
		        		} else
		        			logInfo("Original SQL has no predicates to push down to other nodes");
	            	} else
	            		logInfo("Original SQL has positional parameter markers ('?') in its WHERE clause - cannot push this down to other nodes");
        		} else
        			logInfo("Original SQL has a comment after where clause which may be nested in a string - cannot determine end of whereClause so cannot push predicates down to other nodes");
    		} else
    			logInfo("Original SQL unable to match single table and where clause (potentially a JOIN) - no shallow parsed predicates will be pushed to other nodes");
		} else
			logInfo("Original SQL detected not to have a single FROM and WHERE token - no shallow parsed predicates will be pushed to other nodes");
    	
    	
    	int commentIndex = originalSQL.lastIndexOf("--");
    	if ( -1 < commentIndex ) {
    		String comment = originalSQL.substring(commentIndex);
    		String timeoutString = timeoutPattern.matcher(comment).replaceFirst("$1");
    		if ( !comment.equals(timeoutString) ) {
        		logInfo("Detected/parsed/recording "+GDB_TIMEOUT+" = (query timeout (ms)): " + timeoutString);
        		if ( null != timeoutString && 0 < timeoutString.length() ) queryDetails.put(QRY_TIMEOUT, new Integer(timeoutString));
    		}
    		String widString = widPattern.matcher(comment).replaceFirst("$1");
    		if ( !comment.equals(widString) ) {
        		logInfo("Detected/parsed/recording "+GDB_WID+" (workload id): " + widString);
        		if ( null != widString && 0 < widString.length() ) queryDetails.put(QRY_WID, widString);
    		}
    		String hashString = originalSQLHashPattern.matcher(comment).replaceFirst("$1");
    		if ( !comment.equals(hashString) ) {
        		logInfo("Detected/parsed/recording "+GDB_HASH+" (original sql hash): " + hashString);
        		if ( null != hashString && 0 < hashString.length() ) queryDetails.put(QRY_HASH, hashString);
    		} else
    			// Remove the comment when setting the hash code initially 
    			queryDetails.put( QRY_HASH, Integer.toHexString(originalSQL.substring(0, commentIndex).hashCode()).toUpperCase() );
    	} else
    		// Set the original query hash - to be attached to all propagated and sub-queries resulting from this one throughout the network.
    		// No need for anything more complex than hashCode(). The only important thing is no ensure the query cannot be deduced from it
    		// i.e. this is overkill -> Util.byteArray2HexString(SecurityManager.getChecksumSHA1(originalSQL.getBytes()), false)
    		queryDetails.put( QRY_HASH, Integer.toHexString(originalSQL.hashCode()).toUpperCase() );
	}
	
    public static final String GDB_TIMEOUT = "GDB_TIMEOUT";
    public static final String GDB_WID = "GDB_WID";
    public static final String GDB_HASH = "GDB_HASH";
    
    private static final Pattern timeoutPattern = Pattern.compile(".*"+GDB_TIMEOUT+"[\\s]*=[\\s]*([\\d]+).*");
    // For WIDs, allow any word char and the dash '-' char to allow for UUID.getRandomUUID()toString()
    private static final Pattern widPattern = Pattern.compile(".*"+GDB_WID+"[\\s]*=[\\s]*([-\\w]+).*");
    
    private static final Pattern originalSQLHashPattern = Pattern.compile(".*"+GDB_HASH+"[\\s]*=[\\s]*([\\w]+).*");
	
    
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedRowCount(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {

		// This costing figure denotes row count for this query - after applying predicates filter.
		// We just return the max number of rows returned for the logical table so far
		
		// Original sql cannot be passed to execution phase from here (but it could help to work out stats info for compilation phase...).
		if ( null == originalSQL ) originalSQL = vtiEnvironment.getOriginalSQL().trim();
//		logger.logInfo("Original SQL: " + originalSQL);
		
		// We're not using the full query signature here because Derby hasn't yet called pushProjection() and setQualifiers()...
		// No point including originalSQL as this estimation method only gets called on query compilation, which is only called once per originalSQL...
		Long l = (Long) estimatedRowCounts.get( ltSignature ); //+ originalSQL );
		
		// Invert the number of rows because we actually want smaller tables to be the inner ones in joins (to minimise our disk caching activity)
		double val = null == l ? 1 : Long.MAX_VALUE / l.doubleValue(); // Use 1 as default to encourage hash joins and avoid query re-executions
		
		// NOTE: There could be a strong reason below for switching the GaianTable to be an *inner* table in a join: i.e.
		// IF we know that there are some join predicates that are going to be pushed down in a nested loop join, which would substantially reduce the
		// number of records pulled out of the GaianTable.
		
		val = 1; // overwriting this to ensure GaianTable is always used as outer table
		
		logInfo("Entered getEstimatedRowCount(), returning " + val
//				+ ", orginal sql = " + vtiEnvironment.getOriginalSQL()
				);
		
//		System.out.println("Entered getEstimatedRowCount() for signature: " + ltSignature + ", returning " + val);
		
//		vtiEnvironment.setSharedState("queryID", "null");
		
		// Just return 1 for now - so we control the join order when writing the query... (right-most is inner table)
		// (Otherwise we would have to prime Derby with queries using various LT signatures before running the big JOIN queries)
		return val;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedCostPerInstantiation(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {

		// Only visible in compilation phase of query - which has its own instantiation of GaianTable() -
		if ( null == originalSQL ) originalSQL = vtiEnvironment.getOriginalSQL().trim();
		try {
			double rc = 100;
			if ( testIsSystemQuery(originalSQL) )
				logger.logImportant("INITIALISING SYSTEM QUERY - THIS WILL SUBSEQUENTLY ONLY BE LOGGED WHEN LOGLEVEL=ALL" + "\n" + originalSQL);
	    	logInfo("Entered getEstimatedCostPerInstantiation(), returning " + rc);
			return rc;
		}
		catch ( Exception e ) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}

	/**
	 * Tells Derby if this VTI can be instantiated multiple times.
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {

		// Only visible in compilation phase of query - which has its own instantiation of GaianTable() -
		if ( null == originalSQL ) originalSQL = vtiEnvironment.getOriginalSQL().trim();
		// Not fully sure what this is used for by Derby, but no reason why a GaianTable can't be instantiated multiple times.
		/**
		 * Defect 100448 (October 2014):
		 * supportsMultipleInstantiations() must return true to allow Nested Loop Joins.
		 * Derby raises a NullPointerException on 4-way self-joins otherwise.
		 * Note that "supportsMultipleInstantiations" should be read to mean: "is scrollable" or "is re-fetchable" - NOT "the vti returns constant data"
		 */
		boolean rc = true; // vtiEnvironment.getOriginalSQL().toUpperCase().matches( REGEX_SUB_QUERY_JOIN );
    	logInfo("Entered supportsMultipleInstantiations(), returning " + rc);
		return rc;
	}
	
	private void reinitForReExecutions() throws SQLException {
    	queryID = null;
    	queryPropagationCount = -1;
    	if ( isStatementInitialised ) checkAccessRestrictions(); // Do this even if queryID wasn't set yet...
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.Pushable#pushProjection(org.apache.derby.vti.VTIEnvironment, int[])
	 */
	public boolean pushProjection(VTIEnvironment vtiEnvironment, int[] projectedColumns) throws SQLException {
    	reinitForReExecutions();
		
		if ( null == originalSQL ) originalSQL = vtiEnvironment.getOriginalSQL().trim();
//		if ( !isMetaDataReady ) setupGaianVTIs(true);
//		
//		if ( null == logicalTableRSMD )
//			// This means a long path loop was detected - no point doing any work as the query will be rejected - just pretend everything is ok...
//			return true;
		
//		return true;
		
		isSelectStar = false;
    	this.projectedColumns = projectedColumns; // 1-based
    	
    	// Ignore columns beyond NULL columns range - these are just left as NULL when fetched in the DataValueDescriptor[] row
//    	try {
//    		logicalTableRSMD.excludeNullColumns();
//    	int numRealCols = logicalTableRSMD.getColumnCount();
//    	int realProjectionLength = 0;
//    	for ( ; realProjectionLength<projectedColumns.length; realProjectionLength++ )
//    		if ( numRealCols < projectedColumns[ realProjectionLength ] ) break;
//    	
//    	this.projectedColumns = new int[realProjectionLength];
//    	System.arraycopy(projectedColumns, 0, this.projectedColumns, 0, realProjectionLength);
//    	} catch ( Exception e ) {
//    		System.out.println("Caught exexexexex: " + e);
//    		e.printStackTrace();
//    	}

    	testIsSystemQuery( originalSQL );
    	String projectionString = Util.intArrayAsString(projectedColumns);
    	logInfo("Projected Columns: " + projectionString);
		return true;
	}
	
	private void setupProjectedColumns() {
		
    	try {
			if ( null != dotFileBW ) {
				// This is an explain query, and the explain information is to be written to a DOT script.
				// All explain columns are required to do this. The physical columns are not.
				// Therefore, ignore the pushed projection (the list of columns queried) and just include the explain cols.
				// (Rationale: If we don't query for all explain columns, they will not be set in returned rows (even if we set them 
				// downstream, Derby nulls them out when passing them back). Note that logical table columns will not be set anyway because
				// the query is converted into a 'count(*)' query when querying physical data sources.)
				logInfo("Projected Columns ignored for 'explain in dotfile' query: all explain cols only will be selected");
				
		    	int projectionLength = GaianDBConfig.EXPLAIN_COLS.length;
		    	projectedColumns = new int[projectionLength];
		    	int explainColsOffset = logicalTableRSMD.getExplainColumnsOffset() + 1; // Numbering is from 1
		    	
		    	for ( int i=0; i<projectionLength; i++ )
		    		projectedColumns[i] = explainColsOffset+i;
			
			} else if ( null == projectedColumns ) {
				
				// Include all columns
	//			GaianResultSetMetaData ltrsmd = DataSourcesManager.getLogicalTableRSMD( logicalTable );
				int ltColCount = logicalTableRSMD.getColumnCount();	
				projectedColumns = new int[ltColCount];
				for (int i=0; i<ltColCount; i++)
					projectedColumns[i] = i+1;
				
			} else {
	    	
				// Remove NULL columns from projection - these are ones that exist in another nodes' table def for this LT
		    	int numRealCols = logicalTableRSMD.getColumnCount(); // non-null columns
		    	
		    	int realProjectionLength = 0;
		    	for ( ; realProjectionLength<projectedColumns.length; realProjectionLength++ )
		    		if ( numRealCols < projectedColumns[ realProjectionLength ] ) break;
		    	
		    	int[] newProjection = new int[realProjectionLength];
	
		    	// Only keep columns from the original projection that are not null
		    	System.arraycopy(projectedColumns, 0, newProjection, 0, realProjectionLength);
		    	projectedColumns = newProjection;
			}
		
			logInfo("Logical projectedColumns = " + Util.intArrayAsString(projectedColumns));

    	} catch ( Exception e ) {
    		logger.logException( GDBMessages.ENGINE_PROJECTED_COLUMNS_SETUP_ERROR, "Unable to setup projected columns: ", e );
    	}
	}

	private void setupPhysicalProjectedColumns() {
		
    	try {
	    	// Copy all physical column indexes to the physicalProjectedColumns array
    		
    		if ( isExplain ) {
    			// We do not query physical columns for an explain.
    			// Instead a count(*) query will be issued against physical data sources
    			logInfo("Setting physical projected columns to {1} to receive result of count(*) queries");
    			physicalProjectedColumns = new int[] {1};
    		} else {

    	    	int columnsCount = projectedColumns.length;
    			int physicalColumnsCount = logicalTableRSMD.getPhysicalColumnCount();
    			for ( int i=0; i<=columnsCount; i++ ) {
    				
    				if ( i == columnsCount || projectedColumns[i] > physicalColumnsCount ) {
    					physicalProjectedColumns = new int[i];
    					System.arraycopy( projectedColumns, 0, physicalProjectedColumns, 0, i );
    					break;
    				}
    			}	
    		}
    		
	    	logInfo("Physical projectedColumns = " + Util.intArrayAsString(physicalProjectedColumns));

    	} catch ( Exception e ) {
    		logger.logException( GDBMessages.ENGINE_PHYSICAL_PROJECTED_COLUMNS_SETUP_ERROR, "Unable to setup physical projected columns: ", e );
    	}
	}


/////////////////////////////////////********************* FAST PATH START ************************///////////////////////////////////
	
	private long MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK;
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#executeAsFastPath()
	 */
	public boolean executeAsFastPath() throws StandardException, SQLException {
		
		// DRV - 04/02/2014 - 91558
		// Bug here was to return true immediately when runStatus was not RUN_STATUS_ON - this caused NPEs when Derby subsequently called nextRow().
		// GaianDB can actually service queries as soon as Derby network server is running - which is *before* our own runStatus is RUN_STATUS_ON.
		// We do however want to cut queries short by returning false immediately if the node status is RUN_STATUS_PENDING_OFF or RUN_STATUS_OFF.
		if ( true == GaianNode.isShutdownCompleteOrPending() ) return false;
		
		try {
			clearResultAndInMemoryCache();
//			if ( null != gaianResult ) gaianResult.close();

			GaianResult newGaianResult = executeFastPathQuery();
			
			if ( newGaianResult == null ){
				// take the previous query result out of the cached "in progress" results.
				endQuery();
			}
			
			gaianResult = newGaianResult;
			
			if ( null != gaianResult ) {

				startQuery();
				
				// Prepare to cache returned rows - 
				// We cache the rows to implement a scrollable cursor, so that Derby can carry on calling next() to cycle 
				// through the rows as many times as it likes every time a scan is complete.
				// Only do this if the original query contains a sub-query join, because otherwise Derby will quite happily
				// just do a hash-join. Caching the rows ourselves has no advantage over Derby doing so with a hash-join, but
				// for some reason Derby insists on doing a nested loop join on joins inside sub-queries.
				
				// Known issue here: Rows need to be cached when an N-way join is being computed between multiple GaianTables due to Derby's chosen
				// Join strategy.. The logical table names may appear as view names in the original SQL so it is impossible to test against the original SQL...
				// It might be worth adding a parm to the GaianTable table parms that a user can set to indicate that row caching should be done. A better
				// solution is to code the join such that Derby choses a different strategy (which will not require caching) - this can often be simply achieved by
				// altering the order of joined logical tables in the SQL - e.g. put the tables with most predicates against it at the end..
//				if ( originalSQL.toUpperCase().matches( REGEX_SUB_QUERY_JOIN ) ) {
				if ( null == forwardingNode && isPossibleInnerTableOfJoin ) { //&& !GaianDBConfig.getDisableRowCaching() ) {
					logInfo("Rows will be cached for this query as we are at an entry point node and this invocation is potentially targetting the inner table of a JOIN");
//					Long previousCount = estimatedRowCounts.get( ltSignature ); // + originalSQL );
					cachedResultRows = new ArrayList<DataValueDescriptor[]>( 100 ); //null==previousCount ? 100 : previousCount.intValue() );
					// Persistent cache expiry time 1 second - wd be better if the cache would expire straight away (to guarantee the next isCached(x) clears it)
//					getDefaultVTIProperties().put( PROP_CACHE_PKEY, "CACHEID" );
					MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK = GaianDBConfig.getDiskCachingThresholdForJoinedInnerTables();
				}
//				}
			}
		} catch ( Exception e ) {
			logger.logException(GDBMessages.ENGINE_EXEC_AS_FAST_PATH_ERROR, "Exception in executeAsFastPath(): ", e);
			
		} catch ( Error er ) {

			GaianNode.stop( "Error in GaianTable.executeAsFastPath()", er );
//			System.err.println("OUT OF MEMORY DETECTED IN executeAsFastPath() - Running System.exit(2)");
//			System.exit(2);
		}
		return true;
	}
	
	private boolean isCancelledQuery = false;
	
	public static boolean cancelQuery( String queryID ) {
		synchronized( gResults ) {
			logger.logThreadInfo("Active GaianResult queries: " + gResults.size());
			for ( GaianResult gr : gResults ) if ( gr.checkCancel(queryID) ) return true;
			return false;
		}
	}
	public boolean checkCancel( String queryID ) { return queryID.equals(this.queryID) ? cancelQuery() : false; }
	
	public static int checkAndActOnTimeouts() {
		int numCancelled = 0;
		synchronized( gResults ) { for ( GaianResult gr : gResults ) if ( gr.cancelOnTimeout() ) numCancelled++; }
		return numCancelled;
	}
	public boolean cancelOnTimeout( long timeoutTs ) { return !isCancelledQuery && timeoutTs<System.currentTimeMillis() ? cancelQuery() : false; }
	
	private boolean cancelQuery() {
		if ( !isCancelledQuery )
			try { clearResultAndInMemoryCache(); }
			catch (SQLException e) { logger.logThreadImportant("Unable to clearResultAndInMemoryCache() for GaianTable: " + e); }
		return isCancelledQuery = true;
	}
	
	// Return true if we are in caching mode (even if isCached() would say rows are not cached yet...)
	public boolean isAwaitingReFetch() { return !isCancelledQuery && 0 < reFetchIteration && ( null != cachedResultRows || isPersistentCacheInitialised ); }
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#nextRow(org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public int nextRow(DataValueDescriptor[] row) throws StandardException, SQLException {
		
//		if ( isLogPerfOn ) timeb4Fetch = System.nanoTime();
		int rc = SCAN_COMPLETED;
		
		try {
			
			if ( null == gaianResult ) {

				// happens when query wasn't executed against any child nodes 
				// e.g. due to: longer propagation loop, disqualification by constants, no sources and no gaiandb connections or programming error.
				
				if ( !isExplain || scanWasCompleted ) {
					scanWasCompleted = false; // reset in case the GaianTable is re-executed
					return rc = SCAN_COMPLETED;
				}
				
				// isExplain && !scanWasCompleted ...
				setExplainRow( row );
				
				// Test the explain qualifiers against the single returned row - use qualifiers as explainFullQualifiers wont be set.
				// Use testAndPruneQualifiers rather than testQualifiers because we don't want to apply predicates on the columns
				// that weren't set (e.g. the constant columns that are not filled in)
				if ( null == RowsFilter.testAndPruneQualifiers( row, qualifiers, false ) )
					return rc = SCAN_COMPLETED;
				
				scanWasCompleted = true;
				
			} else {
				
				if ( isCancelledQuery ) return rc = SCAN_COMPLETED;
				
				rc = gaianResult.nextRow( row );
				if ( !isExplain ) {
					
					if ( IFastPath.GOT_ROW == rc ) {
						// We have a row from the original source (after policy filtering) - this will need caching
						if ( null != cachedResultRows )	cacheRowLocal( row );
						return rc;
					}
					
					if ( 0 < reFetchIteration ) { // try to get cached rows instead						

						if ( !isPossibleInnerTableOfJoin )
							throw new Exception("Unable to re-fetch rows (as not cached) for undetected JOIN query - inner table: " + logicalTableName);
						
						if ( Logger.LOG_LESS < Logger.logLevel
								&& ( ( Logger.LOG_MORE < Logger.logLevel && 10 >= reFetchedRowIndex ) ||
									 ( 0 == reFetchedRowIndex % LOG_FETCH_BATCH_SIZE
									&& 1 == reFetchIteration % LOG_FETCH_ITERATION_BATCH_SIZE )	)
							)
							logInfo("nextRow(): Re-Fetching rows from " + (isPersistentCacheInitialised?"disk":"memory") +
									". Current fetch iteration: " + reFetchIteration + ", row index: " + reFetchedRowIndex +
									( Logger.LOG_MORE < Logger.logLevel ? " (logging first 10 only)" :
									" (printed every " + LOG_FETCH_ITERATION_BATCH_SIZE + " iterations and " + LOG_FETCH_BATCH_SIZE + " rows)"));
						
						// If rows were cached to disk - retrieve from there
						if ( isCached() || MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK <= gaianResult.getRowCount() &&
								isCached( "CACHEID="+queryID.hashCode() ) ) // fullQuerySignature.hashCode() ) )
							return rc = nextRowFromCache(row);
						
						// Return a memory cached row - or create a new iterator and return SCAN_COMPLETED again if we got to the end of them
						if ( null != cachedResultRows && cachedResultRowsIterator.hasNext() ) {
//							if ( 0 == reFetchedRowIndex )
//								GaianNode.notifyGaianTableBeingReScanned(this);
							DataValueDescriptor[] cachedRow = (DataValueDescriptor[]) cachedResultRowsIterator.next();
							for ( int i=0; i<row.length; i++ ) row[i].setValue( cachedRow[i] );
							
							return rc = GOT_ROW;
						}
						
					} else { // Record statistics
						long rowCount = gaianResult.getRowCount();
						Long previousCount = (Long) estimatedRowCounts.get( ltSignature ); // + originalSQL 
						if ( null == previousCount || rowCount > previousCount.longValue() )
							estimatedRowCounts.put( ltSignature, new Long( rowCount ) ); // + originalSQL 
//						if ( null != cachedResultRows )
//							GaianNode.notifyArrayElementsAdded(cachedResultRows.size());
					}
					
					if ( null != sqlResultFilter ) sqlResultFilter.close();
					if ( isLogPerfOn ) logQueryPerformanceFigures();
					
					return rc;
				}
				
				// We want explain data	
				
				while ( true ) {
					
					if ( scanWasCompleted ) {
						scanWasCompleted = false; // reset in case the GaianTable is re-executed
						return rc = SCAN_COMPLETED;
					}
					
					if ( SCAN_COMPLETED == rc /* || ! row[ row.length-1 ].isNull() */ ) {
						// All other explain rows have been processed - now create our own and
						// if this succeeds give it to Derby telling it the scan is not complete yet.
						scanWasCompleted = true;
						setExplainRow( row );
					}
					
					// The explain row still needs testing against any predicates on the GDBX_COUNT column - do this
					// only back at the original calling node (as each value at each level in the tree depends on values below it...)
					// Use testAndPruneQualifiers rather than testQualifiers because we don't want to apply predicates on the columns
					// that weren't set (e.g. the physical columns that are not filled in)
					if ( 0 < queryPropagationCount ||
							null != RowsFilter.testAndPruneQualifiers( row, explainFullQualifiers, false ) ) break;
					
					logInfo("Explain row does not satisfy GDBX_COUNT predicates, getting another row...");
					// This is the node where the query originated, and the row doesnt satisfy the COUNT predicates, so discard it
					rc = gaianResult.nextRow( row );
				}
			}
			
			// We have an explain row - write its data to file if requested - this code will only be executed on the original querying node
			if ( null != dotFileBW ) writeExplainRowDataToDotFile( row );			
			// Cache returned row
			if ( null != cachedResultRows ) cacheRowLocal(row);
			
			return rc = GOT_ROW;
			
		} catch ( Exception e ) {
			logger.logException( GDBMessages.ENGINE_NEXT_ROW_ERROR, "Caught Exception in nextRow() (returning SCAN_COMPLETED): ", e );
		
		} catch ( Error er ) {

			gaianResult = null;
			GaianNode.stop( "Error in GaianTable.nextRow()", er );
//			System.err.println("OUT OF MEMORY DETECTED IN nextRow() - Running System.exit(2)");
//			System.exit(2);
			
		} finally {
//			logger.logDetail("nextRow() rc " + rc + ", row: " + Arrays.asList(row));
			if ( GOT_ROW == rc ) {
				if ( 0 < reFetchIteration ) reFetchedRowIndex++;
				unzipBlobsIfRequired( row );
				
//				logger.logDetail("Returning row: " + Arrays.asList(row)); // this ends up logging too many lines to the log file
			}
			else {
				reFetchIteration++;
				// If we were caching rows, create a new iterator for them every time a scan is complete
				if ( null != cachedResultRows ) {
					if ( MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK < gaianResult.getRowCount() ) {
						// Cache last rows to disk - we will read all rows from the cache on disk when re-fetching
						if ( 0 < cachedResultRows.size() ) cacheSpillRowsBatchToDisk();
						cachedResultRows.clear();
						cachedResultRows = null; // invalidate in-memory cache completely now
					} else {
						cachedResultRowsIterator = cachedResultRows.iterator();
					}
					reFetchedRowIndex = 0;
				}
				updateQueriesStats();
			}
		}
		
		return SCAN_COMPLETED;
	}
	
	private void cacheRowLocal( DataValueDescriptor[] row ) throws Exception {
		
		// All exposed columns must be cached - i.e. all those from the row - incl hidden/constant ones
		DataValueDescriptor[] cachedRow = new DataValueDescriptor[row.length];
		for (int i=0; i<row.length; i++) {
			cachedRow[i] = row[i].getNewNull();
			cachedRow[i].setValue(row[i]);
		}
		cachedResultRows.add( cachedRow );
		
		if ( 0 == gaianResult.getRowCount() % MAX_ROWS_TO_CACHE_BEFORE_SPILL_TO_DISK ) {
			cacheSpillRowsBatchToDisk();
			cachedResultRows.clear();
		}
	}
	
	private boolean isPersistentCacheInitialised = false;
	
	private void cacheSpillRowsBatchToDisk() throws SQLException {
		if ( !isPersistentCacheInitialised ) {
			// Start caching to disk: set appropriate properties, initialise cache tables and persist all rows kept in memory so far			
			getDefaultVTIProperties().put( PROP_CACHE_EXPIRES, "10" );
			getDefaultVTIProperties().put( PROP_CACHE_INDEXES, "CACHEID" );
			isCached("CACHEID="+queryID.hashCode()); // initialise cache tables for the table schema meta-data exposed by this GaianTable
			setCacheKeys( new DataValueDescriptor[] { new SQLLongint( queryID.hashCode() ) } ); //fullQuerySignature.hashCode() ) } );
			isPersistentCacheInitialised = true;
		}
		resetCacheExpiryTime(); // reset expiry regularly before each batch of inserts
		cacheRows( cachedResultRows.toArray( new DataValueDescriptor[0][] ) );
	}
	
	private void unzipBlobsIfRequired( DataValueDescriptor[] row ) throws StandardException {
		
//		for (int i=0; i<row.length; i++)
//			if ( row[i] instanceof SQLBlob )
//				logInfo("Column " + (i+1) + " is a SQLBlob, size: " + row[i].estimateMemoryUsage());
		
		if ( unzipLobs ) {
			for (int i=0; i<row.length; i++) {
				if ( row[i] instanceof SQLBlob ) {
					
//					System.out.println("Unzipping blob for column " + (i+1));
					
					try {
						byte[] data = row[i].getBytes();
						ByteArrayInputStream is = new ByteArrayInputStream(data);
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						Util.copyBinaryData(new GZIPInputStream(is), baos);
						byte[] bytes = baos.toByteArray();
						is.close(); // other streams are closed
						row[i].setValue(bytes);
						
//						System.out.println("Successfully set new byte array, size: " + bytes.length);
						
					} catch (Exception e) {
						logger.logException(GDBMessages.ENGINE_BLOB_UNZIP_ERROR, "Unable to unzip blob: ", e);
					}
				}
			}
		}
	}
	
	private void writeExplainRowDataToDotFile( DataValueDescriptor[] row ) throws Exception {
		
		int offset = logicalTableRSMD.getExplainColumnsOffset();
		
		// The link info may describe any other link in the network - we read it from the row
		String from = row[ offset ].getString();
		String to = row[ offset+1 ].getString();
		char linkPrecedence = row[ offset+3 ].getString().charAt(0);
		String rowCountLabel = "label=" + row[ offset+4 ].getString();
		
		String attribs = "";
		switch (linkPrecedence) {
			case DOT_EXPLAIN_FIRST_PATH:  attribs = rowCountLabel; break;
			case DOT_EXPLAIN_SHORTER_PATH: attribs = rowCountLabel + ", color=red"; break;
			case DOT_EXPLAIN_LONGER_PATH:  attribs = "style=dotted"; break;
			default:
				logger.logWarning( GDBMessages.ENGINE_EXPLAIN_PATH_VALUE_ERROR, getContext() + " Unexpected explain path value: " + linkPrecedence);
				attribs = null; break;
		}				
		
		if ( null != attribs ) {
			
			String dotline = "\"" + ("<SQL QUERY>".equals(from)?"SQL Query on " + getLogicalTableName(true):from) + 
				"\" -> \"" + to + "\" [" + attribs + "];\n";
			
			// If scan completed has been set above, write the data to file.
			// Derby will invoke us once more after this and we will return SCAN_COMPLETED
			if ( scanWasCompleted ) {
				logInfo( "Completing DOT graph data with first line: " + dotline );
				dotGraphText.insert( 0, "digraph G {\n" + dotline );
				dotGraphText.append("}\n");
				dotFileBW.write( dotGraphText.toString() );
				dotFileBW.close();
				dotFileBW = null;
			} else {
				logInfo( "Adding DOT line to StringBuffer: " + dotline );
				dotGraphText.append( dotline );
			}
		}
	}
	
//	private static final int DOT_EXPLAIN_DISABLED = 0;
	private static final char DOT_EXPLAIN_UNSET_PATH = 'U'; // Unset - error
	private static final char DOT_EXPLAIN_FIRST_PATH = 'F'; // First
	private static final char DOT_EXPLAIN_SHORTER_PATH = 'S'; // Shorter "color=red";
	private static final char DOT_EXPLAIN_LONGER_PATH = 'L'; // Longer "style=dotted";
	
	/**
	 * Set the explain column with string data that can later be used by GraphViz to draw
	 * a graph of the path of a query.
	 * 
	 * @param row
	 * @throws UnknownHostException
	 * @throws StandardException
	 * @throws SQLException
	 */
	private void setExplainRow( DataValueDescriptor[] row ) throws StandardException, SQLException {
		
		// GDBX_COUNT is the number of records for the LT at each node, or (when using option 'explainds') the number of data sources for the LT at the node.
		long count = numLocalDataSourcesInExplainDsMode + ( null==gaianResult ? 0 : gaianResult.getExplainRowCount() );
		
		// Its easier here to re-construct the explain column values here than to pick them out
		// of the logical table result set meta data
		String fromNode = null!=forwardingNode ? forwardingNode : "<SQL QUERY>"; //"SQL Query on " + getLogicalTable(true); //originalSQL; //"<QUERY>";
		
		logInfo("Setting explain row: " + fromNode + ", " + GaianDBConfig.getGaianNodeID() + ", " +
				queryPropagationCount + ", " + explainPath + ", " + count );
		
		setExplainColumns( row, fromNode, GaianDBConfig.getGaianNodeID(), queryPropagationCount, explainPath, count );
		
		// note even though the count predicates are pushed down we don't evaluate them,
		// and just let Derby do so on the calling node...
		
//		boolean test = testNodeQualifiers( row );
//		logInfo("setExplainRow: Branch qualifier test for count column: " + test);
//		return test;
	}
	
	private void setExplainColumns( DataValueDescriptor[] row,
			String from, String to, int depth, char precedence, long count ) throws StandardException {
		
		int offset = logicalTableRSMD.getExplainColumnsOffset();
		row[ offset ].setValue( from );
		row[ offset+1 ].setValue( to );
		row[ offset+2 ].setValue( depth );
		row[ offset+3 ].setValue( precedence+"" );
		row[ offset+4 ].setValue( count );
	}
	
	private void logQueryPerformanceFigures() {
		
//		fetchTimes.add( System.currentTimeMillis() - timeb4Fetch );
		

		long cumulativePoll=0, cumulativeSpikes=0, cumulativeSpikeSpins=0, spinnedFetches=0;
		
		StringBuffer sb = new StringBuffer();
		
		ArrayList<Long> pollTimes = gaianResult.getPollTimes();
		long rowCount = gaianResult.getRowCount();
		
//			long n = 1;
//			Iterator<Long> itf = fetchTimes.iterator();
//			Iterator<Long> itp = pollTimes.iterator();
//			while( itf.hasNext() && itp.hasNext()) {
//				long tf = itf.next(), tp = itp.next();
//				cumulativeFetch += tf; cumulativePoll +=tp;
//			}
//			
//			double pollSpikeThreshold = 10*(double)cumulativePoll/rowCount;
//			
//			itf = fetchTimes.iterator(); itp = pollTimes.iterator();
//			while( itf.hasNext() && itp.hasNext()) {
//				long tf = itf.next(), tp = itp.next();
//				sb.append(n++ + ", " + tf + ", " + 100*(double)tf/cumulativeFetch + ", " + 
//						tp + ", " + 100*(double)tp/cumulativePoll + ", " + (tf-tp) + "\n");
//				if ( tp > pollSpikeThreshold ) cumulativeSpikes += tp;
//			}
//			
//			fetchTimes.clear();
//
//			sb.insert(0, "Row index, Fetch Time (ns), (%) of Tot Fetch Time, Buffer Poll Time (ns), (%) of Tot Poll Time, Difference (ns)" +
//					",, Original SQL: " + originalSQL + ", Query Time " + gaianResult.getQueryTime() + "ms, Fetch Time " + gaianResult.getFetchTime() +
//					"ms\n\n,,,,,,, Total GaianDB Fetch: " + cumulativeFetch + "ns, Total Poll: " + cumulativePoll + 
//					"ns, Total Poll Spikes: " + cumulativeSpikes + "ns (Threshold " + pollSpikeThreshold + "ns)" + "\n");

		long n = 0;
		Iterator<Long> itp = pollTimes.iterator();
		while( itp.hasNext()) {
			long tp = itp.next();
			if ( tp > 0 ) spinnedFetches++;
			cumulativePoll += tp;
		}
					
//			int factor=100; double pollSpikeThreshold = factor*(double)cumulativePoll/rowCount;
		int factor=10; double pollSpikeThreshold = factor*getHundredthsRatio(cumulativePoll,spinnedFetches);
		
		itp = pollTimes.iterator();
		while( itp.hasNext()) {
			n++;
			long tp = itp.next();
			if ( tp > 0 ) sb.append(n + ", " + tp + ", " + 100*(double)tp/cumulativePoll + "\n");
			if ( tp > pollSpikeThreshold ) { cumulativeSpikes++; cumulativeSpikeSpins += tp; }
		}
		
		fetchTimes.clear();

//			sb.insert(0, "Row index, Buffer Poll Time (micros), (%) of Tot Poll Time,, " +
//					"Original SQL: " + originalSQL + ", Query Time " + gaianResult.getQueryTime() + "ms, Fetch Time " + gaianResult.getFetchTime() +
//					"ms\n\n,,,, Total Poll: " + cumulativePoll + 
//					"micros, Total Poll Spikes: " + cumulativeSpikes + "micros (Threshold " + pollSpikeThreshold + "micros)" + "\n");
		
		sb.insert(0, "Row index, Buffer Poll Spins, (%) of Tot Poll Spins,,Original SQL: " + originalSQL + 
				"\n,,,,Query Time " + gaianResult.getQueryTime() + "ms,Fetch Time " + gaianResult.getFetchTime() +
				"ms\n\n,,,,Number of Spinned Fetches " + spinnedFetches + " (" + getHundredthsRatio(100*spinnedFetches, rowCount) + 
				"%),Total Poll Spins: " + cumulativePoll + ",Average Spins (per spinning fetch): " + pollSpikeThreshold/factor +
				"\n,,,,Total Marginal Fetches: " + cumulativeSpikes + " (" + getHundredthsRatio(100*cumulativeSpikes, rowCount) + "%)" +
				",Total Marginal Spins: " + cumulativeSpikeSpins + " (" + getHundredthsRatio(100*cumulativeSpikeSpins, cumulativePoll) + "%)," +
				"(Threshold="+factor+"*Average)\n" );
		
		String fname = GaianDBConfig.getGaianNodeID() + "_logperf.csv";
		try {
			synchronized( perfFileLock ) {
				File f = new File( fname );
//					if ( null == perfFileBW || !f.exists() )
				BufferedWriter perfFileBW = new BufferedWriter( new FileWriter( f ) );
				perfFileBW.write( sb.toString() );
				perfFileBW.close();
			}
		} catch (IOException e) {
			logger.logException(GDBMessages.ENGINE_PERFORMANCE_LOGS_WRITE_ERROR, "Unable to write performance logs to file " + fname + ": ", e);
		}
	}
	
	private double getHundredthsRatio( long prop, long total ) {
		return 0==total?0:(double)(100*prop/total)/100;
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#currentRow(java.sql.ResultSet, org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public void currentRow(ResultSet rs, DataValueDescriptor[] arg1) throws StandardException, SQLException {
		logInfo("Entered currentRow()");
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#rowsDone()
	 */
	public void rowsDone() throws StandardException, SQLException {
		logInfo("Entered rowsDone()");
	}
	
	
/////////////////////////////////////********************* FAST PATH END ************************///////////////////////////////////
	
	/**
	 * The methods setString(), setInt() and setBytes() below are overriden from the PreparedStatement interface implemented by the 
	 * extended class UpdatableVTITemplate.
	 * They allow the UDP Driver server code - when a GaianNode is in "Lite" mode - to set key positional parameters for propagated Gaian queries.
	 * 
	 * The positional parameters are the queryID, queryPropagationCount and credentialsStringBlock, which are all initially on the client side
	 * in VTIRDBResult.java, before propagating a query on to another node. The UDP server later has to set these again on the server side.
	 * When in Lite mode, The UDP driver server code uses the Lite driver to invoke GaianTable directly, by-passing Derby parsing/compilation code.
	 * 
	 * Reminder:
	 * =========
	 * UDP Driver alone allows us to substitue the Derby TCP network layer with our own UDP network layer:
	 * SQL -> UDP client driver -> serilized bytes sent over UDP -> UDP server -> Derby embedded driver -> SQL parsing/compilation/execution invokes GaianTable
	 * 
	 * UDP Driver combined with Lite driver allows Derby to be removed altogether:
	 * SQL Query initiated and sent as above -> UDP server -> Lite Driver -> Basic query parsing/compilation and direct invocation of GaianTable
	 */
	@Override
		public void setString(int arg0, String arg1) throws SQLException {
			logInfo("Entered setString(" + arg0 + ", " + arg1 + ")");
			
//			cant work out column id from ltrsmd as arg0 index is index of parameter in the SQL (not associated with a column)
//			instead: only allow max 3 parms for now which must be qryid qrysteps and qrycredentials
			
			if ( GaianNode.isLite() ) {
				if ( 1 == arg0 ) queryID = arg1;
				else if ( 3 == arg0 ) credentialsStringBlock = arg1;
				
			} else
				super.setString(arg0, arg1);
		}
	
	@Override
		public void setInt(int arg0, int arg1) throws SQLException {
			logInfo("Entered setInt(" + arg0 + ", " + arg1 + ")");
			
			if ( GaianNode.isLite() ) {
				if ( 2 == arg0 ) queryPropagationCount = arg1;
				
			} else
				super.setInt(arg0, arg1);
		}
	
	@Override
		public void setBytes(int arg0, byte[] arg1) throws SQLException {
			logInfo("Entered setBytes(" + arg0 + ", " + arg1 + ")");
			super.setBytes(arg0, arg1);
		}
	
	/**********************************   DeferModification methods  **********************************/
	
//	public boolean alwaysDefer(int arg0) throws SQLException {
//		// TODO Auto-generated method stub
//		return true;
//	}
//	public boolean columnRequiresDefer(int arg0, String arg1, boolean arg2)
//			throws SQLException {
//		// TODO Auto-generated method stub
//		return false;
//	}
//	public void modificationNotify(int arg0, boolean arg1) throws SQLException {
//		// TODO Auto-generated method stub
//		
//	}
//	public boolean subselectRequiresDefer(int arg0, String arg1)
//			throws SQLException {
//		// TODO Auto-generated method stub
//		return false;
//	}
//	public boolean subselectRequiresDefer(int arg0, String arg1, String arg2)
//			throws SQLException {
//		// TODO Auto-generated method stub
//		return false;
//	}
	
	/***************************************************************************************************/
	
//	public ResultSet executeQuery() throws SQLException {
//		return GaianDBConfig.getResultSetFromQueryAgainstEmbeddedConnection(
//				"select * from new com.ibm.db2j.GaianTable('" + logicalTable + "') T");
//	};
	
	@Override
		public int getResultSetType() throws SQLException {
			return ResultSet.TYPE_FORWARD_ONLY;
		}
	
	
	@Override
	/**
	 * Must return java.sql.ResultSet.CONCUR_UPDATABLE to be a read-write VTI class.
	 * 
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
		public int getResultSetConcurrency() throws SQLException {
			return ResultSet.CONCUR_UPDATABLE;
		}
	public int getRowCount() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
