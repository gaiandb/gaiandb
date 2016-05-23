/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;



import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ibm.db2j.FileImport;
import com.ibm.gaiandb.apps.HttpQueryInterface;
import com.ibm.gaiandb.apps.MetricMonitor;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * The primary purpose of this class is to provide static methods to load GaianDB's Logical Tables 
 * definitions into GaianDBResultSetMetaData objects and their data sources into VTIWrapper objects.
 * This class also provides management of JDBC connection pooling.
 * 
 * @author DavidVyvyan
 */
public class DataSourcesManager {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "DataSourcesManager", 30 );
	
	private static final String GAIAN_NODE_PREFIX = "GAIANNODE";
	public static final String SUBQUERY_PREFIX = "GDB_SUBQ";
	public static final String GATEWAY_PREFIX = "GDB_GTW";
	
	private static final String INMEM_STACK_KEY_PREFIX = "INMEMORY ";

	private static final long MIN_JDBC_CONNECTION_TIMEOUT_MS = 200; // ms
	
	private static long latestGlobalConfigLoadTime = 0;
	
	// Globally used mapping of logical table name -> logical table result set metadata
	// Note entries for old logical tables (that are no longer defined) are not removed in case they are added again later.
	private static final Map<String, GaianResultSetMetaData> ltrsmds = new ConcurrentHashMap<String, GaianResultSetMetaData>();
	
	private static final Map<String, Map<String, String>> ltConfigSignatures = new HashMap<String, Map<String, String>>();
	private static final Map<String, String> ltConfigDefsForViewReloadChecks = new HashMap<String, String>();
	
	// This set may not be the set of keys in ltrsmds if a connection was not obtainable when the lts were created.
	private static Set<String> oldLogicalTableViewNames = Collections.synchronizedSet(new HashSet<String>()),
							   currentLogicalTableViewNames = ltrsmds.keySet();
	
	private static boolean isUsingTableFunctions = false;
	
	private static long viewsReloadIteration = 0; // used to change the "create view" String so that Derby is forced to re-evaluate the VTI meta-data.
	
	private static final int CACHE_SIZE_FOR_TRANSIENT_METADATA_OR_DATASOURCES = 1000;
	// (table def string or sub-query) + modifiers -> GaianResultSetMetaData
	
	// Temporary table structures for transient data sources (materialized for a GaianQuery())
	private static final Map<String, GaianResultSetMetaData> derivedMetaData = Collections.synchronizedMap(
			new CachedHashMap<String, GaianResultSetMetaData>( CACHE_SIZE_FOR_TRANSIENT_METADATA_OR_DATASOURCES ) );
	
	// Temporary data source wrappers for transient data sources (materialized for a GaianQuery())
	private static final Map<String, VTIWrapper> transientDataSources = Collections.synchronizedMap(
			new CachedHashMap<String, VTIWrapper>( CACHE_SIZE_FOR_TRANSIENT_METADATA_OR_DATASOURCES ) );
	
//	private static GaianResultSetMetaData genericNodeMetaData = null;
	
	// Globally used mapping of logical table name -> template row populated with constant values of 
	// special logical table columns.
//	private static Hashtable rowTemplates = new Hashtable();
	
	// Globally used mapping of nodeDefName -> Instance of VTIFile, VTIRDBResult or other
	private static final ConcurrentMap<String, VTIWrapper> dataSources = new ConcurrentHashMap<String, VTIWrapper>();
	
	// Globally used mapping of logicalTableName -> Array of instances of VTIFile, VTIRDBResult or other
	// representing all the data sources attached to a logical table.
	private static final ConcurrentMap<String, VTIWrapper[]> dsArrays = new ConcurrentHashMap<String, VTIWrapper[]>();
	
	// Internally used mapping of: source handle descriptor -> pool of physical handles to the source
	// e.g. connectionPropertiesString "driver'url'usr'pwd" -> JDBC Connection pool
	// or:  File name -> FileImport vti pool
	// Note all vtis share the pools, picking handles out and replacing them when appropriate.
	private static final ConcurrentMap<String, Stack<Object>> sourceHandlesPools = new ConcurrentHashMap<String, Stack<Object>>();
	
	// Hashtable of: source handle descriptor -> HashSet of requestors
//	private static Hashtable sourceHandlesPoolRequestors = new Hashtable();
	
	private static String[] globalGaianConnections = null;
//	private static int minGaianNodesInAnyLogicalTable = -1;
	
//	// These hash sets are used for house keeping. They hold sets of data source ids curently referenced in the properties file.
//	// e.g. jdbc connection details strings and filenames.
	private static final Set<String> allReferencedJDBCSourceIDs = Collections.synchronizedSet(new HashSet<String>());
//	private static final Set allReferencedDataSourceIDs = new HashSet();
	
	enum RDBProvider {
		// List all drivers we know about - Note the first one should be the "preferred" client/server one (i.e. which we have found to work best)
		Derby(Arrays.asList(GaianDBConfig.DERBY_CLIENT_DRIVER, GaianDBConfig.DERBY_EMBEDDED_DRIVER)),
		DB2(Arrays.asList("com.ibm.db2.jcc.DB2Driver")),
		Oracle(Arrays.asList("oracle.jdbc.OracleDriver")),
		MSSQLServer(Arrays.asList("com.microsoft.sqlserver.jdbc.SQLServerDriver")),
		MySQL(Arrays.asList("com.mysql.jdbc.Driver")),
		Hive(Arrays.asList("org.apache.hive.jdbc.HiveDriver")),
		Other(Arrays.asList("")); // Note - JDBC driver name is optional anyway - Java's DriverManager can resolve it from the URL
		
		private final List<String> knownDrivers; // make this public if you need to see the known drivers one day.. but could just delete these really
		private RDBProvider(List<String> knownDrivers) { this.knownDrivers = knownDrivers; } 
		
		@Override public String toString() { return super.toString(); }
		
		public static RDBProvider fromGaianConnectionID(String connID) {
			String[] elmts = Util.splitByTrimmedDelimiter(connID, '\'');
			if ( 4 > elmts.length ) return Other;
			
			String driverString = elmts[0], urlString = elmts[1];
			
			for ( RDBProvider rdbms : new RDBProvider[] { Derby, DB2, Oracle, MSSQLServer, MySQL } )
				if (rdbms.knownDrivers.contains(driverString)) return rdbms;
			
			return fromURL( urlString );
		}
		
		public static RDBProvider fromURL(String jdbcURL) {
			if (jdbcURL.startsWith("jdbc:derby")) return Derby;
			else if (jdbcURL.startsWith("jdbc:db2")) return DB2;
			else if (jdbcURL.startsWith("jdbc:oracle")) return Oracle;
			else if (jdbcURL.startsWith("jdbc:sqlserver")) return MSSQLServer;
			else if (jdbcURL.startsWith("jdbc:mysql")) return MySQL;
			else if (jdbcURL.startsWith("jdbc:hive")) return Hive;
			else return Other;
		}
	};
	
	private static String logicalTableHavingViewsLoaded = "";
	public static boolean isLogicalTableViewsLoading( String ltName ) { return ltName.equals(logicalTableHavingViewsLoaded); }
	
	private static final Map<String, Statement> userdbStatementsForViewReloads = new HashMap<String, Statement>(); // dbname -> jdbc statement on gdb system schema in db
	private static final Map<String, Set<String>> userSchemasInitialisedWithSynonyms = new HashMap<String, Set<String>>(); // dbname -> set of custom user schemas initialised with synonyms
		
	public static void registerDatabaseStatementForLogicalTableViewsLoading( String userdb, Statement userdbStmtWithinGdbSystemSchema ) {
		userdbStatementsForViewReloads.put(userdb, userdbStmtWithinGdbSystemSchema);
	}
	
	public static boolean isUserdbInitialised( String userdb ) { return userdbStatementsForViewReloads.containsKey(userdb); }
	
	public static void initialiseAlternateUserSchemaIfNew( String userDB, String userSchema, Statement userdbStmtWithinUserSchema ) {
		
		Set<String> initialisedUserSchemasForThisDB = userSchemasInitialisedWithSynonyms.get(userDB); // this method shouldnt be called if this is null but we're still defensive...
		
		if ( null == initialisedUserSchemasForThisDB ) {
			initialisedUserSchemasForThisDB = new HashSet<String>();
			userSchemasInitialisedWithSynonyms.put(userDB, initialisedUserSchemasForThisDB);
		}
		
		if ( initialisedUserSchemasForThisDB.contains(userSchema) ) {			
			logger.logInfo("GDB synonyms for logical tables and procs/funcs and are already initialised for userSchema: " + 
					userSchema + " (in database: " + userDB + ") - Nothing to do");
			return;
		}
		
		final String gdbSchema = GaianDBConfig.getGaianNodeUser().toUpperCase();
		
		for ( String symbol : new String[] {
				MetricMonitor.PHYSICAL_TABLE_NAME, HttpQueryInterface.QUERIES_TABLE_NAME, HttpQueryInterface.QUERY_FIELDS_TABLE_NAME } )
			try { userdbStmtWithinUserSchema.execute("create synonym " + userSchema + "." + symbol + " for " + gdbSchema + "." + symbol); }
			catch (Exception e) { logger.logDetail("Cannot create synonym " + userSchema + "." + symbol + " for physical table " + symbol + " (ignored): " + e); }
		
		Set<String> ltNames = currentLogicalTableViewNames;
		
		logger.logInfo("Dropping obsolete synonyms on userdb/schema " + userDB + "/" + userSchema + " for " + ltNames.size()+" logical tables: " + ltNames);		
		
		int updates = 0;
		
//		final String[] viewSuffixes = new String[] { "", "_0", "_1", "_P", "_X", "_XF" }; // obsolete, now using: GaianDBConfig.getLogicalTableRequiredViewSuffixes
		
		for ( String lt : ltNames ) {
			for ( Iterator<String> it = GaianDBConfig.getLogicalTableRequiredViewSuffixes(lt).iterator(); it.hasNext(); ) {
				String symbol = lt + it.next();
				try { userdbStmtWithinUserSchema.execute("drop synonym " + userSchema + "." + symbol); }
				catch (Exception e) { logger.logDetail("Cannot drop synonym " + userSchema + "." + symbol + " for view on old logical table " + lt + " (ignored): " + e); }
				try { userdbStmtWithinUserSchema.execute("create synonym " + userSchema + "." + symbol + " for " + gdbSchema + "." + symbol); updates++; }
				catch (Exception e) { logger.logDetail("Cannot create synonym " + userSchema + "." + symbol + " for view on logical table " + lt + " (ignored): " + e); }
			}
		}
		
		if ( !ltNames.isEmpty() )
			logger.logInfo("Created " + updates + " synonyms for LT views in userSchema '"+userSchema+"'");
		
		updates = 0;
		
		// Setup the API and utility procedures - note this has already been validated to work when executed at startup - so we don't expect any problems.
		for ( String spflist : new String[] { GaianDBConfigProcedures.GAIANDB_API, GaianDBUtilityProcedures.PROCEDURES_SQL } )
			for ( String sql : spflist.split(";") )
				// note that if derby.database.sqlAuthorization=true, then only user 'gaiandb' will have exec permission on this.
				try { userdbStmtWithinUserSchema.execute( sql.startsWith("!") ? sql.substring(1) : sql ); updates++; }
				catch ( SQLException e ) {} // ignore initial exceptions whereby an spf cannot be dropped when it doesn't exist
		
//		// Setup the API and utility procedures - note this has already been validated to work when executed at startup - so we don't expect any problems.
//		for ( String spflist : new String[] { GaianDBConfigProcedures.GAIANDB_API, GaianDBUtilityProcedures.PROCEDURES_SQL } )
//			for ( String sql : spflist.split(";") ) {
//				
//				// Parse a create spf statement
//				sql = sql.toUpperCase().trim(); if ( 2 > sql.length() ) continue; if ( '!' == sql.charAt(0) ) sql = sql.substring(1); sql = sql.trim();
//				if ( sql.startsWith("CREATE") ) sql = sql.substring("CREATE".length()); else continue; sql = sql.trim();
//				if ( sql.startsWith("FUNCTION") ) sql = sql.substring("FUNCTION".length()); else
//				if ( sql.startsWith("PROCEDURE") ) sql = sql.substring("PROCEDURE".length()); else continue;
//				
//				final String symbol = sql.substring(0, sql.indexOf('(')).trim();
//				
////				System.out.println("Creating synonym for spf: " + symbol);
//				 
//				// Note that if derby.database.sqlAuthorization=true, then only user 'gaiandb' will have exec permission on the spfs...
////				try { userdbStmtWithinUserSchema.execute("drop synonym " + userSchema + "." + symbol); }
////				catch (Exception e) { logger.logDetail("Cannot drop procedure or function synonym " + userSchema + "." + symbol + " (ignored): " + e); }
//				try { userdbStmtWithinUserSchema.execute("create synonym " + userSchema + "." + symbol + " for " + gdbSchema + "." + symbol); synonymsCreated++; }
//				catch (Exception e) { logger.logDetail("Cannot create procedure or function synonym " + userSchema + "." + symbol + " (ignored): " + e); }
//			}

		logger.logInfo("Executed " + updates + " DROP/CREATE updates on procs/funcs under userSchema: " + userSchema + " (in database: " + userDB + ")");
		initialisedUserSchemasForThisDB.add(userSchema);
	}
	
	public static void initialiseUserDB( String userdb ) throws Exception {
		
		Statement stmt = userdbStatementsForViewReloads.get(userdb);
		
		// Initialise the *actual* views and spfs in the gaiandb system schema. User schemas can point to these with synonyms.
		final String schema = GaianDBConfig.getGaianNodeUser().toUpperCase(); // MUST BE UPPER CASE!
		
//		// create gdb system schema if it does not exist
//		boolean isFreshUserDB = !stmt.getConnection().getMetaData().getSchemas(null, schema).next();
////		System.out.println("Schema " + schema + " exists? " + !isFreshUserDB);
//		if ( isFreshUserDB ) stmt.execute("CREATE SCHEMA " + schema);
//		stmt.execute("SET SCHEMA " + schema); // write views under the system schema
		
		Util.executeCreateIfDerbyTableDoesNotExist( stmt, schema, MetricMonitor.PHYSICAL_TABLE_NAME, MetricMonitor.getCreateMetricsTableSQL() );
		Util.executeCreateIfDerbyTableDoesNotExist( stmt, schema, HttpQueryInterface.QUERIES_TABLE_NAME, HttpQueryInterface.getCreateQueriesTableSQL() );
		Util.executeCreateIfDerbyTableDoesNotExist( stmt, schema, HttpQueryInterface.QUERY_FIELDS_TABLE_NAME, HttpQueryInterface.getCreateQueryFieldsTableSQL() );
		
		// Setup the API and utility procedures - note this has already been validated to work when executed at startup - so we don't expect any problems.
		for ( String spflist : new String[] { GaianDBConfigProcedures.GAIANDB_API, GaianDBUtilityProcedures.PROCEDURES_SQL } )
			for ( String sql : spflist.split(";") )
				// note that if derby.database.sqlAuthorization=true, then only user 'gaiandb' will have exec permission on this.
				try { stmt.execute( sql.startsWith("!") ? sql.substring(1) : sql ); }
				catch ( SQLException e ) {} // ignore initial exceptions whereby an spf cannot be dropped when it doesn't exist
		
		logger.logInfo("Successfully initialised stored procedures and functions (SPFs) for userDB: " + userdb + ", under gdbSchema: " + schema);
		
		final Set<String> viewNames = new HashSet<String>( currentLogicalTableViewNames ); // create new set in case currentLogicalTableViewNames changes concurrently 
		final int numViews = viewNames.size();
		
		logger.logImportant("Dropping obsolete views on userdb '" + userdb + "' for "+numViews+" logical tables: " + viewNames);
		
		for ( String lt : viewNames )
			try { dropManagedViews( schema, lt, stmt, null ); }
			catch (Exception e) { logger.logDetail("Cannot drop old views for logical table " + lt + " (ignored): " + e); }
		
		Set<String> failedlogicalTables = new HashSet<String>();
		
		boolean isDerbySchemaPrivacyEnabled = "TRUE".equals( Util.getDerbyDatabaseProperty(stmt, "derby.database.sqlAuthorization") );
		
		for ( String lt : viewNames ) {

			logicalTableHavingViewsLoaded = lt;
			
			synchronized( logicalTableHavingViewsLoaded ) {
			
				// The alias changes every time the view is reloaded to force Derby to re-evaluate the GaianTable VTI meta-data, hence the logical table's definition.
				// Note the alias can be the same for all views as its scope is limited to the query itself.
				String alias = lt;
				
				try { createManagedViews( schema, lt, alias, stmt, null, isDerbySchemaPrivacyEnabled ); }
				catch (SQLException e) {
					failedlogicalTables.add(lt + ": " + e);
					
					// Commented line below: keep the logical table loaded even if the view couldn't be created.
					// A valid reason why the view might not have been created is if it already existed from a previous
					// run of GaianDB and couldn't be dropped at startup due to a dependent view. e.g. view xxx = select * from lt0
	//				currentLogicalTableViewNames.remove(lt); // this view won't have been loaded
				} finally {
					// Always unset logicalTableHavingViewsLoaded at the end of the synchro block to re-enable logging in GaianTable
					logicalTableHavingViewsLoaded = ""; // do not make this null - (cannot synchronize on a null object)
				}
			}
		}
		
		logger.logInfo("Successfully initialised " + (numViews - failedlogicalTables.size()) + '/' + numViews + " LT views for userDB: "
				+ userdb + ", under gdbSchema: " + schema);
		
		if ( !failedlogicalTables.isEmpty() ) {
			String msg = "Unable to initialise views for "
				+failedlogicalTables.size()+" (out of "+numViews+") logical tables for userdb '"+userdb+"' (possibly because other views depend on them): " +
				failedlogicalTables;
			
			throw new Exception(msg);
		}
	}
	
	static void resetUpToDateViews( Statement stmt )  throws Exception {
		
		ResultSet rs;
		
		Set<String> loadedLTs = getLogicalTableNamesLoaded();
		
//		for ( String lt : loadedLTs )
//			logger.logInfo("LTDEF for " + lt + ": " +
//					getLogicalTableRSMD(lt).getColumnsDefinitionExcludingHiddenOnesAndConstantValues() );
		
		if ( 0 < loadedLTs.size() ) {
			Set<String> upToDateViews = new HashSet<String>();
			
			StringBuilder loadedTablesCSV = new StringBuilder();
			for ( String lt : loadedLTs ) loadedTablesCSV.append(",'" + lt + '\'');
			
			loadedTablesCSV.deleteCharAt(0);
			
			String sql = "select tablename,columnname,columndatatype" +
						" from sys.syscolumns, sys.systables st, sys.sysschemas ss where referenceid = tableid" +
						" and tabletype='V' and schemaname='GAIANDB' and st.schemaid=ss.schemaid" +
						" and tablename in (" + loadedTablesCSV + ") order by tablename, columnnumber";
			
			logger.logInfo("Checking views status with: " + sql);
//			System.out.println("Checking views status with: " + sql);

			rs = stmt.executeQuery(sql);
			
//			ResultSet rs2 = conn.getMetaData().getColumns(null, "GAIANDB", null, null);
//			showStartupTime( 0, "INIT after executing getColumns() API" );
			
			Map<String, StringBuilder> vdefs = new HashMap<String, StringBuilder>();
			while ( rs.next() ) {
				String vname = rs.getString(1);
				if ( false == DataSourcesManager.isLogicalTableLoaded(vname) )
					continue; // Should never happen...
				StringBuilder vdef = vdefs.get(vname);
				String colName = rs.getString(2);
//				int colType = rs.getInt(5);
				String colTypeString = rs.getString(3);
//				String colSize = rs.getString(7);
//				String coldef = colName + ' ' + colType + ':' + colTypeName + '(' + colSize + ')';
				String coldef = colName + ' ' + colTypeString;
				if ( null == vdef) vdefs.put(vname, vdef = new StringBuilder( coldef ));
				else vdef.append(", " + coldef);
			}
			for ( String lt : loadedLTs) {
				StringBuilder vdefsb = vdefs.get(lt);
				if ( null != vdefsb ) {
					String vdef = vdefsb.toString();
					String ltDef = getLogicalTableRSMD(lt).getColumnsDefinitionExcludingHiddenOnesAndConstantValues();
					
					ltDef = ltDef.replaceAll(" CHAR\\(1\\)", " CHAR").replaceAll(" INTEGER", " INT").replaceAll(" DECIMAL\\(", " DEC\\(");
					vdef  =  vdef.replaceAll(" CHAR\\(1\\)", " CHAR").replaceAll(" INTEGER", " INT").replaceAll(" DECIMAL\\(", " DEC\\(");
					
					logger.logDetail("Current    LT def for " + lt + ": " + ltDef);
					logger.logDetail("Previous view def for " + lt + ": " + vdef);
					
					if ( vdef.equals(ltDef) ) upToDateViews.add(lt);
				}
			}
				
//			logger.logInfo("Existing LT VIEWS: " + vdefs);
			logger.logInfo("UP-TO-DATE VIEWS (" + upToDateViews.size() + '/' + loadedLTs.size() + "): " + upToDateViews);
			
			// Reset up-to-date views
			synchronized( oldLogicalTableViewNames ) {
				isUsingTableFunctions = GaianDBConfig.isManageViewsWithTableFunctions();
				oldLogicalTableViewNames = upToDateViews;
			}
			
//			for ( String vname : DataSourcesManager.getLogicalTableNamesLoaded() ) {
//				try {
//					logger.logAlways("Getting vmd for: " + vname);
//					GaianResultSetMetaData vmd = new GaianResultSetMetaData(
//							conn.createStatement().executeQuery("select * from "+vname+" where 0=1").getMetaData(), "");
////					GaianResultSetMetaData vmd = new GaianResultSetMetaData(
////						conn.prepareStatement("select * from "+vname+" where 0=1").getMetaData(), "");
//					System.out.println("vname: " + vname);
//					System.out.println("ltdef: " + DataSourcesManager.getLogicalTableRSMD(vname).getColumnsDefinitionExcludingHiddenOnesAndConstantValues() );
//					System.out.println(" vdef: " + vmd.getColumnsDefinitionExcludingHiddenOnesAndConstantValues());
//					
//					if ( vmd.getColumnsDefinitionExcludingHiddenOnesAndConstantValues().
//							equals( DataSourcesManager.getLogicalTableRSMD(vname).getColumnsDefinitionExcludingHiddenOnesAndConstantValues() ) )
//					{
//						System.out.println("LT VIEW IS UP TO DATE: " + vname);
//						continue;
//					}
//				} catch ( Exception e ) { System.out.println("EXCEPTION WHILST CHECKING LTVIEW: " + vname + ": " + e); }
//
//				System.out.println("LT VIEW IS NOT UP TO DATE: " + vname);
//			}
//			
//			showStartupTime( 0, "VIEW RSMDS LOOKUP" );
		}

	}

	private static void dropManagedViews( final String gdbSchema, final String lt, final Statement stmt,
			final String usrdbToDropSynonymsFor ) throws SQLException {

		Collection<String> viewSuffixes = GaianDBConfig.getLogicalTableRequiredViewSuffixes( lt );
		
		for ( Iterator<String> it = viewSuffixes.iterator(); it.hasNext(); ) {
			String symbol = lt + it.next();
			if ( null != usrdbToDropSynonymsFor ) {
				Set<String> userSchemas = userSchemasInitialisedWithSynonyms.get(usrdbToDropSynonymsFor);
				if ( null != userSchemas )
					for ( String userSchema : userSchemas ) // delete associated synonyms in user schemas first
						try { stmt.execute("drop synonym " + userSchema + "." + symbol); }
						catch (SQLException e) { logger.logDetail("Cannot drop view synonym " + userSchema + "." + symbol + " (ignored): " + e); }
			}
			
			stmt.execute("drop view " + gdbSchema + "." + symbol); // drop underlying view
		}
		

		for ( String suffix : new String[] { "", "_" } )
		try { stmt.execute("drop function " + gdbSchema + "." + lt + suffix); }
		catch (Exception e) { logger.logDetail("Cannot drop old logical table function: " + (lt+suffix) + " (ignored): " + e); }
	}
	
	private static void createManagedViews( final String gdbSchema, final String lt, final String alias, final Statement stmt, 
			final String usrdbToCreateSynonymsFor, final boolean isDerbySchemaPrivacyEnabled ) throws SQLException {
		
		Collection<String> viewSuffixes = GaianDBConfig.getLogicalTableRequiredViewSuffixes( lt );
		

		logger.logInfo("Creating "+viewSuffixes.size()+ ( isUsingTableFunctions ? " table functions and" : "" )
				+ " views for logical table: " + lt + ", reload iteration alias: " + alias);
		
		String[] tfNames = isUsingTableFunctions ? new String [] { lt, lt + '_' } : null;
		String[] viewColumnsLists = null;
		
		if ( isUsingTableFunctions ) {
			
			// NOTE:
			// Table Functions are not really suitable here because they require a static table shape...
			// This means that ALL provenance and explain columns will appear in a result, even when they are not requested in the arguments...
			// Their purpose for now is limited: to allow us to switch to table functions if we need to reproduce a problem
			
			String[] functionArguments = new String[] {
					"LTNAME VARCHAR(32672)",
					"LTNAME VARCHAR(32672), LTARGS VARCHAR(32672), LTDEF VARCHAR(32672), FWDINGNODE VARCHAR(32672)"
			};

			GaianResultSetMetaData grsmd = ltrsmds.get(lt);
			
			String[] functionTableShapes = new String[] {
					grsmd.getColumnsDefinitionExcludingHiddenOnesAndConstantValues(),
					grsmd.getColumnsDefinitionExcludingConstantValues()
			};
			
			for ( int i : new int[] {0, 1} ) {
				final String createFunctionSQL =
					"CREATE FUNCTION " + gdbSchema+'.'+tfNames[i] + '(' + functionArguments[i] + ") RETURNS TABLE(" + functionTableShapes[i] + ") "
				+	"PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA NOT DETERMINISTIC READS SQL DATA EXTERNAL NAME 'com.ibm.db2j.GaianTable.queryGaianTable'";
				
				try { stmt.execute( createFunctionSQL ); }
				catch (SQLException e) {
					throw new SQLException( createFunctionSQL + ": " + e );
				}
			}
			
			// Find the lists of columns for the views (only necessary when basing the views on table functions...)
			
			String viewColsPhysical = ltrsmds.get(lt).getPhysicalOrConstantColumnNames();
			
//			System.out.println("LT " + lt + ": " + colsPhysical);
			String viewColsWithProvenance = viewColsPhysical + ", " + GaianDBConfig.GDB_NODE + ", " + GaianDBConfig.GDB_LEAF;
			String viewColsWithExplain = viewColsWithProvenance
				+ ", " + GaianDBConfig.EXPLAIN_FROM + ", " + GaianDBConfig.EXPLAIN_TO + ", " + GaianDBConfig.EXPLAIN_DEPTH
				+ ", " + GaianDBConfig.EXPLAIN_PRECEDENCE + ", " + GaianDBConfig.EXPLAIN_COUNT;

			viewColumnsLists = new String[] { viewColsPhysical, viewColsPhysical, viewColsPhysical, viewColsWithProvenance, viewColsWithExplain, viewColsWithExplain };
		}
		
		for ( Iterator<String> it = viewSuffixes.iterator(),
//			tailOpts = Arrays.asList(ltViewTailOptions).iterator(),
			viewColsIt = isUsingTableFunctions ? Arrays.asList(viewColumnsLists).iterator() : null; it.hasNext(); ) {
			String viewSuffix = it.next();
			String symbol = lt + viewSuffix;
			String tableArgs = GaianDBConfig.getLogicalTableTailOptionsForViewSuffix(viewSuffix); //tailOpts.next();
			String viewCols = isUsingTableFunctions ? viewColsIt.next() : null;
			
			stmt.execute("create view " + gdbSchema + "." + symbol +
					( isUsingTableFunctions ?
					( " as select "+viewCols+" from TABLE(" + tfNames[1] + "('"+lt+"'" + tableArgs + ", null, null)) " + alias ) :
					( " as select * from new com.ibm.db2j.GaianTable('" + lt + "'" + tableArgs + ") " + alias) ) );
		
			// Ensure managed views are public to all users - privacy should be managed at the data source level...
			if ( isDerbySchemaPrivacyEnabled )
				try { stmt.execute("GRANT SELECT ON " + gdbSchema + "." + symbol + " TO PUBLIC"); }
				catch (SQLException e) { /* ignore exceptions - shouldn't occur */ }
			
			if ( null != usrdbToCreateSynonymsFor ) {
				Set<String> userSchemas = userSchemasInitialisedWithSynonyms.get(usrdbToCreateSynonymsFor);
				if ( null != userSchemas )
					for ( String userSchema : userSchemas ) // create associated synonyms in user schemas
						try { stmt.execute("create synonym " + userSchema + "." + symbol + " for " + gdbSchema + "." + symbol); }
						catch (SQLException e) { logger.logDetail("Cannot create view synonym " + userSchema + "." + symbol + " (ignored): " + e); }
			}
		}
	}

	/**
	 * We want to keep a transactional state of all loaded views of logical tables so we can save processing when none have changed.
	 * BUT:
	 * WE CANNOT use the DataSourcesManager.class as a lock on the whole method as the "create view" statements would cause a hang when locking again on
	 * DataSourcesManager.class in GaianTable().
	 * WE CANNOT use a secondary lock object (e.g. synchronized( oldLogicalTableViewNames )) around this whole method as it would also run into the deeper lock
	 * on DataSourcesManager.class in GaianTable (as stated above), which would cause a potential deadlock with threads executing API config procedures.
	 * These API threads would lock on DataSourcesManager first and oldLogicalTableViewNames second when performing a remove() in method reloadLogicalTable().
	 * The solution is to use a secondary lock on a SMALL FRAGMENT of code (WHICH EXCLUDES THE CREATE VIEW STATEMENT) which changes the loaded views state
	 * in isolation. If the subsequent "DROP VIEW" or "CREATE VIEW" operations fail to replicate the state change, we raise warnings...
	 * But fundamentally we should still recover because:
	 * 	1) A temporarily lingering view is not that harmful and cleanup would be attempted again on LT reload or node restart.
	 * 	2) A failed "CREATE" would trigger an LT reload (and thus a view reload attempt) on the next refresh() thanks to: ltConfigDefsForViewReloadChecks.remove(lt)
	 * 
	 * @param stmts
	 * @throws Exception
	 */
	public static void checkUpdateLogicalTableViewsOnAllDBs() throws Exception {
		
//		Synchronize any internal threads calling this method (i.e. GaianNode watchdog) with threads driven by SETXXX() API calls.
//		Note to avoid deadlocks in future - There must be NO outer synchronized calls on other locks.
//		e.g. GaianNode must not synchronize on another lock around the call to this method.
		synchronized ( GaianDBConfigProcedures.class ) {
		
			Set<String> viewsOfChangedLogicalTables = null, addedLogicalTables = null;
			
			synchronized( oldLogicalTableViewNames ) {
				boolean newIsUsingTableFunctions = GaianDBConfig.isManageViewsWithTableFunctions();
				if ( isUsingTableFunctions != newIsUsingTableFunctions ) {
					oldLogicalTableViewNames.clear();
					isUsingTableFunctions = newIsUsingTableFunctions;
				}
				if ( currentLogicalTableViewNames.equals(oldLogicalTableViewNames) ) return;
				
				logger.logDetail("Old lt views: " + oldLogicalTableViewNames);
				logger.logDetail("New lt views: " + currentLogicalTableViewNames);
				
				viewsOfChangedLogicalTables = Util.setDisjunction( currentLogicalTableViewNames, oldLogicalTableViewNames );
				
				// Build set of newly added logical tables
				addedLogicalTables = new HashSet<String>( currentLogicalTableViewNames );
				addedLogicalTables.removeAll(oldLogicalTableViewNames);
				
				logger.logDetail("Newly added logical tables: " + addedLogicalTables);
				
				// Update oldLogicalTableViewNames now. We assume the DROP and CREATE operations will work but it's ok if they don't... (see note above)
				// Must create an independent instance (not attached to the dynamic set of keys for ltrsmds)
				// (currentLogicalTableViewNames will grow implicitly as new ltrsmds are added)
				oldLogicalTableViewNames = new HashSet<String>( currentLogicalTableViewNames );
			}
			
			int numDBsToUpdate = userdbStatementsForViewReloads.size();
			
			// Drop views that don't exist anymore or that are new (i.e. that have changed and have a 'new' definition)
			logger.logImportant("Dropping obsolete views on "+numDBsToUpdate+" derby db(s) for "+
					viewsOfChangedLogicalTables.size()+" logical tables: " + viewsOfChangedLogicalTables);

			final String gdbSchema = GaianDBConfig.getGaianNodeUser().toUpperCase();
			
			for ( String usrdb : userdbStatementsForViewReloads.keySet() ) {
				
				Statement userdbStmt = userdbStatementsForViewReloads.get(usrdb);
				Set<String> allViewsInGaianSchema = new HashSet<String>();
				
				ResultSet rs = userdbStmt.executeQuery(
						"select tablename from sys.systables st, sys.sysschemas ss where tabletype='V' and schemaname='"+gdbSchema+"' and st.schemaid=ss.schemaid" );
				while ( rs.next() ) allViewsInGaianSchema.add( rs.getString(1) );
				rs.close();
				
				logger.logDetail("All current views for schema " + gdbSchema + ": " + allViewsInGaianSchema);
				
				for ( String lt : viewsOfChangedLogicalTables )
					if ( allViewsInGaianSchema.contains(lt) )
						try { dropManagedViews( gdbSchema, lt, userdbStmt, usrdb ); }
						catch (Exception e) { logger.logInfo("Cannot drop old views for logical table " + lt + " (ignored): " + e); }
			}
				
			Set<String> failedlogicalTables = new HashSet<String>();
			
			// Remember how many LTs we are trying to create views for - in case some view creations fail.
			final int numLTsRequiringNewViews = addedLogicalTables.size();
			
			if ( !addedLogicalTables.isEmpty() ) {
				
				viewsReloadIteration++;
				
				for ( String usrdb : userdbStatementsForViewReloads.keySet() ) {

					Statement stmt = userdbStatementsForViewReloads.get(usrdb);
					boolean isDerbySchemaPrivacyEnabled = "TRUE".equals( Util.getDerbyDatabaseProperty( stmt, "derby.database.sqlAuthorization") );
					
					for ( Iterator<String> it = addedLogicalTables.iterator(); it.hasNext(); ) {
						
						String lt = it.next();
						logicalTableHavingViewsLoaded = lt;
						
						// The alias changes every time the view is reloaded to force Derby to re-evaluate the GaianTable VTI meta-data, hence the logical table's definition.
						// Note the alias can be the same for all views as its scope is limited to the query itself.
						String alias = lt+viewsReloadIteration;
						
						synchronized( logicalTableHavingViewsLoaded ) {
							try { createManagedViews( gdbSchema, lt, alias, stmt, usrdb, isDerbySchemaPrivacyEnabled ); }
							catch (Exception e) {
//								e.printStackTrace();
								failedlogicalTables.add( "\nViews NOT CREATED for " + lt + ": " + Util.getStackTraceDigest(e));
								ltConfigDefsForViewReloadChecks.remove(lt); // reload this def in future - (this doesn't invalidate the previously loaded def)
	
								it.remove(); // remove this LT from the added ones as its views could not be created.
								
								// Commented line below: keep the logical table loaded even if the view couldn't be created.
								// A valid reason why the view might not have been created is if it already existed from a previous
								// run of GaianDB and couldn't be dropped at startup due to a dependent view. e.g. view xxx = select * from lt0
//								currentLogicalTableViewNames.remove(lt); // this view won't have been loaded
							} finally {
								// Always unset logicalTableHavingViewsLoaded at the end of the synchro block to re-enable logging in GaianTable
								logicalTableHavingViewsLoaded = ""; // do not make this null - (cannot synchronize on a null object)
							}
						}
					}
				}
			}
			
			logger.logInfo("Successfully created all " + ( isUsingTableFunctions ? "table functions and " : "" )
								+ "views for " + addedLogicalTables.size() + '/' + numLTsRequiringNewViews +
					" logical table(s) (reload iteration " + viewsReloadIteration + "): " + addedLogicalTables);
			
			if ( !failedlogicalTables.isEmpty() ) {
				String msg = "Unable to create/reload views for "
					+failedlogicalTables.size()+" (out of "+numLTsRequiringNewViews+") logical tables (possibly because other views depend on them): " +
					failedlogicalTables; // .toString().replaceAll(",", "\n");
				
				throw new Exception(msg);
			}
		}
	}
	
	public static void refresh() { refresh(null); }
	
	static void refresh( Set<String> ltsToRefresh ) {
		// If Seeker has set the flag to load sources for a new gaian connection, unset this because we'll do it here...
		if ( true == GaianNodeSeeker.testAndClearConfigReloadRequiredFlag() )
			ltsToRefresh = null; // this will check/reload all LTs
		int previousLogLevel = Logger.logLevel;
		GaianDBConfig.assignLogLevel( null );
		refreshLogicalTables( Logger.logLevel != previousLogLevel, ltsToRefresh );
		latestGlobalConfigLoadTime = System.currentTimeMillis();
		cleanAndPreloadDataSources();
	}
	
	public static long getLatestGlobalConfigLoadTime() {
		return latestGlobalConfigLoadTime;
	}
	
	private static GaianDBConfigLogicalTables currentLogicalTablesAndDependentDataSources = null;
	
	/**
	 * This method gets called just after the Derby server has started or when the properties file 
	 * has changed. Any connection info that is found to have changed is updated.
	 * 
	 * @param isLogLevelChanged
	 * @throws Exception
	 */
	private static void refreshLogicalTables( final boolean isLogLevelChanged, Set<String> ltsToCheck ) { //throws Exception {
		
		if ( isLogLevelChanged )
		logger.logAlways( "Log level set to: " + Logger.POSSIBLE_LEVELS[Logger.logLevel] );
		
		Set<String> oldGaianConnections = null == globalGaianConnections ? null : new HashSet<String>( Arrays.asList( globalGaianConnections ) );
		globalGaianConnections = GaianDBConfig.getGaianConnections();
		
		boolean isGaianConnectionsUnchanged = null == oldGaianConnections || new HashSet<String>( Arrays.asList(globalGaianConnections) ).equals(oldGaianConnections);
		
//		Set<String> allLogicalTableNames = GaianDBConfig.getAllLogicalTableNames();
		currentLogicalTablesAndDependentDataSources = GaianDBConfig.getAllLogicalTableDefinitionsAndReferences();
		Set<String> allLogicalTableNames = currentLogicalTablesAndDependentDataSources.getLogicalTables();
		
		if ( null == allLogicalTableNames ) {
			logger.logWarning(GDBMessages.ENGINE_LT_REFRESH_ERROR, "Could not get logical table names from properties file, aborting load operation");
			return;
		}
		
		if ( null == ltsToCheck ) ltsToCheck = allLogicalTableNames;
		logger.logInfo("Checking Logical Tables: " + ltsToCheck);
		
		final int numLTsToCheck = ltsToCheck.size();
		
		// data source id -> data source
		Map<String, VTIWrapper> newDataSourcesOfChangedLTs = new HashMap<String, VTIWrapper>();
		List<String> unchangedLTs = new ArrayList<String>();
		
//		synchronized( oldLogicalTableViewNames ) {
		
			for ( Iterator<String> i=ltsToCheck.iterator(); i.hasNext(); ) { //int i=0; i<allLogicalTableNames.length; i++) {
			
				String logicalTable = i.next();
				Map<String, String> ltNewSignature = null;
				
				if ( isGaianConnectionsUnchanged ) {
					Map<String, String> ltOldSignature = ltConfigSignatures.get(logicalTable);
					ltNewSignature = GaianDBConfig.getLogicalTableStructuralSignature(logicalTable);
					
					if ( ltNewSignature.equals(ltOldSignature) ) {
						logger.logDetail("Logical Table and dependencies unchanged for: " + logicalTable);
						unchangedLTs.add(logicalTable);
						continue;
					}
					
					logger.logInfo("Signatures differ for LT: " + logicalTable +
							", *** OLD: " + ltOldSignature + "; *** NEW: " + ltNewSignature);
				}
				logger.logImportant("Re-loading Logical Table: " + logicalTable);
				
	//			VTIWrapper[] vtiArray = (VTIWrapper[]) vtiArrays.remove(logicalTable);
	//			if ( null != vtiArray ) checkCleanupVTIArray( vtiArray, logicalTable );
				reloadLogicalTable( logicalTable, newDataSourcesOfChangedLTs );
				
				if ( null != ltNewSignature ) ltConfigSignatures.put(logicalTable, ltNewSignature);
				
			} // for all logical tables
			
			// Drop all unloaded, new or changed views - only the unchanged views should remain untouched.
//			dropOldLogicalTableViews( Util.setDisjunction( newLogicalTableViewNames, oldLogicalTableViewNames ) );
//		}
		
		logger.logInfo("Number of updated/reloaded Logical Tables (incl dependencies): " + 
				(numLTsToCheck - unchangedLTs.size()) + '/' + numLTsToCheck + ", skipped: " + (allLogicalTableNames.size()-numLTsToCheck));
		
		// Retain only sets of data sources associated to logical tables that are still defined
		dsArrays.keySet().retainAll(allLogicalTableNames);
		
		// Retain only signatures of tables that are still defined
		ltConfigSignatures.keySet().retainAll(allLogicalTableNames);

		// Keep track of old vtis so they can be closed in order to reclaim their memory (esp if they hold in-mem rows)
		Map<String, VTIWrapper> oldDataSources = new HashMap<String, VTIWrapper>( dataSources );

		// Remove all unchanged data sources under changed logical tables from the set of old data sources
		oldDataSources.keySet().removeAll(newDataSourcesOfChangedLTs.keySet());
		
		// Remove all data sources defined under unchanged LTs from the set of old data sources
		for ( String lt : unchangedLTs )
			for ( Iterator<String> it = oldDataSources.keySet().iterator() ; it.hasNext() ; )
				if ( it.next().startsWith(lt) ) it.remove();
				
		// Now notify the GaianNode to cleanup remaining old data sources
		GaianNode.notifyDataSourcesToClose( oldDataSources.values() );
		
		// Update dataSources structure
		dataSources.keySet().removeAll( oldDataSources.keySet() );

		logger.logThreadInfo("refreshLogicalTables.. cleanup requested for oldDataSources: " + oldDataSources.keySet());
//		logger.logThreadInfo("refreshLogicalTables.. remaining dataSources: " + dataSources.keySet());
	}
	
	public static void cleanAndPreloadDataSources() {
		
		if ( null == currentLogicalTablesAndDependentDataSources ) return;
		
		// Prepare to reload referenced data source ids (e.g. jdbc connection details and file names)
		// Start with jdbc connection ids - so these can update the global static var of allReferencedJDBCConnectionIDs
		Set<String> allReferencedDataSourceIDs = new HashSet<String>();
		
		Map<String,String> sourceIDsOfDiscoveredNodesMappedToConnectionIDs = new Hashtable<String, String>();
		
//		String[] gcs = globalGaianConnections; // GaianDBConfig.getDiscoveredConnections();
		// Add gaian connections to referenced data sources
		for ( int i = 0; i<globalGaianConnections.length; i++ )
			try {
				String cid = globalGaianConnections[i];
				String sourceID = GaianDBConfig.getRDBConnectionDetailsAsString(cid);
				allReferencedDataSourceIDs.add( sourceID );
				
				if ( GaianDBConfig.isDiscoveredConnection(cid) )
					sourceIDsOfDiscoveredNodesMappedToConnectionIDs.put( sourceID, cid );
				
			} catch (Exception e) {
				logger.logWarning(GDBMessages.ENGINE_DS_CLEAN_ERROR, "Unable to get JDBC connection details of Gaian Connection (skipped): " + 
						globalGaianConnections[i] + ": " + e);
			}
		
		// These may be referenced by logical table federated node defs or sub-query source lists.
		allReferencedDataSourceIDs.addAll( GaianDBConfig.getConnectionDefsReferencedByAllSourceLists() );
		
		// Always maintain a connection to the local derby as well
		allReferencedDataSourceIDs.add( GaianDBConfig.getLocalDefaultConnectionID() );
		
		// Add all JDBC connection ids referenced indirectly by all logical tables' data sources.
		allReferencedDataSourceIDs.addAll( currentLogicalTablesAndDependentDataSources.getAllReferencedJDBCConnections() );
		
		// Synch up the jdbc source ids - must hold all (and only all) data source ids gathered so far
		// Doing things with addAll() then retainAll() allows us not to clear the list first, so just the stale values get removed.
		allReferencedJDBCSourceIDs.addAll( allReferencedDataSourceIDs );
		allReferencedJDBCSourceIDs.retainAll( allReferencedDataSourceIDs );
		
		// Finally, add all VTI data source ids (e.g. file names for VTIFile sources etc) indirectly
		// referenced by all logical tables' data sources to the global set of referenced sources.
		allReferencedDataSourceIDs.addAll( currentLogicalTablesAndDependentDataSources.getAllReferencedVTIDataSources() );
		
		synchronized( sourceHandlesPools ) {
		
			// Loop through all source pools, cleaning up data sources that are not referenced anymore (incl asociated in-mem rows objects),
			// and JDBC pools where connections don't appear to be valid - for those that are used for a peer GaianDB connection, drop the entire thing.
			Iterator<String> i = sourceHandlesPools.keySet().iterator();
			while ( i.hasNext() ) {
				String sourceID = (String) i.next();
				String rootSourceID = sourceID.startsWith( INMEM_STACK_KEY_PREFIX ) ? sourceID.substring( sourceID.indexOf(' ')+1 ) : sourceID;
				
				int idx = sourceID.lastIndexOf("'");
				
				if ( !allReferencedDataSourceIDs.contains(rootSourceID) )
					logger.logThreadInfo("Clearing data sources pool for ID that is no longer referenced: " +
							( 0 < idx ? sourceID.substring(0, idx) + "'<pwd>" : sourceID ) ); // don't print the password in the logs
				else if ( !isValidAndActiveSourceHandle(rootSourceID) ) {
					logger.logThreadInfo("Clearing data sources pool due to it holding an invalid/inactive handle, Pool ID: " +
						( 0 < idx ? sourceID.substring(0, idx) + "'<pwd>" : sourceID ) ); // don't print the password in the logs
					String cidOfDiscoveredNode = sourceIDsOfDiscoveredNodesMappedToConnectionIDs.get( rootSourceID );
					if ( null != cidOfDiscoveredNode ) {
						GaianNodeSeeker.lostDiscoveredConnection( cidOfDiscoveredNode );
						continue; // pool is purged and source is removed by lostDiscoveredConnection()
					}
				} else
					continue;
				
				purgeSourcesFromPool( sourceID );
				i.remove(); // This implicitly removes the entire pool entry as well
			}
			
			// Now create a stack pool and 1st connection for new referenced jdbc connection defs
//			System.out.println("Pre-loading pools for: " + Arrays.asList(allReferencedJDBCSourceIDs));
			i = allReferencedJDBCSourceIDs.iterator();
			while ( i.hasNext() ) {
				String cprops = (String) i.next();
				Stack<Object> connectionPool = getSourceHandlesPool( cprops );
				if ( connectionPool.empty() ) {
					// Get a first connection asynchronously - there is no pending query needing one straight away.
					logger.logThreadDetail("Getting 1st jdbc connection asynchronously for " + GaianDBConfig.getConnectionTokens(cprops)[1]);
					getRDBHandleInSeparateThread( cprops, connectionPool );
				}
//					try {
//						getRDBHandleQuickly( cprops, statementsPool );
//						logger.logInfo("Obtained 1st jdbc connection to " + GaianDBConfig.getConnectionTokens(cprops)[1]);
//					} catch (SQLException e) {
//						logger.logWarning("Timeout on 1st jdbc connection to " + GaianDBConfig.getConnectionTokens(cprops)[1] + ": " + e);
//					}
				logger.logThreadDetail("Pool size at reload for " + GaianDBConfig.getConnectionTokens(cprops)[1] + ": " + connectionPool.size());
			}
		}
	}
	
//	static void primeJDBCSourceHandlesPoolSynchronously( String connectionID ) throws Exception {
//		String connectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString( connectionID );
//		Stack<?> connectionPool = getSourceHandlesPool( connectionDetails, false );
////		System.out.println("Connection details: " + connectionDetails);
//		if ( connectionPool.empty() ) {
//			// Get a first connection synchronously
//			GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) )
//						.getConnectionToPool( (Stack<Connection>) connectionPool );	
//		}
//	}
	
	private static boolean isValidAndActiveSourceHandle( String sourceID ) {
		
		if ( -1 < sourceID.indexOf("jdbc:neo4j://") ) return true;
		
		Stack<Object> pool = sourceHandlesPools.get( sourceID );
				
		if ( pool.empty() ) return true; // Empty pool is fine... we'll try to prime it later
		Object o = pool.peek();
		
		if ( o instanceof Connection ) return Util.isJDBCConnectionValid((Connection) o);
		else if ( o instanceof GaianChildVTI )
			try { return ((GaianChildVTI) o).reinitialise(); }
			catch (Exception e) { logger.logThreadInfo("Unable to reinitialise VTI data source (pool invalidated - will be cleared): " + e); return false; }
		else return true; // Other types of source handles should not exist - but we assume them to be active for now...
	}
	
	/**
	 * Currently only called to clear a stack pool of RDBMS connections
	 */
	static void clearSourceHandlesStackPool( String srcIDToRemove ) {
		
		synchronized( sourceHandlesPools ) {
			
			// First close and clean up data sources that are not referenced anymore, along with any associated in-mem rows objects as well...
			Iterator<String> i = sourceHandlesPools.keySet().iterator();
			while ( i.hasNext() ) {
				String sourceID = (String) i.next();
				String rootSourceID = sourceID.startsWith( INMEM_STACK_KEY_PREFIX ) ? sourceID.substring( sourceID.indexOf(' ')+1 ) : sourceID;
				
				if ( srcIDToRemove.equals(rootSourceID) ) {
					int idx = sourceID.lastIndexOf("'");
					logger.logInfo("Removing Data Source pool and closing sources for Source ID: " +
							( 0 < idx ? sourceID.substring(0, idx) + "'<pwd>" : sourceID )); // don't print the password in the logs
					purgeSourcesFromPool( sourceID );
					i.remove(); // This implicitly removes the entire pool entry as well
					break;
				}
			}
		}
	}
	
	private static void purgeSourcesFromPool( final String sourceID ) {
		
		Stack<Object> pool = sourceHandlesPools.get( sourceID );
		
		if ( ! pool.empty() )
			try {
				if ( pool.peek() instanceof Connection ) {

					final int idx = sourceID.lastIndexOf("'");
					final String idNoPwd = 0 < idx ? sourceID.substring(0, idx) + "'<pwd>" : sourceID;

					// close() jdbc connection in a separate thread in case they hang...
					// however - clear pool outside of thread so it can be reused quickly...
					final Connection[] connections = (Connection[]) pool.toArray(new Connection[0]);
					pool.clear(); // overridden by RecallingStack, clears all extra references to Connections in HashSet
					allReferencedJDBCSourceIDs.remove(sourceID); // we no longer have this connection loaded
					logger.logInfo("Emptied connection pool for " + idNoPwd + ", connections to close: " + connections.length);

					// Try to close() all connections in the pool in a separate thread so as not to hang
					new Thread("Connections purger for " + idNoPwd) {
						public void run() {
							try {
								int count = 0;
								for ( Connection c : connections ) {
									if ( null != c ) {
										VTIRDBResult.clearPreparedStatementsCacheForConnection(c);
										c.close(); count++;
									}
								}
								logger.logInfo("Closed "+count+"/"+connections.length+" connections from purged pool for id: " + idNoPwd);
							} catch (Exception e) {
								logger.logWarning(GDBMessages.ENGINE_JDBC_CONN_CLOSE_ERROR, "Unable to close() JDBC Connection (aborting purge): " + e);
							}
						}
					}.start();

				} else {
					while ( !pool.empty() )
						try { GaianChildVTI vti = (GaianChildVTI) pool.pop(); if ( null != vti ) vti.close(); }
						catch (Exception e) { logger.logException( GDBMessages.ENGINE_CHILD_CLOSE_ERROR, "Unable to close GaianChildVTI, cause: ", e); }
					pool.clear(); // overridden by RecallingStack, clears all extra references to Connections in HashSet
				}
						
			} catch ( EmptyStackException e ) {}
	}
	
	static synchronized void unloadAllDataSourcesAndClearConnectionPoolForGaianConnection( String connectionID ) {
		
		logger.logInfo("******* Unloading all data sources for Gaian Connection: " + connectionID); // can't also lookup/log node id - as it cdve been deleted from config
		
		ArrayList<VTIWrapper> orphans = new ArrayList<VTIWrapper>();
		
		for ( String ltName : dsArrays.keySet() ) {
			String nodeName = ltName + '_' + connectionID;
			if ( null == dataSources.remove( nodeName ) )
				logger.logInfo("Unexpectedly missing node for removal from dataSources map (ignored): " + nodeName);
			VTIWrapper[] oldWrappers = dsArrays.get(ltName);
			if ( null == oldWrappers || 0 == oldWrappers.length )
				break; // nothing to unload - this may happen if the seeker unloads just before the connection checker
			VTIWrapper[] newWrappers = new VTIWrapper[ oldWrappers.length-1 ];
			System.arraycopy(oldWrappers, 0, newWrappers, 0, newWrappers.length);
			VTIWrapper candidateOrphan = oldWrappers[oldWrappers.length-1];
			
			if ( nodeName.equals( candidateOrphan.getNodeDefName() ) )
				orphans.add( candidateOrphan );
			else // search for unwanted node to swap out for orphan
				for ( int i = newWrappers.length-1; i>=0; i-- ) // go backwards because gaian connection data sources are at the end
					if ( null != newWrappers[i] && nodeName.equals( newWrappers[i].getNodeDefName() ) ) {
						orphans.add( newWrappers[i] ); newWrappers[i] = candidateOrphan; break;
					}
			
			if ( 0 < orphans.size() ) // only change dsArrays if the node to be removed was actually there...
				dsArrays.put(ltName, newWrappers);
			
			// Also remove transient gateway data sources
			for ( Iterator<String> it = transientDataSources.keySet().iterator(); it.hasNext(); ) {
				String dsWrapperName = it.next();
				if ( dsWrapperName.startsWith(GAIAN_NODE_PREFIX+'_'+GATEWAY_PREFIX+'_'+nodeName) ) {
					orphans.add( transientDataSources.get(dsWrapperName) );
					it.remove(); // Also removes the entry in the map
				}
			}
		}
		
		// Also remove transient sub-query data sources
		for ( Iterator<String> it = transientDataSources.keySet().iterator(); it.hasNext(); ) {
			String dsWrapperName = it.next();
			if ( dsWrapperName.startsWith(GAIAN_NODE_PREFIX+'_'+SUBQUERY_PREFIX+'_'+connectionID) ){
				orphans.add( transientDataSources.get(dsWrapperName) );
				it.remove(); // Also removes the entry in the map
			}
		}
				
		try { clearSourceHandlesStackPool( GaianDBConfig.getRDBConnectionDetailsAsString(connectionID) ); }
		catch (Exception e) {
			logger.logWarning( GDBMessages.ENGINE_GAIAN_CONN_UNLOAD_ERROR,
					"Unable to clear source handles pool for gaian connection " + connectionID + " (ignored), cause: " + e );
		}
		
		// Remove cid from globalGaianConnections - note we cannot assume GaianDBConfig is up to date yet 
		List<String> gcs = new ArrayList<String>( Arrays.asList(globalGaianConnections) );
		gcs.remove(connectionID);
		globalGaianConnections = (String[]) gcs.toArray( new String[0] );
		
		logger.logInfo("Orphaned Gaian Node Wrappers (for gc): " + orphans);
		orphans.clear(); // release the structure for garbage collection..
		
		latestGlobalConfigLoadTime = System.currentTimeMillis();
	}
	
	static synchronized void loadAllDataSourcesForNewGaianConnections() {
		
		// Only deal with added GaianDB nodes...
		String[] newGaianConnections = GaianDBConfig.getGaianConnections();
		
		Set<String> addedgcs = new HashSet<String>(Arrays.asList(newGaianConnections));
		addedgcs.removeAll( Arrays.asList(globalGaianConnections) );
		
		if ( addedgcs.isEmpty() ) return;
		
		// Reset globalGaianConnections
		globalGaianConnections = newGaianConnections;
		
		logger.logThreadInfo("************** Growing all logical tables with Gaian Node source wrappers: " + addedgcs);

		// DO NOT USE GaianDBConfig.getAllLogicalTableNames() as it might contain tables that failed to load. Use dsArrays.keySet() instead.
//		Set<String> allLogicalTableNames = GaianDBConfig.getAllLogicalTableNames();
		
		try {
			for ( String connectionID : addedgcs ) {
				
				logger.logThreadInfo("******* Adding Gaian Node source wrapper for connection " + connectionID + 
						" to all loaded Logical Tables: " + dsArrays.keySet());
				
				for ( String ltName : dsArrays.keySet() ) {
					
					GaianResultSetMetaData ltrsmd = (GaianResultSetMetaData) ltrsmds.get( ltName );
					VTIWrapper[] oldWrappers = dsArrays.get(ltName);
					
					if ( null == ltrsmd || null == oldWrappers ) // Defensive programming - this should not happen
						continue; // Ignore this logical table as it is not properly loaded
					
					String nodeName = ltName + "_" + connectionID;
					VTIWrapper dataSource = null;
					
					logger.logThreadDetail("******* Adding/loading data source wrapper " + nodeName + " for Logical Table: " + ltName);
					
					try {
						String sourceID = GaianDBConfig.getRDBConnectionDetailsAsString( connectionID );
						dataSource = new VTIRDBResult( sourceID, nodeName, ltrsmd ); // no point trying to re-use an existing dsWrapper as this isnt a case of ltrsmd changing
						allReferencedJDBCSourceIDs.add( sourceID );
					} catch (Exception e) {
						logger.logThreadWarning( GDBMessages.ENGINE_CONN_DS_LOAD_ERROR, nodeName + " Unable to load gaian connection data source, cause: " + e );
						continue;
					}
					
					dataSources.put(nodeName, dataSource);
					
					VTIWrapper[] newWrappers = new VTIWrapper[ oldWrappers.length + 1 ];
					System.arraycopy(oldWrappers, 0, newWrappers, 0, oldWrappers.length);
					newWrappers[ newWrappers.length-1 ] = dataSource;
					dsArrays.put( ltName, newWrappers );
				}
			}
		} catch ( Exception e ) {
			logger.logThreadException(GDBMessages.ENGINE_LT_REFRESH_ERROR, "Unable to load all new Gaian Node data sources - aborting", e);
		}
		
		latestGlobalConfigLoadTime = System.currentTimeMillis();
	}
	
	
//	public static synchronized void removeLogicalTable( String ltName ) {
//		vtiArrays.remove( ltName );
//	}
	
//	public static synchronized void reloadLogicalTable( String ltName ) {
//		// Just load new sources alongside old ones - but what of keeping in sync with the config file ?
//		reloadLogicalTable( ltName, dataSources ); 
//	}
	
	/**
	 * Reload logical table, re-using existing data sources where they haven't changed.
	 * Load new data sources into newDataSources so that the caller can know which ones are no longer used.
	 * We don't attempt to do this when setting a new table def in the API but rather as part of a global 
	 * refresh from config so that we stay in sync with the config file.
	 * 
	 * @param ltName
	 * @param newDataSources
	 */
	private static synchronized void reloadLogicalTable( String ltName, Map<String,VTIWrapper> newDataSources ) {
		
		String physicalColsDef = GaianDBConfig.getLogicalTableDef(ltName);
		String specialColsDef = GaianDBConfig.getSpecialColumnsDef(ltName);
		
		String ltOldDef = ltConfigDefsForViewReloadChecks.get(ltName);
		String ltNewDef = GaianDBConfig.getLogicalTableViewSignature(ltName);
		
		if ( null == ltNewDef ) {
			logger.logInfo("Removing ltrsmd entry for removed Logical Table: " + ltName);
			ltrsmds.remove( ltName );
			ltConfigDefsForViewReloadChecks.remove( ltName ); // triggers a reload of the ltrsmd and view if the LT is re-created in future.
			return;
		}
		
		GaianResultSetMetaData ltrsmd = null;
		
//		if ( null == ltrsmd || ! ltrsmd.wasBuiltFrom(physicalColsDef, specialColsDef) ) {
		if ( ! ltNewDef.equals(ltOldDef) ) {
			logger.logInfo("Logical Table definition changed for: " + ltName + ", ltNewDef = " + ltNewDef );

			// This view will need reloading as the DEF has changed, so pretend it was never loaded
			oldLogicalTableViewNames.remove(ltName);
			
			try {
				ltrsmd = new GaianResultSetMetaData( physicalColsDef, specialColsDef );
			} catch (Exception e) {
				logger.logWarning(GDBMessages.ENGINE_LT_META_DATA_LOAD_ERROR, ltName + " Could not load table meta data so cannot reload table, cause: " + e);
//				e.printStackTrace();
				ltrsmds.remove( ltName );
				return;
			}
			//		if ( null == ltrsmd ) throw new Exception("No definition found for Logical Table: " + ltName);
			ltrsmds.put( ltName, ltrsmd );
			ltConfigDefsForViewReloadChecks.put(ltName, ltNewDef); // don't reload this view in future if it hasn't changed
			
		} else {
			logger.logInfo("Logical Table definition unchanged for: " + ltName);
			ltrsmd = (GaianResultSetMetaData) ltrsmds.get( ltName );
		}
		
		logger.logInfo( ltName + " logical table ResultSetMetaData: " + ltrsmd );
		
		String[] nodeDefNames = (String []) GaianDBConfig.getDataSourceDefs(ltName).toArray( new String[0] );
		int numNodes = null == nodeDefNames ? 0 : nodeDefNames.length;

		VTIWrapper dataSource = null;
		VTIWrapper[] vtiArray = new VTIWrapper[ numNodes + globalGaianConnections.length ];

		logger.logInfo( ltName + " loading global gaian connections: " + Arrays.asList( globalGaianConnections ) + ", length " + (vtiArray.length - numNodes));
		for (int j=numNodes; j<vtiArray.length; j++) {

			String gaianConnectionId = globalGaianConnections[j-numNodes];
			// We can re-use the gaian connection to become any node on this logical table.
			String nodeName = ltName + "_" + gaianConnectionId;
			
			try {
				String sourceID = GaianDBConfig.getRDBConnectionDetailsAsString( gaianConnectionId );
				
				if ( null == (dataSource = reinitialiseDataSourceWrapperOrDiscardIfEndpointChanged(nodeName, sourceID, ltrsmd)) )
					dataSource = new VTIRDBResult( sourceID, nodeName, ltrsmd );
				
//				allReferencedJDBCSourceIDs.add( sourceID );
//				allReferencedDataSourceIDs.add( sourceID );
				newDataSources.put( nodeName, dataSource );
				
			} catch (Exception e) {
				logger.logWarning( GDBMessages.ENGINE_CONN_DS_LOAD_ERROR, nodeName + " Cannot load gaian connection data source, cause: " + e );
				continue;
			}

			vtiArray[j] = dataSource;
		}
		
		logger.logInfo( ltName + " physical nodes to be loaded: " +
				( 0 == numNodes ? "None" : "" + Arrays.asList( nodeDefNames )) );
		for (int j=0; j<numNodes; j++) {
		
			String nodeDefName = nodeDefNames[j];

			try {
				dataSource = loadDataSource( ltName, nodeDefName );			
			} catch ( Exception e ) {
				String msg = "Unable to load data source " + nodeDefName + ": " + e;
//				System.out.println(msg);
				logger.logThreadWarning( GDBMessages.ENGINE_DS_LOAD_ERROR, msg );
				continue;
			}
			
			newDataSources.put( nodeDefName, dataSource );
			vtiArray[j] = dataSource;
			
		} // for all nodes defs of a logical table

//		logger.logInfo("Number of gaian nodes for " + ltName + ": " + numGaianNodesForThisLogicalTable);
//		if ( 0 == i || minGaianNodesInAnyLogicalTable > numGaianNodesForThisLogicalTable )
//			minGaianNodesInAnyLogicalTable = numGaianNodesForThisLogicalTable;
		
		// Note here! it may be better to get the previous entry that may have existed for ltName out first and empty its array to facilitate garbage collection?
		dsArrays.put( ltName, vtiArray );
	}
	
	/**
	 *  Load data source in dataSources structure, and assign it to a logical table dsArrays list. This will make it active.
	 *   
	 * @param ltName
	 * @param nodeDefName
	 * @throws Exception
	 */
	static synchronized void refreshDataSource( String ltName, String nodeDefName ) throws Exception {

		VTIWrapper dataSource = loadDataSource( ltName, nodeDefName );
		List<VTIWrapper> ltDataSources = new ArrayList<VTIWrapper>( Arrays.asList( dsArrays.get( ltName ) ) );
		
		if ( false == ltDataSources.contains( dataSource ) ) {
			ltDataSources.add( dataSource );
			
			// Note here! it may be better to get the previous entry that may have existed for 
			// ltName out first and empty its array to facilitate garbage collection?
			dsArrays.put( ltName, (VTIWrapper[]) ltDataSources.toArray( new VTIWrapper[0] ) );
		}

		// Update the lt signature (to avoid reload of config) and the latest load timestamp (to avoid reload of ds list in GaianTable)
		ltConfigSignatures.put(ltName, GaianDBConfig.getLogicalTableStructuralSignature(ltName));
		latestGlobalConfigLoadTime = System.currentTimeMillis();
	}
	
	// Not used because there could be properties in the config file overriding the transient ones -
	// so a full refresh will be invoked anyway in persistAndApplyConfigUpdates() after persisting the removal of the overriding properties.
//	static synchronized void removeDataSource( String ltName, String nodeDefName ) throws Exception {
//
//		VTIWrapper dataSource = dataSources.remove(nodeDefName);
//		if ( null == dataSource ) throw new Exception("Unknown data source ID: " + nodeDefName);
//		List<VTIWrapper> ltDataSources = new ArrayList<VTIWrapper>( Arrays.asList( dsArrays.get( ltName ) ) );
//		ltDataSources.remove( dataSource );
//		dsArrays.put( ltName, (VTIWrapper[]) ltDataSources.toArray( new VTIWrapper[0] ) );
//
//		latestGlobalConfigLoadTime = System.currentTimeMillis();
//	}
	
	private static VTIWrapper loadDataSource( String ltName, String nodeDefName ) throws Exception { //GaianResultSetMetaData ltrsmd, VTIWrapper[] vtiArray ) {
		
		VTIWrapper vti = null;
		String sourceID = null;
		GaianResultSetMetaData ltrsmd = (GaianResultSetMetaData) ltrsmds.get(ltName);
		
		if ( null == ltrsmd )
			throw new Exception("Logical Table is not defined: " + ltName);
		
		if ( GaianDBConfig.isNodeDefRDBMS( nodeDefName ) ) { // Note even VTIs can use a DBMS connection (in which case they are invoked via derby)
							
//			if ( isGaian ) numGaianNodesForThisLogicalTable++;
			boolean isGaian = GaianDBConfig.isNodeDefGaian( nodeDefName );
			logger.logInfo( nodeDefName + " is a VTIWrapper RDB Node, isGaian = " + isGaian );
			
			sourceID = GaianDBConfig.getRDBConnectionDetailsAsString( nodeDefName ); // exception if connection not defined
			
			// Check that this Gaian node isn't already defined as a global gaian node in an existing vti already - if so skip this def.
			if ( isGaian )
				for (int i=0; i<globalGaianConnections.length; i++) {

					String nodeName = ltName + "_" + globalGaianConnections[i];
					VTIWrapper gcvti = (VTIWrapper) dataSources.get(nodeName);
					
					if ( null != gcvti && gcvti.isBasedOn( sourceID ) )
						throw new Exception( nodeDefName + " is a duplicate gaian data source for global source: " + nodeName + ", ignoring it." );
				}
//				for (int k=numNodes; k<vtiArray.length; k++)
//					if (((VTIWrapper) vtiArray[k]).isBasedOn( sourceID )) {
//						logger.logInfo( nodeDefName + " is already included in the global gaian connections list, ignoring it.");
//						continue;
//					}
				
//				if ( !connections.containsKey( cprops ) ) {
//					String[] props = GaianDBConfig.getConnectionTokens( cprops );
//					Connection c = null;
//					// try to get a connection now... if this doesn't work, get it later
//					try { c = GaianDBConfig.getDBConnection( props ); } catch ( SQLException e ) {}
//						
//					// If this is a connection to a Derby database, then we want the ability to know when any tables
//					// under it were modified. So we create a stored procedure on this database that will call code in
//					// here to tell us when a certain table has changed.
//					
////					if ( props[1].startsWith( 
////							props[1].startsWith("jdbc:derby://") ? "jdbc:derby://localhost:" + GaianDBConfig.getDerbyServerListenerPort() : "jdbc:derby:") ) {
////						
////						Statement stmt = getRDBStatement( cprops );
////						
////						String procedureSQL = "DROP PROCEDURE GAIANDB_TABLE_CHANGED";
////						logger.logInfo( nodeDefName + " Dropping procedure using SQL: " + procedureSQL);
////						try { stmt.execute(procedureSQL); } catch ( SQLException e ) {}
////						
////						procedureSQL = 
////							"CREATE PROCEDURE GAIANDB_TABLE_CHANGED(IN NODE_NAME VARCHAR(20)) " + 
////							"PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'com.ibm.gaiandb.DataSourcesManager.setTableChangedFlag'";
////					
////						logger.logInfo( nodeDefName + " Creating procedure using SQL: " + procedureSQL);
////						
////						stmt.execute( procedureSQL );	
////					}
//					
//					connections.put( cprops, c );
//				}
			
			if ( null == (vti = reinitialiseDataSourceWrapperOrDiscardIfEndpointChanged(nodeDefName, sourceID, ltrsmd)) )
				vti = new VTIRDBResult( sourceID, nodeDefName, ltrsmd );
			
//			allReferencedJDBCSourceIDs.add( sourceID );
			
		} else if ( GaianDBConfig.isNodeDefVTI( nodeDefName ) ) { // Call a VTIWrapper directly... this assumes it can deal with qualifiers and projected columns
			
			String vtiClassName = GaianDBConfig.getNodeDefVTI( nodeDefName );
			
			try {
				// Do VTIWrapper specific processing
				
				String vtiArgs = GaianDBConfig.getVTIArguments(nodeDefName);
				if ( null == vtiArgs ) //|| 0 == vtiArgs.length )
					throw new Exception("Undefined ARGS property for Node " + nodeDefName);
				
				if ( GaianNode.getClassUsingGaianClassLoader(vtiClassName).getName().equals( FileImport.class.getName() ) ) {

					sourceID = vtiArgs;
					// This is the FileImport VTIWrapper
					logger.logInfo( nodeDefName + " is a VTIWrapper File Node, file name def = " + sourceID );

					if ( null == (vti = reinitialiseDataSourceWrapperOrDiscardIfEndpointChanged(nodeDefName, sourceID, ltrsmd)) )
						vti = new VTIFile( sourceID, nodeDefName, ltrsmd );

				} else {
					// Basic vti plugin - no stack pooling, no in memory rows, no overloaded data sources
					// Just a straight call to the underlying vti
					// If the vti implements NonRelationalResultRows, then qualifiers and columns mapping
					// will be applied to it.
					
					sourceID = vtiClassName + ':' + vtiArgs; //VTIBasic.derivePrefixArgFromCSVArgs( vtiArgs );
					if ( null == (vti = reinitialiseDataSourceWrapperOrDiscardIfEndpointChanged(nodeDefName, sourceID, ltrsmd)) )
						vti = new VTIBasic( vtiClassName, nodeDefName, ltrsmd );
				}
			} catch ( ClassNotFoundException e ) {
				throw new Exception( "VTI class not found: " + vtiClassName );
			}
			
		} else {
			throw new Exception( "No defined RDB CONNECTION or VTI property for this data source" );
		}
		
//		if ( null != sourceID )
//			allReferencedDataSourceIDs.add( sourceID );
		
		dataSources.put(nodeDefName, vti);
		return vti;
	}
	
	/**
	 * Tries to get a cached copy of previously derived meta data for a table definition.
	 * If it doesn't exist, builds the meta data from the table definition.
	 * 
	 * The table modifiers are parameters which impact the meta data, e.g. 'with_provenance'
	 * which specifies that hidden columns be included in the meta data.
	 * 
	 * @param tableDefinition
	 * @param tableModifiers
	 * @return
	 * @throws Exception
	 */
	public static GaianResultSetMetaData deriveMetaDataFromTableDef(
			String tableDefinition, String tableName, String tableModifiers ) throws Exception {
		
		// Note tableDefinition holds all visible columns from propagating node's definition.
		// This includes its constant columns. Therefore we only get hidden columns as special columns.
		GaianResultSetMetaData metaData = (GaianResultSetMetaData) derivedMetaData.get( tableDefinition+tableModifiers );
		if ( null == metaData ) {
			String[] physicalAndConstantCols = GaianDBConfig.getColumnsDefArray(tableDefinition); //.toUpperCase()); // need to preserve case...
			
			StringBuffer physicalColsDefSB = new StringBuffer();
			StringBuffer constantColsDefSB = new StringBuffer();
			
			for (int i=0; i<physicalAndConstantCols.length; i++) {
				
				String colDef = physicalAndConstantCols[i];
				
				String[] tokens = Util.splitByTrimmedDelimiter( colDef, ' ' );
				logger.logInfo("Tokens are: " + Arrays.asList(tokens));
				
				// Is this a constant column definition ? If so it will have 3 tokens.
				if ( 3 > tokens.length )
					// physical cols come first, so just check the value of i to know whether to insert a comma
					physicalColsDefSB.append( (0<i ? "," : "") + colDef );
				else
					// always append a comma at the end as we'll be appending hidden cols after anyway
					constantColsDefSB.append( colDef + "," );
			}
			
			String constantColsDef = constantColsDefSB.toString();
			String hiddenColsDef = GaianDBConfig.getHiddenColumns();
			
			String specialColsDef = 0 == constantColsDef.length() ? 
					hiddenColsDef : constantColsDef + hiddenColsDef; // no need for a comma as already appended above
			
			metaData = new GaianResultSetMetaData( 
					physicalColsDefSB.toString(), specialColsDef, GaianDBConfig.getConstantColumnsDef( tableName ) );
			
			derivedMetaData.put( tableDefinition+tableModifiers, metaData );
		}
		return metaData;
	}
	
	/**
	 * Tries to get a cached copy of previously derived meta data for a subquery.
	 * If it doesn't exist, uses a prepared statement to obtain the meta data.
	 * 
	 * The result modifiers are parameters which impact the meta data, e.g. 'with_provenance'
	 * which specifies that hidden columns be included in the meta data.
	 * 
	 * @param sqlQuery
	 * @param targetCID RDBMS Connection ID; or concatenated connection details in format: 'url'usr'pwd
	 * @param resultModifiers
	 * @return
	 * @throws Exception
	 */
	public static GaianResultSetMetaData deriveMetaDataFromSubQuery( String sqlQuery, final String targetCID, final String resultModifiers ) throws Exception {
		// Note we only call this method to find meta data for the querying node.
		// Constant and Hidden column definitions are in the special columns when building the GaianResultSetMetaData.
		String cdetails = "";
		try { cdetails = GaianDBConfigProcedures.getRDBConnectionDetailsAsString( targetCID ); }
		catch (Exception e) {
			String errmsg = "Unable to resolve name as a Connection ID: " + targetCID + ": " + e;
			logger.logWarning(GDBMessages.CONFIG_LT_SET_RDBT_ERROR, errmsg);
			throw new Exception(errmsg);
		}
		RDBProvider provider = RDBProvider.fromGaianConnectionID(cdetails);
		
		sqlQuery = sqlQuery.trim();
		GaianResultSetMetaData metaData = (GaianResultSetMetaData) derivedMetaData.get( sqlQuery+targetCID+resultModifiers );
		if ( null == metaData || false == metaData.hasRetainedResult() ) {

			String first7chars = 7 < sqlQuery.length() ? sqlQuery.substring(0, 7).toLowerCase().replaceAll("\\s", " ") : null;
			
			if ( null == first7chars )
				throw new Exception("SQL query string is null");
			
			// Try to cover as many possible RDBMS provider SQL keywords as possible - so we have capability to pass through as many statements as possible
			boolean isSelectExpression = first7chars.equals("select ") || first7chars.startsWith("with ");
			boolean isStoredProcedureCall = first7chars.startsWith("call ") || first7chars.startsWith("exec") || -1 < cdetails.indexOf("jdbc:neo4j");
			boolean isOtherStatementReturningAResultSet = first7chars.startsWith("values") || first7chars.equals("xquery ");
			
//			if ( first7chars.startsWith("drop ") || "create ".equals(first7chars) || 
//				"delete ".equals(first7chars) || "insert ".equals(first7chars) || "update ".equals(first7chars) ) {
				
			if ( !isSelectExpression && !isStoredProcedureCall && !isOtherStatementReturningAResultSet ) {
			
				// This may be a drop/create/delete/insert/update/alter/merge/etc...
				// Just create a basic meta-data object with an UPDATE_COUNT column and an unset value for it
				metaData = new GaianResultSetMetaData("UPDATE_COUNT INT", GaianDBConfig.getSpecialColumnsDef(null));

			} else {

				Stack<Object> connectionPool = getSourceHandlesPool( cdetails );
				Connection c = getPooledJDBCConnection( cdetails, connectionPool );
				
				try { // Encapsulate all this in a try block to ensure RDBMS connection is recycled even if an exception occurs. 
					
					if ( isStoredProcedureCall ) {
											
						boolean isProcedureMightReturnResultSet = true; // assume we *HAVE* to execute the procedure locally to derive meta-data.
						
						if ( -1 < cdetails.indexOf("jdbc:neo4j") )
							isProcedureMightReturnResultSet = -1 < sqlQuery.toLowerCase().indexOf(" return ");
						else {
						
							int i1 = sqlQuery.indexOf(' '), i2 = sqlQuery.indexOf('(');
							
							if ( 0 < i1 && i1 < i2 ) {
								String procName = sqlQuery.substring(i1, i2).trim().toUpperCase();
								short sdef = -1;
								if ( -1 == cdetails.indexOf("jdbc:derby") ) {
									// Note that DatabaseMetaData is an unreliable (often incomplete) object
									// For RDBMS such as DB2 and Oracle, we should have the procedures information, but not some of the smaller ones (e.g. Derby)
									DatabaseMetaData dmd = c.getMetaData();
									ResultSet procsRS = dmd.getProcedures(null, null, procName);
									if ( !procsRS.next() ) logger.logInfo("No procedure found with name: " + procName);
									else sdef = procsRS.getShort("PROCEDURE_TYPE");
									procsRS.close();
									String rdef = sdef == DatabaseMetaData.procedureNoResult ? "NO RESULT"
											: sdef == DatabaseMetaData.procedureReturnsResult ? "RETURNS RESULT"
											: sdef == DatabaseMetaData.procedureResultUnknown ? "RESULT UNKNOWN" : "UNMAPPED";
									logger.logImportant("Derived Return Capability for back-end DB Procedure: " + procName + 
											", return capability: " + sdef + " = " + rdef);
									isProcedureMightReturnResultSet = sdef != DatabaseMetaData.procedureNoResult;
								}
								else
									isProcedureMightReturnResultSet = GaianNode.isProcedureMightReturnResultSet(procName);
							}
						}
						
						if ( false == isProcedureMightReturnResultSet ) {
							// This procedure has no return schema - so no need to execute the procedure to derive meta data...
							// This avoids possble side effects here.
							// Unfortunately, for other procedures which DO return a result set, there may be side-effects which may not be
							// desired if the outer GaianQuery() was targeted against other remote nodes only...
							
							// TODO: True solution would be to push procedure to other nodes within the context of the GaianQuery.getMetaData() call...
							// 		 However we dont know the predicates nor the orginal sql yet so we dont know what nodes it should run on!
							// 		 For now we just have to say we don't support targeting procedures at specific nodes if they return a result set...,
							// 		 (... unless the return definition itself is specified as an argument to GaianQuery!...)
							metaData = new GaianResultSetMetaData("UPDATE_COUNT INT", GaianDBConfig.getSpecialColumnsDef(null));
							
						} else {
							
							logger.logInfo("Executing SQL procedure to get RSMD (keeping result): " + sqlQuery);
							Statement s = c.createStatement();
							
							s.execute(sqlQuery);
							
							ResultSet rs = s.getResultSet();
		
//							if ( null != rs ) {
//								try {
//									logger.logThreadInfo("Statements s: " + s + ", rs.getStatement(): " + rs.getStatement());
//									logger.logThreadInfo("###################### Connection off Statement isClosed(): " + 
//											(s.getConnection()).isClosed() );
//									logger.logThreadInfo("###################### Connection off ResultSet.getStatement() isClosed(): " + 
//											(rs.getStatement().getConnection()).isClosed() );
//								} catch (SQLException e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//							}
							
							// Need to ensure the underlying query is actually executed before we exist this method and start extracting rows from a non-executed RS
							// because otherwise, Derby will see the execution of the outer query before that of the inner, and won't allow fetching of the inner before that of the outer... i.e. deadlock
//							Thread.sleep(10);
							
							String dsWrappperID = SUBQUERY_PREFIX+"_"+(-1<targetCID.indexOf('\'')?targetCID.hashCode():targetCID);
							logger.logInfo("Creating rsmd for generated " + (null==rs?"update count":"ResultSet"));
							
							if ( null != rs ) {
								// A ResultSet was returned by the procedure - hold on to it until we need it...
								// NOTE! The resultSet rs is derived from a child connection spawed in the stored procedure, NOT from the parent connection c.
								metaData = new GaianResultSetMetaData( rs, dsWrappperID, resultModifiers.endsWith("falsefalse"), c );
								// *CRUCIAL STEP*: DO NOT recycle this connection yet as it has a retained result - recycling happens after resultSet is fetched...
								c = null;
							} else {
								// The procedure just returned an update count so the connection can be recycled immediately.
								metaData = new GaianResultSetMetaData( s.getUpdateCount(), dsWrappperID );
								s.close(); // Close the statement, making the connection ready to be recycled (in finally{} block)
							}
							
							// DRV 06/02/2015 - CACHE the MD for stored procedures because they have already been run and have a retained result.
							derivedMetaData.put( sqlQuery+targetCID+resultModifiers, metaData );
						}
						
					} else {
						// This is a query (may be select/with/values/xquery)...
						// Note some RDBMS drivers (e.g. ojdbc14.jar) don't support c.prepareStatement( sqlQuery ).getMetaData()
						// The only way around this is to execute the statement before we can get the table def...
						// Solution: wrap subquery in another subquery, adding predicate "where 0=1" then execute this to only get the meta-data.
						PreparedStatement ps = null;
						try {
							logger.logInfo("Preparing subquery to get RSMD: " + sqlQuery);
							ps = c.prepareStatement( sqlQuery );
							metaData = new GaianResultSetMetaData( ps.getMetaData(), GaianDBConfig.getSpecialColumnsDef(null), provider );
							
						} catch (Exception e1) {
							logger.logImportant("Unable to prepare or get metadata for subquery: " + sqlQuery + ", cause: " + e1);
							String subSubQuery = "select * from (" + sqlQuery + ") S where 0=1";
							ResultSet rs = null;
							try {
								logger.logInfo("Attempting to get metadata using sub-subquery instead: " + subSubQuery);
								rs = c.createStatement().executeQuery(subSubQuery);
								metaData = new GaianResultSetMetaData( rs.getMetaData(), GaianDBConfig.getSpecialColumnsDef(null), provider );
								
							} catch ( Exception e2 ) {
								throw new Exception("Unable to get MD for subquery: "+e1+
										". Also unable to get MD using SUB-subquery: " + subSubQuery + ": " + e2);
								
							} finally { if ( null != rs ) rs.close(); }
						} finally { if ( null != ps ) ps.close(); }
					}
				} finally {
					// return Connection to pool for re-use (this won't apply for procedure calls returning ResultSets which still need processing...)
					if ( null != c ) synchronized( connectionPool ) { connectionPool.push( c ); }
				}
			}
			
//			CallableStatement cs = c.prepareCall(sqlQuery);
//			metaData = new GaianResultSetMetaData( cs.getMetaData(), GaianDBConfig.getSpecialColumnsDef(null) );
			
			// DRV 11/09/2014 - Don't cache subquery result structure - because referenced tables may change or be dropped
			// DRV 03/10/2014 - Another reason not to cache it: because we now create it for RDBMS connection details passed in
			// directly - i.e. having no CID registration - so intentionally transient and not subject to re-use.
			// DRV 06/02/2015 - We only cache MD for stored procs (above) because they need running in advance and we put their retained result in the MD.
			//derivedMetaData.put( sqlQuery+resultModifiers, metaData );
		}
		return metaData;
	}
	
	// Transient Node Name -> VTI
	// The transient node name is a combination of: SUBQ(index) or GTW(tableName) and the data source name.
	// SUBQ(index) is a subquery with an index, e.g. SUBQ1, and GTW stands for gateway, used for logical tables that
	// are not defined locally.
	// Examples of transient tables names: SUBQ1_DERBY, SUBQ2_C1 (=A Gaian Node for subquery 2),
	// GTW_LT1_C2 (=A gateway for table LT1 to Gaian Node C2)
//	private static Hashtable transientNodes = new Hashtable();
	
	/**
	 * Dynamic nodes are needed for subqueries.
	 * They are also needed for logical tables which are not defined for the current node being queried.
	 * In the latter case, the node is acting as a Gateway and there are no local sources to be queried (so localDataSources == null).
	 * So local sources only apply to subqueries.
	 * Note - a localDataSource may even be a string directly containing the RDBMS connection details in the format: 'url'usr'pwd
	 * In this later case, the dynamically constructed source will not be cached.
	 * 
	 * As far as GAIAN connections are concerned, we may have some defined for:
	 * 		- a specific transient logical table.
	 * 		- a subquery sourcelist 
	 * 
	 * Additionally, we need the global gaian connections, either discovered automatically or manually configured.
	 * 
	 * @param transientTableName - GATEWAY_PREFIX + "_" + logical table name, or SUBQUERY_PREFIX
	 * @param transientTableMetaData - associated meta data
	 * @param localTargetCIDs - null for transient tables, or the CSV list of local RDBMS connection IDs associated with the subquery.
	 * This list may also contain actual details expressed as "'url'usr'pwd" strings (data source objects for these are not cached)
	 * @return array of VTIWrapper objects which represent the sources associated with the logical table or subquery.
	 * @throws Exception
	 */
	public static VTIWrapper[] constructDynamicDataSources( String transientTableName, GaianResultSetMetaData transientTableMetaData, String[] localTargetCIDs )
	{ //throws Exception {
		
		int numLocalTargetCIDs = null == localTargetCIDs ? 0 : localTargetCIDs.length;
		int numCIDs = numLocalTargetCIDs + globalGaianConnections.length;
//		VTIWrapper[] vtiArray = new VTIWrapper[ dsl + globalGaianConnections.length ];
		
		ArrayList<VTIWrapper> dataSources = new ArrayList<VTIWrapper>();
		
		for ( int i=0; i<numCIDs; i++ ) {
			
			boolean isLocalCID = i < numLocalTargetCIDs;
			
			String targetCID = isLocalCID ? localTargetCIDs[i] : globalGaianConnections[i-numLocalTargetCIDs];
			// We must use a "GAIANNODE" prefix so the node can know it is not a subquery.
			final String srcSuffix = -1 < targetCID.indexOf('\'') ? targetCID.hashCode() + "" : targetCID;
			String nodeName = ( isLocalCID ? transientTableName : GAIAN_NODE_PREFIX + '_' + transientTableName ) + '_' + srcSuffix;
//			String nodeName = transientTableName + '_' + source;

			try {
				
				// NOTE: FOR SUBQUERIES' DATA SOURCES, WE ONLY HANLDE RDBMS ONES - OTHERS COULD BE ADDED HERE LATER.
				
				String cprops = GaianDBConfig.getRDBConnectionDetailsAsString( targetCID );
	//			if ( null == cprops ) kuzhdks continue; // Connection not properly defined - skip this source
				
				// NOTE: DO NOT attempt to re-initialise a vti for a dynamic node - this can cause problems when multiple threads compete
				// for usage of the nodes - node reinitialisation is done on a config refresh only, mainly to save having to reload in-memory rows.
				// Here we just re-use the node as-is, unless the meta-data changed, in which case we build a new one.
				
				VTIWrapper ds = transientDataSources.get( nodeName+cprops+transientTableMetaData );
				if ( null == ds ) {
					ds = new VTIRDBResult( cprops, nodeName, transientTableMetaData );
					// record this transient source for faster processing of repeated requests - but only if the CID was registered (rather than RDBMS details passed in directly)
					if ( srcSuffix.equals(targetCID) ) transientDataSources.put( nodeName+cprops+transientTableMetaData, ds );
				} else if ( transientTableMetaData.hasRetainedResult() ) {
					ds.reinitialise( transientTableMetaData ); // make sure the md is updated in the ds in case we have a new retained result after a new prepare..
				}
				dataSources.add( ds ); //new VTIRDBResult( cprops, nodeName, transientTableMetaData ) );
				
			} catch ( Exception e ) {
				logger.logWarning(GDBMessages.ENGINE_DS_DYNAMIC_LOAD_ERROR, "Cannot load dynamic data source " + nodeName + " (ignored): " + e);
				continue;
			}
		}
		
		return (VTIWrapper[]) dataSources.toArray( new VTIWrapper[0] );
	}
	
//	protected static void recycleTransientNode( String nodeName, VTIWrapper vti ) {
//		// Just keep one vti per name for now - in future it may be worth having a stack of them per name
//		// to avoid rebulding them all the time in contructDynamicNodes().
//		transientNodes.put( nodeName, vti );
//	}
	
	/**
	 * VTIWrapper objects are "federation wrappers".
	 * Their primary purpose is to manage column mappings between the logical table and the endpoint table shape.
	 * They can also manage other optional features such as caching of data (e.g. InMemoryRows) or query history: (Connection/PreparedStatement)
	 *
	 * The purpose of this method is, upon re-load of a logical table, to save re-creating VTIWrapper objects when their properties identifying
	 * the endpoint data-source haven't changed. Only data sources wrappers whose endpoint has changed need to be discarded.
	 * Re-use of a data-source wrapper can also preserve objects which are costly to build (e.g. InMemoryRows, PreparedStatement maps).
	 */
	private static VTIWrapper reinitialiseDataSourceWrapperOrDiscardIfEndpointChanged(
			String nodeDefName, String dsWrapperEndpointSignature, GaianResultSetMetaData rsmd ) {
		
		VTIWrapper vti = (VTIWrapper) dataSources.remove( nodeDefName );

		if ( null != vti ) {
			try {
				if ( vti.isBasedOn( dsWrapperEndpointSignature ) ) {
					
					vti.reinitialise( rsmd );
					logger.logInfo("Attempt to re-use VTIWrapper succeeded for node: " + nodeDefName);
					
				} else {
					
					logger.logInfo("Unable to re-use VTIWrapper for node " + nodeDefName + ", as not based on " + dsWrapperEndpointSignature);
					vti.close();
					vti = null;
				}
			} catch ( Exception e ) {
				logger.logException( GDBMessages.ENGINE_REUSE_VTI_ERROR, "Unable to re-initialise or close VTI (disabled): ", e );
				try { vti.close(); }
				catch ( SQLException e1 ) { logger.logWarning(GDBMessages.ENGINE_VTI_CLOSE_ERROR, "Suppressed Exception closing vti: " + e); }
				finally { vti = null; }
			}
			
//			Tests showed that calling System.gc() can have a severe performance impact if a lot of
//			memory is currently committed to the JVM (i.e. in use and not ready to be garbage collected)
//			However calling System.gc() does encourage the JVM to compact allocated heap and return memory to the system
//			when a lot of memory is allocated but not committed (i.e. when most has been freed).
//			Therefore we must be very careful to only call Systm.gc() when we know that a large structure has been cleared.
//			
//			System.gc();
//			logger.logInfo("Called Garbage Collector: System.gc()");
		}
		
		return vti;
	}
	
	public static Stack<Object> getSourceHandlesPoolForLocalNode() {
		return getSourceHandlesPool( GaianDBConfig.getLocalDefaultConnectionID() );
	}
	
	/**
	 * Get data source pool given a handle (e.g. JDBC connection details ID or a filename).
	 * The objects in the returned pool point to the actual data sources (not cached InMemoryRows objects)
	 * 
	 * @param connectionDetailsID
	 * @return
	 */
	public static Stack<Object> getSourceHandlesPool( String handleDescriptor ) {
		return getSourceHandlesPool( handleDescriptor, false ); // default is not in memory
	}

	/**
	 * Gets a data source Pool for the given handle and in-memory setting (creating a new one if necessary).
	 * Also references the requestor as having requested a pool for that key, so that
	 * the handles can be cleaned up when no more requestors are using them.
	 * 
	 * @param handleDescriptor
	 * @param inMemory
	 * @param requestor
	 * @return
	 */
	public static Stack<Object> getSourceHandlesPool( String handleDescriptor, boolean inMemory ) {
		
		synchronized( sourceHandlesPools ) {
			String key = inMemory ? INMEM_STACK_KEY_PREFIX + handleDescriptor : handleDescriptor;
			
			Stack<Object> pool = sourceHandlesPools.get( key );
			if ( null == pool ) {
				pool = new RecallingStack<Object>();
				sourceHandlesPools.put( key, pool );
				timestampsOfFirstJDBCConnectionAttempts.remove(key); // from this we derive an 'initialisation period' for the JDBC endpoint
			}
			
			return pool;
		}
	}
	
	/**
	 * A stack that remembers its maximum historical size.
	 * Uses an independant set to avoid synchronization issues with the Pool's underlying Vector
	 * 
	 * @author DavidVyvyan
	 *
	 * @param <E>
	 */
	static final class RecallingStack<E> extends Stack<E> {
		private static final long serialVersionUID = 1L;
		//		private int maxSize = 0;
//		public synchronized E push(E object) { maxSize = Math.max( maxSize, size()+1 ); return super.push(object); }
		private Set<E> historicalElements = new HashSet<E>();
		private Object firstPushedObject = null;
		public synchronized int getMaxSize() { return historicalElements.size(); }
		public synchronized Object getFirstPushedObject() { return firstPushedObject; }
		public synchronized E push(E object) {
			
			int maxSize = GaianDBConfig.getMaxPoolsizes();
			if ( this.size() >= maxSize ) {
				logger.logThreadInfo( "Pooling rejected (RecallingStack.push()). Max Pool Size Met: " + maxSize + ". Rejecting "
						+ object.getClass().getSimpleName() + " object");
				return null;
			}
			
//			if (!historicalElements.contains(object)) {
//			final boolean isKnown = historicalElements.contains(object);
//			String msg = "Pooling " + (isKnown ? "known" : "new") 
//			+ " object. Total in stack: "+(historicalElements.size()+(isKnown?0:1))+": " + object + ", stack trace: " + Util.getStackTraceDigest();
//			logger.logThreadInfo(msg); //System.out.println(msg);
//			}
			if ( null == firstPushedObject ) firstPushedObject = object;
			historicalElements.add(object); return super.push(object);
		}
		public synchronized void clear() { historicalElements.clear(); firstPushedObject = null; super.clear(); }		
	}
	
	/**
	 * Returns a synchronized Set of active JDBC source handles. When iterating over this set, one has
	 * to synchronize on it in order to avoid concurrent access which may lead to incorrect data being retrieved or 
	 * a NullPointerException being thrown.
	 * 
	 * @return
	 */
	static Set<String> getLoadedRDBSourceHandles() {
		return allReferencedJDBCSourceIDs;
	}
	
//	/**
//	 * Gets a Stack Pool for the given handle and in-memory setting (creating a new one if necessary).
//	 * Also references the requestor as having requested a stack pool for that key, so that
//	 * the handles can be cleaned up when no more requestors are using them.
//	 * 
//	 * @param handleDescriptor
//	 * @param inMemory
//	 * @param requestor
//	 * @return
//	 */
//	public static Stack getSourceHandlesStackPool( String handleDescriptor, boolean inMemory, VTIWrapper requestor ) {
//		
//		synchronized( sourceHandlesPools ) {
//			String key = inMemory ? handleDescriptor + " INMEMORY" : handleDescriptor;
//			
//			Stack stack = (Stack) sourceHandlesPools.get( key );
//			if ( null == stack ) {
//				stack = new Stack();
//				sourceHandlesPools.put( key, stack );
//			}
//			
//			HashSet requestors = (HashSet) sourceHandlesStackPoolRequestors.get( key );
//			if ( null == requestors ) {
//				requestors = new HashSet();
//				sourceHandlesStackPoolRequestors.put( key, requestors );
//			}
//			requestors.add( requestor );
//			
//			return stack;	
//		}
//	}
//	
//	/**
//	 * Removes the reference to the stack pool that a VTIWrapper will have.
//	 * If this is the last reference, then remove the Stack pool from Hashtable of them as well and return it to the VTIWrapper 
//	 * so it can purge it. 
//	 * 
//	 * @param handleDescriptor
//	 * @param inMemory
//	 * @param requestor
//	 * @return
//	 */
//	public static Stack removeSourceHandlesRequestor( String handleDescriptor, boolean inMemory, VTIWrapper requestor ) {
//		
//		synchronized( sourceHandlesPools ) {
//			
//			String key = inMemory ? handleDescriptor + " INMEMORY" : handleDescriptor;
//			
//			HashSet requestors = (HashSet) sourceHandlesStackPoolRequestors.get( key );
//			// requestors cannot be null - unless there is a programming error
//			
//			if ( null != requestors ) requestors.remove( requestor );
//			
//			Stack removedHandlesStack = null;
//			
//			if ( null == requestors || 0 == requestors.size() ) {
//				sourceHandlesStackPoolRequestors.remove( key );
//				removedHandlesStack = (Stack) sourceHandlesPools.remove( key );
//			}
//			
//			return removedHandlesStack;
//		}
//	}
	
//	public static Connection getRDBHandleInReasonableTime( String connectionDetails, Stack<?> pool ) throws SQLException {
//		
//		GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) )
//					.getConnectionToPoolAsynchronously( (Stack<Connection>) pool, 10*MIN_JDBC_CONNECTION_TIMEOUT_MS );
//		
//		try { return (Connection) pool.pop(); }
//		catch ( EmptyStackException e ) { throw new SQLException("Unable to get JDBC Connection before timeout - connecting in background"); }
//		
////		return GaianDBConfig.getDBConnection( GaianDBConfig.getConnectionTokens(connectionDetails), MIN_CONNECTION_TIMEOUT_MS ).createStatement();
//	}

	public static Connection getRDBHandleQuickly( String connectionDetails, Stack<?> pool ) throws SQLException {		
		return getRDBHandleQuickly( connectionDetails, pool, MIN_JDBC_CONNECTION_TIMEOUT_MS );
	}
		
	/**
	 * This method is called when a query needs a connection and that none are left in the pool.
	 * The code should be such that this does not happen, by getting connections in advance.
	 * 
	 * @param connectionDetails
	 * @return
	 * @throws SQLException
	 */
	private static Connection getRDBHandleQuickly( String connectionDetails, Stack<?> pool, long timeout ) throws SQLException {
		
		Connection c = getJDBCConnectionToPool( connectionDetails, pool, timeout );		
		if ( null == c ) throw new SQLException("Unable to get JDBC Connection before timeout - connecting in background");
	
		return c;
	}
	
	public static void getRDBHandleInSeparateThread( String connectionDetails, Stack<?> pool ) {
		
		try { getJDBCConnectionToPool( connectionDetails, pool, 0 ); } // timeout = 0, so we don't wait for the handle to be available
		catch (SQLException e) { logger.logWarning( GDBMessages.ENGINE_DB_CONN_ASYNC_ERROR, "Asynchronous DB connection attempt failed: " 
				+ Util.getStackTraceDigest(e) ); }
	}
	
	private static Map<String, Long> timestampsOfFirstJDBCConnectionAttempts = new ConcurrentHashMap<String, Long>();

	/**
	 * This method gets a JDBC connection within a timeout for the given connectionDetails id and, if successful, puts it in the connection pool.
	 * If the connection attempt fails within the time threshold then the pool will not have the connection yet, but the connection 
	 * attempt continues in the background and, if successful, the connection will still eventually be put in the pool.
	 * 
	 * We don't want queries to be held up when we poll for RDBMS endpoints to come online, so we only wait a max of 200ms from methods like getRDBHandleQuickly().
	 * Notes - It takes 2.5 seconds to poll an unreachable server socket (a simple telnet proves this). This is too long.
	 *         However if we *could* reduce this timeout, we would also end up sometimes having to abandon successful attempts anyway, so back to square 1...
	 * Bottom line: It is the watchdog's job to poll the referenced data sources and pre-populate their connection pools asap.
	 * 				=> We stick to 200ms from the time-critical methods.
	 * 
	 * On the other hand, for *newly referenced* JDBC connections, we DO want to wait longer on the VERY FIRST connection attempt.
	 * This allows time for structures to be initialised, gives benefit of doubt to the endpoint, provides better user experience on 1st query attempt,
	 * and has a limited 'one-off' impact on the ongoing database network behaviour. Note if the endpoint is not available, the 1st attempt should fail
	 * in about 2.5 seconds.
	 * 
	 * @param connectionDetails
	 * @param pool
	 * @return the connection, if it was obtained within the timeout
	 * @throws SQLException
	 */	
	private static Connection getJDBCConnectionToPool( String connectionDetails, Stack<?> pool, long timeout ) throws SQLException {
		
//		boolean isFirstTimeCriticalRequestForConnection = false == connectionsHavingBeenSoughtQuickly.contains( connectionDetails );
//		if ( isFirstTimeCriticalRequestForConnection ) connectionsHavingBeenSoughtQuickly.add( connectionDetails );
		
		Long ts = timestampsOfFirstJDBCConnectionAttempts.get(connectionDetails);
		if ( null == ts ) timestampsOfFirstJDBCConnectionAttempts.put(connectionDetails, ts = System.currentTimeMillis());
		
		if ( 0 < timeout ) {
			// If the requested timeout falls BEFORE the initialisation period has completed (5s from the first connection attempt),
			// then use the end time for that initialisation period instead of the requested timeout.
			if ( System.currentTimeMillis() + timeout < ts + 5000 ) timeout = 5000;
		}
		
//    	logger.logThreadInfo( "Getting connection (loadDriver) for " + connectionDetails );
		
		return GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) )
			.getConnectionWithinTimeoutOrToPoolAsynchronously( (Stack<Connection>) pool, timeout );
	}

	// This will be treated as "time-critical" if no connection already in pool, but given extra time on the very 1st attempt.
	public static Connection getPooledJDBCConnection( String connectionDetails, Stack<Object> pool ) throws SQLException {
		return getPooledJDBCConnection( connectionDetails, pool,  MIN_JDBC_CONNECTION_TIMEOUT_MS );
	}
	
	public static Connection getPooledJDBCConnection( String connectionDetails, Stack<Object> pool, long timeout ) throws SQLException {
		
		Connection connection = null;
		
		try {
			
			synchronized( pool ) {
				if ( !pool.empty() ) connection = (Connection) pool.pop();
			}
			
			if ( null != connection ) {
				
//				if ( !Thread.currentThread().getName().startsWith( "Connection" ) )
					logger.logThreadDetail( "Extracted an existing DB handle from the Stack Pool" );
				
				if ( pool.empty() ) {
					logger.logThreadInfo( "Creating a new DB handle asynchronously as pool is now empty...");
					getRDBHandleInSeparateThread( connectionDetails, pool );
				}
				
				// Check if connection was closed by JDBC for whatever reason and explicitely throw an SQLException if it was.
				if ( connection.isClosed() )
					throw new SQLException("Statement's Connection is closed");		
			
			} else {
				
				logger.logThreadInfo( "No more connection in pool - Trying to get 2 (1 + 1 spare)..." );
				connection = DataSourcesManager.getRDBHandleQuickly( connectionDetails, pool, timeout );
				// And get another one as a spare
				getRDBHandleInSeparateThread( connectionDetails, pool );
			}
			
		} catch (SQLException e) {
			throw new SQLException( "Currently unable to obtain Connection: " + e );
		}
		
		return connection;
	}
	
	public static boolean isLogicalTableLoaded( String ltname ) {
		return dsArrays.containsKey( ltname );
	}
	
//	static boolean isDataSourceLoaded( String dsID ) {
//		return dataSources.containsKey( dsID );
//	}
	
	public static Set<String> getLogicalTableNamesLoaded() {
		return dsArrays.keySet();
	}
	
	static void setTableChangedFlag( String nodeDefName ) {
		VTIRDBResult vti = (VTIRDBResult) dataSources.get( nodeDefName );
		if ( null != vti ) vti.tableNeedsRefresh();
	}
	
	public static VTIWrapper[] getDataSources( String logicalTableName ) {
		return (VTIWrapper[]) dsArrays.get( logicalTableName );
	}

//	public static GaianResultSetMetaData getLogicalTableRSMDClone( String ltName ) throws Exception {
//		
////		if ( null == ltName ) return null;
////		GaianResultSetMetaData rsmd = (GaianResultSetMetaData) ltrsmds.get( ltName );
//		
////		// ltName should not be null - if it is we assume just the special cols are being queried
////		GaianResultSetMetaData rsmd = null==ltName ? genericNodeMetaData : (GaianResultSetMetaData) ltrsmds.get( ltName );
////		
////		if ( null == rsmd ) {
////			logger.logInfo( "No meta data object found for LT: " + ltName + ", returning generic node meta data");
////			rsmd = genericNodeMetaData;
////		}
//		
////		return (GaianResultSetMetaData) rsmd.clone();
//		return (GaianResultSetMetaData) ((GaianResultSetMetaData) ltrsmds.get( ltName )).clone();
//	}
	
	// Don't set ltrsmds outside of the main refresh() code as the other state variables on table def and 
	// table signatures need to be in step with it... external code should induce a refresh() to reload
	// logical tables and their data sources.
//	/**
//	 * This method *should* be called in the context of a synchronized( DataSourcesManager ) block in an effort
//	 * to apply corresponding data sources updates at the same time.
//	 * However, it is still ok to set a table def that does not match data source col defs (from the corresponding 
//	 * VTIArrays).. we will just not get any data from these until they are set properly.
//	 */
//	static void setLogicalTableRSMD( String ltName, GaianResultSetMetaData ltrsmd ) {
//		ltrsmds.put( ltName, ltrsmd );
//	}
	
	public static GaianResultSetMetaData getLogicalTableRSMD( String ltName ) {
		return (GaianResultSetMetaData) ltrsmds.get( ltName );
	}
	
	static void closeAllDataSourcesAndSourceHandles() throws Exception {
		
		VTIWrapper[] allDS = (VTIWrapper[]) dataSources.values().toArray( new VTIWrapper[0] );
		for (int i=0; i<allDS.length; i++) {
			allDS[i].close();
		}
		
		Stack<Object>[] allHandleStacks = sourceHandlesPools.values().toArray( new Stack[0] );
		for (int i=0; i<allHandleStacks.length; i++) {
			Stack<Object> stack = allHandleStacks[i];
			while ( !stack.empty() ) {
				try {
					Object o = stack.pop();
					if ( o instanceof Connection ) {
						try { ((Connection) o).close(); } catch ( SQLException e ) {} // ignore
					} else if ( o instanceof GaianChildVTI ) {
						((GaianChildVTI) o).close();
					}
				} catch ( EmptyStackException e ) {}
			}
			stack.clear(); // overriden by RecallingStack, clears all extra references to Connections in HashSet
		}
		
		logger.logThreadInfo("Closed all VTIs and their data handles: RDB connections, files, etc." );
	}
}
