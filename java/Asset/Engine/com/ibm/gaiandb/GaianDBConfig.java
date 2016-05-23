/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.derby.iapi.types.TypeId;

import com.ibm.db2j.FileImport;
import com.ibm.gaiandb.apps.SecurityClientAgent;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.lite.LiteDriver;
import com.ibm.gaiandb.policyframework.SQLQueryFilter;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.gaiandb.utils.DriverWrapper;

/**
 * @author DavidVyvyan
 */
public class GaianDBConfig {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "GaianDBConfig", 40 );
	
//	private static String gaiandbWorkspace = null; // Initialised to null, meaning local/working directory.
//	public static final String DEFAULT_NODE_DESCRIPTION_SUFFIX = ":" + DEFAULT_PORT + "/" + DEFAULT_DB;
	
	static final String DEFAULT_CONFIG = "gaiandb_config";

	private static final String UPROPS_EXTN = ".properties";
//	private static final String SPROPS_EXTN = ".sysinfo";

	// Note GAIAN_NODE_DB is commented out, meaning the physical db name for any GaianNode is fixed, in order
	// to make the connection/discovery mechanism a little simpler between Gaian Nodes.
	// For convenience this 'hard-wired' physical db may contain user data, but users may just as well store their data 
	// in another derby db also managed by the Derby Network Server attached to this GaianNode. This would need federating 
	// in the config file. Performance in accessing either of these dbs would be the same.	
//	private static final String GAIAN_NODE_DB = "GAIAN_NODE_DB";
	
	static final String GAIAN_NODE_USR = "GAIAN_NODE_USR"; // Must match user in derby.properties if authentication is enabled there
	static final String GAIAN_NODE_PWD = "GAIAN_NODE_PWD"; // If this is set in gaian config, then it overrides property 'derby.user.gaian' in derby.properties
	public static final String GAIAN_NODE_DEFAULT_USR = "gaiandb"; //"GAIANDB";
	public static final String GAIAN_NODE_DEFAULT_PWD = "passw0rd"; //"passw0rd";
	public static final String GAIAN_NODE_DEFAULT_AUSER = "xpclient"; // default "actual" user
	public static final String GAIAN_NODE_DEFAULT_DOMAIN = "SDPDOM.LOCAL";  // default domain name
	
	private static String derbyGaianNodeUser = null;
	private static String derbyGaianNodePassword = null;
	
	private static int THIRTY_TWO_KB = 32768;
	
//	private static final String GDBADMIN_USER = "gdbadmin";
//	private static final String GDBADMIN_DEFAULT_PASSWORD = "adm1n"; //"passw0rd";
//	private static String derbyGaianNodeAdminPassword = null;
	
	private static Properties derbyProperties = null;
	static Properties getDerbyProperties() { return derbyProperties; }
	
	static {
		try {
			// resolve workspace dir independently from GaianNode initialisation - because methods in this class may be used before the node has been started.
			String workspaceDir = System.getProperty("derby.system.home");
			if ( null == workspaceDir ) workspaceDir = System.getProperty("user.dir");
			if ( null == workspaceDir ) workspaceDir = ".";
			
			logger.logThreadInfo("Loading derby properties at: " + workspaceDir + "/derby.properties");
			
			FileInputStream fis = new FileInputStream( new File( workspaceDir + "/derby.properties") );
			derbyProperties = new Properties();
			derbyProperties.load(new InputStreamReader(fis, "UTF8"));
			fis.close();
		} catch (Exception e) { System.out.println("\n\t***** WARNING: Authentication disabled: " + e + "\n"); }
	}
	
	private static String configFileLocation = DEFAULT_CONFIG + UPROPS_EXTN;
	private static File configFile = null;
//	private static String sysInfoFileName = DEFAULT_CONFIG + SPROPS_EXTN;
//	private static File sysInfoFile = null;
	
	private static long latestLoadedPropertiesFileTimestamp = 0;
	
	// upr contains user properties and spr contains system properties, e.g. DISCOVERED_GAIAN_CONNECTIONS and the connection defs themselves.
	static Properties upr = null, spr = new Properties();
	
	// These are properties of data sources whose definition is kept in memory only. They only exit for the lifetime of the node.
	static Properties inMemoryDataSourceProperties = new Properties();
	
	private static int derbyServerListenerPort = 0; // Initial port value, used if using embedded driver
	private static String gaianNodeID = null;
	
	public static final String GAIANDB_NAME = "gaiandb"; //gdb";
	private static String DBNAME = null; // shd be any db name given on the cmd line
	
	public static final String SUBQUERY_LT="SUBQUERY";
	public static final String LOCALDERBY_CONNECTION="LOCALDERBY";
	
	public static final String DERBY_EMBEDDED_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DERBY_CLIENT_DRIVER="org.apache.derby.jdbc.ClientDriver";
	//public static final String GAIAN_SECURITY_DRIVER="client.SecureClientDriver";
	// Denis : GDB_UDP_DRIVER="com.ibm.gaiandb.udpdriver.client.UDPDriver"
	public static final String GDB_UDP_DRIVER="com.ibm.gaiandb.udpdriver.client.UDPDriver";
	public static final String GDB_LITE_DRIVER="com.ibm.gaiandb.lite.LiteDriver";

	// NOTE PROVENANCE AND EXPLAIN COLS MUST NOT CONTAIN DECIMAL TYPES - 
	// OTHERWISE THE CONSEQUENCES OF USING getColumnDefArray() MUST BE LOOKED INTO AS IT REPLACES ',' with ':'
	public static final String GDB_PREFIX = "GDB";
	public static final String GDB_NODE = GDB_PREFIX + "_NODE";
	public static final String GDB_LEAF = GDB_PREFIX + "_LEAF";	
	private static final String PROVENANCE_COLDEFS = GDB_NODE + " "+Util.TSTR+", " + GDB_LEAF + " VARCHAR(50)"; // these sizes are just default display widths - they are not constraining
	public static final String[] PROVENANCE_COLS = getColumnsDefArray( PROVENANCE_COLDEFS );
	
	public static final String GDB_QRYID = GDB_PREFIX + "_QRYID";
	public static final String GDB_QRYSTEPS = GDB_PREFIX + "_QRYSTEPS";
//	public static final String GDB_QRYFWDER = GDB_PREFIX + "_QRYFWDER";
	// Allow plenty of chars for qry id len.. expected to be generally less than 50 e.g. 'VeryLongLongLongHostname:6415:1233070884453:12345678'
	public static final String QRYID_COLDEFS = GDB_QRYID + " VARCHAR(100), " + GDB_QRYSTEPS + " INT"; //, " + GDB_QRYFWDER + " VARCHAR(50)";
	
	// Data encoded using our RSA encryption code generates a byte array of length: 128 bytes.
	// We allow for up to 255 of these maximum, as the max size of a LONG VARCHAR FOR BIT DATA is 32,700 bytes
	public static final String GDB_CREDENTIALS = GDB_PREFIX + "_CREDENTIALS";
	public static final String GDB_CREDENTIALS_COLDEF = GDB_CREDENTIALS + " VARCHAR(1028)";
	
	public static final String GDB_EXPLAIN_PREFIX = GDB_PREFIX + "X";
	public static final String EXPLAIN_FROM = GDB_EXPLAIN_PREFIX + "_FROM_NODE";
	public static final String EXPLAIN_TO = GDB_EXPLAIN_PREFIX + "_TO_NODE";
	public static final String EXPLAIN_DEPTH = GDB_EXPLAIN_PREFIX + "_DEPTH";
	public static final String EXPLAIN_PRECEDENCE = GDB_EXPLAIN_PREFIX + "_PRECEDENCE";
	public static final String EXPLAIN_COUNT = GDB_EXPLAIN_PREFIX + "_COUNT";
	private static final String EXPLAIN_COLDEFS =
		EXPLAIN_FROM + " VARCHAR(20), " + EXPLAIN_TO + " VARCHAR(20), " +
		EXPLAIN_DEPTH + " INT, " + EXPLAIN_PRECEDENCE + " CHAR, " + EXPLAIN_COUNT + " BIGINT";
	public static final String[] EXPLAIN_COLS = getColumnsDefArray( EXPLAIN_COLDEFS );
	
	public static final String[] HIDDEN_COL_NAMES = getColumnsDefArray( GDB_NODE + "," + GDB_LEAF + "," +
			EXPLAIN_FROM + "," + EXPLAIN_TO + "," +	EXPLAIN_DEPTH + "," + EXPLAIN_PRECEDENCE + "," + EXPLAIN_COUNT );
	
	public static final int NUM_HIDDEN_COLS = GaianDBConfig.PROVENANCE_COLS.length + GaianDBConfig.EXPLAIN_COLS.length;
	
	
	static final String SOURCELIST_SUFFIX = "_SOURCELIST";
	static final String LTDEF_SUFFIX = "_DEF";
	static final String CONNECTION_SUFFIX = "_CONNECTION";
	static final String TABLE_SUFFIX = "_TABLE";
	static final String VTI_SUFFIX = "_VTI";
	static final String ARGS_SUFFIX = "_ARGS";
	static final String SCHEMA_SUFFIX = "_SCHEMA";
	static final String OPTIONS_SUFFIX = "_OPTIONS";
	static final String INMEMORY = "INMEMORY";
	static final String PLURALIZED = "PLURALIZED";
	static final String MAP_COLUMNS_BY_POSITION = "MAP_COLUMNS_BY_POSITION"; // IGNORE_PHYSICAL_SCHEMA, MIRRORED_LT_DEF = Ignore physical columns definition.
	
	static final String CONSTANTS_SUFFIX = "_CONSTANTS";
	static final String NODE_CONSTANTS = "NODE_CONSTANTS";
	
	static final String COLUMN_LABEL = "_C";
	
//	public static final int EXPLAIN_COUNT_COLUMN_INDEX = 1;
//	public static final int EXPLAIN_FROM_COLUMN_INDEX = 2;
//	public static final int EXPLAIN_TO_COLUMN_INDEX = 3;
//	public static final int EXPLAIN_DEPTH_COLUMN_INDEX = 4;
//	public static final int EXPLAIN_PRECEDENCE_COLUMN_INDEX = 5;
	
	
//	public static final String EXPLAIN_COLDEFS = 
//		"GDBX_FROM_NODE VARCHAR(20), GDBX_TO_NODE VARCHAR(20), GDBX_DEPTH INT, GDBX_PRECEDENCE INT, GDBX_COUNT INT";
		
//	// Disable row caching - USE WITH CARE! DISTRIBUTED JOINS WILL FAIL IF THIS IS SET
//	private static final String DISABLE_ROW_CACHING_REQUIRED_FOR_JOINS = "DISABLE_ROW_CACHING_REQUIRED_FOR_JOINS";
//	private static final boolean DEFAULT_DISABLE_ROW_CACHING_REQUIRED_FOR_JOINS = false;
//	public static boolean getDisableRowCaching() {
//		String s = getUserProperty( DISABLE_ROW_CACHING_REQUIRED_FOR_JOINS );
//		return null==s ? DEFAULT_DISABLE_ROW_CACHING_REQUIRED_FOR_JOINS : Boolean.parseBoolean(s);
//	}
	
	// Flag to switch to Derby Table Functions interface (as alternative to VTIs) to obtain support from Derby community if an issue arises.
	static final String MANAGE_LTVIEWS_WITH_TABLE_FUNCTIONS = "MANAGE_LTVIEWS_WITH_TABLE_FUNCTIONS";
	private static boolean DEFAULT_MANAGE_LTVIEWS_WITH_TABLE_FUNCTIONS = false;
	public static boolean isManageViewsWithTableFunctions() {
		return getBooleanPropertyOrDefault(MANAGE_LTVIEWS_WITH_TABLE_FUNCTIONS, DEFAULT_MANAGE_LTVIEWS_WITH_TABLE_FUNCTIONS);
	}
	
	// Timeout for detecting long running queries
	static final String EXEC_TIMEOUT_MS = "EXEC_TIMEOUT_MS";
	private static final int DEFAULT_EXEC_TIMEOUT_MS = 1000;
	public static int getExecTimeoutMillis() {
		return getIntPropertyOrDefault(EXEC_TIMEOUT_MS, DEFAULT_EXEC_TIMEOUT_MS);
	}
	
	// Flag to enable propagated insert/update/delete/call statements (default is disabled)
	static final String ALLOW_PROPAGATED_WRITES = "ALLOW_PROPAGATED_WRITES";
	private static final boolean DEFAULT_ALLOW_PROPAGATED_WRITES = false; // ADD A THIRD VALUE "RESTRICTED" IN FUTURE TO GOVERN BY AUTHORIZED USER
	public static boolean isAllowedPropagatedWrites() {
		return getBooleanPropertyOrDefault(ALLOW_PROPAGATED_WRITES, DEFAULT_ALLOW_PROPAGATED_WRITES);
	}
	
	// Flag to enable SQL API configuration statements (default is disabled), e.g. setxxx(), removexxx() and addxxx() apis
	static final String ALLOW_SQL_API_CONFIGURATION = "ALLOW_SQL_API_CONFIGURATION";
	private static final boolean DEFAULT_ALLOW_SQL_API_CONFIGURATION = false; // ADD A THIRD VALUE "RESTRICTED" IN FUTURE TO GOVERN BY AUTHORIZED USER
	public static boolean isAllowedAPIConfiguration() {
		return getBooleanPropertyOrDefault(ALLOW_SQL_API_CONFIGURATION, DEFAULT_ALLOW_SQL_API_CONFIGURATION);
	}

	// Fetch buffer sizes for result rows and recycled rows in GaianResult.
	private static final String FETCH_BUFFER_SIZE = "FETCH_BUFFER_SIZE";
	private static final int DEFAULT_FETCH_BUFFER_SIZE = 100;
	public static int getFetchBufferSize() {
		return getIntPropertyOrDefault(FETCH_BUFFER_SIZE, DEFAULT_FETCH_BUFFER_SIZE);
	}
	
	// Fetch buffer sizes for result rows and recycled rows in GaianResult.
	private static final String ROWS_BATCH_SIZE = "ROWS_BATCH_SIZE";
	private static final int DEFAULT_ROWS_BATCH_SIZE = 20;
	public static int getRowsBatchSize() {
		return getIntPropertyOrDefault(ROWS_BATCH_SIZE, DEFAULT_ROWS_BATCH_SIZE);
	}
	
	// Fetch buffer sizes for result rows and recycled rows in GaianResult.
	private static final String DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES = "DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES";
	private static final long DEFAULT_DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES = 1000;
	public static long getDiskCachingThresholdForJoinedInnerTables() {
		return getLongPropertyOrDefault(DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES, DEFAULT_DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES);
	}
	
	// Denis : NETWORK_DRIVER definition
	// NETWORK_DRIVER indicates the driver to use for the communication between the nodes
	// At the moment, it can be : JDBC Derby driver (TCP) or the GaianDB UDP Driver
	private static final String NETWORK_DRIVER = "NETWORK_DRIVER";
	public static final String DERBY = "DERBY";
	public static final String GDBUDP = "GDBUDP";
	private static final String DEFAULT_NETWORK_DRIVER = DERBY;
	public static String getNetworkDriver() {
		return getStringPropertyOrDefault(NETWORK_DRIVER, DEFAULT_NETWORK_DRIVER);
	}

	// Denis : NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE definition
	// NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE indicates the datagram size (in bytes) for the GaianDB UDP Driver if this one is selected.
	private static final String NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE = "NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE";
	private static final int DEFAULT_NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE = 1450;
	public static int getNetworkDriverGDBUDPDatagramSize() {
		return getIntPropertyOrDefault(NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE,
				DEFAULT_NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE);
	}
	
	// Denis : NETWORK_DRIVER_GDBUDP_TIMEOUT definition
	// NETWORK_DRIVER_GDBUDP_TIMEOUT indicates the timeout (in milliseconds) for the GaianDB UDP Driver if this one is selected.
	private static final String NETWORK_DRIVER_GDBUDP_TIMEOUT = "NETWORK_DRIVER_GDBUDP_TIMEOUT";
	private static final int DEFAULT_NETWORK_DRIVER_GDBUDP_TIMEOUT = 5000;
	public static int getNetworkDriverGDBUDPTimeout() {
		return getIntPropertyOrDefault(NETWORK_DRIVER_GDBUDP_TIMEOUT, 
				DEFAULT_NETWORK_DRIVER_GDBUDP_TIMEOUT);
	}
	
	// Denis : NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE
	// NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE indicates the buffer size (in bytes) of the sockets used by the GaianDB UDP Driver if this one is selected.
	private static final String NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE = "NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE";
	private static final int DEFAULT_NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE = 64000;
	public static int getNetworkDriverGDBUDPSocketBufferSize() {
		return getIntPropertyOrDefault(NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE, 
				DEFAULT_NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE);
	}
	
	// Heartbeat between checks on maintained connections or connections actively executing long running queries.
	static final String GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS = "GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS";
	private static final int DEFAULT_GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS = 5000;
	public static int getConnectionsCheckerHeartbeat() {
		return getIntPropertyOrDefault(GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS, DEFAULT_GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS);
	}
	
	// DriverManager.setLoginTimeout(timeout_ms) cannot be relied on because not all operating systems support setting 
	// a timeout on blocking socket calls.
	// DEFAULT_JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS and DEFAULT_MAX_CONCURRENT_JDBC_CONNECTION_ATTEMPTS_PER_DATASOURCE
	// are determined by the default JDBC connection timeout with Derby which appears to be about 2mins 30secs.
	
	// 10 concurrent connections, where 3 are made concurrently immediately...
	// This leaves 7 to be made each every 20 seconds, i.e. completing after 2 minutes and 20 secs
	// The Derby JDBC timeout appears to be about 2 minutes 30 secs itself so more connection attempts can be made in a 
	// controlled way as failed ones timeout every 20 seconds after that...
	
	// If our timeout is lower relative to our max number of concurrent connections, then there will be a longer wait when
	// the max number of concurrent connection attempts is reached.
	
	
	// This value must be 1 - Derby locks up on multiple connection attempts
//	// Max number of concurrent connections on an rdbms datasource
//	private static final String MAX_CONCURRENT_JDBC_CONNECTION_ATTEMPTS_PER_DATASOURCE = "MAX_CONCURRENT_JDBC_CONNECTION_ATTEMPTS_PER_DATASOURCE";
//	private static final int DEFAULT_MAX_CONCURRENT_JDBC_CONNECTION_ATTEMPTS_PER_DATASOURCE = 1;
//	public static int getMaxConcurrentConnectionAttempts() {
//		return getIntPropertyOrDefault(MAX_CONCURRENT_JDBC_CONNECTION_ATTEMPTS_PER_DATASOURCE, 
//				DEFAULT_MAX_CONCURRENT_JDBC_CONNECTION_ATTEMPTS_PER_DATASOURCE);
//	}
//	
//	// Time given before abandoning a connection attempt to give room for a new fresh one.
//	private static final String JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS = "JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS";
//	private static final int DEFAULT_JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS = 10000;
//	public static int getConnectionAttemptTimeout() {
//		return getIntPropertyOrDefault(JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS, DEFAULT_JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS);
//	}
	
	// Maximum number of gaian connections that may be discovered (outbound (maintained) + inbound)
	private static final String MAX_DISCOVERED_CONNECTIONS = "MAX_DISCOVERED_CONNECTIONS";
	private static final int DEFAULT_MAX_DISCOVERED_CONNECTIONS = 10;
	public static int getMaxDiscoveredConnections() {
		return getIntPropertyOrDefault(MAX_DISCOVERED_CONNECTIONS, DEFAULT_MAX_DISCOVERED_CONNECTIONS);
	}
	
	private static final String LOGFILE_MAX_SIZE_MB = "LOGFILE_MAX_SIZE_MB";
	private static final int DEFAULT_LOGFILE_MAX_SIZE_MB = 100;
	public static int getLogfileMaxSizeMB() {
		return getIntPropertyOrDefault(LOGFILE_MAX_SIZE_MB, DEFAULT_LOGFILE_MAX_SIZE_MB);
	}
	
	// All properties having default values and their associated default values
	public static String[][] getAllDefaultProperties() {
		
		String defaultInterface = "unresolved";
		try { defaultInterface = Util.stripToSlash( InetAddress.getByName(GaianNodeSeeker.getDefaultLocalIP()).toString() ).trim(); }
		catch ( Exception e ) {}
		
		return new String[][] {
				{ LOGLEVEL, Logger.POSSIBLE_LEVELS[ Logger.LOG_DEFAULT ] },
				{ DISCOVERY_IP, GaianNodeSeeker.DEFAULT_MULTICAST_GROUP_IP },
				{ GAIAN_NODE_USR, GAIAN_NODE_DEFAULT_USR },
				{ ALLOW_PROPAGATED_WRITES, Boolean.toString(DEFAULT_ALLOW_PROPAGATED_WRITES) },
				{ ALLOW_SQL_API_CONFIGURATION, Boolean.toString(DEFAULT_ALLOW_SQL_API_CONFIGURATION) },
				{ FETCH_BUFFER_SIZE, Integer.toString(DEFAULT_FETCH_BUFFER_SIZE) },
				{ ROWS_BATCH_SIZE, Integer.toString(DEFAULT_ROWS_BATCH_SIZE) },
				{ DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES, Long.toString(DEFAULT_DISK_CACHING_THRESHOLD_FOR_JOINED_INNER_TABLES) },
				{ NETWORK_DRIVER, DEFAULT_NETWORK_DRIVER },
				{ NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE, Integer.toString(DEFAULT_NETWORK_DRIVER_GDBUDP_DATAGRAMSIZE) },
				{ NETWORK_DRIVER_GDBUDP_TIMEOUT, Integer.toString(DEFAULT_NETWORK_DRIVER_GDBUDP_TIMEOUT) },
				{ NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE, Integer.toString(DEFAULT_NETWORK_DRIVER_GDBUDP_SOCKET_BUFFER_SIZE) },
				{ GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS, Integer.toString(DEFAULT_GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS) },
//				{ JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS, Integer.toString(DEFAULT_JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS) },
				{ MAX_DISCOVERED_CONNECTIONS, Integer.toString(DEFAULT_MAX_DISCOVERED_CONNECTIONS) },
				{ LOGFILE_MAX_SIZE_MB, Integer.toString(DEFAULT_LOGFILE_MAX_SIZE_MB) },
				{ MAX_PROPAGATION, Integer.toString(DEFAULT_MAX_PROPAGATION) },
				{ MAX_POOLSIZES, Integer.toString(DEFAULT_MAX_POOLSIZE) },
				{ MAX_INBOUND_CONNECTION_THREADS, Integer.toString(DEFAULT_MAX_INBOUND_CONNECTION_THREADS) },
				{ DISCOVERY_PORT, Integer.toString(DEFAULT_DISCOVERY_PORT) },
				
				{ ACCESS_CLUSTERS, "<no cluster membership restrictions>" },
				{ ACCESS_HOSTS_PERMITTED, "<all node connections permitted>" },
				{ ACCESS_HOSTS_DENIED, "<no node connections denied>" },
				{ MIN_DISCOVERED_CONNECTIONS, "0" },
				{ MULTICAST_INTERFACES, defaultInterface }, // default interface only
				{ DISCOVERY_GATEWAYS, "<no gateways for discovery to other subnets>" },
				{ DEFINED_GAIAN_CONNECTIONS, "<no hard-wired gaian connections>" },
				{ MSGBROKER_HOST, "<message storing disabled>" },
				{ MSGBROKER_PORT, "<message storing disabled>" },
				{ MSGBROKER_TOPIC, "<no default message storing topic>" },
				{ MSGSTORER_MSGCOLS, "<use single column for message - no decomposition>" },
				{ MSGSTORER_ROWEXPIRY_HOURS, "<no expiry for stored messages>" },
		};
	}
	
	private static String getStringPropertyOrDefault( String propName, String defaultValue ) {
		String s = getUserProperty( propName );
		return null==s ? defaultValue : s.trim();
	}
	
	private static int getIntPropertyOrDefault( String propName, int defaultValue ) {
		String s = getUserProperty( propName );
		try { return null==s ? defaultValue : Integer.parseInt(s.trim()); }
		catch( Exception e ) { 
			logger.logWarning(GDBMessages.CONFIG_PROP_INT_VALUE_INVALID, "Invalid Int property value for " + propName + ": " + s +
					" (using default: " + defaultValue + ")");
			return defaultValue;
		}
	}
	
	private static long getLongPropertyOrDefault( String propName, long defaultValue ) {
		String s = getUserProperty( propName );
		try { return null==s ? defaultValue : Long.parseLong(s.trim()); }
		catch( Exception e ) { 
			logger.logWarning(GDBMessages.CONFIG_PROP_LONG_VALUE_INVALID, "Invalid Long property value for " + propName + ": " + s +
					" (using default: " + defaultValue + ")");
			return defaultValue;
		}
	}
	
	private static boolean getBooleanPropertyOrDefault( String propName, boolean defaultValue ) {
		String s = getUserProperty( propName );
		try { return null==s ? defaultValue : Boolean.parseBoolean(s.trim()); }
		catch( Exception e ) {
			logger.logWarning(GDBMessages.CONFIG_PROP_BOOLEAN_VALUE_INVALID, "Invalid Boolean property value for " + propName + ": " + s +
					" (using default: " + defaultValue + ")");
			return defaultValue;
		}
	}
	
	private static HashSet<String> loadedJdbcDrivers = new HashSet<String>();
	
	private static char queryIDSequenceCounter = 0;
	
//	private static final String AUTO_GENERATED_PREFIX = "AUTO_";

	static final String LOGLEVEL = "LOGLEVEL";
	static final String LOGPERF = "LOGPERF";
	
	static final String SQL_RESULT_FILTER = "SQL_RESULT_FILTER";
	static final String SQL_QUERY_FILTER = "SQL_QUERY_FILTER";
	
	static final String MSGBROKER_HOST = "MSGBROKER_HOST";
	static final String MSGBROKER_PORT = "MSGBROKER_PORT";
	static final String MSGBROKER_TOPIC = "MSGBROKER_TOPIC";
	
	private static final String MSGSTORER_ROWEXPIRY_HOURS = "MSGSTORER_ROWEXPIRY_HOURS";
	private static final String MSGSTORER_MSGCOLS = "MSGSTORER_MSGCOLS";

	
	static final String MAX_PROPAGATION = "MAX_PROPAGATION";
	private static final int DEFAULT_MAX_PROPAGATION = -1; // Propagate out to the whole network
	public static int getMaxPropagation() {
		return getIntPropertyOrDefault(MAX_PROPAGATION, DEFAULT_MAX_PROPAGATION);
	}

	static final String MAX_POOLSIZES = "MAX_POOLSIZES";
	private static final int DEFAULT_MAX_POOLSIZE = 10; // Don't hold too many connections open
	public static int getMaxPoolsizes() {
		return getIntPropertyOrDefault(MAX_POOLSIZES, DEFAULT_MAX_POOLSIZE);
	}
	
	private static final String MAX_INBOUND_CONNECTION_THREADS = "MAX_INBOUND_CONNECTION_THREADS";
	private static final int DEFAULT_MAX_INBOUND_CONNECTION_THREADS = 1000;
	public static int getMaxInboundConnectionThreads() {
		return getIntPropertyOrDefault(MAX_INBOUND_CONNECTION_THREADS, DEFAULT_MAX_INBOUND_CONNECTION_THREADS);
	}
	
	static final String MIN_DISCOVERED_CONNECTIONS = "MIN_DISCOVERED_CONNECTIONS";
		
	static final String DISCOVERY_IP = "DISCOVERY_IP";
	static final String DISCOVERY_GATEWAYS = "DISCOVERY_GATEWAYS";
	
	private static final String DISCOVERY_HOSTS = "DISCOVERY_HOSTS"; // deprecated - use property below	
	static final String ACCESS_HOSTS_PERMITTED = "ACCESS_HOSTS_PERMITTED";
	static final String ACCESS_HOSTS_DENIED = "ACCESS_HOSTS_DENIED";
	public static final String ACCESS_CLUSTERS = "ACCESS_CLUSTERS";
	
	public static final String CONNECTION_STRATEGY = "CONNECTION_STRATEGY";

	// The values of valid connection strategies (i.e. fitness functions)
	public static final String ATTACHMENT_PREFERENTIAL_ON_HIGH_CONNECTIVITY = "ATTACHMENT_PREFERENTIAL_ON_HIGH_CONNECTIVITY";
	public static final String ATTACHMENT_RANDOM = "ATTACHMENT_RANDOM";
	public static final String ATTACHMENT_TO_USER_DB_NODE = "ATTACHMENT_TO_USER_DB_NODE";
	
	public static final String DISCOVERY_PORT = "DISCOVERY_PORT";
	private static final int DEFAULT_DISCOVERY_PORT = 7777;
	public static int getDiscoveryPort() {
		return getIntPropertyOrDefault(DISCOVERY_PORT, DEFAULT_DISCOVERY_PORT);
	}
	
	static final String MULTICAST_INTERFACES = "MULTICAST_INTERFACES";
	
//	static final String MULTICAST_DOMAINS = "MULTICAST_DOMAINS";
	
	private static final String DISCOVERED_GAIAN_CONNECTIONS = "DISCOVERED_GAIAN_CONNECTIONS";
	static final String DEFINED_GAIAN_CONNECTIONS = "DEFINED_GAIAN_CONNECTIONS";
	
	static final String LABEL_DRIVER = "_DRIVER";
	static final String LABEL_URL = "_URL";
	static final String LABEL_USR = "_USR";
	static final String LABEL_PWD = "_PWD";
	private static final String LABEL_DNODEID = "_DNODEID"; // discovered node id - only used for discovered connections
	
	// Derby authentication mode
	private static final String DERBY_AUTH_MODE_KEY = "derby.authentication.mode";
	private static final String DERBY_SSL_MODE = "derby.drda.sslMode";
	public static final String DERBY_AUTH_MODE_DEFAULT = "1";
	public static final String DERBY_AUTH_MODE_ID_ASSERT = "2";
	
	private static String gaianNodeHostName = null;
	
//	private static Properties getUserProperties() { if ( null == upr ) reloadUserProperties(); return upr; }
//	private static Properties getSystemProperties() { if ( null == spr ) spr = new Properties(); return spr; }
	
    /**
     * Utility to retrieve a resource from the properties file without having to handle
     * the IOException.
     * Instead, when the property can't be accessed, we log an Exception and return null.
     * 
     * @param key
     * @return
     */
    static String getUserProperty( String key ) {
    	
    	// Reload on startup - note that GaianDBConfig can also work as a standalone config class - 
    	// i.e. with a Derby Network Server launched independently...
    	if ( null == upr ) {
    		reloadUserProperties();
    		if ( null == upr ) return null;
    	}
    	
    	synchronized ( upr ) {
    		return getPropertyInEitherCase( upr, key ); //upr.getProperty( key );
    	}
    }
    
    private static String getPropertyInEitherCase( Properties pr, String key ) {
    	String p = pr.getProperty( key );
    	if ( null == p ) p = pr.getProperty( key.toUpperCase() ); // upper case takes priority over lower case
    	if ( null == p ) p = pr.getProperty( key.toLowerCase() );
    	// logger.logDetail("Getting property " + key + "; Returning value " + p);
    	return p;
    }
    
    private static String getSystemProperty( String key ) {
    	synchronized ( spr ) {
    		return spr.getProperty( key );
    	}
    }
        
    static String getCrossOverProperty( String key ) {
    	
    	String p = getUserProperty( key );
    	if ( null == p ) p = getSystemProperty( key );
    	return p;
    }
	
	static void setDerbyServerListenerPort( int port ) {
		derbyServerListenerPort = port;
	}
	
//	static void setWorkspace( String workspace ) {
//		gaiandbWorkspace = workspace;
//	}
	
	public static int getDerbyServerListenerPort() {
		return derbyServerListenerPort;
	}
	
	// Derby listener port must be set before this method is called
	public static void setConfigFile( String configFileLocationWithoutExtention ) throws Exception {
		configFileLocation = configFileLocationWithoutExtention + UPROPS_EXTN;
		configFile = null;
		
//		sysInfoFileName = configFileNameWithoutExtention +
//			( GaianNode.DEFAULT_PORT == derbyServerListenerPort ? "" : "_" + derbyServerListenerPort ) + SPROPS_EXTN;
//		sysInfoFile = null;
				
		// Pre-load properties - if this fails we will know early on
		reloadUserProperties();
		
		if ( null == upr ) throw new Exception("Could not load user properties");
	}

	public static String getVTIProperty( Class<?> klass, String property ) {
		return getVTIProperty( klass, property, true );
	}
	
	public static String getVTIProperty( Class<?> klass, String property, boolean doLog ) {
		String fullProp = klass.getName() + "." + property;
		String val = getUserProperty( fullProp );
		if ( null == val ) {
			fullProp = klass.getSimpleName() + "." + property;
			val = getUserProperty( fullProp );
		}
		if ( doLog ) logger.logInfo("Got property: " + fullProp + " = " + val);
		return val;
	}
	
//	public static synchronized boolean addNewNode( 
//			String connectionDetails, String table, String vtiArgs, String options, String[] columnNameMappings) {
//	}

//	public static synchronized boolean addNewGaianNode( 
//			String connectionDetails, String[] columnNameMappings) {
//	}
	
	public static String getPolicyClassNameForSQLResultFilter() {
		return getUserProperty(SQL_RESULT_FILTER);
	}
	
	private static String lastLoadedPolicyClass = null;
	public static void initialisePolicyClasses() {
		final String className = getPolicyClassNameForSQLResultFilter();
		
		// return if policy is disabled or if it has already been initialized.
		if ( null == className ) { lastLoadedPolicyClass = null; return; }
		if ( className.equals(lastLoadedPolicyClass) ) return;
		
		System.out.println("Loading policy class: " + className);
		
		// GaianDBConfig.class.getClassLoader().loadClass( className ); // not as effective as Class.forName() used below
		try { Class.forName(className, true, GaianDBConfig.class.getClassLoader() ); lastLoadedPolicyClass = className; }
		catch ( Exception e ) { logger.logWarning(GDBMessages.CONFIG_SQL_RESULT_FILTER_ERROR, "Cannot initialise class: "+className+", cause: " + e); }
	}
	
	public static SQLResultFilter getSQLResultFilter() {
		final String className = getPolicyClassNameForSQLResultFilter();
		if ( null != className)
			try { return (SQLResultFilter) GaianNode.getClassUsingGaianClassLoader(className).newInstance(); }
			catch ( Exception e ) { logger.logWarning(GDBMessages.CONFIG_SQL_RESULT_FILTER_ERROR, "Cannot load class: "+className+", cause: " + e); }
		return null;
	}
	
	public static SQLQueryFilter getSQLQueryFilter() {
		final String className = getUserProperty(SQL_QUERY_FILTER);
		if ( null != className)
			try { return (SQLQueryFilter) GaianNode.getClassUsingGaianClassLoader(className).newInstance(); }
			catch ( Exception e ) { logger.logWarning(GDBMessages.CONFIG_SQL_QUERY_FILTER_ERROR, "Cannot load class: "+className+", cause: " + e); }
		return null;
	}
	
	public static int getMinConnectionsToDiscover() {
		String s = getUserProperty( MIN_DISCOVERED_CONNECTIONS );
		return null == s ? 0 : Integer.parseInt(s);
	}
	
//	/**
//	 * Returns false if the property is already set to the same value in the registry.
//	 * This allows us not to have to reload the meta data into new vtis every time.
//	 * 
//	 * Otherwise, sets the property and returns true to indicate that the properties have changed.
//	 * 
//	 * @param ltName
//	 * @param definition
//	 * @return true if the properties need reloading into vtis.
//	 * @throws IOException
//	 */
//	public static boolean setTransientLogicalTableDefinition(String ltName, String definition) throws IOException {
//		Properties props = getProperties();
//		if ( definition.equalsIgnoreCase( (String) props.get( ltName+"_TRANSIENT_DEF" ) ) )
//			return false;
//		
//		props.setProperty( ltName+"_TRANSIENT_DEF", definition );
//		return true;
//	}
	
//	public static String removeTransientDefinition( String ltName ) throws IOException {
//		return (String) getProperties().remove( ltName+"_TRANSIENT_DEF" );
//	}
	
	public static boolean isDiscoveredOrDefinedConnection( String connectionID ) {
		return getGaianConnectionsAsSet().contains( connectionID );
	}
	
	public static String getDiscoveredNodeID( String connectionID ) {
		return getSystemProperty( connectionID + LABEL_DNODEID );
	}
	
	public static boolean isDiscoveredConnection( String connectionID ) {
		return null != getDiscoveredNodeID(connectionID);
	}
	
	public static boolean isDefinedConnection( String connectionID ) {
		for ( String dc : getDefinedConnections() )
			if ( dc.equals(connectionID) ) return true;
		return false;
	}
	
//	public static String getDiscoveredConnectionID( String nodeID ) {
//		synchronized( spr ) {
//			for ( Iterator<Object> keys = spr.keySet().iterator(); keys.hasNext(); ) {			
//				String key = (String) keys.next();
//				if ( key.endsWith( LABEL_DNODEID ) && spr.getProperty(key).equals(nodeID) )
//					return key;
//			}
//		}
//		return null;
//	}
	
	public static String getDiscoveredConnectionID( String nodeID ) {
		if ( null == nodeID ) return null;
		String dnodes = getSystemProperty(DISCOVERED_GAIAN_CONNECTIONS);
		if ( null == dnodes ) return null;
		
		synchronized ( spr ) {
			for ( String dnode : Util.splitByCommas(dnodes) )
				if ( nodeID.equals(spr.getProperty(dnode+LABEL_DNODEID)) ) return dnode;
		}
		return null;
	}
	
	public static boolean isDiscoveredNode( String nodeID ) {
		return null != getDiscoveredConnectionID(nodeID);
	}

	
//	/**
//	 * If the connection is a DEFINED gaian connection then it is not only removed from config, but config is
//	 * also reloaded such that its associated stack pool is purged and all VTIs referencing the connection are refreshed.
//	 * 
//	 * If the connection is a user defined one, then the connections stack pool is cleared for that connection.
//	 */
//	public static void lostConnection( String connectionID ) {
//		
//		// For now only do something about lost DISCOVERED connections (which are under system properties).
//		// Manually defined connections need dealing with differently. They must not be removed from config, but rather 
//		// just have all their copies cleaned out of the connections pool.
//		if ( spr.containsKey( connectionID + LABEL_HOST ) ) // no need to synchronize on spr - as either it exists already or it is a user property
//			GaianNodeSeeker.lostConnection( connectionID );
//		else
//			DataSourcesManager.clearSourceHandlesStackPool( getRDBConnectionDetailsAsString(connectionID) );
//	}

	public static boolean removeDiscoveredGaianNode( String connectionID ) {
		
		logger.logInfo("Removing Discovered Node: " + connectionID);
		
		synchronized ( DISCOVERED_GAIAN_CONNECTIONS ) {
		
			String discoveredConnectionsList = getSystemProperty( DISCOVERED_GAIAN_CONNECTIONS );
			int index;
			
			if ( null == discoveredConnectionsList || -1 == ( index = discoveredConnectionsList.indexOf(connectionID) ) ) {
				logger.logThreadInfo("Connection does not exist - no changes made to registry");
				return false;
			}
			
			StringBuffer buf = new StringBuffer( discoveredConnectionsList );
			
			if ( 0 == index ) index++;
			
			buf.delete( index-1, index + connectionID.length() );
			
			synchronized( spr ) {
				spr.put( DISCOVERED_GAIAN_CONNECTIONS, buf.toString() );
				spr.remove( connectionID + LABEL_DRIVER );
				spr.remove( connectionID + LABEL_URL );
				spr.remove( connectionID + LABEL_USR );
				spr.remove( connectionID + LABEL_PWD );
				spr.remove( connectionID + LABEL_DNODEID );
			}
		}
		
//		dumpRegistryIfLoggingOn();
		
		return true;
	}
		
//	public static void addGaianNode( String connectionDetails, String details ) {
//		addDiscoveredGaianNode( connectionDetails, details );
//	}
	
//	public static boolean arraysMatch( String[] a, String[] b ) {
//		return Arrays.asList(a).toString().intern() == Arrays.asList(b).toString().intern();
//	}
	
	/**
	 * Attaches a new gaian node child definition to every logical table.
	 * The definition is described by the connection details.
	 * 
	 * If any of the child nodes of any logical table is a gaian node and uses the same
	 * connection details, then this method returns null to indicate that the gaian node
	 * child is already attached to this node - this is a valid outcome (i.e. not an exception)
	 * 
	 * The nodeID arg is a unique description of the db connection in the form: "hostname:port/dbname"
	 * 
	 * @return The new connection ID upon success, or null if a connection with these properties already exists
	 */
	public static String addDiscoveredGaianNode(
			final String driver, final String url, final String usr, final String scrambledpwd, final String nodeID ) {
			
		logger.logDetail("Attempting to add Discovered Node: " + nodeID);
		
		// unscramble pwd based on foreign node id
		String pwd = unscramble(scrambledpwd, nodeID);
//		System.out.println("unscramble: " + scrambledpwd + ", nodeID " + nodeID + " -> " + pwd);
		String connectionDetails = /*GaianDBConfig.DERBY_CLIENT_DRIVER*/ driver + "'" + url + "'" + usr + "'" + pwd;
		
		String ckey = null; // The new connection key
		
		synchronized ( DISCOVERED_GAIAN_CONNECTIONS ) {
			
//			final String cid = lookupAllRegisteredCIDs().get( connectionDetails );
//			if ( null != cid ) { logger.logDetail("Gaian connection ("+cid+") already exists to: " + nodeID); return cid; }
		
			String[] ggcs = getDiscoveredConnections();
			
			// Check if the new connection already exists amongst global gaian ones
			// Note that any overlapping user defined ones will just be ignored when the DataSourcesManager loads them.
			for ( int i=0; i<ggcs.length; i++ ) {				
				try {
//					System.out.println("ggcsi resolved: " + getRDBConnectionDetailsAsString( ggcs[i] ) + " new connection details: " + connectionDetails);
					if ( getRDBConnectionDetailsAsString( ggcs[i] ).equalsIgnoreCase(connectionDetails) ) {
						logger.logDetail("Gaian connection (" + ggcs[i] + ") already exists to: " + nodeID);
						return ggcs[i];
					}
				} catch (Exception e) { continue; } //ignore (shouldnt happen) - warning is logged at load time anyway
			}
	
			int i=1;
			do { ckey = "C"+(i++); } while (
					null != getCrossOverProperty(ckey+LABEL_DNODEID ) ||
					null != getCrossOverProperty(ckey+LABEL_DRIVER ) ||
					null != getCrossOverProperty(ckey+LABEL_URL ) ||
					null != getCrossOverProperty(ckey+LABEL_USR ) ||
					null != getCrossOverProperty(ckey+LABEL_PWD )
			);
	
			logger.logInfo("New Discovered Node: " + nodeID + " - Creating associated Gaian Connection: " + ckey);
			
//			System.out.println("Current spr: " + Arrays.asList(spr));
			synchronized(spr) {
				
				spr.setProperty( ckey+LABEL_DRIVER, driver );
				spr.setProperty( ckey+LABEL_URL, url );
				spr.setProperty( ckey+LABEL_USR, usr );
				spr.setProperty( ckey+LABEL_PWD, '\'' + scramble( pwd, ckey ) ); // re-scramble pwd based on new connection key
				spr.setProperty( ckey+LABEL_DNODEID, nodeID );
			
				String discoveredConnectionsList = getSystemProperty( DISCOVERED_GAIAN_CONNECTIONS );
				spr.setProperty( DISCOVERED_GAIAN_CONNECTIONS,
						( null == discoveredConnectionsList || 0 == discoveredConnectionsList.length() ? ckey :
							discoveredConnectionsList + "," + ckey ) );
				
				if ( null != registeredConnectionIDs ) {
					String cids = registeredConnectionIDs.get(url+"'"+usr);
					registeredConnectionIDs.put(url+"'"+usr, null==cids?ckey:cids+","+ckey);
				}
			}
		}
		
//		dumpRegistryIfLoggingOn();
		return ckey;
	}

	// This holds all RDBMS URLs/USRs refs in config - including system ones in memory, and those that are not referenced/loaded.
	private static Map<String, String> registeredConnectionIDs = null; // Reverse map lookup for: url'usr -> cid
	private static Set<String> registeredBlueMixUrlUsrKeys = null;
	
	static final Map<String, String> lookupAllRegisteredCIDs() {
		
		if ( null == registeredConnectionIDs ) {
			registeredConnectionIDs = new HashMap<String, String>();
			for ( Properties props : new Properties[] { upr, spr } )
				synchronized( props ) {
					for ( Iterator<Object> keys = props.keySet().iterator(); keys.hasNext(); ) {
						String key = (String) keys.next(), url=null;
						if ( key.endsWith( LABEL_URL ) && (url = props.getProperty(key)).startsWith("jdbc:") ) {
							final String cid = key.substring(0, key.length()-LABEL_URL.length());
							final String usr = (String)props.get(cid+LABEL_USR);
							if ( null != usr && props.containsKey(cid+LABEL_PWD) ) {
								String cids = registeredConnectionIDs.get(url+"'"+usr);
								registeredConnectionIDs.put(url+"'"+usr, null==cids?cid:cids+","+cid);
							}
						}
					}
				}	
		}
		
		return registeredConnectionIDs;
	}

	/**
	 * Adds/removes registered connection IDs based on whether they are in availableConnectionsToSynchTo
	 * 
	 * @param cidPrefix
	 * @param latestAvailableConnections Mapping of: "url'usr" -> pwd
	 * @return boolean to indicate whether in-memory config was changed (true) or stayed the same (false)
	 */
	public static final boolean synchronizeSystemRDBMSConnections( final String cidPrefix, final Map<String,String> availableConnectionsToSynchTo ) {
		
		boolean isConfigChanged = false;
		
		// Remove system cids for url/usr keys that are no longer available
		if ( null != registeredBlueMixUrlUsrKeys ) {
			if ( null != registeredConnectionIDs )
				for ( String urlAndUsr : registeredBlueMixUrlUsrKeys )
					if ( null == availableConnectionsToSynchTo || false == availableConnectionsToSynchTo.keySet().contains(urlAndUsr) ) {
						String cid = registeredConnectionIDs.remove(urlAndUsr); // The value for this key can only be a single cid (not a list)
						if ( null != cid ) synchronized(spr) { spr.remove(cid+LABEL_URL); spr.remove(cid+LABEL_USR); spr.remove(cid+LABEL_PWD); }
						isConfigChanged = true;
					}
			
			// Now ignore ones we have already registered
			if ( null != availableConnectionsToSynchTo )
				availableConnectionsToSynchTo.keySet().removeAll(registeredBlueMixUrlUsrKeys);
			registeredBlueMixUrlUsrKeys.clear();
		}

		if ( null == availableConnectionsToSynchTo || 1 > availableConnectionsToSynchTo.size() )
			return isConfigChanged; // no new available connections
		
		registeredBlueMixUrlUsrKeys = availableConnectionsToSynchTo.keySet();
		
		// Add new ones
		for ( String urlAndUsr : registeredBlueMixUrlUsrKeys ) {
			
			if ( lookupAllRegisteredCIDs().containsKey(urlAndUsr) ) continue;

			int idx = urlAndUsr.lastIndexOf('\'');
			String url = urlAndUsr.substring(0,idx), usr = urlAndUsr.substring(idx+1), pwd = availableConnectionsToSynchTo.get(urlAndUsr);
			
			int idx1 = "jdbc:".length(), idx2 = url.indexOf(':', idx1); if ( 0 > idx2 ) continue; // strange jdbc url, skip it
			final String rdbmsName = url.substring(idx1, idx2).trim().toUpperCase();
			
			String cid = null; int i=1;
			do { cid = cidPrefix+"_"+rdbmsName+"_C"+(i++); } while (
					null != getCrossOverProperty(cid+LABEL_DRIVER ) ||
					null != getCrossOverProperty(cid+LABEL_URL ) ||
					null != getCrossOverProperty(cid+LABEL_USR ) ||
					null != getCrossOverProperty(cid+LABEL_PWD )
			);
			
//			System.out.println("Registering new cid for url: " + url + " -> " + cid);
			synchronized(spr) {
				spr.setProperty( cid+LABEL_URL, url );
				spr.setProperty( cid+LABEL_USR, usr );
				spr.setProperty( cid+LABEL_PWD, '\'' + scramble( pwd, cid ) ); // scramble pwd based on connection id
				
				// Register this RDBMS - cannot have an existing entry for this urlAndUsr because an API or file update resets registeredConnectionIDs.
				if ( null != registeredConnectionIDs ) registeredConnectionIDs.put(url+"'"+usr, cid);
			}
			isConfigChanged = true;
		}
		return isConfigChanged;
	}
		
//	public static final String addSystemRDBMSConnectionIfNotExists(
//			final String cidPrefix, final String url, final String usr, final String pwd ) {
//		
//		final String urlAndUsr = url+"'"+usr;
//		if ( lookupAllRegisteredCIDs().containsKey(urlAndUsr) ) return null;
//		
//		String cid = null; int i=1;
//		do { cid = cidPrefix+(i++); } while (
//				null != getCrossOverProperty(cid+LABEL_DRIVER ) ||
//				null != getCrossOverProperty(cid+LABEL_URL ) ||
//				null != getCrossOverProperty(cid+LABEL_USR ) ||
//				null != getCrossOverProperty(cid+LABEL_PWD )
//		);
//		
////		System.out.println("Registering new cid for url: " + url + " -> " + cid);
//		synchronized(spr) {
//			spr.setProperty( cid+LABEL_URL, url );
//			spr.setProperty( cid+LABEL_USR, usr );
//			spr.setProperty( cid+LABEL_PWD, '\'' + scramble( pwd, cid ) ); // scramble pwd based on connection id
//			if ( null != registeredConnectionIDs ) {
//				String cids = registeredConnectionIDs.get(url+"'"+usr);
//				registeredConnectionIDs.put(url+"'"+usr, null==cids?cid:cids+","+cid);
//			}
//		}
//		
//		return cid;
//	}
	
	private static File getConfigFile() throws FileNotFoundException {

		// If configFile was set but the file has been replaced entirely with a new one then canRead() will fail
		if ( null == configFile || !configFile.canRead() ) {
			configFile = new File( configFileLocation );
			if ( false == configFile.exists() ) throw new FileNotFoundException("File not found: " + configFileLocation);
		}
		return configFile;
	}
	
	public static String getConfigFileName() throws FileNotFoundException { return getConfigFile().getName(); }
	public static String getConfigFilePath() throws IOException { return getConfigFile().getCanonicalPath(); }

	protected static long getConfigFileLastModified() throws FileNotFoundException { return getConfigFile().lastModified(); }
		
	private static void reloadUserProperties() {

		if ( null == upr ) upr = new Properties();
		String cfgFileAbsolutePath = "<Unresolved Location>";
		
		try {
			long latestConfigFileLastModified = getConfigFileLastModified();
			
			File cfgFile = getConfigFile();
			FileInputStream fis = new FileInputStream( cfgFile );
			cfgFileAbsolutePath = cfgFile.getCanonicalPath();
			
			synchronized( upr ) {
				upr.clear();
				if ( null != registeredConnectionIDs ) { registeredConnectionIDs.clear(); registeredConnectionIDs = null; }
				if ( null != registeredBlueMixUrlUsrKeys ) { registeredBlueMixUrlUsrKeys.clear(); registeredBlueMixUrlUsrKeys = null; }
				upr.putAll( inMemoryDataSourceProperties ); // these could be overridden by persistent properties...
				upr.load( new InputStreamReader(fis, "UTF8") );
				
				// The properties below can be overridden by re-defining them manually *IN UPPERCASE* in the config file
				// (Note that the GAIAN_NODE_VERSION is already in upper case here so it cannot be changed)
				upr.setProperty("derby_tables_def", "TABNAME VARCHAR(30), TABTYPE CHAR, COLNAME VARCHAR(30), COLTYPE VARCHAR(80)");
				upr.setProperty("derby_tables_ds0_connection", "localderby sys.systables,sys.syscolumns where tableid=referenceid");
				upr.setProperty("derby_tables_ds0_c1", "TABLENAME");
				upr.setProperty("derby_tables_ds0_c2", "TABLETYPE");
				upr.setProperty("derby_tables_ds0_c3", "COLUMNNAME");
				upr.setProperty("derby_tables_ds0_c4", "COLUMNDATATYPE");
				
				upr.setProperty("gdb_ltlog_def", "COLUMN1 VARCHAR(255)");
				upr.setProperty("gdb_ltlog_ds0_vti", "com.ibm.db2j.FileImport");
				upr.setProperty("gdb_ltlog_ds0_args", GaianNode.getWorkspaceDir() + "/" + GaianDBConfig.getGaianNodeDatabaseName() + ".log");

				upr.setProperty("GAIAN_NODE_VERSION", GaianNode.GDB_VERSION + " - " + GaianNode.JAR_TSTAMPS[0] + " - " + GaianNode.JAR_SIZES[0] + " bytes");

				upr.setProperty("gdb_ltnull_def", "CNULL CHAR(1)"); // getColumnCount() on a VTI must be > 0
				
				// Only now should we set the definitive latest config file timestamp of loaded properties.

//				logger.logInfo("Updating config file lastModified from: " + latestLoadedPropertiesFileTimestamp + ", to: " + latestConfigFileLastModified + ", stack trace: " + Util.getStackTraceDigest());
				latestLoadedPropertiesFileTimestamp = latestConfigFileLastModified;
				
				if ( null != derbyProperties ) {
					derbyGaianNodePassword = derbyProperties.getProperty("derby.user." + getGaianNodeUser());
//					derbyGaianNodeAdminPassword = derbyProperties.getProperty("derby.user." + GDBADMIN_USER);
				}
			}
			fis.close();
			
			// special case below generates a password from "passw0rd" that has a backslash '\' in it - 
			// this is silently dropped by the load() so had to write a new method: Util.escapeBackslashes()
//			System.out.println("pwd: " + upr.getProperty("G105-39_PWD"));
//			System.out.println("pwd: " + unscramble( upr.getProperty("G105-39_PWD").substring(1), "G105-39") );
			
//			logger.logInfo("Reloaded user properties: " + upr);
			
		} catch ( IOException e ) {
			logger.logWarning( GDBMessages.CONFIG_USER_PROPS_RELOAD_ERROR, "Unable to reload user properties at: " + cfgFileAbsolutePath +
					", cause: " + Util.getStackTraceDigest(e) );
		}
	}

	// Commented out for now: No need for a path.
	// Derby already looks up it's dbs relative to property: derby.system.home (Unless a full path is specified in the URL)
//	public static String getGaianNodeDatabasePath() {
//		return getGaianNodeDatabaseName(); // This works, as its a fixed physical db and always in the working directory.
//	}
	
	public static String getGaianNodeDatabaseName() {
//		String db = getUserProperty( GAIAN_NODE_DB );
//		if ( null != db ) return db; //null == gaiandbWorkspace ? db : gaiandbWorkspace + File.separator + db;
		
		if ( null == DBNAME ) {
//			db = DEFAULT_ROOT_DBNAME + (derbyServerListenerPort == GaianNode.DEFAULT_PORT ? "" : derbyServerListenerPort+"");
//			DBNAME = null == gaiandbWorkspace ? db : gaiandbWorkspace + File.separator + db;
			
			DBNAME = GAIANDB_NAME + (derbyServerListenerPort == GaianNode.DEFAULT_PORT ? "" : derbyServerListenerPort+"");
		}
		
		return DBNAME;
	}
	
	public static String getGaianNodeHostName() {
		if ( null == gaianNodeHostName ) {
			try {
				gaianNodeHostName = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				logger.logException( GDBMessages.CONFIG_GET_NODE_HOSTNAME_ERROR, "Unable to resolve Gaian Node host name: ", e );
			}
		}
		return gaianNodeHostName;
	}
	
	public static String getGaianNodeUser() {
		if ( null == derbyGaianNodeUser ) {
			derbyGaianNodeUser = getUserProperty( GAIAN_NODE_USR );
			if ( null == derbyGaianNodeUser ) derbyGaianNodeUser = GAIAN_NODE_DEFAULT_USR;
		}
		return derbyGaianNodeUser;
	}
	
	// If authentication is switched off (i.e. derby.properties file not set up for it) return a dummy password
	// Otherwise, return the password set in the derby.properties file for the gaiandb user.
	public static String getGaianNodePassword() {
		String pwd = getUserProperty( GAIAN_NODE_PWD );
		return null == pwd ? ( null == derbyGaianNodePassword ? GAIAN_NODE_DEFAULT_PWD : derbyGaianNodePassword ) :
			0 < pwd.length() && '\'' == pwd.charAt(0) ? unscramble(pwd.substring(1), GAIAN_NODE_PWD) : pwd;
//		return null == derbyGaianNodePassword ? GAIAN_NODE_DEFAULT_PWD : derbyGaianNodePassword;
	}
	
	public static String getDerbyAuthMode() {
		return null == derbyProperties ? null : derbyProperties.getProperty(DERBY_AUTH_MODE_KEY);
	}
	
	public static String getSSLMode() {
		if ( null == derbyProperties ) return "off";
		final String p = derbyProperties.getProperty(DERBY_SSL_MODE);
		return null == p || ( !p.equals("basic") && !p.equals("peerAuthentication") ) ? "off" : p;
	}
	
//	public static String getGaianNodeAdminUser() {
//		return GDBADMIN_USER;
//	}
//	
//	static String getGaianNodeAdminPassword() {
//		return null == derbyGaianNodeAdminPassword ? GDBADMIN_DEFAULT_PASSWORD : derbyGaianNodeAdminPassword;
//	}

	static String getGaianNodePasswordScrambled() {
		return scramble( getGaianNodePassword(), getGaianNodeID() );
	}
	
//	public static String getGaianNodeDescription() {
////		String nid = getUniqueGaianNodeID();
////		return null==nid ? null : nid + "/" + getGaianNodeDatabase();
//		return getGaianNodeName() + ":" + derbyServerListenerPort + "/" + getGaianNodeDatabase();
//	}
	
//	public static String getGaianNodeConnectionDetails() {
//		return SQLDerbyRunner.DERBY_CLIENT_NETWORK_DRIVER + " " + getGaianNodeDatabase() + " " + 
//		getGaianNodeUser() + " " + getGaianNodePassword();
//	}
	
	public static final String removeProblematicChars( String s ) {
		if ( null == s ) return s;
//		return s.replaceAll("[\\s'\",;*@]", ""); // Remove all whitespace chars, single/double quotes, comma/semi-colons and *,@		
		return s.replaceAll("\\W", ""); // Remove all chars other than word chars [a-zA-Z_0-9]
	}
	
	public static void setGaianNodeName( String nodeName ) {
		if ( null != nodeName && null == gaianNodeID ) {
			gaianNodeID = removeProblematicChars( nodeName );
			gaianNodeID += GaianNode.DEFAULT_PORT == derbyServerListenerPort ? "" : ":" + derbyServerListenerPort;
		}
	}
	
	public static synchronized String getGaianNodeID() {
		if ( null == gaianNodeID || 0 == gaianNodeID.length()) {			
			gaianNodeID = removeProblematicChars( getGaianNodeHostName() );
			if ( null == gaianNodeID || 0 == gaianNodeID.length() )
				gaianNodeID = new Integer( new Random().nextInt() ).toString();
			gaianNodeID += GaianNode.DEFAULT_PORT == derbyServerListenerPort ? "" : ":" + derbyServerListenerPort;
		}
		return gaianNodeID;
	}
	
	/**
	 * This method must be synchronized because 2 threads could both call 'counter++' before either
	 * of them evaluates the encapsulating expression - thus returning the same query id.
	 * 
	 * @return a unique query id.
	 */
	public synchronized static String generateUniqueQueryID() {
		
		String nid = getGaianNodeID();
		// Use currentTimeMillis() timestamp to differentiate queries coming from the same node after a restart of the node.
		return null==nid ? null : nid + ":" + System.currentTimeMillis() + ":" + (int)queryIDSequenceCounter++;
	}

	public static String getNodeDefVTI( String nodeDefName ) {
		return getUserProperty( nodeDefName + VTI_SUFFIX );
	}
	
	private static String[] getDataSourceOptions( String nodeDefName ) {
		String optionsProp = getUserProperty( nodeDefName + OPTIONS_SUFFIX );
		return Util.splitByCommas( optionsProp );
	}

	public static boolean isNodeInMemoryOptionSet( String nodeDefName ) { return isNodeOptionSet( nodeDefName, INMEMORY ); }
	public static boolean isNodePluralizedOptionSet( String nodeDefName ) { return isNodeOptionSet( nodeDefName, PLURALIZED ); }
	
	private static final List<String> permittedSuffixes = Arrays.asList( "", "_0", "_1", "_P", "_X", "_XF" );	
	private static final Map<String, String> ltViewTailOptions = new HashMap<String, String>() {{
		put( "", ", ''"); put( "_0", ", 'maxDepth=0'"); put( "_1", ", 'maxDepth=1'");
		put( "_P", ", 'with_provenance'"); put( "_X", ", 'explain'"); put( "_XF", ", 'explain in graph.dot'");
	}};
//	private static final String[] ltViewTailOptions	= { ", ''", ", 'maxDepth=0'", ", 'maxDepth=1'", ", 'with_provenance'", ", 'explain'", ", 'explain in graph.dot'" };
	
	public static String getLogicalTableTailOptionsForViewSuffix( String suffix ) { return ltViewTailOptions.get(suffix); }
	
	private static final String LOGICAL_TABLES_HAVING_BASIC_VIEWS_ONLY = "LOGICAL_TABLES_HAVING_BASIC_VIEWS_ONLY";
	
	public static Collection<String> getLogicalTableRequiredViewSuffixes( String ltName ) {
//		String propVal = getUserProperty( ltName + "_VIEW_SUFFIXES" );
//		if ( null == propVal ) return permittedSuffixes;
//		Set<String> userSuffixesSet = new HashSet<String>( Arrays.asList( (',' + propVal).trim().split("[\\s]*,[\\s]*", -1 ) ) );
//		for ( String s : userSuffixesSet ) if ( false == permittedSuffixes.contains(s) ) return permittedSuffixes;
		
		String propVal = getUserProperty( LOGICAL_TABLES_HAVING_BASIC_VIEWS_ONLY );
		if ( null == propVal ) return permittedSuffixes;
		Set<String> ltsWithBasicViewOnly = new HashSet<String>( Arrays.asList( Util.splitByCommas( propVal ) ));
		
		for ( String s : ltsWithBasicViewOnly ) {
			int slen = s.length();
			if ( ltName.equals(s) || ( '*' == s.charAt(slen-1) && ltName.startsWith( s.substring(0,slen-1) ) ))
					return Arrays.asList("");
		}
		
//		logger.logInfo("Resolved " + userSuffixesSet.size() + " user-defined view suffixes for LT: " + ltName + " -> " + userSuffixesSet);
		
		return permittedSuffixes;
	}
	
	private static boolean isNodeOptionSet( String nodeDefName, String optionID ) {
		
		String[] options = getDataSourceOptions( nodeDefName );
		for (int i=0; i<options.length; i++) {
//			System.out.println("option " + i + ":" + options[i]);
			if ( Util.splitByTrimmedDelimiter( options[i], ' ' )[0].equals( optionID ) )
				return true;
		}
		
		return false;
	}
	
	public static boolean isPluralizedOptionUsingRegex( String nodeDefName ) {
		String[] options = getDataSourceOptions( nodeDefName );
		
		for (int i=0; i<options.length; i++) {
			final String option = options[i];
			
			if ( option.startsWith(PLURALIZED) ) {
				String[] tokens = Util.splitByWhitespace( option );
				if ( 2 > tokens.length ) break;
				
				if ( 3 == tokens.length && "USING".equals(tokens[1]) )
					if ("REGEX".equals(tokens[2])) return true;
					else if ("WILDCARD".equals(tokens[2])) return false;
					
				logger.logWarning(GDBMessages.CONFIG_INVALID_FILE_OPTION_PLURALIZED_SYNTAX,
						"Invalid keywords declaration (" + option + "). Syntax should be: PLURALIZED [USING WILDCARD|REGEX]");
			}
		}
		
		return false; // Not pluralized using a regex mask
	}

	private static final String PLURALIZED_COL_MAPPINGS_SYNTAX_HELP =
		"Syntax should be for example to map to LT cols 5 and 3: LTX_DSY_OPTIONS=PLURALIZED WITH ENDPOINT CONSTANTS C5 C3";
	
	public static int[] getDataSourceWrapperPluralizedEndpointConstantColMappings( String nodeDefName ) {
		
		String[] options = getDataSourceOptions( nodeDefName );
		
		for (int i=0; i<options.length; i++) {
			final String option = options[i];
			
			if ( option.startsWith(PLURALIZED) ) {
				String[] tokens = Util.splitByWhitespace( option );
				if ( 2 > tokens.length ) break;
				
				String errTxt = null;
				if ( 4 < tokens.length && "WITH".equals(tokens[1]) && "ENDPOINT".equals(tokens[2]) && "CONSTANTS".equals(tokens[3]) ) {
					int[] colMappings = new int[ tokens.length - 4 ];
					for ( int j=4; j<tokens.length; j++ ) {
						String s = tokens[j];
						try { if ( 'C' == s.charAt(0) ) colMappings[j-4] = Integer.parseInt( s.substring(1) ); continue; }
						catch ( Exception e ) { errTxt = e.toString(); }
						if ( null == errTxt ) errTxt = PLURALIZED_COL_MAPPINGS_SYNTAX_HELP;
						logger.logWarning(GDBMessages.CONFIG_INVALID_ENDPOINT_COL_MAPPING_SYNTAX, "Invalid mapping syntax (" + s + "): " + errTxt);
						break;
					}
					
					if ( null == errTxt ) return colMappings;
					
				} else logger.logWarning(GDBMessages.CONFIG_INVALID_ENDPOINT_COL_MAPPING_SYNTAX,
						"Invalid keywords declaration (" + option + "). " + PLURALIZED_COL_MAPPINGS_SYNTAX_HELP);
			}
		}
		
		return null; // no PLURALIZED option or not specified mappings for: endpoint constants -> lt cols
	}
	
    private static final Pattern inMemoryOptionExpiryPattern = Pattern.compile("[\\s]*INMEMORY[\\s]+.*EXPIRY[\\s]+([1-9][\\d]*)[\\s]*");
	
	public static long getDataSourceCacheExpirySeconds( String nodeDefName ) {

		String[] options = getDataSourceOptions( nodeDefName );
		for (int i=0; i<options.length; i++) {
			String expiryAsString = inMemoryOptionExpiryPattern.matcher(options[i]).replaceFirst("$1");
			if ( expiryAsString.equals(options[i]) ) continue;			
			return Long.parseLong( expiryAsString );
		}
		
		return -1;
	}

	public static boolean isLogPerformanceOn() {
		String logPerf = getUserProperty(LOGPERF);
		return "ON".equals(logPerf);
	}
	
	/**
	 * Returns a Hashtable mapping of: physical source colId -> logical column type
	 * 
	 * The physical col ids are those that should be indexed if the INMEMORY option is set.
	 * 
	 * If INMEMORY is not set - or if PLURALIZED *is set* - then null is returned.
	 * 
	 * @param nodeDefName
	 * @return
	 */	
	public static ConcurrentMap<Integer, Integer> getInMemoryIndexes( String nodeDefName, 
			String[] ltPhysicaColNames, int[] columnsMapping, GaianResultSetMetaData ltrsmd ) {
		
		String[] options = getDataSourceOptions( nodeDefName );
		
		for (int i=0; i<options.length; i++)
			if ( options[i].startsWith( PLURALIZED ) ) return null;
		
		for (int i=0; i<options.length; i++) {
//			logger.logInfo("Looking for InMemory indexes: checking relevance of option: " + options[i]);
			if ( options[i].startsWith( INMEMORY ) ) {
				
				String[] elmts = Util.splitByTrimmedDelimiter( options[i] , ' ' ); //.split("[ |\t]*");
				
				ConcurrentMap<Integer, Integer> indexes = new ConcurrentHashMap<Integer, Integer>();
				
				if ( 1 < elmts.length && elmts[1].equals("INDEX") ) {
				
					if ( 3 < elmts.length && elmts[2].equals("ON")) {
						
						if ( 4 < elmts.length && false == elmts[4].equals("EXPIRY") ) {
							logger.logThreadWarning( GDBMessages.CONFIG_INDEX_DEF_IGNORE, 
									nodeDefName + " getInMemoryIndexes(): Only 1 unique column index may be specified - ignoring index definition" );
						} else {
							int j = 3;
//						for ( int j=4; j<elmts.length; j++ ) {
							String col = elmts[j];
							
							String ltPhysicalColName = null;
							for ( int k=0; k<ltPhysicaColNames.length; k++ ) {
								
								ltPhysicalColName = ltPhysicaColNames[k];
								if ( ltPhysicalColName.equals(col) ) {
									int pSourceColIndex = columnsMapping[k];
									int ltColType = ltrsmd.getColumnType(k+1);
									indexes.put( new Integer( pSourceColIndex ), new Integer( ltColType ) );
									logger.logThreadInfo( nodeDefName + " getInMemoryIndexes(): Recognised physical column " + col + 
											" to be source col ID "+ pSourceColIndex + ", logical col type " + 
											TypeId.getBuiltInTypeId(ltColType).getSQLTypeName());
									break;
								}
							}
							
							if ( 0 == indexes.size() )
								logger.logThreadWarning( GDBMessages.CONFIG_COLUMN_NOT_RECOGNISED, 
										nodeDefName + " getInMemoryIndexes(): Unrecognised physical column name for indexing: " + col );
//						}
						}
											
					} else
						logger.logThreadWarning( GDBMessages.CONFIG_INDEX_DEF_ERROR,
								nodeDefName + " getInMemoryIndexes(): Incorrect index definition (ignored), should be: INMEMORY INDEX ON <col_name>" );
				}
				
//				int[] idxs = new int[ indexes.size() ];
//				for ( int j=0; j<idxs.length; j++ ) {
//					idxs[j] = ((Integer) indexes.get(j)).intValue();
//				}

//				logger.logInfo("returning in mem indexes: " + Arrays.asList( indexes.keySet() ));
				
				return indexes;
			}
		}
		return null; // only return null if INMEMORY is not even set.
	}
	
	public static boolean isPropertyPotentiallyImpactingALogicalTable( String key ) {
		
		if ( null == key ) return false;
		if ( key.endsWith(LTDEF_SUFFIX) || key.endsWith(CONSTANTS_SUFFIX) ) return true;
		
		int dsIdx = -1;
		
		// Determine if this might be a data source property of a defined logical table.
		while ( -1 < ( dsIdx = key.indexOf("_DS", dsIdx+1) ) )
			if ( null != getUserProperty( key.substring(0, dsIdx) + LTDEF_SUFFIX ))
				return true;
		
		if ( key.endsWith(LABEL_DRIVER) || key.endsWith(LABEL_URL) || key.endsWith(LABEL_USR) || key.endsWith(LABEL_PWD) )
			return true;
		
		return false;
	}
	
	static boolean isNodeDefExists( String nodeDefName ) {
		return isNodeDefRDBMS(nodeDefName) || isNodeDefVTI(nodeDefName);
	}
	
	public static boolean isNodeDefVTI( String nodeDefName ) {
		return ( null != getUserProperty( nodeDefName + VTI_SUFFIX ) );
	}
	
	public static boolean isNodeDefRDBMS( String nodeDefName ) {
		return ( null != getCrossOverProperty( nodeDefName + CONNECTION_SUFFIX ) );
	}
	
	public static boolean isNodeDefGaian( String nodeDefName ) {
		return (  null != getCrossOverProperty( nodeDefName + CONNECTION_SUFFIX )
				&& null == getNodeDefRDBMSTable( nodeDefName ) 
				&& null == getNodeDefVTI( nodeDefName ) );
	}

	public static String getNodeDefRDBMSTable( String nodeDefName ) {
		String table = getUserProperty( nodeDefName + TABLE_SUFFIX );
		if ( null != table ) return table;
		
		// shorthand version for defining table names: e.g. LT0_N0_CONNECTION=DERBY TABLE1
		String c = getCrossOverProperty( nodeDefName + CONNECTION_SUFFIX );
		if ( null==c ) return null;
		int spacepos = c.indexOf(' ');
		if ( -1 == spacepos ) return null;
		else return c.substring( spacepos+1 );
	}
	
	public static String getNodeDefRDBMSConnectionID( String nodeDefName ) {
		String c = getCrossOverProperty( nodeDefName + CONNECTION_SUFFIX );
		if ( null == c ) return null;
		c = c.toUpperCase();
		int spacepos = c.indexOf(' ');
		if ( -1 == spacepos ) return c;
		else return c.substring( 0, spacepos );
	}
	
	public static String getVTIArguments( String nodeDefName ) {
		String prop = getUserProperty( nodeDefName + ARGS_SUFFIX );
		return null == prop ? null : resolvePathTags( prop );
	}
	
	private static final String GW_TAG = "<GAIAN_WORKSPACE>";
	public static String resolvePathTags( String path ) {
		return path.startsWith(GW_TAG) ? GaianNode.getWorkspaceDir() + path.substring(GW_TAG.length()) : path;
	}

	public static String getVTISchema( String nodeDefName ) {
		String prop = getUserProperty( nodeDefName + SCHEMA_SUFFIX );
		if ( null == prop ) return null;
		return prop;//.split(",");
	}
	
	public static boolean isRegistryNeedsReloadingFromFile() {
		try {
			
//			Class klass = getRB().getClass().getSuperclass();
//			Field field = klass.getDeclaredField("cacheList");
//			field.setAccessible(true);
//			sun.misc.SoftCache cache = (sun.misc.SoftCache)field.get(null);
//			cache.clear();
//			field.setAccessible(false);
//			loadedConfigLastModified = getConfigFileLastModified();
			
			// No need to refresh if we haven't loaded the properties at all yet, or if they haven't changed
			if ( null == upr ) return false;

			long latestConfigFileLastModified = getConfigFileLastModified();
			if ( latestLoadedPropertiesFileTimestamp == latestConfigFileLastModified ) return false;
						
		} catch ( Exception e ) {
			logger.logException( GDBMessages.CONFIG_CHECK_PROPS_ERROR, "Unable to check resource properties file modification timestamp: ", e);
			return false;
		}
		
		return true;
	}
	
	/**
	 * No refresh if we haven't loaded the properties at all yet, or if they haven't changed
	 * @throws FileNotFoundException
	 *
	 */
	public static boolean refreshRegistryIfNecessary() {
		
		if ( !isRegistryNeedsReloadingFromFile() )
			return false;
		reloadUserProperties();
		return true;
	}

	static Set<String> getGaianConnectionsAsSet() {		
		return new HashSet<String>( Arrays.asList(getGaianConnections()) );
	}
	
	public static String[] getGaianConnections() {
		return Util.splitByCommas(getGaianConnectionsList());
	}
	
	public static String[] getGaianConnectedNodes( String[] gcs ) {

		String[] nodes = new String[gcs.length];
		for ( int i=0; i<gcs.length; i++ ) {
			nodes[i] = getGaianNodeID( gcs[i] );
		}
		
		return nodes;
	}

	public static String[] getDiscoveredConnections() { return Util.splitByCommas(getSystemProperty(DISCOVERED_GAIAN_CONNECTIONS)); }
	public static String[] getDefinedConnections() { return Util.splitByCommas(getUserProperty(DEFINED_GAIAN_CONNECTIONS)); }
		
	public static String getAccessHostsPermitted() {
		String ahp = getUserProperty(ACCESS_HOSTS_PERMITTED);
		return null != ahp ? ahp : getUserProperty(DISCOVERY_HOSTS);
	}
	
	public static String getAccessHostsDenied() { return getUserProperty(ACCESS_HOSTS_DENIED); }
	public static String getAccessClusters() { return getUserProperty(ACCESS_CLUSTERS); }
	
	public static String getConnectionStrategy() { return getUserProperty(CONNECTION_STRATEGY); }
	
	public static String getDiscoveryIP() { return getUserProperty(DISCOVERY_IP); }
	public static String getDiscoveryGateways() { return getUserProperty(DISCOVERY_GATEWAYS); }
	public static String getMulticastInterfaces() { return getUserProperty(MULTICAST_INTERFACES); }
	
//	public static String[] getMulticastDomains() {
//		String mifs = getUserProperty(MULTICAST_DOMAINS);
//		return null == mifs ? null : Util.splitByCommas(mifs);
//	}
	
	private static String getGaianConnectionsList() {
		
		String gcl1 = getSystemProperty(DISCOVERED_GAIAN_CONNECTIONS);
		String gcl2 = getUserProperty(DEFINED_GAIAN_CONNECTIONS);
		
		String gcl;
		
		if (null == gcl1 || 0 == gcl1.length()) {
			if (null == gcl2 || 0 == gcl2.length())
				return "";
			gcl = gcl2;
			
		} else if (null == gcl2 || 0 == gcl2.length())
			gcl = gcl1;
		else
			gcl = gcl1 + "," + gcl2;
		
		return gcl;
	}
		
    /**
     * Returns all property keys that have the given suffix, without the suffix
     * @param prefix
     * @return
     */
    static Set<String> getAllUserPropertiesWithSuffixAndRemoveSuffix( String suffix ) {
    	
		Set<String> set = new HashSet<String>();
    	if ( null == upr ) reloadUserProperties();
		
		synchronized( upr ) {
			for ( Iterator<Object> keys = upr.keySet().iterator(); keys.hasNext(); ) {
				String key = ((String) keys.next()).toUpperCase();
				if ( key.endsWith(suffix) ) {
					int offset = key.lastIndexOf(suffix);
					set.add( key.substring(0, offset) );
				}
			}
		}
		return set; //(String []) set.toArray( new String[0] );
    }

    /**
     * Returns all property values mapped to keys which end with the given suffix.
     * @param suffix
     * @return
     */
    static String[] getAllUserPropertyValuesForKeysWithSuffix( String suffix ) {
    	
		HashSet<String> s = new HashSet<String>();
		
    	if ( null == upr ) reloadUserProperties();
		
		synchronized( upr ) {
			for ( Iterator<Object> keys = upr.keySet().iterator(); keys.hasNext(); ) {
				String key = ((String) keys.next()).toUpperCase();
				if ( key.endsWith(suffix) ) {
					String val = upr.getProperty(key);
					if ( null != val ) s.add( val );
				}
			}
		}
		return s.toArray( new String[0] );
    }
    
    public static GaianDBConfigLogicalTables getAllLogicalTableDefinitionsAndReferences() {
    	
		Set<String> lts = getAllUserPropertiesWithSuffixAndRemoveSuffix( LTDEF_SUFFIX ); // getAllLogicalTableNames();
		Set<String> vtiDefs = new HashSet<String>();
		Set<String> jdbcRefs = new HashSet<String>();
		
    	if ( null == upr ) reloadUserProperties();
		
		synchronized( upr ) {
			
			for ( Iterator<Object> keys = upr.keySet().iterator(); keys.hasNext(); ) {
				String key = ((String) keys.next()).toUpperCase();
				
//				The logical table name itself may have a sufix of "_DS" - this is a valid case
				int ltEndIdx = key.lastIndexOf("_DS");
				
				// We are only interested in properties that define a data source
				if ( -1 < ltEndIdx && ( key.endsWith(VTI_SUFFIX) || key.endsWith(CONNECTION_SUFFIX) ) ) {
					
					final String ltName = key.substring(0, ltEndIdx);
					if ( false == lts.contains( ltName ) ) continue; // ignore data sources that are not attached to a logical table
				
					int offset = key.indexOf('_', ltName.length() + 2);
					
					String dsName = key.substring(0, offset);
					
					if ( key.equals(dsName+CONNECTION_SUFFIX) ) {
						String val = upr.getProperty(key);
						if ( null != val )
						try {
							jdbcRefs.add( getRDBConnectionDetailsAsString(dsName) );
						} catch (Exception e) {
							logger.logWarning(GDBMessages.CONFIG_LT_DS_GET_CONN_DETAILS_ERROR, "Unable to get JDBC connection details of LT data source (skipped): " +
									dsName + ": " + e);
						}
					} else if ( key.equals(dsName+VTI_SUFFIX) ) {
						String val = upr.getProperty(key);
						if ( null != val ) {
							if ( FileImport.class.getName().equals(val) )
								// Special case - if we consolidate with other VTIs in future, we'd also need to VTIFile's sourceID in VTIFile constructor
								vtiDefs.add( getVTIArguments(dsName) );
							else
								vtiDefs.add( val + ':' + getVTIArguments(dsName) );
//								vtiDefs.add( val + ':' + VTIBasic.derivePrefixArgFromCSVArgs(getVTIArguments(dsName)) );
						}
					}
				}
			}
		}
		
		return new GaianDBConfigLogicalTables( lts, vtiDefs, jdbcRefs );
    }
    
    /**
     * Returns all property keys that have the given prefix
     * @param prefix
     * @return
     */
    static String[] getAllUserPropertiesWithPrefix( String prefix ) {
    	
		HashSet<String> set = new HashSet<String>();
		
    	if ( null == upr ) reloadUserProperties();
		
		synchronized( upr ) {
			for ( Iterator<Object> keys = upr.keySet().iterator(); keys.hasNext(); ) {
				String key = ((String) keys.next()).toUpperCase();
				if ( key.startsWith(prefix ) )
					set.add( key );
			}
		}
		return set.toArray( new String[0] );
    }
    
    static String getLogicalTableViewSignature( String ltName ) {
    	String ltDef = getLogicalTableDef(ltName);
    	String ltConstants = getUserProperty( ltName + CONSTANTS_SUFFIX );
		return null == ltDef && null == ltConstants ? null : ltDef + "; Constant Cols: " + getConstantColumnsDef(ltName);
    }

    static Map<String, String> getLogicalTableStructuralSignature( String ltname ) {
    	
		Map<String, String> m = new HashMap<String, String>();
		m.put( NODE_CONSTANTS, getUserProperty(NODE_CONSTANTS) );
		
		Set<String> referencedConnections = new HashSet<String>();
		
    	if ( null == upr ) reloadUserProperties();
		
		synchronized( upr ) {
			for ( Iterator<Object> i = upr.keySet().iterator(); i.hasNext(); ) {
				String key = ((String) i.next()).toUpperCase();
				if ( key.equals( ltname + LTDEF_SUFFIX ) || key.equals( ltname + CONSTANTS_SUFFIX ) || key.startsWith( ltname + "_DS" ) ) {
					String value = getUserProperty(key);
					m.put( key, value );
					if ( key.endsWith(CONNECTION_SUFFIX) && null != value && 0 < value.length() ) {
						int spacepos = value.indexOf(' ');
						referencedConnections.add( -1 == spacepos ? value : value.substring(0, spacepos) );
					}
				}
			}
			
			synchronized( spr ) {
				for ( Iterator<String> i = referencedConnections.iterator(); i.hasNext(); ) {
					String key = i.next();
					String[] ks = new String[] { key + LABEL_DRIVER, key + LABEL_URL, key + LABEL_USR, key + LABEL_PWD };
					for ( int k=0; k<ks.length; k++ ) m.put( ks[k], getCrossOverProperty(ks[k]) );
				}
			}
		}
		return m;
    }
    		
	public static Set<String> getDataSourceDefs( String logicalTableName ) {

		HashSet<String> nodeDefs = new HashSet<String>();
		
    	if ( null == upr ) reloadUserProperties();
		
		synchronized( upr ) {
			for ( Iterator<Object> keys = upr.keySet().iterator(); keys.hasNext(); ) {
				String key = ((String) keys.next()).toUpperCase();
				if ( key.startsWith( logicalTableName + "_DS" ) && ( key.endsWith(VTI_SUFFIX) || key.endsWith(CONNECTION_SUFFIX) ) ) {
					
					// DRV 19/01/2013 - Fixed nasty bug here... take the index of the LAST underscore ('_') to remove the "_VTI" or "_CONNECTION" suffix
					int offset = key.lastIndexOf('_'); //, logicalTableName.length() + 2);
					nodeDefs.add( key.substring(0, offset) );
	//				logInfo("--remove this comment: getLatestLogicalTableNodeDefNames(): Added node def: " + key.substring(0, offset));				
				}
			}
		}
		return nodeDefs;
	}
		
	public static String getColumnName( String physicalNodeName, int columnIndex, ResultSetMetaData ltrsmd ) throws SQLException {
		
		// Try getting a physical column name mapping first...
		String columnName = getUserProperty( physicalNodeName + COLUMN_LABEL + (columnIndex+1) ); // 1-based
			
		if ( null != columnName )
			return columnName; // Sybase is case sensitive, so return case as-is.
		    //return columnName.toUpperCase();
		
		// No physical name mapping was defined, so default to the logical column one
		columnName = ltrsmd.getColumnName( columnIndex+1 ); // 1-based
		
		// Its OK if the default column is already used in the physical mapping definiton for another column
		// If types don't match then a failure will occur at runtime. 
		
//		// Check that the logical column name isn't used in the physical mapping definiton for another column
//		int colCount = ltrsmd.getColumnCount();		
//		for (int i=0; i<colCount; i++) {
//			String nextMappedCol = physicalNodeName + COLUMN_LABEL + i;
//			if ( columnName.equals( nextMappedCol ) )
//				return null; // This column cannot be used by this 
//		}
		
		return columnName;
	}
	
	public static boolean isColumnMappingExplicitlyDefined( String physicalNodeName, int columnIndex ) {
		return null != getUserProperty( physicalNodeName + COLUMN_LABEL + (columnIndex+1) );
	}
	
	public static boolean isColumnNamesMirroredFromLTDef( String physicalNodeName ) {

		String[] dsOptions = Util.splitByCommas( getUserProperty( physicalNodeName + OPTIONS_SUFFIX ) );
		return new HashSet<String>( Arrays.asList(dsOptions) ).contains( MAP_COLUMNS_BY_POSITION );
	}
	
//	private static String getWrappingLogicalTable( String physicalNodeName ) {
//		return physicalNodeName.substring( 0, physicalNodeName.lastIndexOf('_') );
//	}
	
	private static boolean cmdLineLogLevel = false;
	
	public static void assignLogLevel( String logLevelSetOnCmdLine ) {

		// Command line option takes precedence over user property (config file value)
		if ( cmdLineLogLevel )
			return; // Value already assigned on cmd line - cannot be changed
			
		if ( null != logLevelSetOnCmdLine ) {
			cmdLineLogLevel = true;
			Logger.setLogLevel( logLevelSetOnCmdLine );
			return;
		}
		
		// Get log level from config
		String s = getUserProperty( LOGLEVEL );
		if ( s == null ) return;
		
		Logger.setLogLevel(s);
	}
	
	public static String getBrokerHost() {
		return getUserProperty( MSGBROKER_HOST );
	}

	public static int getBrokerPort() {
		String port = getUserProperty( MSGBROKER_PORT );
		if ( null == port ) return -1;
		try { return Integer.parseInt( port ); } 
		catch ( NumberFormatException e ) {
			logger.logWarning(GDBMessages.CONFIG_BROKER_PORT_GET_ERROR, "Invalid property value: " + MSGBROKER_PORT + " must be a number");
			return -1;
		}
	}
	
	// Call this only on a refresh
	public static String getBrokerTopic() {
		return getUserProperty( MSGBROKER_TOPIC );
	}
	
	// Call this only on a refresh
	public static int getMsgStorerRowExpiry() {
		String expiry = getUserProperty( MSGSTORER_ROWEXPIRY_HOURS );
		if ( null == expiry ) return -1;
		try { return Integer.parseInt( expiry ); } 
		catch ( NumberFormatException e ) {
			logger.logWarning(GDBMessages.CONFIG_ROW_EXPIRY_GET_ERROR, "Invalid property value: " + MSGSTORER_ROWEXPIRY_HOURS + " must be a number");
			return -1;
		}
	}
	
	public static String getMsgStorerMsgCols() {
		return getUserProperty( MSGSTORER_MSGCOLS );
	}

//	public static String getBrokerTopic() {
//		return getUserProperty( MSGBROKER_TOPIC );
//	}
	
	static void loadDriver( String driver ) throws SQLException {
		
		if ( loadedJdbcDrivers.contains( driver ) ) return;
        try {
//        	if ( driver.equals(GDB_LITE_DRIVER) ) return;
        	if ( GaianNode.isLite() && driver.equals(DERBY_EMBEDDED_DRIVER) )
        		throw new Exception("Unable to load embedded derby driver in LITE mode: " + driver);
        	DriverManager.registerDriver( new DriverWrapper( (Driver) GaianNode.getClassUsingGaianClassLoader(driver).newInstance() ));
//            ClassLoader.getSystemClassLoader().loadClass( driver );
        	loadedJdbcDrivers.add( driver );
        	logger.logInfo("Loaded JDBC driver: " + driver);
        } catch (Exception e) {
        	logger.logInfo("Unable to load JDBC driver: " + driver + ", cause: " + e);
//            throw new SQLException("Unable to load db driver: " + driver + ", cause: " + e);
        }
	}
	
//	public static String[] getIPsOfDiscoveredNodes() {
//		
//		String[] discoveredConnections = getDiscoveredConnections();
//		String[] discoveredIPs = new String[discoveredConnections.length];
//		for ( int i=0; i<discoveredIPs.length; i++ )
//			discoveredIPs[i] = getIPFromConnectionID( discoveredConnections[i] );
//		
//		return discoveredIPs;
//	}
	
//	private static String getIPFromConnectionID( String cid ) {
//		String url = getCrossOverProperty( cid + LABEL_URL );
//		if ( null == url ) return null;
//		return url.substring( urlPrefixLength, url.lastIndexOf(':') ); //url.length() - urlSuffixLength );
//	}
	
	public static String getConnectionURL( String cid ) {
		return getCrossOverProperty( cid + LABEL_URL );
	}
	
	/**
	 * Get the url of the given gaian node. This must be a connection identifier, not a user
	 * defined node with an indirect _CONNECTION reference.
	 * The later is not considered a GAIAN node identifier.
	 * 
	 * @param nodeDefName
	 * @return
	 */
	private static String getGaianNodeIDFromURL( String nodeDefName ) {
		String url = getCrossOverProperty( nodeDefName + LABEL_URL );
		if ( null == url ) return null;
		return url.substring( urlPrefixLength, url.lastIndexOf('/') ); //url.length() - urlSuffixLength );
	}
	private static final int urlPrefixLength = "jdbc:derby://".length();
//	private static final int urlSuffixLength = ";create=true".length();
	
	// Note a Gaian Node URL connection (which is not the local one) must be defined as: 
	// "jdbc:derby://<host or ip>:<port>/<db>[;create=<false or true>]"
	public static String getGaianNodeID( String nodeDefName ) {
				
		String nodeID = getSystemProperty( nodeDefName.substring(nodeDefName.lastIndexOf('_')+1) + LABEL_DNODEID );
		if ( null == nodeID ) // Derive node id from url if it is not defined explicitely
			// By design, the nodeID *should not* start with an IP as it should have a NODEID property when the URL contains an IP.
			nodeID = getGaianNodeIDFromURL( nodeDefName );
		if ( null == nodeID )
			nodeID = "UNIDENTIFIED_NODE";
		
		int standardPortIndex = nodeID.lastIndexOf(":" + GaianNode.DEFAULT_PORT);
		
		if ( -1 != standardPortIndex ) // Remove port def in nodeID if the port is the default 6414
			nodeID = nodeID.substring( 0, standardPortIndex );
		
//		logger.logInfo(nodeDefName + " getGaianNodeID() returning: " + nodeID);
		return nodeID;
	}
	
	// Check the gaian connection nodeID (<host or ip> + :<port>) doesn't already exist for:
	// 	1) An ipaddress: check that it doesn't exist in a URL def
	//	2) A hostname:   check that it doesn't exist in a NODEID or in a URL def
	public static boolean isNodeConnectionDefined( String nodeID ) {
		
		for ( String cid : getGaianConnections() )
			if ( 	nodeID.equals( getCrossOverProperty(cid+LABEL_DNODEID) ) ||
					nodeID.equals( getGaianNodeIDFromURL(cid) ) )
				return true;
		
		return false;
	}
	
	// Matches host as a defined gateway - matches either an ipaddress or a hostname
	public static boolean isGatewayConnectionDefined( String host ) {
		
		for ( String cid : getGaianConnections() ) {
			String chost = getHostFromNodeID( getCrossOverProperty(cid+LABEL_DNODEID) );
			if ( null != chost && chost.startsWith(host) ) return true;
			chost = getHostFromNodeID( getGaianNodeIDFromURL(cid) );
			if ( null != chost && chost.startsWith(host) ) return true;
		}
		
		return false;
	}
	
	private static String getHostFromNodeID( String nodeid ) {
		if ( null == nodeid ) return null;
		int idx = nodeid.lastIndexOf(':');
		if ( -1 < idx ) return nodeid.substring(0,idx);
		return nodeid;
	}
	
	public static String getLocalDefaultConnectionID() {
		
		return GaianNode.isLite() ? 
		GDB_LITE_DRIVER + "'" + LiteDriver.LITE_DRIVER_URL_PREFIX + "'" + getGaianNodeUser() + "'" + getGaianNodePassword() : 
		getLocalDerbyConnectionID();
		
//		return new String[] { DERBY_EMBEDDED_DRIVER , "jdbc:derby:" + getGaianNodeDatabase() + ";create=true",
//								getGaianNodeUser(), getGaianNodePassword() };
	}
	
	public static String getLocalDerbyConnectionID() {
		// Note that when the db name is not given a path, then it is relative to property "derby.system.home".
		// If the property is not set, then it is relative to the working directory of the process  
		return DERBY_EMBEDDED_DRIVER + "'" + "jdbc:derby:" + getGaianNodeDatabaseName() + ";create=true" + 
			"'" + getGaianNodeUser() + "'" + getGaianNodePassword();
	}
	
	public static String getRDBConnectionDetailsAsString( String connectionKey ) throws Exception {
		return getRDBConnectionDetailsAsString( connectionKey, true );
	}
	
	/**
	 * Direct or indirect lookup of the RDB connection details identified by the key.
	 * 
	 * When a direct key is passed, the properties will include: 
	 * directkey_DRIVER=xxx, directkey_URL=yyy, etc.
	 * 
	 * When indirect, they will include: 
	 * indirectkey_CONNECTION=directkey, directkey_DRIVER=xxx, directkey_URL=yyy, etc.
	 * 
	 * When a mixture of both exist, the direct properties take precedence.
	 * 
	 * @param connectionKey
	 * @return
	 */
	static String getRDBConnectionDetailsAsString( String connectionKey, boolean exceptionOnEmptyDef ) throws Exception {

		// The default connection details are those for the local gaian node derby db
		if ( 1 > connectionKey.length() || connectionKey.equalsIgnoreCase(LOCALDERBY_CONNECTION) )
			return getLocalDefaultConnectionID();
		
		// If key contains a single quote, then assume it actually already holds the connectionDetails in format driver'url'usr'pwd
		if ( -1 < connectionKey.indexOf('\'') ) return connectionKey;
		
		// Don't blank nulls here - if they are null, they will be set again via the indirect connection property
    	String driver = getCrossOverProperty( connectionKey + LABEL_DRIVER );
    	String url = getCrossOverProperty( connectionKey + LABEL_URL );
    	String usr = getCrossOverProperty( connectionKey + LABEL_USR );
    	String pwd = getCrossOverProperty( connectionKey + LABEL_PWD );
    	
    	String connectionProperty = getCrossOverProperty( connectionKey + CONNECTION_SUFFIX );
    	
    	boolean isIndirectDef = null != connectionProperty;
    	
    	// Indirect connection definition - However the direct method would take precedence if it returned results.
    	if ( isIndirectDef ) {
    		// e.g. To expose an rdbms table "SALES" of a db connection specified under connection property "CDEF", as a data 
    		// source "DS1" of logical table "LT", the syntax is: 				LT_DS1_CONNECTION=CDEF SALES
    		// Note that the table name can also be specified independantly: 	LT_DS1_TABLE=SALES
    		connectionProperty = connectionProperty.split(" ")[0];

    		// The default connection details are those for the local gaian node derby db
    		if ( connectionProperty.equalsIgnoreCase(LOCALDERBY_CONNECTION) )
    			return getLocalDefaultConnectionID();
    		    		
        	if ( null == driver ) driver = getCrossOverProperty( connectionProperty + LABEL_DRIVER );
        	if ( null == url ) url = getCrossOverProperty( connectionProperty + LABEL_URL );
        	if ( null == usr ) usr = getCrossOverProperty( connectionProperty + LABEL_USR );
        	if ( null == pwd ) pwd = getCrossOverProperty( connectionProperty + LABEL_PWD );    		
    	
    	} else
    		connectionProperty = connectionKey; // We need the root value for this for unscrambling
    	
    	if ( /*null == driver ||*/ null == url || null == usr || null == pwd ) { // driver can be null
    		if ( null == driver && null == url && null == usr && null == pwd )
    			if ( exceptionOnEmptyDef ) throw new Exception("Connection Not Defined in Config: " + connectionKey);
    			else return null;
    		
    		String p = isIndirectDef ? connectionProperty : connectionKey;
    		throw new Exception("Missing Connection Configuration Properties:" +
    			/*( null == driver ? " " + p + LABEL_DRIVER : "" ) + */ ( null == url ? " " + p + LABEL_URL : "" ) +
    			( null == usr ? " " + p + LABEL_USR : "" ) + ( null == pwd ? " " + p + LABEL_PWD : "" )
    		);
    	}
    	
//    	System.out.println("connection property: " + connectionProperty);
    	logger.logDetail( connectionKey + " connection: " + driver + ", " + url + ", " + usr + ", pwd" );
    	
//    	System.out.println( connectionKey + " connectionA: " + driver + ", " + url + ", " + usr + ", " + pwd );
    	pwd = '\'' == pwd.charAt(0) ? unscramble(pwd.substring(1), connectionProperty) : pwd;
//    	System.out.println( connectionKey + " connectionB: " + driver + ", " + url + ", " + usr + ", " + pwd);
    	
    	return (null==driver?"":driver) + "'" + url + "'" + usr + "'" + pwd;
	}
    
    /**
     * Gets the connection token needed from the connection details string given 
     * by getRDBConnectionDetailsAsString().
     * 
     * Note the connection details string passed in must be the same as the one produced by
     * the method getRDBConnectionDetailsAsString(), or it may also have extra characters
     * at the end of it as long as they are separated from the beggining of the string by
     * a single quote ("'") character.
     * 
     * @param connectionDetails
     * @return
     */
    public static String[] getConnectionTokens( String connectionDetails ) {
    	return connectionDetails.split("'", -1); // -1 means blank trailing usr/pwd values are not discarded
    }

    public static DatabaseConnector getNewDBConnector( String[] cargs ) throws SQLException {

    	String driver = cargs[0];
    	String url = cargs[1];
    	String usr = cargs[2];
    	String pwd = cargs[3];

//    	logger.logThreadInfo( "loadDriver() " + Arrays.asList(cargs) );
    	
    	if ( null != driver && 0 < driver.trim().length() ) {

//    		int lt = DriverManager.getLoginTimeout();
//    		DriverManager.setLoginTimeout(1);
//        	logger.logThreadInfo( "loadDriver( " + driver + " ), setLoginTimeout from " + lt + " to " + 1 );
        	loadDriver( driver );
    	}
    	    	
//    	Connection c = DriverManager.getConnection( url, usr, pwd );
//    	c.setAutoCommit( false ); // when we re-enable this, we need to explicitely commit, esp. before closing ResultSets
    	
    	return new DatabaseConnector( url, usr, pwd );
    }
	
	public static Connection getEmbeddedDerbyConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:derby:" + getGaianNodeDatabaseName(), getGaianNodeUser(), getGaianNodePassword());
//		return DriverManager.getConnection("jdbc:derby://localhost:6414/gaiandb", getGaianNodeUser(), getGaianNodePassword());
	}

    public static Set<String> getConnectionDefsReferencedByAllSourceLists() { // getAllReferencedJDBCConnections() {
    	
    	HashSet<String> connectionDefs = new HashSet<String>();
    	
    	Set<String> allSourceLists = getAllUserPropertiesWithSuffixAndRemoveSuffix( SOURCELIST_SUFFIX );
    	
		for ( Iterator<String> i=allSourceLists.iterator(); i.hasNext(); ) { //int i=0; i<allSourceLists.length; i++ ) {
			String[] dataSources = getSourceListSources( i.next() ) ; //allSourceLists[i] );
			for ( int j=0; j<dataSources.length; j++ ) {
				try { connectionDefs.add( getRDBConnectionDetailsAsString(dataSources[j]) ); }
				catch (Exception e) { continue; } // ignore/skip - warning is logged at load time
			}
		}
    	
//    	String[] allNodeConnectionDefs = getAllUserPropertiesWithSUFFIX( CONNECTION_SUFFIX );
//    	for ( int j=0; j<allNodeConnectionDefs.length; j++ )
//    		connectionDefs.add( getRDBConnectionDetailsAsString( allNodeConnectionDefs[j].split(" ")[0] ) );
    	
    	return connectionDefs;
    }
    
    public static Set<String> getAllSourceListsConnectionsIDs() {
    	Set<String> asls = new HashSet<String>();
    	Set<String> sourceListSources = getAllUserPropertiesWithSuffixAndRemoveSuffix( SOURCELIST_SUFFIX );
    	for (Iterator<String> i=sourceListSources.iterator(); i.hasNext(); ) { //int i=0; i<sourceListSources.length; i++) {
    		asls.addAll( Arrays.asList( Util.splitByCommas(i.next()) ) ); //sourceListSources[i] ) );
    	}    	
    	return asls;
    }
    
    private static final String URLUSR = ";user=", URLPWD = ";password=";
    public static String[] getSourceListSources(String sourceList) {
    	
    	// We need to check whether the sourceList is the list itself, or if it is an ID to reference another list.
    	String[] sources = Util.splitByCommas(sourceList);
    	
    	if ( 1 > sources.length ) return new String[] { LOCALDERBY_CONNECTION };
    	else if ( 1 == sources.length ) {
    		String dereferencedList = getUserProperty( sources[0] + SOURCELIST_SUFFIX );
    		if ( null != dereferencedList ) sources = Util.splitByCommas(dereferencedList);
    	}
    	
    	for ( int i=0; i<sources.length; i++ ) {
    		String s = sources[i];
    		int idx1 = s.lastIndexOf(URLUSR), idx2 = s.lastIndexOf(URLPWD);
    		if ( 0>idx1 || idx1 > idx2 ) continue;
    		// transform: "<url>;user=<usr>;password=<pwd>" to Gaian connection details format: "<driver (can be empty)>'<url>'<usr>'<pwd>"
    		sources[i] = "'" + s.substring(0,idx1) + "'" + s.substring(idx1+URLUSR.length(),idx2) + "'" + s.substring(idx2+URLPWD.length());
    	}
    	
    	return sources; // list of cids
    }
    
//	public static int getColumnCount( String ltName ) {		
//		return new StringTokenizer( ltName + LTDEF_SUFFIX || _TRANSIENT_DEF, "," ).countTokens();
//	}
	
    // This method deals with trimming size, scale and precision type modifier definitions, e.g:
    // "CUSTOMER VARCHAR( 12  )" or "COST DECIMAL(	2 , 3 )" => "CUSTOMER VARCHAR(12)" "COST DECIMAL(2,3)"
    // Note: scale can be negative in some cases (e.g. with oracle, type numeric(5,-2) for value 12345 rounds values to multiples of 100, i.e. 12300)
    public static String[] getColumnsDefArray( String colDefs ) {
    	
    	// Use this line below to start working on defect 45015. This would replace the code under it but has repercussions elsewhere...
//    	return Util.splitByTrimmedDelimiterNonNestedInBracketsOrDoubleQuotes(colDefs, ',');
    	
    	String colDefsWithMassagedTypeArgs = colDefs.
			replaceAll( "\\s*\\(\\s*([0-9]+)\\s*", "($1" ). // Trim around first int arg of any type
			replaceAll( ",\\s*(-?[0-9]+)\\s*\\)\\s*", ":$1)" ); // Trim around subsequent int type modifiers, and replace comma delimiter with a colon
//			replaceAll( "[ |\t]*\\([ |\t]*([0-9]+)[ |\t]*", "($1" ). // Trim around first int arg of any type
//			replaceAll( ",[ |\t]*(-?[0-9]+)[ |\t]*", ":$1" ); // Trim around subsequent int type modifiers, and replace comma delimiter with a colon
    	
    	String[] colsArray = Util.splitByCommas( colDefsWithMassagedTypeArgs );
    	for ( int i=0; i<colsArray.length; i++ )
    		colsArray[i] = colsArray[i].replace( ':', ',' );
    	
    	return colsArray;
    }
	
	public static String getLogicalTableDef( String ltName ) {
		String ltdef = getUserProperty(ltName+LTDEF_SUFFIX);
		
		// Note: this code will need changing in future to avoid folding columns to upper case if they are delimited with double quotes.
		// Note that the 'shortcut apis': setltforfile(), setltforrdbtable(), setltforexcel(), etc. also fold columns to upper case by default when
		// creating the logical table def... if users wanted to avoid this, they would have to use setlt() followed by setdsxxx() instead of the shortcut api.
		return null == ltdef ? null : ltdef.toUpperCase();
	}
		
	public static String getLogicalTableVisibleColumns( String ltName ) {
		
		String variableColumns = getLogicalTableDef( ltName );
		if ( null == variableColumns ) return null;
		String constantCols = getConstantColumnsDef( ltName );
		return null == constantCols || 0 == constantCols.length() ? variableColumns : 
			( 0 == variableColumns.length() ? constantCols : variableColumns + ", " + constantCols );
	}

//	/**
//	 * Split definition into columns array, and include any constant columns that may be
//	 * associated with the given table name. As constant node columns are associated with
//	 * any logical table, they will always be included, even if 'null' is specified as table name.
//	 * 
//	 * @param tableDefinition
//	 * @param ltName
//	 * @return
//	 */
//	private static String[] getColumnsIncludingConstants( String tableDefinition, String ltName ) {		
//		String constCols = getConstantColumnsDefinition(ltName);
//		return Util.splitByCommas( null == constCols || 0 == constCols.length() ?
//				tableDefinition :  tableDefinition + ", " + constCols );
//	}
//	
//	public static String[] getColumnsIncludingConstantsUsingDefinition( String tableDefinition ) {		
//		return getColumnsIncludingConstants( tableDefinition, null );
//	}
	
//	public static String[] getLogicalColumns( String ltName ) {
////		
//////		logInfo("Getting column names for logical table: " + ltName);
////		
////		Vector names = new Vector();
////		String name = null;
////		int colIndex = 0;
////		
////		while ( null != ( name = getUserProperty( ltName + COLUMN_LABEL + colIndex++ ) ) ) 
////			names.add( name );
////		
////		return (String[]) names.toArray( new String[0] );
//		
//		String ltDef = getLogicalTableDefinition(ltName);
////		if ( null == ltDef ) ltDef = getLogicalTableDefinition(ltName+"_TRANSIENT");
//		if ( null == ltDef ) return null;
//		
//		return getColumnDefArray( ltDef );
//	}
	
	
	/**
	 * Returns constant columns for a specific table.
	 * If ltName is null, only global constant columns are returned.
	 */	
	public static String getConstantColumnsDef( String ltName ) {
		
		String constantGlobalCols = getUserProperty( NODE_CONSTANTS );
		String constantLTCols = null==ltName ? null : getUserProperty( ltName + CONSTANTS_SUFFIX );
		if ( null == constantGlobalCols && null == constantLTCols ) return "";
		
		if ( null==constantLTCols || 1 > constantLTCols.length() )
			return constantGlobalCols;
		
		if ( null==constantGlobalCols || 1 > constantGlobalCols.length() )
			return constantLTCols;
		
		return constantLTCols + ", " + constantGlobalCols;
	}
	
	/**
	 * Returns special columns for a specific table.
	 * If ltName is null, no table-specific constant columns will be included - just global ones are.
	 */	
	public static String getSpecialColumnsDef( String ltName ) {
		String hiddenColDefs = getHiddenColumns();
		String constantColsDef = getConstantColumnsDef( ltName );
		return /*getColumnDefArray(*/ null == constantColsDef || 0 == constantColsDef.length() ? 
				hiddenColDefs : constantColsDef + ", " + hiddenColDefs; // );
	}
	
	public static String getHiddenColumns() {
		return PROVENANCE_COLDEFS + ", " + EXPLAIN_COLDEFS;
	}
	
	public static String[] getColumnSplitBySpaces( String colDef ) {
		return colDef.split("[\\s]+");
	}
	
	/**
	 * Simple mirror image scrambling function
	 * @param s
	 * @return
	 */
//	private static String scramble( String s ) {
//		// For this to succeed, all chars of s must be in the ascii range: 33-126, i.e one of the following:
//		// !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~
//		
//		char[] cs = s.toCharArray();
//		char[] scrambled = new char[cs.length];
//		for ( int i=0; i<cs.length; i++ ) {
//			int ci = cs[i];
//			if ( ci<33 || ci>126 ) return s;
//			scrambled[i] = (char) (159-ci);
//		}
//		
//		return new String( scrambled );
//	}
	
	/**
	 * Slightly more complex scrambling/unscrambling function to make same pwds have different scrambled char sequences
	 * based on another input string.
	 * 
	 * @param s
	 * @param k
	 * @param undo
	 * @return
	 */
	private static String scramble( String s, String k ) {
		return scramble( s, k, false );
	}
	public static String unscramble( String s, String k ) {
		return scramble( s, k, true );
	}
	private static String scramble( String s, String k, boolean undo ) {
		// For this to succeed, all chars of s must be in the ascii range: 33-126, i.e one of the following:
		// !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~
		
		if ( null == k || 0 == k.length() ) {
			logger.logWarning(GDBMessages.CONFIG_KEY_UNSCRAMBLE_ERROR, "Cannot (un)scramble pwd off key: " + k);
			return null;
		}
		
		char[] ka = k.toCharArray();
		int klen = ka.length;
		
		int[] shifts = new int[klen];
		for ( int i=0; i<klen; i++ ) shifts[i] = Math.abs(79-ka[i]);
		
//		System.out.println("avg " + avg + (char)avg + ", len " + ka.length + ", shift = " + shift);
		
		char[] cs = s.toCharArray();
		int cslen = cs.length;
		char[] scrambled = new char[cslen];
		for ( int i=0; i<cslen; i++ ) {
			
			// Cycle through chars of k to use as shift values for each char of s to be scrambled.
			int shift = shifts[i%klen];
			int c = cs[i];
			
			// Apply a shift function of the 2nd string so that same char sequences will have different scrambled sequences based on it.
			// Cycle the shift if it causes the scrambled char to fall out of bounds
			if ( !undo ) { c -= shift; /* if ( c<33 ) c+=94; */ if ( c<33 || c<80 && c>79-shift ) c+=47; }
			c = 159-c; // mirror image of half the chars onto the other
			if ( undo ) { c += shift; /* if ( c>126 ) c-=94; */ if ( c>126 || c>79 && c<80+shift ) c-=47; }
			
			scrambled[i] = (char)c;
		}
		
		return new String( scrambled );
	}
	
	// Node credentials here are used to implement specific GaianDB security permitting policy access control to GaianDB data source.
	// This is in contrast to Derby user/password which is lightweight security used simply to connect to the Derby/GaianDB node.
	private static final String CREDENTIALS_PREFIX = "CREDENTIALS_";
	private static final String GDB_USR = GDB_PREFIX + LABEL_USR;
	private static final String GDB_PWD = GDB_PREFIX + LABEL_PWD;
	
	public static void refreshRemoteAccessCredentials( SecurityClientAgent securityClientAgent ) {		
		int idx = 1;
		String node;
		
		Set<String> allNodesHavingValidCredentialsDefs = new HashSet<String>();
						
		while ( null != ( node = getUserProperty( CREDENTIALS_PREFIX + idx + "_" + GDB_NODE ) ) ) {
			String usr = getUserProperty( CREDENTIALS_PREFIX + idx + "_" + GDB_USR );
			String pwd = getUserProperty( CREDENTIALS_PREFIX + idx + "_" + GDB_PWD );
		
			if ( null == usr || null == pwd || 0 == node.length() || 0==usr.length() || 0==pwd.length() ) {
				logger.logWarning(GDBMessages.CONFIG_CREDENTIALS_DEF_ERROR, "Ignoring incorrectly defined field definitions for: " + CREDENTIALS_PREFIX + idx);
				continue;
			}
			
			// Unscramble pwd if necessary - remember the scrambling key is based off the property key minus the _PWD label
			if ( 0 < pwd.length() )
				pwd = '\'' == pwd.charAt(0) ? unscramble(pwd.substring(1), CREDENTIALS_PREFIX + idx + "_" + GDB_PREFIX) : pwd;
			
			securityClientAgent.setRemoteAccessCredentials(node, usr, pwd);
			allNodesHavingValidCredentialsDefs.add(node);
			idx++;
		}
		
		// Avoid synchronization issue by removing only the credentials for nodes that are no longer defined.
		securityClientAgent.retainAllRemoteAccessCredentialsForNodes( allNodesHavingValidCredentialsDefs );
	}
	
	
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static void persistAndApplyConfigUpdates( Map<String, String> updates ) throws Exception {
		persistAndApplyConfigUpdatesForSpecificLTs( updates, null ); // updates could affect all logical tables.
	}
	
	public static void persistAndApplyConfigUpdatesAffectingNoLogicalTables( Map<String, String> updates ) throws Exception {
		persistAndApplyConfigUpdatesForSpecificLTs( updates, Util.EMPTY_SET ); // updates could affect all logical tables.
	}
	
	static void persistAndApplyConfigUpdatesForSpecificLT( Map<String, String> updates, String ltAffected ) throws Exception {
		persistAndApplyConfigUpdatesForSpecificLTs( updates, new HashSet<String>(Arrays.asList( new String[] { ltAffected } )) );
	}
	
	// Incomplete batch-update implemention - see commented out code in GaianDBConfigProcedures as well... GaianDBConfigProcedures.isCommitAPIsOn() ...
//	private static Map<String, String> batchUpdatesToPersist = null;
//	private static Set<String> batchUpdatesAffectedLTs = null;
	
	/**
	 * Persist and apply config file updates to the GaianDB config file.
	 * 
	 * @param updates
	 * @param ltsAffected Set of logical tables affected by the updates (if known). This is to limit/optimise refresh requirements.
	 * @return true if update went well, e.g. without concurrent updates which may have been overwritten.
	 * @throws Exception if updates could not be persisted/loaded
	 */
	static void persistAndApplyConfigUpdatesForSpecificLTs( Map<String, String> updates, Set<String> ltsAffected ) throws Exception {
		
		// Incomplete batch-update implemention - see commented out code in GaianDBConfigProcedures as well... GaianDBConfigProcedures.isCommitAPIsOn() ...
//		if ( false == GaianDBConfigProcedures.isCommitAPIsOn() ) {
//			if ( null == batchUpdatesToPersist ) batchUpdatesToPersist = updates; else batchUpdatesToPersist.putAll( updates );
//			if ( null == batchUpdatesAffectedLTs ) batchUpdatesAffectedLTs = ltsAffected; else batchUpdatesAffectedLTs.addAll( ltsAffected );
//		}
		
		boolean wasConfigFileUntouchedSinceLastAPIUpdate = persistAndApplyConfigUpdates(updates, getConfigFile());
		
		if ( false == wasConfigFileUntouchedSinceLastAPIUpdate ) {
			logger.logWarning(GDBMessages.CONFIG_FILE_CONCURRENT_UPDATE,
					"GaianDB config file was changed and not reloaded before this config update completed - " + 
					"any concurrent changes (which happened DURING our update) will be lost");
			ltsAffected = null; // Override - check ALL logical tables for reload to ensure consistency.
		}
		
		// Only need to reload if this is being called internally.
		
		synchronized( DataSourcesManager.class ) {
//			if ( false == refreshRegistryIfNecessary() )
//				synchronized( upr ) {
//					for ( String key : updates.keySet() ) {
//						String val = updates.get(key);
//						if ( null == val ) upr.remove(key);
//						else upr.put(key, val);
//					}
//				}
			reloadUserProperties();
			logger.logInfo("Reloading config after auto-update...");
			DataSourcesManager.refresh( ltsAffected );

//			// We don't need to reload the file + data sources if the file didn't change...
//			if ( refreshRegistryIfNecessary() ) {
//				logger.logInfo("Reloading config after auto-update...");
//				DataSourcesManager.refresh();
//			}
		}
	}
	
	/**
	 * This method should really just be named "persistConfigUpdates()" - but it is used externally as a utility method by other projects...
	 * Persist and apply config file updates to a config file.
	 * 
	 * @param updates
	 * @return false if the config file was updated concurrently - this means we may have overwritten some updates.
	 * @throws Exception if updates could not be persisted/loaded
	 */
	public static boolean persistAndApplyConfigUpdates( Map<String, String> updatesIn, File configFile ) throws Exception {
		
		Set<String> alreadyUpdated = new HashSet<String>(); // properties already found and which we created a newProperties line for
		boolean isUpdateRequired = false; // whether or not we will need to re-write the file
		
		try {
			FileReader fr = new FileReader( configFile );
			BufferedReader br = new BufferedReader( fr );
			StringBuffer newProperties = new StringBuffer();
			
			if ( null == updatesIn ) updatesIn = new HashMap<String, String>();
			Map<String, String> updatesReduced = new HashMap<String, String>( updatesIn ); // used later to hold encrypted PWDs
			
			// Don't log the updates as they potentially contain unencrypted PWDs
//			logger.logInfo("Processing API Update: " + updates );
			
			int lineNumber = 0;
			
			// Get wilcard null updates out of updates Map. These are prefixes designating collections of properties that need to be removed...
			Set<String> wildcardPrefixesOfPropertiesToBeRemoved = new HashSet<String>();
			for ( Iterator<String> iter = updatesReduced.keySet().iterator(); iter.hasNext(); ) {
				String key = iter.next();
				if ( '*' == key.charAt(key.length()-1) ) {
					iter.remove(); // We know this is a wildcard property (they should not finish with a star character) with no value attached to it
					wildcardPrefixesOfPropertiesToBeRemoved.add( key.substring(0, key.length()-1) );
				}
			}
			
			// Go through all properties creating a newProperties StringBuffer to include all existing properties and updated values for the required ones.
			
			// Note we could use: System.getProperty("line.separator") instead of just appending "\n"... this would also need doing for new properties added after this loop.
			// ..the result is that it would append "\r\n" on windows instead of "\n", thus allowing simple editors like notepad to read the config file properly...
			// However - we'd need to test that if a user copied a "\r\n" delimited file to a unix platform, it wouldn't cause GaianDB to break on that system...
			for ( String line; null != ( line = br.readLine() ); newProperties.append( null==line ? "" : line+"\n" ) ) {
				
				lineNumber++;
				int idx = line.trim().indexOf('=');
				if ( -1 == idx ) continue;
				String key = line.substring(0, idx).trim().intern();
				String oldVal = Util.stripBackSlashesDownOneNestingLevel( line.substring(idx+1).trim() ).intern();
				if ( -1 != key.indexOf('#') ) continue;
				
				if ( alreadyUpdated.contains(key) ) {
					// Comment out duplicate defs of properties that were updated.
					logger.logInfo("Updating duplicate def line " + lineNumber + ", " + key + " (commenting it out)");
					isUpdateRequired = true;	
					line = "#" + line;
					continue;
				}
				
				String val = null;
				
				if ( !updatesReduced.containsKey(key) ) {
					// Check if the property matches one of the wildcards designating properties that need removing - if so delete the line.
					for ( String wildcardPrefix : wildcardPrefixesOfPropertiesToBeRemoved )
						if ( key.startsWith(wildcardPrefix) ) {
							logger.logInfo("Removing line " + lineNumber + ", " + key);
							isUpdateRequired = true;
							line = null; // set it to null, dont bother commenting it out, e.g. "#" + line;
							break;
						}
					if ( null == line ) continue; // this line has been removed, move to the next one...
					
					if ( key.endsWith(LABEL_PWD) && 0 < oldVal.length() && '\'' != oldVal.charAt(0) ) {
						// Of the properties not in the updates, scramble any existing passwords that haven't been scrambled yet
						val = '\'' + Util.escapeBackslashes( scramble(oldVal, key.substring(0, key.length()-LABEL_PWD.length())));
						updatesIn.put(key, val);
						
					} else continue; // nothing we want to do to this property - so continue...
					
				} else {
					// update existing property that has been changed
					
					val = (String) updatesReduced.remove(key); // Note val could be null (if we are removing this property)
					
					// property not previously found and which needs updating
					alreadyUpdated.add(key);
					
					if ( null!=val && val.intern() == oldVal ) continue; // no change
					
					if ( null == val ) { // property needs removing - delete the line
						logger.logInfo("Removing line " + lineNumber + ", " + key);
						isUpdateRequired = true;
						line = null; // set it to null, dont bother commenting it out, e.g. "#" + line;
						continue;
					}
					
					logger.logInfo("Updating line " + lineNumber + ", " + key + " value: " + oldVal + " -> " + val);
					
					if ( key.endsWith(LABEL_PWD) ) {
						val = '\'' + Util.escapeBackslashes( scramble(val, key.substring(0, key.length()-LABEL_PWD.length())));
						updatesIn.put(key, val);
					}
				}
				
				isUpdateRequired = true;
				line = key + "=" + val; // apply update
			}
			
			br.close(); br = null;
			fr.close(); fr = null;
			
			final int numUpdates = updatesReduced.size();

			// Did update have an impact on the file ?
			// If not, no need to report whether some manual update occured... this will be detected by the watchdog.
			if ( 1 > numUpdates && !isUpdateRequired ) return true;
			
			if ( 0 < numUpdates ) {
				// Add new properties remaining in the updates - make sure to scramble the passwords
				
				if ( 1 < numUpdates ) newProperties.append('\n');
				
				for ( String key : updatesReduced.keySet() ) {
					String val = (String) updatesReduced.get(key);
					if ( null == val ) continue; // this update has no effect - the key didnt exist in the first place!
					if ( key.endsWith(LABEL_PWD) ) {
						val = '\'' + Util.escapeBackslashes( scramble(val, key.substring(0, key.length()-LABEL_PWD.length())));
						updatesIn.put(key, val);
					}
					String newProp = key + "=" + val + "\n";
					newProperties.append( newProp );
				}
			}
			
			// Now there should no longer be any unscrambled PWDs in updatesOrig - so we can log the updates
			if ( Logger.logLevel > Logger.LOG_LESS )
				logger.logInfo("Applying Config Updates: " + updatesIn);
			
			if ( Logger.logLevel > Logger.LOG_MORE ) {
				newProperties.append("# Auto Update: " +
					SDF.format( new Date(System.currentTimeMillis()) ) + ": " + updatesIn + "\n" );
			}
			
			// Resolve folder location of the config file
			String configFileName = configFile.getName();
			String configFilePath = configFile.getCanonicalPath();
			configFilePath = configFilePath.substring(0, configFilePath.lastIndexOf(configFileName));
		
			
			File writeAheadFile = new File( configFilePath + "." + configFileName + ".tmp" );
			BufferedWriter bw = new BufferedWriter( new FileWriter(writeAheadFile), THIRTY_TWO_KB );
			bw.write( newProperties.toString().trim() );
			bw.close();
			
			// Config file might have been updated by another thread, between the time we read it and the time we are about to write it.
			// This should be advised against and we should warn that concurrent updates may sometimes be lost.. however if it happens,
			// we need to make sure we reload the whole config after at least, so that the loaded config at least is always consistent with the file.
			long cfgLastModified = getConfigFileLastModified();
			
			boolean wasConfigFileUntouchedSinceLastAPIUpdate = latestLoadedPropertiesFileTimestamp == cfgLastModified;
			
			// lastModified is often rounded to the second.. not precise enough to detect changes sometimes.
			// Substract 1 second from the lastModified TS so that we can detect any subsequent manual updates to the file...
			final long newCfgLastModified = writeAheadFile.lastModified() - 1000;
			writeAheadFile.setLastModified( newCfgLastModified );
			
			Util.moveFileAndLogOutcome( writeAheadFile, configFile );
			
			logger.logInfo("Config file lastModified was: " + cfgLastModified + ", onLastLoad: " + latestLoadedPropertiesFileTimestamp +
					" (untouched since last API call?: " + wasConfigFileUntouchedSinceLastAPIUpdate +
					"), writeAhead lastModified: " + newCfgLastModified + ", new file lastModified: " + configFile.lastModified());
			
			return wasConfigFileUntouchedSinceLastAPIUpdate;
			
		} catch ( Exception e ) {
			logger.logException( GDBMessages.CONFIG_PERSIST_UPDATE_ERROR, "Unable to persist config updates, cause: ", e);
			throw new Exception("See Persistence Warning");
		}
	}
}
