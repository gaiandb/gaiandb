/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.derby.drda.NetworkServerControl;

import com.ibm.db2j.AbstractVTI;
import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.apps.HttpQueryInterface;
import com.ibm.gaiandb.apps.MetricMonitor;
import com.ibm.gaiandb.apps.SecurityClientAgent;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.tools.MQTTMessageStorer;
import com.ibm.gaiandb.tools.SQLDerbyRunner;
import com.ibm.gaiandb.udpdriver.common.SocketHelper;
import com.ibm.gaiandb.udpdriver.server.UDPDriverServer;
import com.ibm.gaiandb.utils.DriverWrapper;

/**
 * @author DavidVyvyan
 */
public class GaianNode {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger("GaianNode", 25);
	
//	static {
//		try {
//		System.out.println("Install path: " + INSTALL_PATH);
//		System.setProperty( "derby.system.home", INSTALL_PATH ); // "C:\\Code\\W5\\GAIANDB\\build\\GAIANDB_2.0" );
//		} catch ( Exception e ) {}
//	}
	
	private static boolean showStartupTimes = false;
	private static final long initt = System.currentTimeMillis();
	private static long[] t0 = { initt, initt };
	private static long[] t1 = { -1, -1 };

	// DO NOT CHANGE THIS CONSTANT (without also changing it in the build script)
	static final String GDB_VERSION = "2.1.8"; // This constant is referenced and updated by build script: gdbProjectBuilder.xml
	static final String GDB_TIMEBOMB = "-1"; // format is "dd/mm/yyyy" when enabled: This constant is referenced and updated by build script: gdbProjectBuilder.xml
	
	// Possible modes:
	//		Standard node		=>		UDP driver excluded from bytecode. Only TCP Derby driver handles transport and parsing
	//		UDP node			=>		No Derby server. UDP server started, however Derby is used in embedded mode for parsing/compilation/storage.
	//		Lite node 			=>		No Derby server. No access to Derby structures, stored procedures, views etc. UDP driver must be included.
	//		Hybrid node			=>		Derby server and UDP server both start. This node can bridge queries between lite and standard nodes.

	// It makes no sense to start a node in lite mode if the GAIANDB jar does not include the UDP driver.
	// However it is possible to use the UDP driver instead of the Derby network server, yet still use Derby for query parsing/compilation/storage
	// Currently, one cannot start the node in UDP mode alone. The 3 other modes are possible.
	
	// These final statics are used to eliminate code at compile time if required. The Zelix ZKM trimmer will remove all unreferenced classes. 
	public static final boolean IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE = false;
	public static final boolean IS_SECURITY_EXCLUDED_FROM_RELEASE = true;
	
	static final String javaVersionS = System.getProperty("java.version");
	
//	private static final String INSTALL_PATH = Util.getInstallPath();
	private static final String BYTECODE_PATH = Util.getBytecodeLocation();
	private static final String BYTECODE_PATH2 = BYTECODE_PATH.replaceFirst("GAIANDB.jar$", "GAIANDB-tools.jar");
	
	// java version is "0" on Android
	static final boolean isJavaVersion6OrMore = -1 == javaVersionS.indexOf('.', 3) ? true : //( "0".equals(javaVersionS) ? true : false ) 
		Float.parseFloat(javaVersionS.substring(0, javaVersionS.indexOf('.', 3))) >= 1.6;
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
	static final String[] JAR_TSTAMPS = {
		sdf.format( new Date(new File(BYTECODE_PATH).lastModified()) ),
		sdf.format( new Date(new File(BYTECODE_PATH2).lastModified()) ) };

	static final long[] JAR_SIZES = { new File(BYTECODE_PATH).length(), new File(BYTECODE_PATH2).length() };
	
	static String GAIANDB_JAR_MD5 = Util.getFileMD5( BYTECODE_PATH ); // OTT ?
	
	private static final String VERSION_INFO =
	"java version \"" + javaVersionS + "\"\n" +
	System.getProperty("java.runtime.name") + " (build " + System.getProperty("java.runtime.version") + ")\n" +
	System.getProperty("java.vm.name") +
	" (build " + System.getProperty("java.vm.version") + ", " + System.getProperty("java.vm.info") + ")\n" +
	"\nGaianDB V" + GDB_VERSION + " - JAR sizes: " + Util.longArrayAsString(JAR_SIZES) + ", timestamps: " + Arrays.asList(JAR_TSTAMPS) +// ", MD5s: " + Arrays.asList(jarMD5s) +
	"\n\nFeatures Summary: Federated access to JDBC RDBMS tables and CSV Flat files using a logical table abstraction layer"+
	";\nA stored procedures management API, e.g. to set and view all logical tables and data sources in the network"+
	";\nSQL Prepared Statement caching; Connection Pooling; Buffered and batched multi-threaded record fetching"+
	";\nExplain queries to show a query's route and row counts on node; Constant Column definitions for contextual annotations of logical tables"+
	";\nDiscovery of other Gaian Nodes when 'MIN_DISCOVERED_CONNECTIONS' is set, with preferential attachment to highly connected nodes"+
	";\nAutonomic maintenance of 2-way connections, or re-discovery initiation upon loss of connections"+
	";\nAutomated dynamic management of SQL VIEWS for logical tables and their derivative definitions containing provenance columns or explain columns"+
	";\nA Policy plugin interface to manipulate the incoming SQL queries or filter returned records"+
	";\nFlexible GaianDB node discovery using config variable 'DISCOVERY_IP' to specify a broadcast mask or multicast group address"+
	";\nNew configuration parameters defining permitted hosts, denied hosts and cluster IDs to restrict connectivity between nodes"+
	";\nConnectivity resilience to variable network latency using property 'GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS'"+
	";\nMQTT listener & Message Storer; Command line processor; Logging levels; In Memory Tables and Indexing"+
	";\nAbility to also access an Omnifind text index using com.ibm.db2j.ICAREST('<search_string>')"+
	";\nExcel Spreadsheet Data Source Type; 'Pass-through' SQL capability for insert/update/delete/call; Discovery Gateways for discovery across networks"+
	";\nAdvanced API calls: listnodes(), listflood(), addgateway()"+//, listltmatches()"+
	";\nAccess control to allow/disallow SQL API configuration and propagated writes"+
	";\nBasic security: User authentication and low-level automatic scrambling of passwords in the config file"+
	";\nDetection and cancellation of hanging queries (not mistaken for long-running queries)"+
	";\nPass-through sub-queries using 'GaianQuery': define exposed data sources on each node which the query will be executed against"+
	";\nPerformance metrics; Provenance columns: GAIAN_NODE, GAIAN_LEAF (queryable using option: 'with_provenance')";
	
	private NetworkServerControl nsc = null;
	
	private static PreparedStatement procDefStatement = null;
	
	public static boolean isProcedureMightReturnResultSet(String procName) {
		if ( null == procDefStatement ) return true;
		ResultSet rs = null;
		try {
			procDefStatement.setString(1, procName);
			rs = procDefStatement.executeQuery();
			if ( rs.next() && -1 == rs.getString(1).indexOf(" RESULT SETS ") ) return false;
			
		} catch (SQLException e) {
			logger.logException( GDBMessages.NODE_UNABLE_TO_RESOLVE_PROCEDURE_ERROR, "Unable to resolve procedure: " + procName, e);
		} finally { try { if ( null != rs ) rs.close(); } catch (Exception e1) {} }

		return true; // only return false if it exists and doesn't return a result set.
	}

	private static boolean isDerbyPortUsedByAnotherProcess = false;
	private static String exitRequest = null;
	
//	private static final String DERBY_EMBEDDED = "DERBY_EMBEDDED";
//	private static final String DERBY_MQTT = "DERBY";
	
	public static final String DEFAULT_HOST = "localhost";
//public static final String DEFAULT_HOST = "was7.sdpdom.local";
	public static final int DEFAULT_PORT = 6414; //NetworkServerControl.DEFAULT_PORTNUMBER;
	private static final String DEFAULT_PROVENANCE_HOST = "0.0.0.0";
	static final String DEFAULT_CONFIG = GaianDBConfig.DEFAULT_CONFIG;
	static final String DEFAULT_LOGLEVEL = Logger.POSSIBLE_LEVELS[ Logger.LOG_DEFAULT ];
	
	//private static final String CREATE_CONFIG_STORED_PROCEDURES_FILE = "createConfigProcedures.sql";
	
	// Removed the feature below because it is equivalent to just run this from launchGaianServer.bat: SQLDerbyRunner -td@ -standalone -createdb initScript.sql
	// Not equivalent actually... this feature would execute the queries *before* the node would accept queries.. so it would be true initialisation...
	private static String gdbInitFileSQL = null; // = "gaiandb_init.sql";
	
	static final String THREADNAME_WATCHDOG = "GaianNode Watchdog";
	static final String THREADNAME_CONNECTIONS_CHECKER = "DB Connections checker";
	static final String THREADNAME_NODE_SEEKER = "GaianNodeSeeker";
	
	// Could add UDP driver and maybe the Message Storer listener thread names here in future...	
	private static final Set<String> gdbThreadNamesOfPermanentThreads =
		new HashSet<String>( Arrays.asList( THREADNAME_WATCHDOG, THREADNAME_CONNECTIONS_CHECKER, THREADNAME_NODE_SEEKER ) );
	
	// Timeout used to repeat requests after a long-ish period of inactivity
	private final static int THROUGHPUT_SAMPLING_PERIOD = 1000;
	private final static int THROUGHPUT_PERIODS_PER_WATCHDOG_PERIOD = 5;
	public static final int WATCHDOG_POLL_TIMEOUT = 
		THROUGHPUT_PERIODS_PER_WATCHDOG_PERIOD * THROUGHPUT_SAMPLING_PERIOD; // 5 seconds
	
	private static String USAGE =
		"\nUSAGE: GaianNode [-n <nodename>] [-p <gaian node port>] [-c <config file name>] [-mt <message broker topic>] [-log [<log level>]] [-console] [-g <gateways>] [-initscript <sql init file>]" + // [-w <working directory>]" + // [-lite]" + //[-h <provenance host>]" +
//		"\nDefault host: " + DEFAULT_PROVENANCE_HOST + ", '0.0.0.0' means this node will let any host connect to it" +
		"\n" +
		"\n-n		Node ID. Default is the local hostname." +
		"\n-p		SQL/JDBC TCP Port number. Default: " + DEFAULT_PORT +
//		"\nDefault workspace: local/working directory. This is where the config file, database, log file and sysinfo file are located." +
		"\n-c		Config file location. Default: " + DEFAULT_CONFIG + ".properties (must have '.properties' extention). This is the name of the config file for Gaian (in workspace dir)." +
		"\n-mt		Message broker topic. Default: None. The GaianDB Message Storer will only be activated if a message broker topic is specified on the cmd line or in the config file." +
//		"\nDefault log file: " + DEFAULT_DB + ".log or " + DEFAULT_DB + "<portnumber>.log, in install directory (must have '.log' extention)" +
		"\n-log		Log level. Default is: " + DEFAULT_LOGLEVEL + ", this can only be one of: " + Arrays.asList( Logger.POSSIBLE_LEVELS ) + ", and can otherwise be dynamically updated using the config file." +
		"\n-console	Log to console. Logging is normally done to file gaiandbXXX.log (in working dir) by default. To log to System.out, use option: '-console'." +
		"\n-g		List of Discovery Gateways. A discovery gateway is a node outside of your subnet that acts as a relay allowing you to join nodes in its network." +
		"\n-initscript	File location of optional custom SQL initialisation script. For example this may setup logical or physical tables; or stored procedures/functions." +
//		"\n-w:	allows you specify the folder that will hold: gaiandb.log,  files and the node's database" +
		"\n" +
		"\nThe node starts in full Derby enabled mode unless flag '-lite' is specified, in which case no derby network server is started and the node uses an inbuilt minimal JDBC driver.";
	
	private String mNodeName = null;
	private int mPort = -1;
	private String mHost = null;
	private String mGateways = null;
	
//	private String mWorkspace = null; // Not supported - change the working directory in a launch script to achieve same result.
	private String mConfig = null;
	private String mLogLevel = null;
	private PrintStream mLogPrintStream = null;
	private String mLogFile = null;

	private static final String USER_DIR = System.getProperty("user.dir");
		
	// GDB_WORKSPACE is the root location for the 'gaiandb' physical db folder, gaiandb.log and derby.properties; and the default location for gaiandb_config.properties
	private static String GDB_WORKSPACE = null;
	private static String LOG_DIR = null; // "/logs"
	
	public static final String getWorkspaceDir() { return GDB_WORKSPACE; }
	
	private static boolean isLiteNode = false;
	public static boolean isLite() { return false == IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && isLiteNode; }

	private static boolean isTestModeOn = false;
	public static boolean isInTestMode() { return isTestModeOn; }
	
	private static ThreadGroup gdbNodeThreadGroup = null, parentThreadGroup = null;
	
	public static final byte RUN_STATUS_ON = 0, RUN_STATUS_OFF = 1, RUN_STATUS_PENDING_ON = 2, RUN_STATUS_PENDING_OFF = 3;

	private static AtomicInteger runStatus = new AtomicInteger( RUN_STATUS_OFF );
	
	public static byte getRunStatus() {
//		if ( RUN_STATUS_PENDING_OFF == runStatus )
//			runStatus = isShutdownComplete() ? RUN_STATUS_OFF : RUN_STATUS_PENDING_OFF;
		return runStatus.byteValue();
	}
	public static boolean isRunning() { return RUN_STATUS_ON == getRunStatus(); } // *ONLY* when RUN_STATUS_ON - otherwise watchdog won't exit
	
	public boolean isStarted() { return isRunning(); } // *ONLY* when RUN_STATUS_ON - otherwise watchdog won't exit
	
	public static boolean isShutdownCompleteOrPending() {
		final byte rs = getRunStatus();
		return RUN_STATUS_OFF == rs || RUN_STATUS_PENDING_OFF == rs;
	}
	
	// Convenience method for code which may be wrapping GaianDB.
	// Should check that all GaianDB threads and all static structures of classes are cleaned up (or classes unloaded entirely)
	public static boolean isShutdownComplete() {
		
		return RUN_STATUS_OFF == getRunStatus();
		
//		if ( null == gdbNodeThreadGroup ) return true;
//		
//		// Not all threads need (or even can) to be stopped - some are standard VM ones which can happily be left running for a later restart.
//		Thread[] gdbThreads = new Thread[ gdbNodeThreadGroup.activeCount() + 10 ];
//		gdbNodeThreadGroup.enumerate( gdbThreads );
////		logger.logInfo("isShutdownComplete(): Threads remaining: " + Arrays.asList( gdbThreads ));
//		for ( Thread t : gdbThreads )
//			if ( null != t && gdbThreadNamesOfPermanentThreads.contains( t.getName() ) ) return false;
//		
//		// TODO: MUST check here that all static structures in classes are cleaned up - for now we don't check...
//		return true;
	}

	public static boolean isNetworkDriverGDB() {
		return isLite() || GaianDBConfig.getNetworkDriver().equals( GaianDBConfig.GDBUDP );
//		return isLite() || ( !IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianDBConfig.getNetworkDriver().equals( GaianDBConfig.GDBUDP ) );
	}

	private String mqttTopic = null;
	private MQTTMessageStorer messageStorer = null;
	private Connection mqttMessageStorerDBConnection = null;
	private boolean isMessageStorerAvailable = false;
	
	private static InetAddress localHostAddress;

	private static String gdbPID = "0"; // Initially unknown to the running process (-1 would mean the node wasn't running at all)
	public static int getPID() { return null != gdbPID && gdbPID.matches("[\\d]+") ? Integer.parseInt(gdbPID) : 0; }
	
	private static SecurityClientAgent securityClientAgent = new SecurityClientAgent();
	public static SecurityClientAgent getSecurityClientAgent() { return securityClientAgent; }
	
	// Tests show that GC compacts and releases at least the equivalent of 10,000 in-mem rows at a time
	private static final int GC_INCREMENTAL_COLLECTION = 10000;
	private static long clearedArrayElements = 0;
	
	private static final ArrayList<VTIWrapper> oldvtis = new ArrayList<VTIWrapper>();
	
	private static Vector<GaianTable> scannedGaianTables = new Vector<GaianTable>();
	private static Vector<GaianTable> scannedGaianTablesPossiblyStale = new Vector<GaianTable>();
	
	public synchronized static void notifyArrayElementsCleared( long numberOfArrayElements ) {
		clearedArrayElements += numberOfArrayElements;
	}
	
	public synchronized static void notifyArrayElementsAdded( long numberOfArrayElements ) {
		clearedArrayElements -= numberOfArrayElements;
		if ( 0 > clearedArrayElements ) clearedArrayElements = 0;
	}
	
	public synchronized static void notifyGaianTableScanned( GaianTable gt ) {
		scannedGaianTables.add(gt);
	}
	
	public synchronized static void notifyGaianTableBeingReScanned( GaianTable gt ) {
		scannedGaianTables.remove(gt);
		scannedGaianTablesPossiblyStale.remove(gt);
	}
	
	protected static void notifyDataSourcesToClose( Collection<VTIWrapper> vtis ) {
		synchronized( oldvtis ) { oldvtis.addAll( vtis ); }
	}
	
//	private static boolean deleteDirectory(File path) {
//		if( path.exists() ) {
//			File[] files = path.listFiles();
//			
//			for(int i=0; i<files.length; i++)
//				files[i].delete();
//		}
//		return( path.delete() );
//	}
	
	public static void main(String[] args) throws Exception {
		
		String exitReason = "No Exception";
		
		try { new GaianNode().start( args ); }
		catch ( Throwable e ) { exitReason = e.toString(); }
		
		System.out.println("\nGaianNode exiting: " + exitReason);

//		// Experimented here with restarting a node after termination... could only be done when the previous instance isStopped()
//		System.out.println("\nisStopped: " + isStopped());
//		Thread[] gdbThreads = new Thread[ gdbNodeThreadGroup.activeCount() + 10 ];
//		gdbNodeThreadGroup.enumerate( gdbThreads );
//		for ( Thread t : gdbThreads ) if ( null != t && t != Thread.currentThread() ) t.interrupt();
//		Thread.sleep(1000);
//		gdbNodeThreadGroup.list();
//		// need to ignore the watchdog thread here in isStopped()... (or have main running in a parent thread)
//		System.out.println("\nisStopped: " + isStopped());
//		
//		try { new GaianNode().start( args ); }
//		catch ( Throwable e ) { exitReason = e.toString(); }
		
		// This is the only place where we can call System.exit() - as we should not be inside wrapping code.
		// The purpose is to pass the exit code to the calling script...
		if ( -1 != exitCode ) System.exit( exitCode );
	}
	
//	private Object initLock = null;
	private static GaianNode gdbNodeSingleton = null;
	
	public void start( final String[] args ) throws Exception {
		
		gdbNodeSingleton = this;
		
		if ( RUN_STATUS_OFF != getRunStatus() ) throw new Exception("Node must be in RUN_STATUS_OFF before it can be started, current status: " + runStatus);
		
		runStatus.set( RUN_STATUS_PENDING_ON );
		
		final long startTime = System.currentTimeMillis();
		localHostAddress = InetAddress.getLocalHost();
//		System.out.println("HOSTNAME: " + localHostAddress.getHostName());
		final long timeDiff = System.currentTimeMillis() - startTime;
		if ( 1000 < timeDiff )
			System.out.println("WARNING: Java invocation of 'InetAddress.getLocalHost()' took a long time to complete: " + timeDiff + "ms."
				+ "\nPlease check your network interfaces and disable or restart any that are unused or trying/failing to connect.\n");
		
		mNodeName = null; mPort = -1; mHost = null; mGateways = null; mConfig = null; mqttTopic = null; mLogLevel = null;
		
		ThreadGroup tg = gdbNodeThreadGroup = Thread.currentThread().getThreadGroup();
		while ( null != tg ) { parentThreadGroup = tg; tg = tg.getParent(); }
		
		try {
			// DERBY_SYSTEM_HOME cannot be set statically in the field definition higher up because that would be executed before the calling code has 
			// a chance to set this property in java before calling GaianNode.start()...
			final String DERBY_SYSTEM_HOME = System.getProperty("derby.system.home");
			
			// NOTE: We DO NOT use the install path (INSTALL_PATH) as a fallback location, because we may not have write permission to that folder...
			GDB_WORKSPACE = LOG_DIR = null != DERBY_SYSTEM_HOME ? DERBY_SYSTEM_HOME : null != USER_DIR ? USER_DIR : ".";
			
//			synchronized ( initLock = new Object() ) {
				
			final String[] tb = Util.splitByTrimmedDelimiter( GDB_TIMEBOMB, '/' ); // contains format: dd/mm/yyyy
			if ( 2 < tb.length ) {
				// Convert to arguments: (Year, Month, Day) - Note the month is 0-based !!!
				java.util.Calendar autoDestructDeadline =
					new java.util.GregorianCalendar( Integer.parseInt(tb[2]), Integer.parseInt(tb[1])-1, Integer.parseInt(tb[0]), 0, 0 );
				
//				System.out.println("Comparing times: now = " + System.currentTimeMillis() + " > tb = " + autoDestructDeadline.getTimeInMillis() + " ?");
				if ( System.currentTimeMillis() > autoDestructDeadline.getTimeInMillis() ) {
					System.out.println("___________________________________________________________________________________________________________________\n");
					System.out.println("This Gaian install expired on " + GDB_TIMEBOMB + ". Please check your license.");
					System.out.println("The latest public version is available at: http://www.ibm.com/developerworks (search for 'gaiandb')\n");
					System.out.println("Please contact members of the ETS team in IBM-Hursley (UK) if you have a business need for an up-to-date version:\n");
					System.out.println("David Vyvyan: drvyvyan@uk.ibm.com\nGraham Bent: GrahamBent@uk.ibm.com\nPatrick Dantressangle: dantress@uk.ibm.com");
					System.out.println("___________________________________________________________________________________________________________________\n");
//					deleteDirectory(new File("lib"));
					System.exit(1);
				}
			}
			
			showStartupTime( 0, "Timebomb check" );
			
			Thread.currentThread().setName("INIT");
			setArgs( null == args ? new String[0] : args );
			
			if ( RUN_STATUS_PENDING_ON != getRunStatus() ) {
				shutdownRequestReason = "Incorrect GaianNode arguments (check gaiandb.log) " + shutdownRequestReason;
				return; // via finally
			}
			
			// Note that establishNodeIdentity() spawns off a separate initialisation thread which
			// connects to the database, this is done in parallel to make the startup faster.
			// This connection is needed by initialiseNode().
			establishNodeIdentity();
			
			showStartupTime( 0, "establishNodeIdentity" );
			
//				if ( false == establishNodeIdentity() ) return;
			initialiseNode();

			if ( false == runStatus.compareAndSet( RUN_STATUS_PENDING_ON, RUN_STATUS_ON ) ) {
				shutdownRequestReason = "Early shutdown request (RUN_STATUS " + getRunStatus() +  "): " + shutdownRequestReason;
				return; // via finally
			}
//			}
			
			runWatchdog();
			
		} catch (Throwable e) {
			
			String msgPrefix = "GaianNode terminating due to Exception, cause: ";
			logger.logWarning(GDBMessages.NODE_START_EXCEPTION_ERROR, msgPrefix + Util.getStackTraceDigest(e)); // Util.getExceptionAsString(e));
			
			if ( null != mLogFile ) { System.out.println("\n" + msgPrefix + e); }
			if ( null == shutdownRequestReason ) shutdownRequestReason = msgPrefix + e;
			
		} finally {
			
			runStatus.set( RUN_STATUS_PENDING_OFF );

			logger.logAlways( "Gaian Node Exiting: " + shutdownRequestReason );
			
			try {
				if ( null != messageStorer ) messageStorer.terminate();
				if ( null != mqttMessageStorerDBConnection ) mqttMessageStorerDBConnection.close();
			} catch (Throwable e) {
				logger.logException(GDBMessages.NODE_UNSUBSCRIBE_ERROR, "Gaian Node Exception whilst unsubscribing to topic and disconnecting from MQTT and DB: ", e);
			}
			
			try {
				logger.logAlways("Number of remaining GaianResult objects to close: " + GaianTable.getGresults().size());
				Set<GaianResult> gResults = GaianTable.getGresults();
				synchronized( gResults ) {
					for ( Iterator<GaianResult> gaianResultsIterator = gResults.iterator(); gaianResultsIterator.hasNext(); )
						try { gaianResultsIterator.next().close(); }
						catch (Throwable e) { logger.logAlways("Unable to close a GaianResult, cause: " + Util.getStackTraceDigest(e)); }
				}
			} catch (Throwable e) {
				logger.logAlways("Unable to get set of GaianResult objects to close, cause: " + Util.getStackTraceDigest(e));
			}
				
			try { GaianResult.purgeThreadPools(); }
			catch (Throwable e) { logger.logAlways("Unable to purge GaianResult Thread Pools, cause: " + Util.getStackTraceDigest(e)); }
			
			if ( false == isDerbyPortUsedByAnotherProcess ) {
				try {
					if ( null != nsc ) {
						nsc.shutdown(); boolean isFirstCheck = true;
						while ( true ) {
							try { nsc.ping(); if ( isFirstCheck ) logger.logAlways("Waiting for DerbyNetworkServer to stop responding..."); Thread.sleep( 100 ); isFirstCheck = false; }
							catch (Exception e) { break; } // Server is gone... could also check that the actual tcp socket/port was released?
						}
					}
				}
				catch (Throwable e) { logger.logAlways("Unable to shutdown Derby Network Server, cause: " + Util.getStackTraceDigest(e)); }	
			}
			
			// Don't try and do anything about interrupting or killing any remaining gaiandb threads (from gdbThreadGroup) -
			// This could impact threads of code wrapping us.
			logger.logAlways("Node thread count before exit: " + gdbNodeThreadGroup.activeCount());
//			gdbNodeThreadGroup.list();
			
//			System.out.println("Run status = " + getRunStatus() + ", isShutdownComplete() ? " + isShutdownComplete());
			
			runStatus.set( RUN_STATUS_OFF ); // Assume shutdown is complete... later we should revisit this to check more thoroughly thread + object cleanup
			
			if ( 0 != exitCode )
				// Need to pass diags up to potentially wrapping code
				throw new Exception( shutdownRequestReason );
		}
	}
/*	
	public void waitForStartedStatus() {
		while ( null == initLock ) { try { System.out.println("Sleeping 50ms"); Thread.sleep(50); } catch (InterruptedException e) {} };
		synchronized( initLock ) {}
	}
*/
	private static boolean isInitDataUpToDate = false;
	private static long gdbDbTimestampAtStartup = 0L;
	private static Thread parallelInitThread = null;
	
	/**
	 * This method performs a variety of initialisation actions including
	 * establishing the configuration file, loading the default database driver and
	 * registering the connection maintenance procedure (which is done in a separate thread 
	 * to avoid holding up other initialisation activities).
	 * 
	 * @throws Exception 
	 */
	private void establishNodeIdentity() throws Throwable {

		// Note each GaianNode starts in its own JVM - so each has its own "instance" of the static GaianDBConfig class
		// Establish basic config properties before logging can start

		GaianDBConfig.setDerbyServerListenerPort( mPort );
		GaianDBConfig.setGaianNodeName( mNodeName );
		
//		GaianDBConfig.setWorkspace( mWorkspace );
		// Set the config file name
		GaianDBConfig.setConfigFile( mConfig );
		
		showStartupTime( 0, "Set derbyPort, nodeName and configFile" );
		
		// Cater for the fact that the java.lang.management classes are not always available (e.g. when cross-compiling to Dalvik)
		String processName = null;
		try { showStartupTime( 0, "PID resol0" ); processName = ManagementFactory.getRuntimeMXBean().getName(); 
		showStartupTime( 0, "PID resol" ); }
		catch ( Throwable e ) { logger.logWarning(GDBMessages.NODE_PID_RESOLVE_ERROR, "Unable to resolve process name or pid (ignored): " + e); }
		
		showStartupTime( 0, "PID resolution" );
		
		gdbPID = null == processName || -1 == processName.indexOf('@') ? "Unresolved" : processName.substring(0, processName.indexOf('@'));

		String pidText = "PROCESS ID:\t\t" + gdbPID;
		String nodeIDText = "NODE ID:\t\t" + GaianDBConfig.getGaianNodeID();
		String wdText = "WORKING DIRECTORY:\t" + new File(USER_DIR).getCanonicalPath();
		String bytecodePathMsg = "BYTECODE PATH:\t\t" + new File(BYTECODE_PATH).getCanonicalPath();
		String workspaceText = "WORKSPACE:\t\t" + new File(GDB_WORKSPACE).getCanonicalPath();
		
		if ( null == mLogPrintStream ) {

			System.out.println( pidText );
			System.out.println( nodeIDText );
			System.out.println( wdText );
//			System.out.println( bytecodePathMsg );
			System.out.println( workspaceText );
//			System.out.println( "DERBY SYSTEMHOME:\t" + System.getProperty("derby.system.home") );
			
			String dbName = GaianDBConfig.getGaianNodeDatabaseName();
			
			new File(LOG_DIR).mkdir();
			for ( String extn : new String[] { "", "#0",  "#1" } ) {
				String fName = dbName + extn + ".log";
				String fNameOld = dbName + "-previous" + extn + ".log";
				new File( LOG_DIR, fNameOld ).delete();
				new File( LOG_DIR, fName ).renameTo( new File( LOG_DIR, fNameOld ) );
			}
			
			// The print stream is not already set to System.out, so we log to file
			// Note log file must be different for every gaiandb instance running off the same working directory (i.e. one per db and port)
			mLogFile = dbName +".log";
			mLogPrintStream = new PrintStream( new FileOutputStream( new File(LOG_DIR, mLogFile) ));
			
			Logger.setPrintStream( mLogPrintStream );
		}
		
		if ( !isLite() ) {
		
			GaianDBConfig.loadDriver( GaianDBConfig.DERBY_CLIENT_DRIVER );	
			
			final File gdbJarFile = new File(BYTECODE_PATH);
			final File gdbDbFolder = new File(GDB_WORKSPACE, GaianDBConfig.getGaianNodeDatabaseName());
			
			gdbDbTimestampAtStartup = gdbDbFolder.lastModified();
			
			final String gdbJarMD5_previous = GaianDBConfig.getUserProperty("GAIANDB_JAR_MD5");

			// The UDFs required by GaianDB and all API & utility spfs are "up-to-date" if the GAIANDB.jar has not changed AND if the gaiandb derby db exists and is more recent than the Jar.
			// Otherwise, we will be reloading these procedures and functions.
			isInitDataUpToDate = GAIANDB_JAR_MD5.equals(gdbJarMD5_previous) && gdbDbTimestampAtStartup > gdbJarFile.lastModified();
			
			logger.logAlways("isInitDataUpToDate = " + isInitDataUpToDate
					+ " = GAIANDB_JAR_MD5 " + GAIANDB_JAR_MD5 + " = gdbJarMD5_previous " + gdbJarMD5_previous
					+ " && gdbDbTimestampAtStartup "+gdbDbTimestampAtStartup + " > gdbJarFile.lastModified() " + gdbJarFile.lastModified());
			
			// Load API and Views on the loaded logical tables using an embedded connection to the gaiandb database so that
			// no other process can also connect to it while we are doing this.
			
			parallelInitThread = new Thread( new Runnable() {
				public void run() {
					try {
//						System.out.println("Checking socket port"); long st = System.currentTimeMillis();
//						new ServerSocket(mPort).close();
						ServerSocket tcpSocket = new ServerSocket();
						tcpSocket.setReuseAddress(true); // critical step to avoid "ADDRESS ALREADY IN USE" messages when socket is in TIME_WAIT state.
						tcpSocket.bind( new InetSocketAddress(mPort) );
						tcpSocket.close();
//						System.out.println("Checked socket port in " + (System.currentTimeMillis() - st) + "ms");
					} catch ( IOException e ) {
						isDerbyPortUsedByAnotherProcess = true;
						exitRequest = "Unable to start GaianNode on port " + mPort + " (GaianDB may already be running), cause: "+e;
					}

					if ( null == exitRequest /*&& false == isInitDataUpToDate*/ )
					try {
						initialiseGaianNodeData0();
					} catch ( Throwable e ) {
						exitRequest = "Unable to initialise GaianNode data (aborting): Another GaianNode or Derby instance may be attached to db folder 'gaiandb'? " +
						"Otherwise, authentication may have failed or the database may be corrupted (delete it & restart to recycle it). Also check optional messages below."; // - database may be booted by another instance?
						String digest = exitRequest + "\n" + Util.getStackTraceDigest(e); // getAllExceptionCauses(e);
	//					if ( null != mLogFile ) { System.out.println(digest); }
	//					logger.logWarning( exitRequest + e );
						logger.logWarning(GDBMessages.NODE_ID_EXCEPTION_ERROR, "GaianNode terminating due to Exception: " + digest); // Util.getExceptionAsString(e));
						if ( null != mLogFile ) { System.out.println("\n" + digest); }
//						System.exit(1);
	//					throw new Exception( digest );
					}

					showStartupTime( 1, "init data 0 + check on whether a GaianNode is already started on this port" );
				}
			},"InitialisationThread");
			parallelInitThread.start();
		}
		
		logger.logRaw( VERSION_INFO );
		logger.logRaw( "\n" + pidText );
		logger.logRaw( "\n" + nodeIDText );
		logger.logRaw( "\n" + wdText );
		logger.logRaw( bytecodePathMsg );
		logger.logRaw( workspaceText );
		
		// Quick logging to show file structure when deployed in BlueMix
//		for ( String xpr : new String[] {  "*", "apps/*", "apps/myapp/*", "apps/myapp/*/*" } )
//			for ( String f : Util.findFilesTreeMatchingMask(xpr, false) )
//				logger.logRaw("ls " + xpr + ": " + f);
	}
	
//	 public static int getProcessId() {
//		 try {
//			 // Only works with Sun Java
//			 java.lang.reflect.Field jvmField = Class.forName("sun.management.RuntimeImpl").getDeclaredField("jvm");
//			 java.lang.reflect.Method getProcessIdMethod = Class.forName("sun.management.VMManagementImpl").getDeclaredMethod("getProcessId");
//			 jvmField.setAccessible(true);
//			 getProcessIdMethod.setAccessible(true);
//			 return (Integer) getProcessIdMethod.invoke( jvmField.get( ManagementFactory.getRuntimeMXBean() ) );
//		 } catch (Exception e) {
//			 initLogger.logWarning("Unable to resolve PID: " + e);
//			 return -1;
//		 }
//	 }
	
	private void initialiseNode() throws Throwable {
						
//		createLogicalTableViews();
//		String configFileName = GaianDBConfig.getConfigFile().getCanonicalPath();
		
		String dbmsg = "PHYSICAL DATABASE:\t" + new File(GDB_WORKSPACE, GaianDBConfig.getGaianNodeDatabaseName()).getCanonicalPath() + "\n";
		String logmsg = "LOG FILE:\t\t" + ( null == mLogFile ? "None (console)" : new File(LOG_DIR, mLogFile).getCanonicalPath()) + "\n";
		String cfgmsg = "CONFIG FILE:\t\t" + GaianDBConfig.getConfigFilePath() + "\n";
		
		if ( !isLite() )
			logger.logRaw("\n" + dbmsg + logmsg + cfgmsg + "\nDERBY SERVER PORT:\t" + mPort + "\nPROVENANCE HOST(S):\t" + mHost + "\n");
		
		if ( !IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianNode.isNetworkDriverGDB() )
			logger.logRaw( (!isLite() ? "" : "\n") + "UDP SERVER PORT:\t" + mPort + "\n" );

		// Don't repeat the above if logging goes to System.out anyway
		if ( null != mLogFile ) {
			System.out.print(dbmsg);
			System.out.print(logmsg);
			System.out.print(cfgmsg);
			System.out.println(
				"VERSION INFO:\t\tV" + GDB_VERSION + (null!=GDB_TIMEBOMB && 9<GDB_TIMEBOMB.length() ? " (expires "+GDB_TIMEBOMB+")" : "")
				+ " - JAR sizes: " + Util.longArrayAsString(JAR_SIZES) + ", timestamps: " + Arrays.asList(JAR_TSTAMPS) );
		}
		
		String hostname = localHostAddress.getHostName();
		
		Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
		logger.logRaw("Network Interfaces: ");
		while ( en.hasMoreElements() ) {
			NetworkInterface ni = en.nextElement();
			
			Enumeration<InetAddress> ias = ni.getInetAddresses();
			StringBuffer sb = new StringBuffer();
			if ( ias.hasMoreElements() ) sb.append(ias.nextElement());
			while( ias.hasMoreElements() ) sb.append(", " + ias.nextElement());

//			System.out.println("Next ni: " + ni);
			
			// Java 6 only:...
			if (isJavaVersion6OrMore) {
				for ( InterfaceAddress ifa : ni.getInterfaceAddresses() ) {
					String ifaString = null == ifa ? null : "Address: " + ifa.getAddress() + " -> Broadcast: " + ifa.getBroadcast();
					if ( 0 == sb.length() ) sb.append(ifaString);
					else sb.append(", " + ifaString);
				}
			}
			
			logger.logRaw( ni.getName() + ": " +
//					Util.byteArray2HexString(ni.getHardwareAddress(),true) + ": " +
					ni.getDisplayName() + " " + sb );
		}
		
		logger.logRaw("\nHostname: " + hostname + "\nJava localhost ip: " + localHostAddress.getHostAddress() +
				"\nLocal address designating initial default interface for multicast: " + GaianNodeSeeker.getDefaultLocalIP() );

		logger.logRaw("\n");
		
		showStartupTime( 0, "Net interfaces" );
		
//		int min = 33, max = 127;
//		StringBuffer sb = new StringBuffer();
//		for ( int i=min; i<max; i++ ) sb.append((char) i);
//		logger.logRaw("Character sets should match for ascii range 33-126:");
//		logger.logRaw("local:     " + sb);
//		logger.logRaw("reference: !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~");
		
		// Initialise everything
		logger.logAlways("Initialising GaianNode...");
		if ( isInitDataUpToDate )
			logger.logAlways("GAIANDB.jar library has not been changed (and db folder is more recent), so initialisation data (e.g. stored procs/funcs) is expected to be already loaded");
		else
			logger.logAlways("GAIANDB.jar library has been changed (or db folder didn't exist or was older), so initialisation data (stored procs/funcs) is being re-loaded");
		
		logger.logAlways("Node details: " + GaianDBConfig.getGaianNodeID() + " " +
				GaianDBConfig.getGaianNodeUser() + " <password>" ); // + GaianDBConfig.getGaianNodePasswordScrambled() );
		String dip = GaianDBConfig.getDiscoveryIP(), dg = GaianDBConfig.getDiscoveryGateways(), mi = GaianDBConfig.getMulticastInterfaces(),
		ac = GaianDBConfig.getAccessClusters(), ahp = GaianDBConfig.getAccessHostsPermitted(), ahd = GaianDBConfig.getAccessHostsDenied();
		logger.logAlways("Autonomic Discovery: Maintained connections sought through discovery: " +
				GaianDBConfig.MIN_DISCOVERED_CONNECTIONS + " = " + GaianDBConfig.getMinConnectionsToDiscover() );
		
		logger.logAlways("Autonomic Discovery: " + GaianDBConfig.DISCOVERY_IP + " = " +
				(null==dip?"<Default="+GaianNodeSeeker.DEFAULT_MULTICAST_GROUP_IP+">":dip) );
				
		logger.logAlways("Autonomic Discovery: " + GaianDBConfig.DISCOVERY_GATEWAYS + " = " +
				(null==dg?"<Default=No gateways to other networks>":dg) );
		
		logger.logAlways("Autonomic Discovery: " + GaianDBConfig.MULTICAST_INTERFACES + " = " +
				(null==mi?"<DefaultInterface="+InetAddress.getByName(GaianNodeSeeker.getDefaultLocalIP())+">":mi) );
		
		logger.logAlways("Connectivity: " + GaianDBConfig.ACCESS_CLUSTERS + " = " +
				(null==ac?"<Default=Connect to nodes which are not cluster members>":ac) );
		logger.logAlways("Connectivity: " + GaianDBConfig.ACCESS_HOSTS_PERMITTED + " = " +
				(null==ahp?"<Default=Accept further connections from any host outside the defined clusters>":ahp) );
		logger.logAlways("Connectivity: " + GaianDBConfig.ACCESS_HOSTS_DENIED + " = " +
				(null==ahd?"<Default=Do not deny connections from any host in or outside the defined clusters>":ahd) );
		
		// Assign the log level early on
		GaianDBConfig.assignLogLevel( mLogLevel );

		logger.logAlways( "Log level set to: " + Logger.POSSIBLE_LEVELS[Logger.logLevel] );
		
		showStartupTime( 0, "Config reporting" );
		
		// Load logical tables and their data sources
		// Also run through the persist code to scramble any PWDs that are in the clear before initialising/loading data structures	- also set GAIANDB_JAR_MD5 property
		Map<String, String> initUpdates = new LinkedHashMap<String, String>() {
			private static final long serialVersionUID = -6272844140878031300L;
			{ put ( "GAIANDB_JAR_MD5", GAIANDB_JAR_MD5 ); }
		};
		if ( null != mGateways ) initUpdates.put(GaianDBConfig.DISCOVERY_GATEWAYS, mGateways );
//		if ( null != mExecTimeout ) initUpdates.put(GaianDBConfig.EXEC_TIMEOUT_MS, mExecTimeout );
		
		showStartupTime( 0, "JAR_MD5 and DISCOVERY_GATEWAYS init" );
		
		GaianDBConfig.persistAndApplyConfigUpdates( initUpdates );
		
		showStartupTime( 0, "Config + LT + DS init" );
		
		// Initialise access credentials - potentially needed when first queries come through
		GaianDBConfig.refreshRemoteAccessCredentials(securityClientAgent);

		Statement tmpStmt = null;
		
		if ( !isLite() ) {
		
			// Start a Derby network server  which we will use to connect to the initialised gaiandb database
			
	//		NetworkServerControl.main( new String[] { "start", "-h", mHost, "-p", new Integer(mPort).toString() } );
			InetAddress addr = InetAddress.getByName(mHost);
			logger.logAlways("Starting Derby Network Server, host mask " + addr.getHostAddress() + ", port " + mPort );
			
			nsc = new NetworkServerControl( addr, mPort,
					GaianDBConfig.getGaianNodeUser(), GaianDBConfig.getGaianNodePassword() );
			
			logger.logRaw(""); // Put a new line in before Derby prints out its Network Server started status
	
			showStartupTime( 0, "Derby NS constructor" );
			
			// If sslMode is set, ensure we have properties for the keystore file name and password (use defaults if necessary).
						
			final String sslMode = GaianDBConfig.getSSLMode();
			if ( "basic".equals(sslMode) || "peerAuthentication".equals(sslMode) ) {
				
				String keyStoreFileName = System.getProperty("javax.net.ssl.keyStore");
				String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

				if ( null == keyStoreFileName ) System.setProperty("javax.net.ssl.keyStore",
						keyStoreFileName = GDB_WORKSPACE + "/.keyStore");
				if ( null == keyStorePassword )	System.setProperty("javax.net.ssl.keyStorePassword", 
						keyStorePassword = GaianDBConfig.getGaianNodePassword());
				
//				// Auto-generation of Key-pair into a keystore for this server node - to avoid requirement to create one manually...
//				// See comments in derby.properties for more detail on manual configuration.
//				// Parked because: 
//				//		1) Use of Java security methods requires a new Export Regs classification, and 
//				//		2) below implementation doesn't work (keystore needs more to it I think..) and maybe keystore/pwd could also be managed by the JVM.
//
//				// Create keyStore file if it doesn't exist
//				if ( false == new File( GDB_WORKSPACE + '/' + keyStoreFileName ).exists() ) {
//					
//					KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
//					final char[] passwordChars = keyStorePassword.toCharArray();
//					ks.load(null, passwordChars);
//					FileOutputStream fos = new FileOutputStream( GDB_WORKSPACE + '/' + keyStoreFileName );
//					ks.store(fos, passwordChars);
//					fos.close();
//				}
			}
			
			// The parallelInitThread was run by establishNodeIdentity and connected to the database to perform
			// Initialisation actions. It must be finished at this point so that subsequent database calls in 
			// initialiseGaianNodeData below succeed.
			parallelInitThread.join(); // must be done before initialiseGaianNodeData() is called

			showStartupTime( 0, "init thread joining" );
			
			if ( null != exitRequest ) throw new Exception(exitRequest);
			
			// Load API and Views on the loaded logical tables using an embedded connection to the gaiandb database so that
			// no other process can also connect to it while we are doing this.
			try {
				initialiseGaianNodeData();
			} catch ( Throwable e ) {
				String msg = "Unable to initialise GaianNode data - aborting - (Derby db folder 'gaiandb' may be corrupted - delete it & restart to recycle it)"; // - database may be booted by another instance?
//				String digest = msg + "\n" + Util.getAllExceptionCauses(e);
	//			if ( null != mLogFile ) { System.out.println(digest); }
	//			logger.logWarning( msg + e );
//				throw new Exception( digest );
				throw new Exception( msg + "\nStack:" + Util.getStackTraceDigest(e));
			}
			
			nsc.start( new PrintWriter( mLogPrintStream ) );
			
//			System.out.println("nsc descr: " + nsc.toString() );
//			System.out.println("nsc sysinfo: " + nsc.getSysinfo() + ", runtime info: " + nsc.getRuntimeInfo());
//			System.out.println("nsc properties: " + Arrays.asList( nsc.getCurrentProperties() ));
			
			while ( true ) {
//				logger.logRaw("Network Server Ping...");
				try { nsc.ping(); break; }
				catch (Exception e) {
					if ( -1 < e.getMessage().indexOf("Keystore") ) throw e; // Abort startup if we have incorrect SSL keystore or pwd
					logger.logInfo("Re-trying Derby NetworkServer ping() after Exception: " + e);
				}
				Thread.sleep( 100 );
			}
			
			showStartupTime( 0, "Derby NS start/ping" );
			
//			logger.logDetail( "NSC status: " + nsc.getSysinfo() );
			
			// Derby Network Server Started
			if ( showStartupTimes )
				System.out.println("\nGaianDB startup time incl Derby: " + (System.currentTimeMillis() - initt));
			
			String derbySysInfo = nsc.getSysinfo();
			int idx = derbySysInfo.indexOf("Version:");
			int idx2 = derbySysInfo.indexOf("Build", idx);
			if ( -1 == idx2 ) idx2 = derbySysInfo.indexOf('\n', idx);
			
			String derbyVersionInfo = derbySysInfo.substring(idx, idx2);
			
			String startedMsg = "\n"
				+ "Derby " + derbyVersionInfo + "- Java Version: " + javaVersionS + " (" + System.getProperty("java.vm.name") + ")\n\n"
				+ "GaianNode started for Derby network server with sslMode="+nsc.getCurrentProperties().getProperty("derby.drda.sslMode")
				+ " on port: " + mPort + " at " + new Date(System.currentTimeMillis())
				+ ", startup time: " + (System.currentTimeMillis() - initt) + "ms\n";
			
			if ( null != mLogFile ) System.out.println( startedMsg );
			logger.logAlways( /*"GaianNode started\n" +*/ startedMsg );
			
			logger.logInfo("Derby Network Server maxThreads: " + nsc.getMaxThreads()
					+ ", timeSliceBeforeYieldPerThread: " + nsc.getTimeSlice()
			);
			
			// Get connection for periodic refresh of LT views - also validates that we can get a connection to the Derby NS
			Connection initConn = GaianDBConfig.getEmbeddedDerbyConnection();
//				DriverManager.getConnection(
//					"jdbc:derby://localhost:" + mPort + "/" + GaianDBConfig.getGaianNodeDatabaseName() + ";ssl=" + GaianDBConfig.getSSLMode(),
//					GaianDBConfig.getGaianNodeUser(), GaianDBConfig.getGaianNodePassword() );
			
			// Prepare a statement that will give us the ability to derive return capabilities of stored procedures.
			// This is needed because:
			//		Procedures that don't return a result can be targeted at remote nodes (using GaianQuery()) without having to execute them locally.
			// 		This is because we can just use a basic static result structure (int update_count) to return when derby calls GaianTable.getMetaData().
			// DatabaseMetaData is not completed by Derby. therefore unreliable.
			// To get Derby procedure return capabilities, we have to look at their registration alias info string...
			procDefStatement = initConn.prepareStatement("select aliasinfo from sys.sysaliases where alias = ?");
			
			try {
				tmpStmt = initConn.createStatement();
			} catch ( SQLException e ) {
				String digest = "Cannot obtain connection to GaianDB database: "
					+ GaianDBConfig.getGaianNodeDatabaseName() + ", causes: " + Util.getAllExceptionCauses(e);
				logger.logInfo( digest );
				throw new Exception( digest );
			}

			showStartupTime( 0, "First Derby Connection" );

			// Refresh LT views immediately after startup -
			// Note that we cannot avoid running this code if the timestamp of the gaiandb_config.properties file is older than timestamp of the database because
			// the config file may have been copied from another location, keeping its old timestamp.
			DataSourcesManager.resetUpToDateViews( tmpStmt );
			
			showStartupTime( 0, "Recomputed up-to-date views" );
			
			try { DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs(); }
			catch ( Exception e ) { logger.logWarning(GDBMessages.NODE_LT_VIEW_UPDATE_ERROR, "Failed to update some logical table views (ignored): " + e); }

			showStartupTime( 0, "Updated logical table views on all dbs" );
			
			if ( null != gdbInitFileSQL &&  new File(gdbInitFileSQL).exists() )
				sdr.processSQLs( gdbInitFileSQL );

			showStartupTime( 0, "Init SQL script " + gdbInitFileSQL);
			
			// Do some early cleanup
			AbstractVTI.dropCacheTables( tmpStmt ); //sdr.createStatementOffInternalConnection() );
//			DataSourcesManager.registerDatabaseStatementForLogicalTableViewsLoading( GaianDBConfig.getGaianNodeDatabaseName(), tmpStmt ); too late... test code is already setting a new LT and trying to use this statement to create its views...
			
			showStartupTime( 0, "Cache table dropping" );
		}
		
		// Denis: Start UDP JDBC server on mPort
		if ( !IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianNode.isNetworkDriverGDB() )
		{
			logger.logInfo( "Starting UDP Driver server: datagramSize : "+
					GaianDBConfig.getNetworkDriverGDBUDPDatagramSize()+" bytes - timeout : "+
					GaianDBConfig.getNetworkDriverGDBUDPTimeout()+ " ms" );
			
			SocketHelper.setBufferSize( GaianDBConfig.getNetworkDriverGDBUDPSocketBufferSize() );
			UDPDriverServer udpJDBCServer = new UDPDriverServer( mHost, mPort, GaianDBConfig.getGaianNodeDatabaseName() );
			udpJDBCServer.setDatagramSize( GaianDBConfig.getNetworkDriverGDBUDPDatagramSize() );
			udpJDBCServer.start();

			if ( showStartupTimes )
				System.out.println("\nGaianDB startup time incl UDP server: " + (System.currentTimeMillis() - initt));
			
			String startedMsg = (!isLite() ? "" : "\n") + "GaianNode startup complete for UDP network server on port: " +
				mPort + " at " + new Date(System.currentTimeMillis()) + "\n";
			if ( null != mLogFile ) System.out.println( startedMsg );
			logger.logRaw( /*"GaianNode started\n" +*/ startedMsg );
		}

		//TODO:PDA add Web server  start here: 

		// Preload JDBC data sources
		// Also done on config updates and seeker node discovery or loss
		DataSourcesManager.cleanAndPreloadDataSources();
		
		// Load specialised EntityAssociations for all files specified in config...
//		loadNewEntityAssociations();

//		MetricMonitor metricMonitor = MetricMonitor.getInstance(conn);
//		metricMonitor.addJVMMonitors();
		
		// Test whether MQTTMessageStorer can be used and started - i.e. if wmqtt.jar was placed somewhere under the lib folder
		try { getClass().getClassLoader().loadClass(MQTTMessageStorer.class.getName()); isMessageStorerAvailable = true; }
		catch ( Throwable e ) { logger.logInfo("Could not load MessageStorer (install wmqtt.jar from IBM Microbroker): " + e); }
	}
    
	private static final PrintStream BIT_BUCKET = new PrintStream(new OutputStream() { @Override public void write(int b) {} });
	
	private static Thread watchdogThread = null;
	
	private void runWatchdog() throws Exception {
		
		try {
			DatabaseConnectionsChecker.checkConnectionsInBackground();
			
			boolean logConnections = false;
			int maxDerbyInboundConnectionThreads = -1;

			int lastLoggedthreadCount = -1;
			double memoryUsedPrevious = 0;
			
			watchdogThread = Thread.currentThread();
			watchdogThread.setName(THREADNAME_WATCHDOG);
			
			// Commented out initialisation for-loop below.
			// We want to explicitly register ALL jdbc drivers that were on the system class-path - so users don't have to specify driver names in config.
			// DriverManager itself only auto-registers a selected few well-known drivers at startup (so it will ommit Hive for example).
			
//			for ( String libDir : new String[] { "lib", "lib/ext" } ) {
//				String[] newLibs = new File( GDB_WORKSPACE + "/" + libDir ).list();
//				if (null != newLibs) for ( String lib : newLibs ) if ( lib.endsWith(".jar") )
//					loadedUsrLibURLs.add( new File(GDB_WORKSPACE+"/"+libDir+"/"+lib).toURI().toURL() );	
//			}
			
			computeCPUsForThreadsAndNodeInPeriod(); // get an early measure before the first sleep
			
			while ( THREADNAME_WATCHDOG.equals( Thread.currentThread().getName() ) && isStarted() ) {
				
				GaianNodeSeeker.maintainSeeker();

//				try { autoExpandClassPathAndLoadNewlyAvailableJDBCDriversWithDynamicClassLoading(); }
//				catch ( Exception e ) { logger.logException("ENGINE_WATCHDOG_AUTO_LOAD_JDBC_DRIVER", "Unable to auto-load JDBC drivers", e); }

				boolean isDataServiceCidUpdatesWereRegistered = false;
				// Auto-load RDBMS connections for available RDBMS services in BlueMix - exposed through system environment properties
				try { isDataServiceCidUpdatesWereRegistered = autoLoadGaianConnectionsForNewlyAvailableBlueMixRDBMSServices(); }
				catch ( Exception e ) { logger.logException("ENGINE_WATCHDOG_AUTO_LOAD_RDBMS_CID_ERROR", "Unable to auto-load BlueMix RDBMS services", e); }
				
				if ( isMessageStorerAvailable && null == messageStorer ) initialiseMessageStorer();
				
				try {
					if ( -1 < memoryUsedPrevious ) {
						double memoryUsed = GaianDBUtilityProcedures.jMemory();
						if ( Math.abs( memoryUsed - memoryUsedPrevious ) > 10000000 ) {
							logger.logAlways("Used Heap Memory changed by >10MB. jMemory (MB): " + (memoryUsed/1000000) + 
									" (= " + GaianDBUtilityProcedures.jMemoryPercent() + 
									"%, jMemoryMax (MB): " + (double)GaianDBUtilityProcedures.jMemoryMax()/1000000 + ")" + 
									". jMemoryNonHeap (MB): " + (double)GaianDBUtilityProcedures.jMemoryNonHeap()/1000000 +
									" - (suspected hanging queries being checked: "+DatabaseConnectionsChecker.getNumberOfSuspectedHangingQueriesBeingChecked()+")");
							memoryUsedPrevious = memoryUsed;
						}
					}
				} catch ( Throwable e ) {
					logger.logWarning(GDBMessages.NODE_MEMORYMXBEAM_ERROR, "Unable to access/process MemoryMXBean for monitoring used memory (ignored): " + e);
					memoryUsedPrevious = -1;
				}
				
//				printSystemStats();
				
				// Refresh resources if necessary
				synchronized( DataSourcesManager.class ) {
					if ( GaianDBConfig.isRegistryNeedsReloadingFromFile() ) {
						// If an update has just been made to config by the seeker, tell it not to bother loading
						// it as we have the lock on it and are about to do it now.
						logger.logInfo("Watchdog reloading registry config...");
						GaianDBConfig.persistAndApplyConfigUpdates(null); // scramble out any newly entered pwds - also does a DataSourcesManager.refresh();
					} else if ( isDataServiceCidUpdatesWereRegistered )
						DataSourcesManager.refresh();
					else
						DataSourcesManager.cleanAndPreloadDataSources(); // try loading data sources that may have just become available...
//					loadNewEntityAssociations();
				}
				try { DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs(); }
				catch ( Exception e ) { logger.logWarning(GDBMessages.NODE_LT_VIEW_UPDATE_ERROR, "Failed to update some logical table views (ignored): " + e); }

				// Initialise newly defined policy classes
				// NOTE: static code in an old class should clean itself down if/when it detects it is no longer set to be the SQL_RESULT_FILTER class.
				GaianDBConfig.initialisePolicyClasses();
				
				for ( int i=0; i<THROUGHPUT_PERIODS_PER_WATCHDOG_PERIOD; i++ ) {
					Thread.sleep( THROUGHPUT_SAMPLING_PERIOD );
					dataThroughputInLastPeriod = GaianTable.getDataThroughput();
					queryActivityInLastPeriod = GaianTable.getQueryActivity();
					int numCancelled = GaianTable.checkAndActOnTimeouts();
					if ( 0 < numCancelled ) logger.logThreadInfo("Number of queries cancelled by timeouts: " + numCancelled);
					computeCPUsForThreadsAndNodeInPeriod();
				}
				
//				Thread.sleep( WATCHDOG_POLL_TIMEOUT );
				
				VTIWrapper.reloadCachedRowsForAllDataSourceWrappersRequiringIt();
				
				// Refresh credentials every time we loop
				GaianDBConfig.refreshRemoteAccessCredentials(securityClientAgent);
				
//				DataSourcesManager.createLogicalTableViews( null ); // must be outside of synchronized block to avoid deadlock
				
				// Drop connections to unwanted nodes from main watchdog loop because housekeeping must continue when discovery is disabled
				GaianNodeSeeker.dropConnectionsToNodesNotMeetingAccessRestrictions();
				
				if ( null != messageStorer ) {
					messageStorer.checkRefreshConfig();
					messageStorer.runRoutinePeriodicTasks();
				}
				
				synchronized( oldvtis ) {
					for ( VTIWrapper dsWrapper : oldvtis )
						try { dsWrapper.close(); }
						catch ( SQLException e ) { logger.logException( GDBMessages.NODE_START_CLOSE_OLD_VTI_ERROR_SQL, "Unable to close old VTIWrapper: ", e); }
						
					oldvtis.clear();
				}
				
				for ( GaianTable gt : scannedGaianTablesPossiblyStale )
					gt.close();
				
				scannedGaianTablesPossiblyStale.removeAllElements();
				
				synchronized ( scannedGaianTables ) {
					scannedGaianTablesPossiblyStale.addAll(scannedGaianTables);
					scannedGaianTables.removeAllElements();
				}
				
				if ( clearedArrayElements >= GC_INCREMENTAL_COLLECTION ) {
					
					System.gc();
					
					logger.logInfo("** Called GC, estimated blocks freed: " +  
							GC_INCREMENTAL_COLLECTION + "/" + clearedArrayElements);
					
					clearedArrayElements -= GC_INCREMENTAL_COLLECTION;
					if ( 0 > clearedArrayElements ) clearedArrayElements = 0;
				}
				
				// Log to a new log file (under logs/ (todo)) every hour - and (TBD) remove old and empty ones...
//				if ( 0 == cycles % (360000/WATCHDOG_POLL_TIMEOUT) ) { //mLogPrintStream.checkError() ) {

				// Swap log files when max log size is reached
				if ( null != mLogFile && 1000000 * GaianDBConfig.getLogfileMaxSizeMB() < new File(LOG_DIR, mLogFile).length() ) {
					PrintStream oldPrintStream = mLogPrintStream;
//					mLogFile = GaianDBConfig.getGaianNodeDatabaseName() + "-" + sdf.format(new Date(System.currentTimeMillis())) + ".log";
					String suf = mLogFile.equals(GaianDBConfig.getGaianNodeDatabaseName()+".log") || mLogFile.endsWith("#1.log") ? "#0.log" : "#1.log";
					mLogFile = GaianDBConfig.getGaianNodeDatabaseName() + suf;
					mLogPrintStream = new PrintStream( new FileOutputStream( new File(LOG_DIR, mLogFile) ) );
					Logger.setPrintStream( mLogPrintStream );
					oldPrintStream.close();
					logger.logAlways("Recycled Log File PrintStream" );
				}
				
//				gdbNodeThreadGroup.list();
				int threadCount = gdbNodeThreadGroup.activeCount();
				if ( lastLoggedthreadCount != threadCount ) {
					lastLoggedthreadCount = threadCount;
					logger.logInfo("New node thread count: " + lastLoggedthreadCount);
				}
				
				if ( !isLite() ) {
					if ( Logger.LOG_NONE < Logger.logLevel ) {
						if ( false == logConnections) { nsc.logConnections(true); logConnections = true; }
					} else if ( true == logConnections ) { nsc.logConnections(false); logConnections = false; }
					
					int newMaxThreads = GaianDBConfig.getMaxInboundConnectionThreads();
					if ( newMaxThreads != maxDerbyInboundConnectionThreads ) {
						nsc.setMaxThreads(newMaxThreads);
						logger.logInfo("Derby Network Server maxThreads set to: " + nsc.getMaxThreads());
						maxDerbyInboundConnectionThreads = newMaxThreads;
					}
				}
			}
		} catch ( InterruptedException e ) {
			
			shutdownRequestReason = THREADNAME_WATCHDOG + " was explicitly interrupted: " + e
									+ (null==shutdownRequestReason?"":", linked reason: " + shutdownRequestReason);
			watchdogThread = null;
			exitCode = 0;
			
		} catch ( Throwable e ) {
			
			// This exception might be silently replaced with another exception from the finally block - so we need to print it here.
			
			String msgPrefix = "Gaian Node Exception in Watchdog loop (shutting down), cause: ";
			logger.logWarning(GDBMessages.NODE_WATCHDOG_LOOP_ERROR, msgPrefix + Util.getStackTraceDigest(e));
			throw new Exception( msgPrefix + Util.getStackTraceDigest(e) );
		
		} finally {

			DatabaseConnectionsChecker.interruptConnectionsChecker();	
		}
		
//		metricMonitor.stop();
	}
    
    // Dynamic classloader code (yet to be tested) - To add a Jar at runtime, use: myCL.addURL( file.toURI().toURL() )
    private DynamicClassLoader gdbCL = null;
    private DynamicClassLoader getGdbCL() {
		return null != gdbCL ? gdbCL : (gdbCL = new DynamicClassLoader( (URLClassLoader) ClassLoader.getSystemClassLoader() ));
	}

	private class DynamicClassLoader extends URLClassLoader {
	    public DynamicClassLoader(URLClassLoader cl) { super(cl.getURLs()); }
	    @Override public void addURL(URL url) { super.addURL(url); }
    }
    private HashSet<URL> loadedUsrLibURLs = new HashSet<URL>();
    
	private final static Class<Driver> JDBC_DRIVER_INTERFACE = Driver.class;
    
    private final void autoExpandClassPathAndLoadNewlyAvailableJDBCDriversWithDynamicClassLoading() throws Exception {

		final Set<URL> jarURLs = new HashSet<URL>();
    	ArrayList<String> libDirs = new ArrayList<String>( Arrays.asList(GDB_WORKSPACE + "/lib") );
    	
    	while ( false == libDirs.isEmpty() ) {
    		
    		String libDir = libDirs.remove( libDirs.size()-1 );

    		final String[] fnames = new File( libDir ).list();
    		if ( null != fnames )
	    		for ( String fname : fnames ) {
	    			String fpath = libDir + "/" + fname;
	    			File f = new File( fpath );
	    			if ( f.isDirectory() ) { libDirs.add( fpath ); continue; }
	    			if ( fname.endsWith(".jar") ) jarURLs.add( f.toURI().toURL() );
	    		}
    	}
		
		jarURLs.removeAll( loadedUsrLibURLs );
		
		if ( 0 < jarURLs.size() ) {
			logger.logInfo("Auto-expanding classpath and loading JDBC drivers from newly found user libs: " + jarURLs);
			
			// Expand the system class-loader, so classes other than JDBC drivers can also be loaded dynamically
			for ( URL url : jarURLs ) getGdbCL().addURL(url);
//			final URLClassLoader cl = new URLClassLoader((URL[]) fileURLs.toArray(new URL[0]));
			int numFiles = 0, numDriverClasses = 0, numDrivers = 0;
			List<Class<?>> loadedClasses = new ArrayList<Class<?>>();
			List<String> registeredDrivers = new ArrayList<String>();

			final PrintStream originalSysOut = System.out, originalSysErr = System.err;
			System.setOut( BIT_BUCKET ); System.setErr( BIT_BUCKET );
			
			for ( URL url : jarURLs )
				for ( Enumeration list = new ZipFile(url.getFile()).entries(); list.hasMoreElements(); numFiles++ ) {
					final String zipEntryName = ((ZipEntry) list.nextElement()).getName();
					if ( false == zipEntryName.endsWith("Driver.class") ) continue;
					numDriverClasses++;
					final String className = zipEntryName.substring(0, zipEntryName.length()-6).replace('/','.').replace('\\','.');
					
					try { loadedClasses.add( Class.forName(className, false, getGdbCL()) ); } // just load the class for now
					catch ( Throwable e ) { logger.logDetail("Unable to load class: " + className + ", cause: " + e); } // ignore/skip
				}
//			Collections.sort( loadedClasses, // sort classes by length of their fully qualified names.. to register most likely required ones first 
//				new Comparator<Class<?>>() { public int compare(Class<?> c1, Class<?> c2) { return c1.getName().length() - c2.getName().length(); }}
//			);
			for ( Class<?> candidateDriverClass : loadedClasses ) {
				if ( JDBC_DRIVER_INTERFACE.isAssignableFrom( candidateDriverClass ) ) {
					numDrivers++;
					try { // Explicitly register this driver - (DriverManager only pre-registers known drivers from system class-loader..)
						DriverManager.registerDriver( new DriverWrapper( (Driver) candidateDriverClass.newInstance() ));
						registeredDrivers.add( candidateDriverClass.getName() );
					}
					catch ( Throwable e ) { logger.logDetail("Unable to register jdbc driver: " + candidateDriverClass.getName() + ", cause: " + e); } // ignore/skip
				}
			}
			
			System.setOut(originalSysOut); System.setErr(originalSysErr);

			loadedUsrLibURLs.addAll(jarURLs);
			
			logger.logInfo("Added jars to child classloader: " + jarURLs.size() + " (new total " + loadedUsrLibURLs.size() + ")"
					+ ". Total files in new jars: " + numFiles + "; Driver classes loaded: " + loadedClasses.size() + "/" + numDriverClasses
					+ "; registered JDBC drivers: " + registeredDrivers.size() + "/" + numDrivers + " = " + registeredDrivers);
		}
    }
    
	static Class<?> getClassUsingGaianClassLoader(final String className) throws Exception {
		// Android Studio has an issue with converting URLClassLoader to Dalvik - so don't attempt to use URLCLassLoader (used by getGdbCL()) for Lite nodes.
		if ( isLite() ) return Class.forName(className);
		else {
			try { return Class.forName(className); }
			catch ( ClassNotFoundException e ) { return Class.forName(className, true, gdbNodeSingleton.getGdbCL()); }
		}
	}
	
	private static final String ENV_VARIABLE_HOLDING_SERVICE_HANDLE_INFO_IN_BLUEMIX = "VCAP_SERVICES";
    private static final String VCAP_SERVICES_CREDS_START = "\"credentials\":{";
	
    /**
     * Auto-synch (i.e. load/unload) Gaian connections that newly appear/disappear from set of bound BlueMix data services.
     * 
     * @return whether connections were added or removed. false if no changes occured.
     */
    private final boolean autoLoadGaianConnectionsForNewlyAvailableBlueMixRDBMSServices() {
    	
		String vcapServicesJsonString = System.getenv( ENV_VARIABLE_HOLDING_SERVICE_HANDLE_INFO_IN_BLUEMIX );
		if ( null == vcapServicesJsonString ) return false; // config did not change
		Map<String,String> availableConnectionsToSynchTo = null;
		
		int idxOfNextCredsBlock = 0;
		
		while (true) {
			idxOfNextCredsBlock = vcapServicesJsonString.indexOf( VCAP_SERVICES_CREDS_START, idxOfNextCredsBlock );
			if ( -1 == idxOfNextCredsBlock ) break;
			idxOfNextCredsBlock += VCAP_SERVICES_CREDS_START.length();
			
			String[] nextCredsBlockInSingleElementList = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes(
					vcapServicesJsonString.substring(idxOfNextCredsBlock), '}', false, 1, null, null); // Use this method to specify maxElmts = 1
			
			if ( 1 != nextCredsBlockInSingleElementList.length ) break;
			String[] props = Util.splitByCommas( nextCredsBlockInSingleElementList[0] );
			
			String url = null, usr = null, pwd = null;
			for ( String s : props ) { // get each property
				String[] tuple = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrDoubleQuotes(s, ':');
				if ( 2 > tuple.length ) continue;
				final String prop = tuple[0].toLowerCase(), val = tuple[1];
				if 		( prop.equals("\"jdbcurl\"") ) url = val.substring(1, val.length()-1);
				else if ( prop.equals("\"username\"") ) usr = val.substring(1, val.length()-1);
				else if ( prop.equals("\"password\"") ) pwd = val.substring(1, val.length()-1);
				
				if ( null != url && null != usr && null != pwd && url.startsWith("jdbc:") ) {
					// String driver = RDBProvider.fromURL(url).knownDrivers.get(0); // No need to resolve a driver - optional
					
					int idx1 = "jdbc:".length(), idx2 = url.indexOf(':', idx1);
					if ( 0 > idx2 ) break; // invalid jdbcurl in credentials block
					
					if ( null == availableConnectionsToSynchTo ) availableConnectionsToSynchTo = new HashMap<String,String>();
					
					String redundantUsrPwdInURL = usr+":"+pwd+"@";
					if ( -1 < (idx1 = url.indexOf(redundantUsrPwdInURL)) )
						url = url.substring(0, idx1) + url.substring(idx1+redundantUsrPwdInURL.length());
					
					availableConnectionsToSynchTo.put(url+"'"+usr, pwd);
					
					break; // Done: found/registered this RDBMS service - move on to the next ones..
				}
			}
		}

		// Register cids as system/transient ones. Only add if doesn't exist + generate new cids (avoiding clashes with user ones). Also remove old ones.
		return GaianDBConfig.synchronizeSystemRDBMSConnections("BLUEMIX", availableConnectionsToSynchTo);
    }
	
    
	// NOTE: Kill methods must not kill the whole JVM with potential parent and sibling threads of GDB. There should be no System.exit() calls anywhere.
//	public static int killNode( int exitCode ) { stopNode(exitCode); return 1; } // isKillRequested = true; killExitCode = exitCode; return 1; }

	private static String shutdownRequestReason = null;
	private static int exitCode = -1;

	public void stop() { stop(0); }
	public static int stop( int exitCode ) { stop("Explicit stop. Exit code: " + exitCode); GaianNode.exitCode = exitCode; return 1; }
	public static void stop( String stopReason ) { stop(stopReason, null); }
	public static void stop( String stopReason, Throwable e ) {
		byte rStatus = getRunStatus();
		if ( RUN_STATUS_OFF == rStatus || RUN_STATUS_PENDING_OFF == rStatus ) return;
		if ( null == shutdownRequestReason )
			shutdownRequestReason = stopReason + (null==e?"":": " + Util.getStackTraceDigest(e));
		if ( -1 == exitCode && null != e && e instanceof OutOfMemoryError ) exitCode = 2;
		runStatus.set( RUN_STATUS_PENDING_OFF );
		if ( null != watchdogThread ) watchdogThread.interrupt();
	}
	
//	private static boolean isKillRequested = false;
//	private static int killExitCode = 0; lkjl
//	public static boolean isKillRequested() { return isKillRequested; }
//	public static int getKillExitCode() { return killExitCode; }
	
	private static long dataThroughputInLastPeriod = 0;
	public static long getDataThroughput() { return dataThroughputInLastPeriod; }
	
	private static int queryActivityInLastPeriod = 0;
	public static int getQueryActivity() { return queryActivityInLastPeriod; }
	
	private static int nodeCPUInLastPeriod = 0;
	public static int getNodeCPUInLastPeriod() { return nodeCPUInLastPeriod; }
	
	public static final String THREADINFO_COLNAMES =
		"ID, GRP, NAME, PRIORITY, STATE, CPU, CPUSYS, ISSUSPENDED, ISINNATIVE, BLOCKCOUNT, BLOCKTIME, WAITCOUNT, WAITTIME";
	
//	private static int dsCpuInLastPeriod = 0;
//	public static int getDsCpuInLastPeriod() { return dsCpuInLastPeriod; }
	
	private static ThreadMXBean threadMXBean;
	
	static {
		try { threadMXBean = ManagementFactory.getThreadMXBean(); }
		catch( Throwable e ) { logger.logWarning(GDBMessages.NODE_THREADMXBEAM_ERROR, "Unable to get threadMXBean - will not be able to compute CPU utilisation (ignored): " + e); }
	}

	// Limit total number of threads that we hold time values for - this caused memory leak with Tivoli APM product in August 2015. 
	private static final Map<Long, Long> previousCPUTimes 		= new CachedHashMap<Long, Long>(10000);
	private static final Map<Long, Long> previousUserCPUTimes 	= new CachedHashMap<Long, Long>(10000);
	
	private static final Map<Long, Short> threadsCPU			= new CachedHashMap<Long, Short>(10000); // percentage values
	private static final Map<Long, Short> threadsUserCPU		= new CachedHashMap<Long, Short>(10000); // percentage values
	
	private static long lastSampleTime = System.currentTimeMillis();
	

	private static void computeCPUsForThreadsAndNodeInPeriod() {
		
		try {
			long totalCpuTimeInPeriod = 0;
//			int sumds=0, sumother=0;
			
	        final long timeNow = System.currentTimeMillis();
	        final long timeInterval = timeNow - lastSampleTime;
	        final int numProcs = Runtime.getRuntime().availableProcessors();
	        lastSampleTime = timeNow;
			
			Thread[] threads = new Thread[ parentThreadGroup.activeCount()+100 ];
			final int numThreads = parentThreadGroup.enumerate(threads);
			
			Set<Long> currentThreadIDs = new HashSet<Long>();
			for ( Thread t : threads ) if ( null != t ) currentThreadIDs.add( t.getId() );

			// Clean up HashMaps - removing entries for old threads
			previousCPUTimes.keySet().retainAll( currentThreadIDs );
			previousUserCPUTimes.keySet().retainAll( currentThreadIDs );
			threadsCPU.keySet().retainAll( currentThreadIDs );
			threadsUserCPU.keySet().retainAll( currentThreadIDs );
			
			currentThreadIDs.clear();
			
//			System.out.println("\n*** Updated Threads CPU Information ***");
//			System.out.println("Num threads/size of array holding them: " + numThreads + '/' + threads.length);
//			long ts = System.currentTimeMillis();
			
			for ( int i=0; i<numThreads; i++ ) {
				final Thread t = threads[i];
				if ( null == t ) continue;
				final long tid = t.getId();

				// Compute %cpu values, given that cpuTime is in nanos and timeInterval is in millis
				// Note the ratio of millis to nanos is 1 million, so to obtain a percentage value we divide by 10000
//				int cpuPercent = -1, cpuUsrPercent = -1;
				final long cpuTime = null == threadMXBean ? -1 : threadMXBean.getThreadCpuTime(tid);
				if ( -1 != cpuTime ) {
					long cpuTimePrevious = previousCPUTimes.containsKey(tid) ? previousCPUTimes.get(tid) : 0;
					threadsCPU.put( tid, (short) ( (cpuTime-cpuTimePrevious) / (timeInterval*10000*numProcs) ) );
					previousCPUTimes.put(tid, cpuTime);
					totalCpuTimeInPeriod += cpuTime - cpuTimePrevious;
//		        	if ( -1 == tInfo.getThreadName().indexOf(GaianResult.DS_EXECUTOR_THREAD_PREFIX) ) sumother += cpuTime - cpuTimePrevious;
//		        	else sumds += cpuTime - cpuTimePrevious;
				}
				final long cpuUsrTime = null == threadMXBean ? -1 : threadMXBean.getThreadUserTime(tid);
				if ( -1 != cpuUsrTime ) {
					long cpuUsrTimePrevious = previousUserCPUTimes.containsKey(tid) ? previousUserCPUTimes.get(tid) : 0;
					threadsUserCPU.put( tid, (short) ( (cpuUsrTime-cpuUsrTimePrevious) / (timeInterval*10000*numProcs) ) );
					previousUserCPUTimes.put(tid, cpuUsrTime);
				}
			}
			
//			System.out.println("Num threads checked: " + numThreads + ", time taken (ms): " + (System.currentTimeMillis() - ts));
	        
			// Compute a % value, given that totalCpuTimeInPeriod is in nanos and timeInterval is in millis
			// Note the ratio of millis to nanos is 1 million, so to obtain a percentage value we divide by 10000
			nodeCPUInLastPeriod = (int) (totalCpuTimeInPeriod/(timeInterval*10000*numProcs));
			
//			dsCpuInLastPeriod = (int) (sumds/(timeInterval*10000*numProcs));
//			System.out.println("\nSum Non DS: " + ((double)sumother)/1000000 + ", Sum DS: " + ((double)sumds)/1000000 + ", CPU %: " + nodeCPUInLastPeriod);
//			System.out.println("CPU values: total (ns): " + totalCpuTimeInPeriod
//				+ ", timeInterval (ms): " + timeInterval + ", available processors: " + numProcs + " -> CPU (%): " + nodeCPUInLastPeriod);
		} catch ( Exception e ) {
			logger.logWarning(GDBMessages.NODE_CPU_COMPUTE_ERROR, "Unable to compute thread info or node CPU: " + Util.getStackTraceDigest(e));
		}
	}
	
	
	public static List<String> getJvmThreadsInfo() {

		final List<String> jvmThreadsInfo = new ArrayList<String>();
		
		try {
			Thread[] threads = new Thread[ parentThreadGroup.activeCount()+100 ];
			final int numThreads = parentThreadGroup.enumerate(threads);
			
//			System.out.println("\n*** Getting Threads Information ***");
//			System.out.println("Num threads/size of array holding them: " + numThreads + '/' + threads.length);
//			long ts = System.currentTimeMillis();
			
			for ( int i=0; i<numThreads; i++ ) {
				final Thread t = threads[i];
				if ( null == t ) continue;
				final long tid = t.getId();
				final ThreadGroup tGroup = t.getThreadGroup();
				final String tGroupName = null == tGroup ? null : Util.escapeSingleQuotes( tGroup.getName() );
				final String tInfoNull = "cast (null as boolean), cast (null as boolean), cast (null as int), "
									   + "cast (null as int), cast (null as int), cast (null as int)";
				final String tName = Util.escapeSingleQuotes( t.getName() );
				final int tPriority = t.getPriority();
				final String tState = Util.escapeSingleQuotes( t.getState().toString() );
				
				// Columns are:
				// ID, GRP, NAME, PRIORITY, STATE, CPU, CPUSYS, ISSUSPENDED, ISINNATIVE, BLOCKCOUNT, BLOCKTIME, WAITCOUNT, WAITTIME
				if ( null == threadMXBean ) {
					jvmThreadsInfo.add( tid + ",'" + tGroupName + "','" + tName + "'," + tPriority + ",'" + tState + "',"
							+ "cast (null as int), cast (null as int)," + tInfoNull
							// Add Locks, Block+Wait count/time, Lock monitors/synchronizers ?
					);
					continue;
				}

				ThreadInfo tInfo = threadMXBean.getThreadInfo(tid); // not supported by some gnu versions of java

				short cpuPercent = threadsCPU.containsKey(tid) ? threadsCPU.get(tid) : 0;
				short cpuUsrPercent = threadsUserCPU.containsKey(tid) ? threadsUserCPU.get(tid) : 0;
				
				// Columns are:
				// ID, GRP, NAME, PRIORITY, STATE, CPU, CPUSYS, ISSUSPENDED, ISINNATIVE, BLOCKCOUNT, BLOCKTIME, WAITCOUNT, WAITTIME
				String tInfoString = 
					tid + ",'" + tGroupName + "','" + tName + "'," + tPriority + ",'" + tState + "',"
					+ cpuPercent + "," + (cpuPercent - cpuUsrPercent) + ","
					+ ( null == tInfo ? tInfoNull :
						tInfo.isSuspended() + "," + tInfo.isInNative() + ","
						+ tInfo.getBlockedCount() + "," + tInfo.getBlockedTime() + ","
						+ tInfo.getWaitedCount() + "," + tInfo.getWaitedTime()
						// Add Locks, Block+Wait count/time, Lock monitors/synchronizers ?
					  );
				
				jvmThreadsInfo.add( tInfoString );
			}
//			System.out.println("Num threads checked: " + numThreads + ", time taken (ms): " + (System.currentTimeMillis() - ts));
			
		} catch ( Exception e ) {
			logger.logWarning(GDBMessages.NODE_CPU_COMPUTE_ERROR, "Unable to compute thread info or node CPU: " + Util.getStackTraceDigest(e));
		}
		
		return jvmThreadsInfo;
	}
	
//	private static void printSystemStats() {
//		OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
//		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
////		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
//		for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) printSystemStat(operatingSystemMXBean, method);
////		for (Method method : memoryMXBean.getClass().getDeclaredMethods()) printJvmStat(memoryMXBean, method);
//		for (Method method : threadMXBean.getClass().getDeclaredMethods()) printSystemStat(threadMXBean, method);
//	}
//	
//	private static void printSystemStat( Object bean, Method method ) {
//	    method.setAccessible(true);
////	    if ( method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers()) ) {
////	    	Object value; try { value = method.invoke(bean); } catch (Exception e) { value = e; }
////	        System.out.println(method.getName() + " = " + value);
////	    }
////	    System.out.println("Trying to execute private methods...");
//	    if ( method.getName().startsWith("get") && Modifier.isPrivate(method.getModifiers()) ) {
//	    	Object value; try { value = method.invoke(bean); } catch (Exception e) { value = e; }
//	        System.out.println(method.getName() + " = " + value);
//	    }
//	}

//	private String inputFileName = null;
//	private String previousFileName = null;
//	private String maxGroupSizeProperty = null;
//	private String previousGroupSize = null;
//	private String numEntitiesProperty = null;
//	private String previousEntities = null;
//	private String outputFileNameProperty = null;
//	private String previousOutputFile = null;
	
//	private void loadNewEntityAssociations() {		
//		int maxGroupSize = 0;
//		int numEntities = 0;
//		
//		try {
//			inputFileName = GaianDBConfig.getVTIProperty( EntityAssociations.class, EntityAssociations.PROPERTY_ENTITY_ASSOCIATIONS_INPUT_FILE );
//			numEntitiesProperty = GaianDBConfig.getVTIProperty( EntityAssociations.class, EntityAssociations.PROPERTY_ENTITY_ASSOCIATIONS_NUM_ENTITIES );
//			maxGroupSizeProperty = GaianDBConfig.getVTIProperty( EntityAssociations.class, EntityAssociations.PROPERTY_ENTITY_ASSOCIATIONS_GROUP_SIZE );
//			outputFileNameProperty = GaianDBConfig.getVTIProperty( EntityAssociations.class, EntityAssociations.PROPERTY_ENTITY_ASSOCIATIONS_OUTPUT_FILE );
//			
//			boolean configIsDefined =
//				null != inputFileName && null != numEntitiesProperty && null != maxGroupSizeProperty;
//			
//			if ( ! configIsDefined ) {
//				if ( null != previousFileName ) {
//					EntityAssociations.unloadMatrix( previousFileName );
//					previousFileName = null;
//				}
//			} else {
//				// If the config was modified, unload and reload the entity associations
//				
//				boolean configWasDefined =
//					null != previousFileName && null != previousEntities && null != previousGroupSize;
//				
//				// Check that config was defined so we dont always load at startup
//				boolean configChanged = configWasDefined && ( ! inputFileName.equals( previousFileName ) || 
//						! numEntitiesProperty.equals( previousEntities ) || ! maxGroupSizeProperty.equals( previousGroupSize ) ||
//						( null == outputFileNameProperty ? null != previousOutputFile : !outputFileNameProperty.equals( previousOutputFile ) ));
//				
//				// Also check if input file is more recent than output file
//				
//				File ipf = new File( inputFileName );
//				File opf = new File( outputFileNameProperty );
//				long loadedTime = null == outputFileNameProperty ? EntityAssociations.getLoadTime(inputFileName) : 
//					( 0 == opf.length() ? 0 : opf.lastModified() );
//				boolean inputFileNeedsReloading = loadedTime < ipf.lastModified();
//	
//				if ( configChanged || inputFileNeedsReloading ) {				
//					EntityAssociations.unloadMatrix( previousFileName ); // does nothing if previousFileName is null
//					
//					numEntities = Integer.parseInt( numEntitiesProperty );
//					maxGroupSize = Integer.parseInt( maxGroupSizeProperty );
//					
//					String msg = "Loading Entity Associations for file " + inputFileName + 
//								 ", numEntities: " + numEntities + ", maxGroupSize: " + maxGroupSize +
//								 (null == outputFileNameProperty ? "" : ", outputFile: " + outputFileNameProperty);
//					
//					logger.logInfo( msg );
//					System.out.println( msg + " (please wait) ... " );
//					long t = System.currentTimeMillis();
//										
//					if ( ! ipf.exists() ) {
//						msg = "Failed: File does not exist";
//						logger.logWarning( GDBMessages.NODE_ENTITY_ASSOC_LOAD_FILE_NOT_FOUND, "Load of EntityAssociations " + msg );
//						System.out.println( msg );
//					} else {
//						
//						boolean rc = EntityAssociations.loadMatrix( inputFileName, numEntities, maxGroupSize, outputFileNameProperty );
//					
//						if ( false == rc )
//							System.out.println( "Failed - Could not load Entity Associations - please check logs" );
//						else {
//							System.out.println( "Done - Entity Associations Loaded in "  + (System.currentTimeMillis() - t) + "ms" );
//							
//							previousFileName = inputFileName;
//							previousEntities = numEntitiesProperty;
//							previousGroupSize = maxGroupSizeProperty;
//							previousOutputFile = outputFileNameProperty;
//						}
//					}
//				}
//			}
//			
//		} catch ( Exception e ) {
//			logger.logException( GDBMessages.NODE_LOAD_ENTITY_ERROR, "Unable to read EntityAssociations properties (fileName: " +
//					inputFileName + ", numEntities: " + numEntities + ", maxGroupSize: " + maxGroupSize + ")", e);			
//		}
//	}
	
//	public static void reloadRegistryIfConfigChanged() throws Exception {
//		
//		synchronized ( DataSourcesManager.class ) {
//			// Refresh resources if they have changed
//			if ( GaianDBConfig.refreshRegistryIfNecessary() )
//				DataSourcesManager.refresh();
//		}
//	}
	
	private void initialiseMessageStorer() {
		
		String brokerHost = GaianDBConfig.getBrokerHost();
		int brokerPort = GaianDBConfig.getBrokerPort();
		
		if ( null == brokerHost || -1 == brokerPort ) return;
		
		boolean cmdLineTopic = null != mqttTopic;
		if ( !cmdLineTopic )
			mqttTopic = GaianDBConfig.getBrokerTopic();
		
		logger.logInfo("Initialising Message Storer: host " + brokerHost + ", port " + brokerPort + ", topic " + mqttTopic);
		
		try {
			if ( null == mqttMessageStorerDBConnection )
				mqttMessageStorerDBConnection = GaianDBConfig.getEmbeddedDerbyConnection();
					// Use embedded connection - not the one below...
//					DriverManager.getConnection( "jdbc:derby://localhost:" + mPort + "/" + GaianDBConfig.getGaianNodeDatabaseName(),
//						GaianDBConfig.getGaianNodeUser(), GaianDBConfig.getGaianNodePassword() );
		} catch (SQLException e) {
			logger.logInfo("Unable to obtain connection to local gaiandb derby database (will retry periodically): " + e);
			return;
		}
		
		try {
			String ip = localHostAddress.getHostAddress();
			messageStorer = new MQTTMessageStorer(
					"GDB" + ip + ":" + mPort,
					brokerHost, brokerPort, mqttTopic, cmdLineTopic,
					mqttMessageStorerDBConnection, true ); // last boolean says if we are using refreshable broker config from gaiandb config file
		
		} catch ( Exception e ) {
			logger.logException(GDBMessages.NODE_MQTT_CONSTRUCT_ERROR, "Exception caught constructing " + MQTTMessageStorer.MQTTMessageStorerBaseClassName, e);
		}
	}
	
	protected void finalize() throws Throwable {
		
		try {
			DataSourcesManager.closeAllDataSourcesAndSourceHandles();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			super.finalize();
		}
	}
	
	private void setArgs(String[] args) {
		
		for( int i=0; i<args.length; i+=2 ) {

			String arg = args[i];
			
			if ( "-console".equals( arg ) ) { mLogPrintStream = System.out; i--; continue; }
			if ( "-lite".equals( arg ) ) { isLiteNode = true; i--; continue; }
			if ( "-test".equals( arg ) ) { isTestModeOn = true; i--; continue; }
			if ( "-showtimes".equals( arg ) ) { showStartupTimes = true; i--; continue; }
			
			if ( i+1 == args.length ) syntaxError( args, "Aborting on argument: " + arg);
			String val = args[i+1];
			
//			logger.logAlways("arg: " + arg + ", val: " + val);
			
			if ( "-n".equals( arg ) ) {
				if ( null != mNodeName ) syntaxError(args, "mNodeName value cannot be defined more than once");
				mNodeName = val;
			} else if ( "-p".equals( arg ) ) {
				if ( mPort != -1 ) syntaxError(args, "Port value cannot be defined more than once");
				mPort = Integer.parseInt( val );
			} else if ( "-h".equals( arg ) ) { // hidden option... wouldn't normally be used but may come in handy (?)
				if ( null != mHost ) syntaxError(args, "Host value cannot be defined more than once");
				mHost = val;
			} else if ( "-g".equals( arg ) ) { // list of gateways
				if ( null != mGateways ) syntaxError(args, "Gateways value cannot be defined more than once");
				mGateways = val;
//			} else if ( "-exto".equals( arg ) ) { // execute timeout for hanging data sources - config hack for Android demo which used UDP driver.
//				if ( null != mExecTimeout ) syntaxError("Exec timeout value cannot be defined more than once");
//				mExecTimeout = val;
//			} else if ( "-w".equals( arg ) ) {
//				if ( null != mWorkspace ) syntaxError("Workspace value cannot be defined more than once");
//				mWorkspace = val;
			} else if ( "-c".equals( arg ) ) {
				if ( null != mConfig || !val.endsWith(".properties") ) syntaxError(args, "Config file name must be specified once only and have the extention '.properties'");
				mConfig = val.substring(0, val.indexOf( ".properties" ));
			} else if ( "-mt".equals( arg ) ) {
				if ( mqttTopic != null ) syntaxError(args, "Topic argument cannot be specified more than once");
				mqttTopic = val;
//			} else if ( "-initsql".equals( arg ) ) {
//				if ( null != initsql ) syntaxError("initsql cannot be defined more than once");
//				initsql = val;
			} else if ( "-initscript".equals( arg ) ) {
				gdbInitFileSQL = val;
			} else if ( "-log".equals( arg ) ) {
				if ( null != mLogLevel /*|| null != mLogFile*/ ) syntaxError(args, "-log argument cannot be specified more than once");
				
//				if ( mLogFile == null && val.endsWith(".log") ) {
//					mLogFile = val.substring(0, val.indexOf( ".log" ));
//					if ( i+2 == args.length || args[i+2].startsWith("-") )
//						continue;
//					val = args[ i+ 2 ];
//				}
				
				mLogLevel = val;
				if ( !Logger.isValidLogLevel( mLogLevel ) )
					syntaxError(args, "Cannot set log level to: " + mLogLevel + ", possible levels are: " + Arrays.asList( Logger.POSSIBLE_LEVELS ));
//					syntaxError("-log arguments must have a .log extention or be one of the valid log levels: " + Arrays.asList( Logger.POSSIBLE_LEVELS ));
				
			} else {
				syntaxError(args, "Unrecognised argument: " + arg);
			}
		}
		
		if ( -1 != mPort ) {
//			if ( null == mConfig ) syntaxError("A config file must be specified when a port value is specified"); // A config file is required if a different port is specified
		} else {
			mPort = DEFAULT_PORT;
		}
		if ( null == mConfig ) mConfig = GDB_WORKSPACE + "/" + DEFAULT_CONFIG;
		if ( null == mHost ) mHost = DEFAULT_PROVENANCE_HOST;
//		if ( null == mWorkspace ) mWorkspace = "";
	}
	
//	private static void syntaxError() {
//		logger.logAlways( USAGE ); System.exit(1);
//	}
	
	private static void syntaxError(String[] args, String help) {
		logger.logRaw( "\n" + help + "\nGaianNode args[] = " + Arrays.asList(args) + "\n" + USAGE + "\n" ); stop("Usage error on start-up: " + help);
	}
	
	private static void showStartupTime( int threadIndex, String codeDescription ) {
		if ( showStartupTimes ) {
//			if ( 0 == t1[threadIndex] ) System.out.println();
			t1[threadIndex] = System.currentTimeMillis();
			System.out.println("Thread " + threadIndex + " did " + codeDescription + " in (ms): " + (t1[threadIndex]-t0[threadIndex]));
			t0[threadIndex] = t1[threadIndex];
		}
	}
	
	// define a query runner against which we can perform node initialisation.
	private static SQLDerbyRunner sdr = null;
	
	/**
	 * Performs part initialisation of the Gaian Node. 
	 * This creates the connection maintenance procedure for a new database.
	 * 
	 * @throws Exception 
	 */ 
	private void initialiseGaianNodeData0() throws Exception {
		
//		Use the SQLDerbyRunner because it has extra functionality to define positional parms and
//		to ignore failing DROP commands easily (with a '!' prefix); and it remembers prepared statements on it
		
		sdr = new SQLDerbyRunner(
				GaianDBConfig.getGaianNodeUser(),
				GaianDBConfig.getGaianNodePassword(),
				GaianDBConfig.getGaianNodeDatabaseName() );
		
		sdr.processSQLs( "-quiet" );
		sdr.processSQLs( "-standalone" );
		
		// Already done when starting the network server
//		sdr.processSQLs( s, "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('"+
//				GaianDBConfig.getGaianNodeUser()+"', '"+GaianDBConfig.getGaianNodePassword()+"')", "" );
//		long t1 = System.currentTimeMillis();

		// Run any user specific initialisation SQL
		sdr.processSQLs( stripSQLDropsIfInitialisingNewGDB( DatabaseConnectionsChecker.INIT_SQL ) );
		showStartupTime( 1, "standalone connection and INIT_SQL" );
	}
	
	/**
	 * Performs part initialisation of the Gaian Node. 
	 * If the database is new (the gaian jar file is the same and the modification 
	 * date of the database is newer than the Jar date) then we perform initialisation:
	 * creating default tables,  registers procedures and functions, grants the default 
	 * user priviledge, other security configuration and then executes any initialisation script
	 * that may have been specified on the command line.
	 * This is only performed for new databases and not checked on existing databases to speed 
	 * up the startup of GaianDB
	 * 
	 * NOTE: When this method is called, the logical tables are loaded but not necessarily their 
	 * wrapping views (only if they were created on a previous node initialisation).
	 * 
	 * @throws Exception 
	 */ 
	private void initialiseGaianNodeData() throws Exception {
		
////		Use the SQLDerbyRunner because it has extra functionality to define positional parms and
////		to ignore failing DROP commands easily (with a '!' prefix); and it remembers prepared statements on it
//		SQLDerbyRunner sdr = new SQLDerbyRunner(
//				GaianDBConfig.getGaianNodeUser(),
//				GaianDBConfig.getGaianNodePassword(),
//				GaianDBConfig.getGaianNodeDatabaseName() );
//		
//		sdr.processSQLs( "-quiet" );
//		sdr.processSQLs( "-standalone" );
//								
//		// Already done when starting the network server
////		sdr.processSQLs( s, "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('"+
////				GaianDBConfig.getGaianNodeUser()+"', '"+GaianDBConfig.getGaianNodePassword()+"')", "" );
////		long t1 = System.currentTimeMillis();
//
//		// Run any user specific initialisation SQL
//		sdr.processSQLs( DatabaseConnectionsChecker.INIT_SQL );
//		showStartupTime( "standalone connection and INIT_SQL" );

		Statement stmt = sdr.createStatementOffInternalConnection();
		DataSourcesManager.registerDatabaseStatementForLogicalTableViewsLoading( GaianDBConfig.getGaianNodeDatabaseName(), stmt );
		
		if ( false == isInitDataUpToDate ) {
		
			sdr.processSQLs( stripSQLDropsIfInitialisingNewGDB( GaianDBConfigProcedures.GAIANDB_API ) );			// Setup the API
			sdr.processSQLs( stripSQLDropsIfInitialisingNewGDB( GaianDBUtilityProcedures.PROCEDURES_SQL ) );		// Setup utility procedures
			
			// Replaced 3 lines here with code further down that checks if the tables already exist
//			sdr.processSQLs( "!" + MetricMonitor.getCreateMetricsTableSQL() );
//			sdr.processSQLs( "!" + HttpQueryInterface.getCreateQueriesTableSQL() );
//			sdr.processSQLs( "!" + HttpQueryInterface.getCreateQueryFieldsTableSQL() );
			
			// GRANT access to the GDBINIT_USERDB() procedure to everyone if schema privacy is enabled - so that alternate users can clone our database.
			if ( "TRUE".equals( Util.getDerbyDatabaseProperty(stmt, "derby.database.sqlAuthorization") ) )
				stmt.executeQuery( "grant execute on procedure GDBINIT_USERDB to public" );

			final String schema = GaianDBConfig.getGaianNodeUser().toUpperCase(); // MUST BE UPPER CASE!
			
			// Create special table for metric monitoring (e.g. CPU, Memory etc)
			Util.executeCreateIfDerbyTableDoesNotExist( stmt, schema, MetricMonitor.PHYSICAL_TABLE_NAME, MetricMonitor.getCreateMetricsTableSQL() );
			
			// Create special tables for storing encapsulated queries, e.g. for the Web Query Module (part of DWQT - Database Web Query Tools)
			Util.executeCreateIfDerbyTableDoesNotExist( stmt, schema, HttpQueryInterface.QUERIES_TABLE_NAME, HttpQueryInterface.getCreateQueriesTableSQL() );
			Util.executeCreateIfDerbyTableDoesNotExist( stmt, schema, HttpQueryInterface.QUERY_FIELDS_TABLE_NAME, HttpQueryInterface.getCreateQueryFieldsTableSQL() );
			
			sdr.processSQLs(
				// Allow access by user GAIANDB on database GAIANDB - this is necessary in case users wish to disable default user access by enabling
				// the following system property in derby.properties: derby.database.defaultConnectionMode=noAccess
				"CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.fullAccessUsers', '"+ GaianDBConfig.getGaianNodeUser() +"');"
				// Defect 93155 - Avoid adding Derby property settings in code here, because it will appear as hidden behaviour that differs from a plain Derby install.
			);
			
			showStartupTime( 0, "data GAIANDB_API and utility procedures" );
	
			// Views check/update is done in DataSourcesManager.resetUpToDateViews()
//			// Setup the logical table views
//			try { DataSourcesManager.checkUpdateLogicalTableViews( sdr.createStatementOffInternalConnection() ); }
//			catch ( Exception e ) { logger.logWarning(GDBMessages.NODE_LT_VIEWS_SETUP_ERROR, "Failed to setup logical table views: " + e); }
//			
//			showStartupTime( 0, "data LT VIEWS" );
					
			SecurityManager.initialiseUsersTableAndItsUpdateTrigger( sdr );
			showStartupTime( 0, "users table initialisation" );
		}
	}
	
	private static String stripSQLDropsIfInitialisingNewGDB( final String initSQL ) {
//		logger.logInfo("SQL with stripped DROPs: " + initSQL.replaceAll("!DROP[^;]*;", ""));
		return 0L < gdbDbTimestampAtStartup ? initSQL : initSQL.replaceAll("!DROP[^;]*;", "");
	}
}
