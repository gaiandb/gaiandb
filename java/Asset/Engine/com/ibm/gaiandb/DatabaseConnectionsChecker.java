/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ibm.gaiandb.diags.GDBMessages;

/**
 * This class runs a set of threads to check the health of JDBC connections against peer GaianDB nodes and to take corrective action when 
 * required - in particular when a connection is hanging.
 * 
 * The outer class and its 2 inner classes are all Runnable.
 * The outer class implements a perpetual watch-dog thread (not to be confused with the GaianDB main watch-dog) which manages the other inner-class threads.
 * The inner class threads are used to test individual JDBC connections:
 * 		- ConnectionMaintainer inner class: Uses a short-lived SQL procedure on a JDBC connection established against a discovered peer GaianDB node - 
 * 											to establish/maintain a "two-way connection" with it.
 *  	- ConnectionTester inner class: 	Uses a simple SQL query "values 1" to poll a peer GaianDB node against which an in-progress query is suspected 
 *  										to be hanging. The poll is run on a separate JDBC connection.
 *  
 *  NOTE: As said above - ConnectionTester only currently tests long-running queries against peer GaianDB nodes.
 *  Faulty leaf JDBC source are only currently semi-handled by DataSourcesManager.cleanAndPreloadDataSources()/isValidAndActiveSourceHandle(). - i.e. hanging ones are not.
 *  
 *  The outer class periodically wakes up - based on configurable parameter: GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS - and then checks the status for each
 *  inner class threads it kicked off.
 *  If an inner class thread fails to respond successfully within the GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS delay, then the Gaian connection is 
 *  dropped - i.e. All JDBC connections to the peer node are closed and associated GaianDB data-sources are removed.
 *  See VTIRDBResult.lostConnection() -> GaianNodeSeeker.lostDiscoveredConnection() -> DataSourcesManager.unloadAllDataSourcesAndClearConnectionPoolForGaianConnection().
 *  
 * The outer class thread only dies when GaianNode.isRunning() is no longer true.
 * 
 * ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 * Change history comments:
 * 
 * 12/04/2016 - Removed use of redundant "initialConnectionTimeout" in ConnectionMaintainer inner class.
 * 				User property GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS should be tuned to be sufficiently large to account for initial connection establishment.
 * 
 * TODO:
 * ??/??/???? - Add use of java thread pools - rather than starting a new thread for each connection maintenance/testing attempt.
 * ??/??/???? - Re-factor monitoring code for outcome of ConnectionTester into one-off thread runs of the outer class - to ensure that hanging queries are rooted out
 * 				as fast as possible after GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS has elapsed - rather than waiting for a previous iteration to complete.
 *  
 * ...
 * 
 * @author drvyvyan
 */

public class DatabaseConnectionsChecker implements Runnable {

//	private final static String connectionMaintainerFunction = "maintainConnection";
//	public final static String INIT_SQL =
//		"!DROP FUNCTION " + connectionMaintainerFunction + ";CREATE FUNCTION " + connectionMaintainerFunction + 
//		" (nodeid VARCHAR(100), usr VARCHAR(10), pwd VARCHAR(10)) RETURNS VARCHAR(500)" +
//		" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures." + 
//		connectionMaintainerFunction + "'";

	private final static String connectionMaintainerFunction = "maintainConnection2";
	public final static String INIT_SQL =
		"!DROP FUNCTION " + connectionMaintainerFunction + ";CREATE FUNCTION " + connectionMaintainerFunction + 
		" (nodeid VARCHAR(100), usr VARCHAR(10), pwd VARCHAR(10), extraInfo "+ Util.XSTR+") RETURNS "+ Util.XSTR +
		" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures." + 
		connectionMaintainerFunction + "'";
	
	public final static String SUCCESS_TAG = "SUCCESS:";
	public final static String DISTANCE2SERVER_TAG = "D2S:";
	public final static String SSLMODE_TAG = "SSL:";
	
	private static int distanceToServerNode = GaianNode.isLite() ? -1 : 0;
	public static int getDistanceToServerNode() { return distanceToServerNode; }
	private static String bestPathToServer = null;
	public static String getBestPathToServer() { return bestPathToServer; }
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "DatabaseConnectionsChecker", 35 );
	
	private static final String TESTER_NAME = "ConnectionTester";
	private static final String MAINTAINER_NAME = "ConnectionMaintainer";

	private static final DatabaseConnectionsChecker dcc = new DatabaseConnectionsChecker();
	private static Thread connectionsCheckerThread = null;
	
	// Set of all sets of executing vtis
	private static final Set<Set<VTIWrapper>> executingDataSourceSets = new HashSet<Set<VTIWrapper>>();
	// Set of all vtis currently being checked
	private static final Set<VTIWrapper> dataSourcesBeingChecked = new HashSet<VTIWrapper>();
	
	// Mapping of Set executingDataSources -> LinkedBlockingQueue nodeResults
//	private static ConcurrentMap<Set<VTIWrapper>, Queue<DataValueDescriptor[][]>> nodeResultsForExecutingDataSources = new ConcurrentHashMap<Set<VTIWrapper>, Queue<DataValueDescriptor[][]>>();
	private static final ConcurrentMap<Set<VTIWrapper>, GaianResult> gaianResultsForExecutingDataSources =
		new ConcurrentHashMap<Set<VTIWrapper>, GaianResult>();
	
	static int getNumberOfSuspectedHangingQueriesBeingChecked() { return gaianResultsForExecutingDataSources.size(); }
	
	// Set of String connection ids
	private static final Set<String> maintainedConnections = new HashSet<String>();
	
	// VTIWrapper -> ConnectionTester ; String (gaian outgoing connection id) -> ConnectionMaintainer
	private static final Hashtable<VTIWrapper, ConnectionTester> testers = new Hashtable<VTIWrapper, ConnectionTester>();
	private static final Hashtable<String, ConnectionMaintainer> maintainers = new Hashtable<String, ConnectionMaintainer>();
	
	private static int connectionsCheckerHeartbeat = GaianDBConfig.getConnectionsCheckerHeartbeat();
	
	public static void maintainTwoWayConnection( String connID ) {
		
		synchronized ( maintainedConnections ) {
			maintainedConnections.add( connID );
			dcc.kickOffMaintainerThread( connID );
		}
	}

//	public static void rootOutHangingDataSources( Set<VTIWrapper> executingDataSources, Queue<DataValueDescriptor[][]> resultRowsBuffer ) {
	public static void rootOutHangingDataSources( Set<VTIWrapper> executingDataSources, GaianResult gResult ) {
		
		boolean isDataSourceSetContainsAGaianNode = false;
		for ( Iterator<VTIWrapper> it = executingDataSources.iterator(); it.hasNext() && false == isDataSourceSetContainsAGaianNode; )
			isDataSourceSetContainsAGaianNode = it.next().isGaianNode();
		
		if ( isDataSourceSetContainsAGaianNode ) {
			synchronized ( dataSourcesBeingChecked ) {
				dataSourcesBeingChecked.addAll( executingDataSources );
				executingDataSourceSets.add( executingDataSources );
			}
	
			gaianResultsForExecutingDataSources.put( executingDataSources, gResult );
		}
	}
	
	// Low value feature to skip maintenance for certain connections
//	private static ConcurrentSkipListSet<String> connectionsExcludedFromNextMaintenanceCycle = 
//		new ConcurrentSkipListSet<String>();
//	public static void excludeConnectionFromNextMaintenanceCycle( String connID ) {
//		System.out.println("Adding connection to maintenance exclusion set: " + connID);
//		connectionsExcludedFromNextMaintenanceCycle.add(connID);
//	}
	
	static void checkConnectionsInBackground() {
		if ( null == connectionsCheckerThread )
			connectionsCheckerThread = new Thread(dcc, GaianNode.THREADNAME_CONNECTIONS_CHECKER);
		connectionsCheckerThread.start();
	}
	
	static void interruptConnectionsChecker() {
		if ( null != connectionsCheckerThread ) connectionsCheckerThread.interrupt();
	}
	
	/**
	 * This method should really be called within a code block that waits or sleeps
	 * after calling it before checking whether the maintenance call succeeded.
	 * However we also call it the first time from maintainTwoWayConnection just to get the 
	 * connection set up as quickly as possible - later calls will check for success status. 
	 * 
	 * @param gaianConnectionID
	 */
	private void kickOffMaintainerThread( String gaianConnectionID ) {
		
		ConnectionMaintainer cm = (ConnectionMaintainer) maintainers.get(gaianConnectionID);
		if ( null == cm ) {
			cm = new ConnectionMaintainer( gaianConnectionID );
			maintainers.put( gaianConnectionID, cm );
		} else
			cm.reinitialise();

//		if ( !cm.isInitialConnectionAttemptInProgress() )
			new Thread( cm, MAINTAINER_NAME + " for " + gaianConnectionID ).start();
	}
	
    public void run() {
    	
    	try {
    		
    		Set<VTIWrapper> latestDataSourcesBeingChecked = new HashSet<VTIWrapper>();
    		Set<Set<VTIWrapper>> latestExecutingDataSourceSets = new HashSet<Set<VTIWrapper>>();
    		Set<VTIWrapper> hangingDataSources = new HashSet<VTIWrapper>();
    		
    		Set<String> latestMaintainedConnections = new HashSet<String>();
    		int numSuspectDataSourceSetsPrevious = -1;
    		
    		while ( GaianNode.isRunning() ) {

    			connectionsCheckerHeartbeat = GaianDBConfig.getConnectionsCheckerHeartbeat();
    			
    			// Clear temporary sets for next iteration (should already be done... just being defensive)
    			latestDataSourcesBeingChecked.clear();
    			hangingDataSources.clear();
    			latestExecutingDataSourceSets.clear();
    			latestMaintainedConnections.clear();
    			
    			synchronized ( maintainedConnections ) {
    				// Use temporary set, so seeker thread can register new maintenance connection
    				// concurrently without affecting the code that modifies the set after this synchronized block
    				latestMaintainedConnections.addAll(maintainedConnections);
    				// Note we don't clear the maintained connections here
    				
    				// Low value feature to skip maintenance for certain connections
//    				for ( Iterator<String> it = connectionsExcludedFromNextMaintenanceCycle.iterator(); it.hasNext() ; ) {
//    					String alreadyQueriedConnection = it.next();
//    					latestMaintainedConnections.remove(alreadyQueriedConnection);
//    					it.remove();
//    				}
    			}

//    			System.out.println("Connection maintenance set: " + Arrays.asList(latestMaintainedConnections));
    			
    			for (String conn : latestMaintainedConnections)
    				kickOffMaintainerThread( (String) conn );
    			
    			synchronized( dataSourcesBeingChecked ) {
    				// Use temporary sets, so executing threads can register their executing vtis 
    				// concurrently without affecting the code that modifies these sets after this synchronized block
    				latestExecutingDataSourceSets.addAll( executingDataSourceSets );
    				executingDataSourceSets.clear();
    				latestDataSourcesBeingChecked.addAll( dataSourcesBeingChecked );
    				dataSourcesBeingChecked.clear();
    			}

    			for (VTIWrapper dataSource : latestDataSourcesBeingChecked) {
    				if ( ! ( dataSource instanceof VTIRDBResult ) ) continue; // No need to check non RDBMS data sources
    				
    				if ( !dataSource.isGaianNode() ) {
    					logger.logInfo("No need to check dataSource as it is not a Gaian Node: " + dataSource);
    					continue;
    				}
    				
    				ConnectionTester ct = (ConnectionTester) testers.get(dataSource);
    				if ( null == ct ) {
    					ct = new ConnectionTester( (VTIRDBResult) dataSource );
    					testers.put( dataSource, ct );
    				}
    				
    				ct.reinitialise();
    				new Thread( ct, TESTER_NAME + " for " + dataSource ).start();
    			}
    			
//    			logger.logDetail( "Maintained connections: " + latestMaintainedConnections.size() + 
//    					", DataSources being checked: " + latestDataSourcesBeingChecked.size());
    						
    			try { Thread.sleep( connectionsCheckerHeartbeat ); }
    			catch (InterruptedException e) {
    				if ( !GaianNode.isRunning() ) break;
    				// restart the loop if interrupted while running, with no other suspected hanging connections being tested.
    				else if ( latestDataSourcesBeingChecked.isEmpty() && latestExecutingDataSourceSets.isEmpty() ) continue;
    			}
    			
    			for (VTIWrapper dataSource : latestDataSourcesBeingChecked) {
    				
    				ConnectionTester ct = (ConnectionTester) testers.get(dataSource);
    				if ( null == ct ) continue; // it will be null if we don't test this type of dataSource (e.g. if dataSource is not a VTIRDBresult)
    				
    				if ( false == ct.pollSucceeded() ) {
    					logger.logInfo("Poll hanging for active jdbc connection of data source: " + dataSource);
    					// cant close jdbc connection as it will hang aswell (as it waits for the hanging exec to clear itself up)
//    					try {
//    						ct.closeJDBCConnection();
//    						logger.logInfo("Closed JDBC connection for hanging data source " + vti);
//    					} catch (SQLException e) {
//    						logger.logWarning("Unable to close JDBC connection for hanging data source " + vti + ": " + e);
//    					}
    					hangingDataSources.add(dataSource);
    					((VTIRDBResult) dataSource).lostConnection();
//    					testers.remove( dataSource ); // not actually necessary, the hanging thread can't be harmful when it gets unstuck
    				}
    			}
    			latestDataSourcesBeingChecked.clear(); // vtis all checked
    			
//    			logger.logInfo("All hanging data sources: " + Arrays.asList( hangingDataSources ) );
//    			logger.logInfo("Number of data source sets being checked for executing queries: " + latestExecutingDataSourceSets.size() );
    			int numSuspectDataSourceSets = gaianResultsForExecutingDataSources.size();
    			if ( numSuspectDataSourceSetsPrevious != numSuspectDataSourceSets || numSuspectDataSourceSets > 0 ) {
    				logger.logInfo("New number of potentially hanging queries being checked (involving a GaianNode data source): " + numSuspectDataSourceSets );
    				numSuspectDataSourceSetsPrevious = numSuspectDataSourceSets;
    			}    			
    			for (Set<VTIWrapper> executingDataSources : latestExecutingDataSourceSets) {
    				logger.logInfo("Checking set of executing data sources: " + executingDataSources );
    				GaianResult gResult = gaianResultsForExecutingDataSources.remove( executingDataSources );

    				if ( null == gResult ) continue; // should not happen but possible if latestExecutingDataSourceSets was not cleared properly				
    												
    				synchronized( executingDataSources ) {
    					
    					int sizeBeforeRootOut = executingDataSources.size();
    					if ( true == executingDataSources.removeAll( hangingDataSources ) ) {
    	
    	//					executingDataSources.notify(); // wake up fetcher thread to tell it about its cancelled hanging executors
    						
    						// Some of the hanging vtis were in this set of executing vtis.. so notify the GaianResult using
    						// a Poison Pill: An empty result with the number hanging vtis that have been rooted out of this set.
    						logger.logInfo( "Rooted out " + (sizeBeforeRootOut - executingDataSources.size()) + " hanging data sources for query" );
    						if ( 0 == executingDataSources.size() ) {
//    							try {
    								gResult.endResults();
    								logger.logInfo("Put poison pill on rowResultsBuffer queue as there are no more executing threads");
//    							} catch (InterruptedException e) {
//    								logger.logException("Interrupted while putting final termination row on queue: ", e);
//    							}
    						}
    					}
    				}
    				
    				gResult.reEnableCheckingOfHangingQueries();
    			}
    			// Clear temporary sets for next iteration
    			hangingDataSources.clear();
    			latestExecutingDataSourceSets.clear();
    			
    			for (String gc : latestMaintainedConnections) {
    				ConnectionMaintainer cm = maintainers.get(gc);
    				
    				if ( /*false == cm.isInitialConnectionAttemptInProgress() &&*/ false == cm.isTwoWayConnected() ) {
    					logger.logInfo("Maintenance fct call failed for outbound connection " + gc + " to discovered node, dropping it");
    					
    					// This synchronized block encapsulates both stmts in case an "add node" tries to come in in-between the two.
    					// The "add node" would have to wait for the maintaining state for gc to be removed before it is added again...
    					// Note a node cannot be found to be lost in between it being added and reloaded so that case is safe.
    					synchronized( maintainedConnections ) {
    						if ( GaianDBConfig.isDiscoveredConnection(gc) )
    							if ( 	false == cm.isConnectionMaintainedByTheOtherNode()
    								||	false == GaianNodeSeeker.reverseMaintenanceDirectionToIncoming( gc ) )
    								GaianNodeSeeker.lostDiscoveredConnection( gc );
    						maintainedConnections.remove(gc);
//    						maintainers.remove(gc); // not actually necessary
    					}
    				}
    			}

    			// Clear temp set after use
    			latestMaintainedConnections.clear();
    		}
    		
    	} catch ( Throwable e ) {
			GaianNode.stop( "Error/Exception in DatabaseConnectionsChecker", e );
    	} finally {
    		connectionsCheckerThread = null;
    	}
	}
	
	private class ConnectionTester implements Runnable {

		private static final String POLL_SQL = "values 1"; // "select 1 from sysibm.sysdummy1";
//		private static final String MAINTAIN_CONNECTION_SQL = "values checkConnection()"; // "select 1 from sysibm.sysdummy1";
		
		private Connection c;
		private final VTIRDBResult dataSource;
		private boolean pollSucceeded = false;
		
		public ConnectionTester( VTIRDBResult dataSource ) {
			this.dataSource = dataSource;
		}
		
		public void reinitialise() {
			pollSucceeded = false;
			
			// can't use a statement on same connection :( - Derby locks up when trying to exec simultaneously on 2 statements of a same connection
//			this.c = dataSource.getConnectionOfLongestRunningStatement(); 
			Connection c = null;
			try { c = dataSource.getConnectionFromApplicablePool(); // dataSource.getConnectionOfLongestRunningStatement();
			} catch (SQLException e) { logger.logInfo("Unable to get connection for ConnectionTester for data source: " + dataSource); }
			this.c = c;
		}
		
		public boolean pollSucceeded() {
			return pollSucceeded;
		}
		
		public void run() {
			
			logger.logInfo("Checking database connection "+c+" for data source: " + dataSource);
			try {
				// Note if c is null it means the statement we are checking actually completed execution just as we were about to check...
				if ( null == c )
					logger.logInfo("Poll not executed against data source: " + dataSource + ", cause: could not obtain connection (data source query completed?)");
				else {
					Statement s = c.createStatement();
					logger.logInfo("Executing poll query against new statement of connection "+c+" for data source: " + dataSource);
					if ( GaianNode.isInTestMode() && GaianDBConfigProcedures.internalDiags.containsKey("hang_on_suspect_connection_poll") ) {
						GaianDBConfigProcedures.internalDiags.remove("hang_on_suspect_connection_poll"); // disable this straight away
						logger.logInfo("Executing Poll replaced with a simulated hang using jsleep(60000) for data source: " + dataSource );
						s.executeQuery("values jsleep(60000)").close(); // sleep remotely for one minute to simulate hang for testing
						logger.logInfo("Poll simulating a hang with jsleep(60000) completed for data source: " + dataSource );
					} else
						s.executeQuery(POLL_SQL).close();
					
					logger.logInfo("Poll succeeded for active jdbc connection of data source: " + dataSource );
				}
				pollSucceeded = true;
				dataSource.reEnableNow();
				dataSource.returnConnectionToApplicablePool(c);
			} catch (SQLException e) {
				logger.logWarning(GDBMessages.ENGINE_DS_CONN_POLL_ERROR, "Unable to poll active jdbc connection of data source: " + dataSource + ", cause: " + e);
			}
		}
	}
	
	private class ConnectionMaintainer implements Runnable {
		
		private final String gc;
		private boolean isTwoWayConnected = false;
		
		// Previously used to skip verification of this connection allow more time for a first maintenance call - because the establishment of a first connection to the peer node is time-consuming.
//		private int connectionTimeout = 0;
		
		private boolean isFirstMaintenanceCall = true;
		private String errmsg = null;
		
		public ConnectionMaintainer( String gc ) { // , int initialConnectionTimeout ) {
			this.gc = gc;
//			this.connectionTimeout = connectionsCheckerHeartbeat;
		}
		
		public void reinitialise() {
			isTwoWayConnected = false;
			isFirstMaintenanceCall = false;
			errmsg = null;
		}

		public boolean isTwoWayConnected() { return isTwoWayConnected; }
		
		public boolean isConnectionMaintainedByTheOtherNode() {
			return null != errmsg && -1 != errmsg.indexOf( GaianNodeSeeker.VALID_CONNECTION_BUT_INVALID_MAINTENANCE_DIRECTION );
		}
		
//		public boolean isInitialConnectionAttemptInProgress() {
//			return connectionTimeout != connectionsCheckerHeartbeat;
//		}
		
		public void run() {
			
			String connectionProperties = null;
			Stack<Object> pool = null;
			Connection c = null;
			String nodeID = null;
			String sql = null;
			
			// Do some validation
			try { 
				connectionProperties = GaianDBConfig.getRDBConnectionDetailsAsString( gc );
				// Check that gc exists since we last checked - hasnt been removed by the Seeker for being on an unwanted host
				if ( !GaianDBConfig.isDiscoveredOrDefinedConnection(gc) ) {
					errmsg = "Not a discovered or defined connection: " + gc;
				} else {
					nodeID = GaianDBConfig.getDiscoveredNodeID(gc);
				}
			}
			catch (Exception e) { errmsg = "Cannot lookup " + gc + ": " + e; }
			
			if ( null == errmsg ) {
				pool = DataSourcesManager.getSourceHandlesPool( connectionProperties );
	//			logger.logInfo("Checking database connection for: " + gc);
				try {
//					System.out.println("!!!!!!!!!!   Getting connection for " + connectionProperties);
					c = DataSourcesManager.getPooledJDBCConnection( connectionProperties, pool, connectionsCheckerHeartbeat ); // connectionTimeout );
//					System.out.println("!!!!!!!!!!   GOTTTTTTTTTTTTT connection for " + connectionProperties);
					final String sslMode = GaianDBConfig.getSSLMode();
					sql = "values " + connectionMaintainerFunction + "('" + GaianDBConfig.getGaianNodeID() + "', '" +
						GaianDBConfig.getGaianNodeUser() + "', '" +
						Util.escapeSingleQuotes( GaianDBConfig.getGaianNodePasswordScrambled() ) + "', '" +
						(isFirstMaintenanceCall?"INIT,":"") + (null==sslMode?"":SSLMODE_TAG+sslMode+',') +
						DISTANCE2SERVER_TAG + distanceToServerNode + "')";
//					System.out.println("Maintenance check SQL: " + sql);
					Statement stmt = c.createStatement();
//					stmt.setQueryTimeout( connectionTimeout-1000 );
					ResultSet rs = stmt.executeQuery( sql );
					if ( rs.next() ) {
						errmsg = rs.getString(1);
						isTwoWayConnected = null == errmsg || errmsg.startsWith(SUCCESS_TAG);
						if ( isTwoWayConnected )
							resolveBestPathToNonLiteNodeFromMaintenanceMessage(errmsg, nodeID);
					} else errmsg = "No result rows returned by maintenance function!";
					rs.close();
				}
				catch (SQLException e) { errmsg = "SQLException caught: " + e.toString(); }
//				finally { connectionTimeout = connectionsCheckerHeartbeat; }
			}
			
			if ( isTwoWayConnected )
				logger.logDetail("Maintenance check succeeded for gaian connection: " + gc + " to " + nodeID + ", pool size: " + pool.size());
			else
				logger.logWarning(GDBMessages.ENGINE_CONN_MAINTENANCE_CHECK_ERROR, "Maintenance check failed for gaian connection " + gc + " to " + nodeID + 
						", sql = " + sql + "; cause: " + errmsg);
//						(null==errmsg ? "rc=" + rc : errmsg) );
			
			if ( null != c ) pool.push(c);
		}
	}
	
	public static void invalidatePotentialPathToServerNode( String node ) {
		if ( 0 < distanceToServerNode && null != node && node.equals(bestPathToServer) ) {
			distanceToServerNode = -1;
			bestPathToServer = null;
		}
	}
	
	public static void resolveBestPathToNonLiteNodeFromMaintenanceMessage( String info, String candidateNode ) {
		if ( null != info ) {
			int idx = info.indexOf(DISTANCE2SERVER_TAG);
			int idx2 = info.indexOf(",", idx);
			if ( -1 == idx ) {
				logger.logInfo("No extra info detected from maintainConnection() call");
			} else {
				if ( -1 == idx2 ) idx2 = info.length();
				try {
					int dist = Integer.parseInt( info.substring(idx+DISTANCE2SERVER_TAG.length(), idx2).trim() );
					if ( -1 < dist )
						if ( -1 == distanceToServerNode || dist+1 < distanceToServerNode ) {
							distanceToServerNode = dist+1;
							bestPathToServer = candidateNode;
							logger.logInfo("Updated distance to nearest derby enabled node: "
									+ distanceToServerNode + ", via " + bestPathToServer);
						}
				} catch ( Exception e ) {
					logger.logInfo("Unable to retrieve a valid distanceToServerNode from maintainConnection() info: " + e);
				}
			}
		}
	}
}
