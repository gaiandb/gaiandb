/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ibm.gaiandb.diags.GDBMessages;


/**
 * @author DavidVyvyan
 */
public class DatabaseConnector implements Runnable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "DatabaseConnector", 35 );

	// Max response time accepted for a connection attempt - note that this should
	// take account of the JDBC timeout of 2 minutes as the number of uninterrupted threads will grow for
	// every new connection attempted in those 2 minutes.
//	private static final int LONGEST_EXPECTED_TIME_FOR_CONNECTION_ATTEMPT_MS = 10000; // i.e. there could be 120000 / 10000 = 12 concurrent failing connections
	
	// Stagger the connection attempts into the following duration evenly (unless JDBC_CONNECTION_ATTEMPT_TIMEOUT_MS is set)
	//private static final int EXPECTED_JDBC_CONNECTION_TIMEOUT_MS = 150000; // 2m30s
	
	// jdbc url -> threads connecting to it
	// This is used to cap the number of connections being sought concurrently on a single url.
	// Indeed there is no need for this number to be too high - if it is it means connections are all timing out anyway 
	// and each new attempt is just wasting resources.
	private static ConcurrentMap<String, Vector<Thread>> connectionsInProgress = new ConcurrentHashMap<String, Vector<Thread>>();
	
//	private static ConcurrentMap<String, Long> latestConnectionAttemptTimes = new ConcurrentHashMap<String, Long>();
	
	// The list of connecting threads for this instance of DatabaseConnector, mapped 121 with urls.
	private Vector<Thread> connectingThreads = null;
	
//	private static final int MAX_CONCURRENT_CONNECTION_ATTEMPTS_PER_SOURCE = 1;
	
	private final String url;
	private final String usr;
	private final String pwd;
	
	private Stack<Connection> connectionPool = null;
//	private String exceptionMsg = null;
	
	public DatabaseConnector( String url, String usr, String pwd ) {
		
		this.url = url;
		this.usr = usr;
		this.pwd = pwd;
	}
	
	public ResultSetMetaData getTableMetaData( String table ) throws SQLException {
		
		ResultSet rs = null;
		if ( null == usr || 0 == usr.length() ) {
			logger.logInfo( "Preparing JDBC Statement and getting RSMD with no authentication parms");

			// Do not use getMetaData() on a non executed prepared statement - Oracle does not allow it
//			return DriverManager.getConnection( url ).prepareStatement(sql).getMetaData();
			
			rs = DriverManager.getConnection( url ).createStatement().
				executeQuery("select * from " + table + " where 0=1");

		} else {
			logger.logInfo( "Preparing JDBC Statement and getting RSMD with authentication parms for usr " + usr);
//			System.out.println( "Preparing JDBC Statement, url " + url + ", usr " + usr + ", pwd " + pwd );
			
			rs = DriverManager.getConnection( url, usr, pwd ).createStatement().
				executeQuery("select * from " + table + " where 0=1");
		}

		return rs.getMetaData();
	}
	
	public Connection getConnection() throws SQLException {
		if ( null == usr || 0 == usr.length() ) {
			logger.logDetail( "Creating JDBC Connection for url: " + url + 
					", concurrent attempts: " + (null == connectingThreads ? 1 : connectingThreads.size()));
			return DriverManager.getConnection( url );
		} else {
			logger.logDetail( "Creating JDBC Connection for usr: " + usr + ", url: " + url + 
					", concurrent attempts: " + (null == connectingThreads ? 1 : connectingThreads.size()));
			return DriverManager.getConnection( url, usr, pwd );
		}
	}
	
	/**
	 * Get a jdbc statement and put it in the given pool
	 * Once the statement is obtained, return.
	 * If the statement is not obtained within timeoutMs ms, return anyway while it keeps trying to get it.
	 */
	public Connection getConnectionWithinTimeoutOrToPoolAsynchronously( Stack<Connection> connectionPool, long timeoutMs ) {
    	
		long t0 = System.currentTimeMillis();
		
    	synchronized( this.connectionPool = connectionPool ) {
    		
    		do {    	
		    	connectingThreads = connectionsInProgress.get( url );
		    	if ( null == connectingThreads ) connectingThreads = new Vector<Thread>();
		    	
		    	int numConcurrentAttempts = connectingThreads.size();
		    	
//		    	int maxConcurrentConnectionAttempts = GaianDBConfig.getMaxConcurrentConnectionAttempts(); // this must be 1
		    	
		    	if ( numConcurrentAttempts < 1 ) //maxConcurrentConnectionAttempts && numConcurrentAttempts < 3 )
		    		launchConnectionThread( timeoutMs );		    	
//		    	else if ( numConcurrentAttempts < maxConcurrentConnectionAttempts ) {
//		    		        	
//		       		long timeSinceLatestConnectionAttempt = System.currentTimeMillis() - latestConnectionAttemptTimes.get(url);
//		       		
//		       		int jdbcConnectionAttemptTimeout = GaianDBConfig.getConnectionAttemptTimeout();
//		       		if ( 0 > jdbcConnectionAttemptTimeout )
//		       			jdbcConnectionAttemptTimeout = EXPECTED_JDBC_CONNECTION_TIMEOUT_MS / (maxConcurrentConnectionAttempts-3);
//		       		
//		       		// Stagger connection attempts during the JDBC driver timeout (which is usually about 2min30secs for Derby)
//		       		// in case the destination is reachable again.
//		    		if ( timeSinceLatestConnectionAttempt > jdbcConnectionAttemptTimeout ) {
//		    			logger.logThreadWarning("Making new connection (currently pending: " + numConcurrentAttempts + 
//		    					", time since last attempt: " + timeSinceLatestConnectionAttempt + "ms), url:" + url);
//		//	        	Thread t = (Thread) connectingThreads.get(0);
//		//	        	t.interrupt(); // this will prob have no effect, we'll still be waiting 2mins on the jdbc socket timeout.
//		    			
//			        	// Now launch a new connection thread replacing the old one - the new attempt might have more chances.
//			        	launchConnectionThread( timeoutMs );
//		    		}
//		    		
//		    		// Now wait for a connection to be established... (whether or not we launched a connection attempt)
//		    	} 
		    	else {
					logger.logThreadImportant( "Reached max concurrent connection attempts: " + 1 //maxConcurrentConnectionAttempts 
			    			+ ", for url: " + url + ( 1 > timeoutMs ? " - abandoning as timeout is 0ms" : " - waiting " + timeoutMs + "ms") );
					// for connection or timeout"); //+ " (killing 1)"); //" (ignoring request)");
		    	}
				
				if ( 1 > timeoutMs ) return null;
				
				try { this.connectionPool.wait( timeoutMs ); } // releases pool synchro lock while waiting
				catch (InterruptedException e) { // unexpected
					logger.logException(GDBMessages.ENGINE_CONN_WAIT_INTERRUPTED_ERROR, "Caught InterruptedException whilst waiting for DB Connection: ", e );
				}
				
				if ( !this.connectionPool.isEmpty() ) return this.connectionPool.pop();
				
    		} while ( ( timeoutMs -= System.currentTimeMillis() - t0 ) > 0 ); // false ); //
		}
    	
    	return null; // No connection could be obtained within the timeout. If/when it is obtained, it will be left in the pool.
	}
	
	private void launchConnectionThread( long timeoutMs ) {
		
    	int tIndex = connectingThreads.size() + 1;
    	
    	Thread t = new Thread(this, "Connector " + tIndex + " from " + Thread.currentThread().getName());
    	connectingThreads.add(t);
    	
    	logger.logThreadInfo( "Getting DB Connection for: " + url +
    			" (" + tIndex + " concurrent, synchronous timeout " + timeoutMs + "ms)" );
    	
		t.start();

    	connectionsInProgress.put( url, connectingThreads );
	}
	
	public void run() {

		long cstart = System.currentTimeMillis();
		try {
			Connection c = getConnection();
			connectionPool.push( c );
			logger.logThreadImportant( "Obtained JDBC Connection " + c + " in " + (System.currentTimeMillis()-cstart) + " ms for: " + 
					url + ", Pool size: " + connectionPool.size() );
		} catch ( Exception e ) {
			String db2Info = null;
			logger.logThreadWarning(GDBMessages.ENGINE_JDBC_CONN_ATTEMPT_ERROR, "Failed JDBC Connection attempt in " + (System.currentTimeMillis()-cstart) + " ms for: " + 
					url + ", cause: " + e +
					( e instanceof SQLException && null!=url && url.startsWith("jdbc:db2:")
							&& null!=(db2Info=Util.getDB2Msg((SQLException) e, false)) ? ", DB2 Info: " + db2Info  : "" )
					+ "; Common issues: missing jdbc driver, network/database unavailability (e.g. firewall), incorrect user/password and/or insufficient database access rights"
					+ (null!=url && url.startsWith("jdbc:derby") ? " (e.g. if derby.database.defaultConnectionMode=noAccess in derby.properties)" : "")
//					+ "\n" + Util.getStackTraceDigest(e)
					);
		}
		
		if ( connectingThreads.remove( Thread.currentThread() ) ) {
//	    	int maxConcurrentConnectionAttempts = GaianDBConfig.getMaxConcurrentConnectionAttempts();
//			if ( connectingThreads.size()+1 == maxConcurrentConnectionAttempts )
//				logger.logThreadImportant( "Dropped below max concurrent connection attempts: " +
//						maxConcurrentConnectionAttempts + " for url: " + url + " - new attemps can now be made");
		}
		
		synchronized (connectionPool) {
			connectionPool.notifyAll();
		}
	}
}
