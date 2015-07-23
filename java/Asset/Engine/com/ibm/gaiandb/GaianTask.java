/* 
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.vti.IFastPath;

import com.ibm.db2j.GaianQuery;

/**
 * This class is a wrapper class for a GaianNode.
 * This can be used to embed GaianDB in another project and run in a shared process.
 * The main points of interest are: startup/shutdown/restart, and the capability to invoke the primary GaianDB VTI objects 'GaianTable' and 'GaianQuery' directly. 
 * 
 * @author DavidVyvyan
 */

public class GaianTask {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";
	
	private static final Logger logger = new Logger("GaianTask", 20);
	
	public static final String CONFIG_CATEGORY = GaianTask.class.getPackage().getName();
	public static final String CONFIG_CLUSTER_ID = "clusterid";

	private final static GaianNode gdbNode = new GaianNode();
	private static String gdbThreadEndMessage = null;
	private final AtomicBoolean isGaianParentThreadRunning = new AtomicBoolean(false);
	
	private final List<String> taskArgs;

	public GaianTask() { taskArgs = new ArrayList<String>(); }
	public GaianTask( List<String> args ) { taskArgs = new ArrayList<String>( args ); }
	
	/**
	 * Called by a component wishing to embed a GaianNode 
	 * 
	 */
	public synchronized void startTask() throws Exception {
		
		String gdbHome = System.getProperty("derby.system.home");
		
		if ( null == gdbHome ) {
			gdbHome = Util.getInstallPath();
			if ( null == gdbHome ) gdbHome = ".";
			System.setProperty( "derby.system.home", gdbHome );
		}
		
		logger.logInfo("Resolved GaianDB home for config and log file locations to: " + gdbHome);
		
		if ( false == taskArgs.contains("-c") ) { taskArgs.add("-c"); taskArgs.add( gdbHome + "/gaiandb_config.properties" ); }
		
		 // e.g new String[] { "-p", "6414", "-n", "FabricNode1" }
		final String[] gaianTaskStartupOptions = (String[]) taskArgs.toArray( new String[0] );
		
		final Thread gdbParentThread = new Thread( "GaianDB parent thread" ) {
			public void run() {
				try {
					gdbNode.start( gaianTaskStartupOptions );
					gdbThreadEndMessage = "GaianNode parent thread exiting cleanly (no Exception)";
					logger.logInfo( gdbThreadEndMessage );
				}
				catch (Throwable e) {
					gdbThreadEndMessage = "GaianNode parent thread Exception: " + Util.getStackTraceDigest(e);
					logger.logInfo( gdbThreadEndMessage );
				}
				finally { isGaianParentThreadRunning.set(false); }
			}
		};
		
		logger.logInfo( "GaianTask runStatus before start(): " + GaianNode.getRunStatus() + ", isStarted(): " + gdbNode.isStarted() +
				", isGaianParentThreadRunning ? " + isGaianParentThreadRunning );
		
		// Ensure the node was not already started
		if ( true == isGaianParentThreadRunning.compareAndSet(false, true) ) {
			
			try { gdbParentThread.start(); }
			catch ( Throwable t ) {
				isGaianParentThreadRunning.set(false);
				logger.logInfo("Unable to start() GaianNode Parent Thread");
			}
			
			while ( false == gdbNode.isStarted() ) {
				if ( false == isGaianParentThreadRunning.get() ) { // Do not use gdbParentThread.isAlive() - this sometimes returns false when the node is still running.
					logger.logInfo( "GaianNode parent thread is not running" );
					throw new Exception("Unable to start Task - " + gdbThreadEndMessage);
				}
				Thread.sleep( 100 );
			}
		} else
			logger.logInfo("GaianNode parent thread is already running");
	}
	
	public synchronized void shutDown() {
		gdbNode.stop(); // tell the node to shut itself down
		
		// Wait for the gdb parent thread to exit
		while ( true == isGaianParentThreadRunning.get() ) { // Do not use gdbParentThread.isAlive() - this sometimes returns false when the node is still running.
			try { Thread.sleep( 50 ); } catch (InterruptedException e) { e.printStackTrace(); }
		}
	}
	
	// This method has to be here (rather than in GaianNode or GaianDBUtilityProcedures for instance)
	// because we invoke GaianQuery directly rather than in SQL.
	public static InetAddress[] getGaianClusterIPs() {
		
		GaianQuery gaianQuery = null;
		
		try {
			
			gaianQuery = new GaianQuery("select substr(curl,14,locate(':',curl,14)-14) gdbip"
				+			" from new com.ibm.db2j.GaianConfig('RDBCONNECTIONS') GC where cnodeid is not null");
			gaianQuery.executeAsFastPath();
			
			Set<String> ips = new HashSet<String>();
			ips.add( InetAddress.getLocalHost().getHostAddress() );
			
			DataValueDescriptor[] row = new DataValueDescriptor[] { new SQLChar() };
			while ( IFastPath.SCAN_COMPLETED != gaianQuery.nextRow( row ) )
				ips.add( row[0].getString() );
			
			InetAddress[] ipAddresses = new InetAddress[ ips.size() ];
			int i = 0; for ( String ip : ips ) ipAddresses[i++] = InetAddress.getByName( ip );

			logger.logInfo("Resolved ipAddresses: " + Arrays.asList( ipAddresses ));
			return ipAddresses;
			
		} catch ( Exception e ) {
		} finally {
			if ( null != gaianQuery ) try { gaianQuery.close(); } catch (Exception e1) {}; // cleanup as much as possible
		}
		
		return null;
	}
	
//	public static InetAddress[] getGaianClusterIPs() {
//
//			ResultSet rs = null;
//			
//			try {	
//				rs = GaianDBConfig.getEmbeddedDerbyConnection().createStatement().executeQuery(
//						"select distinct gdbip from new com.ibm.db2j.GaianQuery('"
//					+		"select substr(curl,14,locate('':'',curl,14)-14) gdbip"
//					+			" from new com.ibm.db2j.GaianConfig(''RDBCONNECTIONS'') GC where cnodeid is not null"
//					+	"') GQ"
//				);
//	
////			// Other alternative
////			rs = GaianDBConfig.getEmbeddedDerbyConnection().createStatement().executeQuery(
////				+ "select ipv4 from new com.ibm.db2j.GaianQuery('call listnet(''" + ipPrefix + "'')', 'with_provenance') GQ"
//				
//				List<InetAddress> ips = new ArrayList<InetAddress>();
//				
//				while ( rs.next() ) ips.add( InetAddress.getByName( rs.getString(1) ) );
//				
////				logger.logInfo("Resolved ips: " + Arrays.asList( (InetAddress[]) ips.toArray( new InetAddress[0] ) ));
//				
//				return ips.toArray( new InetAddress[0] );
//				
//			} catch ( Exception e ) {
//				logger.logInfo( "Unable to resolve Gaian cluster IPs (returning null), cause: " + e );
//			} finally {
//				if ( null != rs ) try { rs.getStatement().getConnection().close(); } catch (Exception e1) {}; // cleanup as much as possible
//			}
//			
//			return null;
//		}
}