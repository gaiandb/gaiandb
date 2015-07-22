/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.dsm;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianDBProcedureUtils;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.watson.dsm.services.gaian.GaianOpinionManager;
import com.ibm.watson.dsm.services.gaian.INeighborProvider;

public class OpinionsBasedRouterDSM implements SQLResultFilter {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private boolean doRestrictLocalDataAccessToQueriesComingFromTrustedNeighboursOnly = true; // TODO: to be configurable in future...
	private boolean doForwardQueriesFromTrustedNeighboursOnly = true; // TODO: to be configurable in future...
	private boolean doForwardQueriesToTrustedNeighboursOnly = false; // TODO: to be configurable in future...
	
	private boolean isSimpleQueryRestrictionToTrustedForwardingNodes =
		doForwardQueriesFromTrustedNeighboursOnly && doRestrictLocalDataAccessToQueriesComingFromTrustedNeighboursOnly &&
		! doForwardQueriesToTrustedNeighboursOnly;
	
	private static String sensitiveLogicalTablesCSV = "LT0, LT1";
//	private static List<String> sensitiveLogicalTables = Arrays.asList( "LT0" );

	private static AtomicInteger queriesInProgressCount = new AtomicInteger(0);
	private static GaianOpinionManager gom = null;
	private static INeighborProvider neighbourInfo = null;
	
	private static String myNodeID = null;
	private static String myGaianNodeDerbyDatabaseName = null;
	
	static {
		myNodeID = GaianDBConfig.getGaianNodeID();
		int portIndex = myNodeID.lastIndexOf(':');
		myGaianNodeDerbyDatabaseName = "gaiandb" + (-1 == portIndex ? "" : myNodeID.substring(portIndex+1));
	}

	private boolean isRoutingRestricted = true;
	private Map<String, String> neighbours = null;
	private Set<String> trustedNeighbours = null;
//	private String mostTrustedNeighbour = null;
	
	private static AtomicBoolean isInitialised = new AtomicBoolean(false);
	
	private static final String OPINIONS_FUNCTION = "getOpinionsDSM";
	
	@Override
	public boolean setLogicalTable(String logicalTableName, ResultSetMetaData logicalTableResultSetMetaData) {
		
		if ( isInitialised.compareAndSet(false, true) ) {
			
			// Register stored function used to get opinions in a query
			Connection c = null; Statement s = null;
			try {
				c = GaianDBConfig.getEmbeddedDerbyConnection();
				s = c.createStatement();
				
				System.out.println("Creating function " + OPINIONS_FUNCTION);
				
				if ( false == c.getMetaData().getProcedures(null, null, OPINIONS_FUNCTION).next() )
//					s.execute( 	"CREATE FUNCTION getOpinionsAsCSV() RETURNS VARCHAR(32672) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL" +
//								" EXTERNAL NAME 'com.ibm.gaiandb.plugins.dsm.OpinionsBasedRouterDSM.getOpinionsAsCSV'" );
					s.execute( 	"CREATE FUNCTION "+OPINIONS_FUNCTION+"() RETURNS TABLE(NODE VARCHAR(80), OPINION DOUBLE) PARAMETER STYLE DERBY_JDBC_RESULT_SET" +
								"  LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.plugins.dsm.OpinionsBasedRouterDSM."+OPINIONS_FUNCTION+"'" );
				
//				+ ";" // Example Table Function equivalent of a GaianTable VTI call against LT0 (with limitations)
//				+ "!DROP FUNCTION LT0;!CREATE FUNCTION LT0() RETURNS TABLE(LOCATION "+TSTR+", NUMBER INTEGER, MISC "+TSTR+", BF2_PLAYER INTEGER)"
//				+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.db2j.GaianTable.queryGaianTable'"
				
				else
					System.out.println("Already exists");
				
				System.out.println("Succesfully created function " + OPINIONS_FUNCTION);
				
			} catch ( SQLException e ) {
				System.out.println("Unable to register stored function " + OPINIONS_FUNCTION + ", cause: " + e);
			} finally { if ( null != c ) try { if ( null != s ) s.close(); c.close(); } catch( Exception e1 ) {} }			
		}
		
		String restrictedTablesProperty = getEntryFromPropertiesTable("DSM_ROUTING", "APPLICABLE_LOGICAL_TABLES", sensitiveLogicalTablesCSV);
		isRoutingRestricted = Arrays.asList( splitByCommas( restrictedTablesProperty ) ).contains( logicalTableName );
//		( null == restrictedTablesProperty ? sensitiveLogicalTables : Arrays.asList( splitByCommas( restrictedTablesProperty ) ) ).contains( logicalTableName );
		
		printInfo("Exiting setLogicalTable(" + logicalTableName + ", " + logicalTableResultSetMetaData +
				"), isRoutingRestrictedForThisTable: " + isRoutingRestricted);
		
//		if ( !isRoutingRestricted ) return true; // don't restrict routing for this logical table
		
		// If the LT *is* restricted, then we handle it in later methods
		return true;
	}

	private String forwardingNode = null;
	
	// We want to resolve trusted neighbours only once per query cycle, when needed.
	// For each query cycle, we don't know which of setForwardingNode() and setUserCredentials() will be called. Either or both might.
	// All we know is that setForwardingNode() would never be called after setUserCredentials()
	private boolean isTrustedNeighboursInfoStale; // used to only resolve the info once per query cycle
	
	@Override
	public boolean setForwardingNode(String nodeName) {
		if ( !isRoutingRestricted ) return true;
		printInfo("Entered setForwardingNode(" + nodeName + ")");
		
		forwardingNode = nodeName;
		
		isTrustedNeighboursInfoStale = true;
		resolveTrustedNeighbours(); // only resolves it once per query cycle
		
		// if forwardingNode is null then we're on the entry-point node
		boolean isForwardingNodeTrusted = null == forwardingNode || trustedNeighbours.contains(forwardingNode);
		
		if ( !isForwardingNodeTrusted && doForwardQueriesFromTrustedNeighboursOnly && doRestrictLocalDataAccessToQueriesComingFromTrustedNeighboursOnly ) {
			printInfo( "  => Forwarding node is NOT trusted - stopping local data access and query propagation");
			return false;
		}
		
		return true;
		
//		boolean doForwardQueriesFromTrustedNeighboursOnly = false;
//		
//		if ( doForwardQueriesFromTrustedNeighboursOnly ) {
//			// if forwardingNode is null then we're on the entry-point node
//			if ( null != forwardingNode && ! trustedNeighbours.contains(forwardingNode) ) {
//				printInfo("Forwarding node " + forwardingNode + " is NOT trusted (opinion too low). Query will NOT proceed to neighbours");
//				return false;
//			}
//			printInfo("Forwarding node " + forwardingNode + " is trusted (opinion high enough). Query WILL proceed to selected neighbours");	
//		}
	}
	
	@Override
	public boolean setUserCredentials(String credentialsStringBlock) {
		if ( !isRoutingRestricted ) return true;
		printInfo("Entered setUserCredentials(" + credentialsStringBlock + ")");
		
		// Initialise GaianOpinionManager and check neighbours and opinions status HERE, NOT in a constructor or in setLogicalTable().
		// This is because this method is called for every query execution, even when the query is already prepared and is being re-executed...
		resolveTrustedNeighbours();
		isTrustedNeighboursInfoStale = true;
		return true;
	}
	
	private void resolveTrustedNeighbours() {
		
		if ( false == isTrustedNeighboursInfoStale ) return;
		
		// Initialise GaianOpinionManager and check neighbours and opinions status HERE, NOT in a constructor or in setLogicalTable().
		// This is because this method is called for every query execution, even when the query is already prepared and is being re-executed...
		
		if ( null == gom ) {
			try {
				String dsmHome = System.getenv("DSM_HOME");
				if ( null == dsmHome ) throw new Exception("DSM_HOME environment variable is not set");
//				String rulesFile = new File( dsmHome ).getAbsolutePath() + "/samples/com/ibm/watson/dsm/samples/opinions/opinions.dsmr";
				String rulesFile = new File( "./lib/opinions.dsmr" ).getAbsolutePath();
				printInfo("Entered OpinionsBasedRouterDSM() - Using rules file: " + rulesFile  + ". File exists? " + new File(rulesFile).exists());
				neighbourInfo = new GaianNeighbourProvider();
				printInfo("Neighbours are : " + neighbourInfo.getNeighborInfo());
				gom = new GaianOpinionManager(GaianDBConfig.getGaianNodeID(), rulesFile, neighbourInfo);
				
				synchronized(gom) { gom.start(); } // Do this only once here

				printInfo("GaianOpinionManager has started");
				
			} catch ( Exception e ) {
				printInfo("Unable to instantiate or start GaianOpinionManager, cause: " + e);
				e.printStackTrace();
				queriesInProgressCount.set(0);
				isRoutingRestricted = false;
				return;
			}
		}

//		Starting/Stopping the gom takes too long (about 1 second) to be done for every query... - so we do it just once on initialisation
//		if ( 0 == queriesInProgressCount.getAndIncrement() ) synchronized(gom) { gom.start(); }
		
		trustedNeighbours = new HashSet<String>();
		
		Map<String, Double> opinions = null;
		synchronized( gom ) { opinions = gom.getOpinions(); }

		neighbours = neighbourInfo.getNeighborInfo();
		printInfo("Neighbours are : " + neighbours);
		printInfo("Opinions map is: " + opinions);
		
//		double bestOpinion = -2;
//		if ( null != opinions )
//			for ( String n : neighbours.keySet() ) {
//				Double op = opinions.get(n);
//				if ( null != op && op > bestOpinion ) {
//					bestOpinion = op;
//					mostTrustedNeighbour = n;
//				}
//			}
//		
//		if ( 0 > bestOpinion ) {
//			List<String> nodesHavingNoOpinionScore = new ArrayList<String>( neighbours.keySet() );
//			if ( null != opinions ) nodesHavingNoOpinionScore.removeAll( opinions.keySet() );
//			
//			if ( 0 < nodesHavingNoOpinionScore.size() ) {
//				printInfo("Choosing a mostTrustedNeighbour out of those having no opinion score: " + nodesHavingNoOpinionScore);				
//				mostTrustedNeighbour = nodesHavingNoOpinionScore.get(0);
//			}
//		}
//
//		printInfo("Best opinion score is: " + bestOpinion + "; Most trusted node is: " + (null==mostTrustedNeighbour?"none to choose":mostTrustedNeighbour));
		
		if ( null != opinions ) {
			String minOpinionString = getEntryFromPropertiesTable("DSM_ROUTING", "MIN_ACCEPTED_OPINION_SCORE", "0.0");
			double minOpinion = null == minOpinionString ? 0 : Double.parseDouble( minOpinionString );
			
			for ( String n : neighbours.keySet() ) {
				Double op = opinions.get(n);
				if ( null == op ) op = 0.0;
//				TODO: Add configurable policy on trust threshold for neighbour
				if ( op >= minOpinion ) trustedNeighbours.add(n);
			}
		}
		
		isTrustedNeighboursInfoStale = false;
	}
	
	@Override
	public boolean setQueriedColumns(int[] queriedColumns) {
		if ( !isRoutingRestricted ) return true;
		printInfo("Entered setQueriedColumns(" + intArrayAsString(queriedColumns) + ")");
		return true;
	}
	
	/**
	 * This is where we make routing decisions based on:
	 * 		1. Logical table queried
	 * 		2. forwarding node
	 * 		3. other neighbours to forward to...
	 */
	@Override
	public int nextQueriedDataSource(String dataSource, int[] columnMappings) {
		if ( !isRoutingRestricted ) return -1;
		
		printInfo("Entered nextQueriedDataSource(" + dataSource + ", " + intArrayAsString(columnMappings) + ")");
		
		// NOTE - this method is not needed if ( isSimpleQueryRestrictionToTrustedForwardingNodes == true ) and if the fix is implemented
		// in GaianDB whereby setForwardingNode() is called for every query re-execution of initialised statements.
		// However - it does provide some potentially useful logging anyway.
		
		// if forwardingNode is null then we're on the entry-point node
		boolean isForwardingNodeTrusted = null == forwardingNode || trustedNeighbours.contains(forwardingNode);
		
		if ( !isSimpleQueryRestrictionToTrustedForwardingNodes )
			printInfo("Forwarding node " + forwardingNode + ( isForwardingNodeTrusted ?
					" is trusted (opinion high enough). Query WILL reach local data source" : //proceed to neighbours");
					" is NOT trusted (opinion too low). Query will NOT reach local data sources" //proceed to neighbours");
			));
		
		Map<String, Double> opinions = null;
		synchronized( gom ) { opinions = gom.getOpinions(); }
		
		String nodeID = null;
		Double opinion = null;
		
		if ( null != neighbours )
			for ( String n : neighbours.keySet() ) {
				String ipAddressOfNode = neighbours.get(n);
				int portSuffixIndex = n.indexOf(':');
				if ( -1 != dataSource.indexOf( ipAddressOfNode ) && ( -1 == portSuffixIndex || -1 != dataSource.indexOf(n.substring(portSuffixIndex)) ) ) {
					nodeID = n;
					opinion = opinions.get(nodeID);
//						printInfo("Identified source to be nodeID: " + nodeID + ", opinion score is: " + (null==opinion ? "null (i.e. 0.0)" : opinion) ); // printed later
					break;
				}
			}
		
		if ( null == nodeID ) {
			if ( !isSimpleQueryRestrictionToTrustedForwardingNodes && doRestrictLocalDataAccessToQueriesComingFromTrustedNeighboursOnly ) {
				if ( isForwardingNodeTrusted )
					printInfo("  => Data source is a LOCAL data source. Forwarding node is trusted so query can access local data");
				else {
					printInfo("  => Data source is a LOCAL data source. Forwarding node is NOT trusted - so query will not access the data source");
					return 0;
				}
			}
			
		} else {
			
			if ( doForwardQueriesFromTrustedNeighboursOnly && !isForwardingNodeTrusted ) {
				printInfo( "  => Data source is nodeID: " + nodeID + ". However Forwarding node is NOT trusted, so we don't forward the query to others");
				return 0;
			}
			
			if ( doForwardQueriesToTrustedNeighboursOnly ) {
				String dsInfo = "  => Data source is nodeID: " + nodeID + ", with opinion score: " + opinion + (null==opinion ? " (i.e. 0.0)" : "");
				
				if ( ! trustedNeighbours.contains( nodeID ) ) {
					printInfo( dsInfo + " which is deemed too low - routing WILL NOT proceed to it");
					return 0;
				}				
				printInfo(dsInfo + " which is high enough - routing proceeding to it");
			}
		}
		
		return -1;
	}

	@Override
	public boolean filterRow(DataValueDescriptor[] row) {
		if ( !isRoutingRestricted ) return true;
//		printInfo("Entered filterRow(" + Arrays.asList(row) + ")");
		return true;
	}
	
	@Override
	public void close() {
		if ( !isRoutingRestricted ) return;
		
		printInfo("Entered close()\n");
		if ( null != trustedNeighbours ) trustedNeighbours.clear();
		if ( null != neighbours ) neighbours.clear();
		trustedNeighbours = null; neighbours = null;
		
//		 Stopping the gom takes too long (about 1 second) to be done after every query...
//		try { if ( 0 == queriesInProgressCount.decrementAndGet() ) synchronized( gom ) { gom.stop(); } }
//		catch (DSMException e) { printInfo("Unable to stop GaianOpinionManager, cause: " + e); e.printStackTrace(); }
	}
	
	private static String intArrayAsString(int[] a) {
		if ( null==a ) return null; int len = a.length;
		String pcs = new String( 0<len ? "[" + a[0] : "[" );
		for (int i=1; i<len; i++) pcs += ", " + a[i]; pcs += "]";
		return pcs;
	}
	
	private static String[] splitByCommas( String list ) {
    	return splitByTrimmedDelimiter( list, ',' );
    }
    
	private static String[] splitByTrimmedDelimiter( String list, char delimiter ) {
		if ( null == list || 0 == list.length() ) return new String[0];
		return list.trim().split("[\\s]*" + delimiter + "[\\s]*");
	}
	
	private String getEntryFromPropertiesTable( String table, String property, String defaultValue ) {
		
		try {
			Connection c = getEmbeddedConnectionToGaianDB();
			Statement stmt = c.createStatement();
			
			if ( false == Util.isExistsDerbyTable( c, "GAIANDB", table ) ) {
				// table does not exist - create it and add entry for null property as it is not defined
				stmt.execute("CREATE TABLE " + table + " ( property varchar(32672), value varchar(32672) )");
				stmt.execute("INSERT INTO " + table + " values ( '" + property + "', '" + defaultValue + "' )");
				return defaultValue;
			}
			
			ResultSet rs = stmt.executeQuery("SELECT value FROM " + table + " WHERE property = '" + property + "'");
			if ( !rs.next() ) {
				stmt.execute("INSERT INTO " + table + " values ( '" + property + "', '" + defaultValue + "' )");
				return defaultValue;
			}
			
			return rs.getString(1);
			
		} catch (Exception e) {
			printInfo("Unable to getEntryFromPropertiesTable(" + Arrays.asList(table, property) + "), cause: " + e);
			e.printStackTrace();
			if ( null != embeddedConnection ) try { embeddedConnection.close(); } catch (SQLException e2) {}
		}

		return defaultValue;
	}
	
	private static Connection embeddedConnection = null;
	private static Connection getEmbeddedConnectionToGaianDB() throws Exception {
		if ( null == embeddedConnection || embeddedConnection.isClosed() ) {
			if ( null == embeddedConnection ) Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
			embeddedConnection = DriverManager.getConnection("jdbc:derby:" + myGaianNodeDerbyDatabaseName, "gaiandb", "passw0rd");
		}
		return embeddedConnection;
	}
	
	private static void printInfo( String s ) {
		System.out.println( sdf.format( new Date(System.currentTimeMillis()) ) + ' ' + s );
	}
	
	
	
//	// I've written a stored function for this that can be accessed from the GRAPH_SQL...
//	// When data sources are protected individually, it is too late for accepting queries from other forwarding nodes...
//	// We would need to record *which* data sources have not been queried yet...
	public static String getOpinionsAsCSV() {
		Map<String, Double> opinions = null;
		synchronized( gom ) { opinions = gom.getOpinions(); }
		
		System.out.println("Got my opinions of my neighbours: " + opinions);
		if ( null == opinions || 0 == opinions.size() ) return null;
		StringBuffer sb = new StringBuffer();
		for ( String node : opinions.keySet() )
			sb.append(node+' '+opinions.get(node)+',');
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	public static ResultSet getOpinionsDSM() throws SQLException {
		Map<String, Double> opinions = null;
		synchronized( gom ) { opinions = gom.getOpinions(); }
		
		System.out.println("Got my opinions of my neighbours: " + opinions);
		
		// Row schema: NODE VARCHAR(80), OPINION DOUBLE
		List<String[]> rows = new ArrayList<String[]>();
		
		if ( null != opinions && 0 < opinions.size() )
			for ( String node : opinions.keySet() )
				rows.add(new String[] { node, opinions.get(node)+"" });
		
		return 0 == rows.size() ? null : getDataAsResultSet( rows, new String[] { "NODE", "OPINION" }, new boolean[] { true, false } );
//		opinionsOfNeighbours[0].getStatement().getConnection().close();
	}
	
	private static ResultSet getDataAsResultSet( List<String[]> rows, String[] colAliases, boolean[] isQuoted ) throws SQLException {
		StringBuffer sb = new StringBuffer();
		
		for ( int i=0; i<rows.size(); i++ ) {
			sb.append( ( 0<i ? " UNION ALL " : "" ) + "SELECT " );
			String[] row = rows.get(i);
			for ( int j=0; j<row.length; j++ )
				sb.append( ( 0<j ? ", ": "" ) +
					(isQuoted[j]?"'":"") + row[j] + (isQuoted[j]?"' \"":" \"") + colAliases[j] + "\"" );
			sb.append( " FROM SYSIBM.SYSDUMMY1");
		}
		
		System.out.println("executing sql: " + sb);
		return GaianDBProcedureUtils.getResultSetFromQueryAgainstDefaultConnection( sb.toString() );
	}
}

/*

SQL to retrieve opinions:
-------------------------

select * from NEW com.ibm.db2j.GaianQuery(
		'select * from TABLE ( getopinionsdsm() ) T', 'with_provenance') Q
		
		
Display the opinions onlongside the edges returned by the explain query:
------------------------------------------------------------------------

SELECT DISTINCT gdbx_from_node src, gdbx_to_node tgt, Q1.opinion src_opinion, Q2.opinion tgt_opinion
FROM
	NEW com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T
LEFT OUTER JOIN
	NEW com.ibm.db2j.GaianQuery(
		'select * from TABLE ( getopinionsdsm() ) T', 'with_provenance') Q1
ON gdbx_from_node = Q1.gdb_node AND gdbx_to_node = Q1.node
LEFT OUTER JOIN
	NEW com.ibm.db2j.GaianQuery(
		'select * from TABLE ( getopinionsdsm() ) T', 'with_provenance') Q2
ON gdbx_from_node = Q2.node AND gdbx_to_node = Q2.gdb_node
WHERE GDBX_DEPTH > 0


Optimised equivalent:
---------------------

SELECT DISTINCT gdbx_from_node n1, gdbx_to_node n2,
	CASE WHEN gdbx_from_node = Q.gdb_node THEN opinion ELSE NULL END n1_o_n2, 
	CASE WHEN gdbx_from_node = node THEN opinion ELSE NULL END n2_o_n1
FROM
	NEW com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T
LEFT OUTER JOIN
	NEW com.ibm.db2j.GaianQuery(
		'select * from TABLE ( getopinionsdsm() ) T', 'with_provenance') Q
ON ( gdbx_from_node = Q.gdb_node AND gdbx_to_node = node ) OR ( gdbx_from_node = node AND gdbx_to_node = Q.gdb_node )
WHERE GDBX_DEPTH > 0


Optimised equivalent - with minimal set of unique bi-directional links:
-----------------------------------------------------------------------

SELECT DISTINCT
	CASE WHEN gdbx_from_node > gdbx_to_node THEN gdbx_to_node ELSE gdbx_from_node END n1,
	CASE WHEN gdbx_from_node > gdbx_to_node THEN gdbx_from_node ELSE gdbx_to_node END n2,
	CASE WHEN ( gdbx_from_node < gdbx_to_node AND gdbx_from_node = Q.gdb_node ) OR ( gdbx_from_node > gdbx_to_node AND gdbx_to_node = Q.gdb_node ) THEN opinion ELSE NULL END n1_o_n2,
	CASE WHEN ( gdbx_from_node > gdbx_to_node AND gdbx_from_node = Q.gdb_node ) OR ( gdbx_from_node < gdbx_to_node AND gdbx_to_node = Q.gdb_node ) THEN opinion ELSE NULL END n2_o_n1
FROM
	NEW com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T
LEFT OUTER JOIN
	NEW com.ibm.db2j.GaianQuery(
		'select * from TABLE ( getopinionsdsm() ) T', 'with_provenance') Q
ON ( gdbx_from_node = Q.gdb_node AND gdbx_to_node = node ) OR ( gdbx_from_node = node AND gdbx_to_node = Q.gdb_node )
WHERE GDBX_DEPTH > 0

*/

