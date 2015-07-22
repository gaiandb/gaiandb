/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.Util;

public class PolicyRowFilter implements SQLResultFilter {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final String MISSION_TASKS_LOGICAL_TABLE = "LTLBB"; //"LT0"; //"MISSION_TASKS";
	private static final int MISSION_TASKS_DECOY_TARGET_COLUMN = 2;
	
	// Mapping of:: Data source -> HashSet of authorized users
	private static ConcurrentMap<String, Set<String>> authorizedDataSourceUsers = new ConcurrentHashMap<String, Set<String>>();
	
	static {
		String[] ds = { "jdbc:derby:gaiandb;create=true::MYTABLE", "./datafile.dat" }; // example data sources
		String[] u = { "Alfred", "Bob", "Chris" }; // example users
		
		// Alfred can only access MYTABLE and Chris can only access datafile.dat
		// Bob can access both data sources...
		
		authorizedDataSourceUsers.put(ds[0], new HashSet<String>( Arrays.asList( new String[] { u[0], u[1] } ) ) );
		authorizedDataSourceUsers.put(ds[1], new HashSet<String>( Arrays.asList( new String[] { u[1], u[2] } ) ) );
		
		// ...
		// To query the configured data sources from GaianDB, use the API procedures:
		// call listds(): Returns DataSource handle (DSHANDLE) and 
		// Connection ID (DSCID) for each node - will be implemented in the new version of GaianDB... -
		// call listrdbc(): Returns Connection IDs for each node and associated User (CUSR).
	}
	
	private String logicalTable = null;
	private ResultSetMetaData logicalTableRSMD = null;
	private int logicalTableColumnCount = -1;
	private int rowCount = 0;
	
	private int[] queriedColumns = null;
	
	private String forwardingNode;
	
	public boolean setForwardingNode( String node ) {
		forwardingNode = node;
		System.out.println("Forwarding node: " + forwardingNode);
		return true;
	}
	
	public int nextQueriedDataSource(String dataSource, int[] columnMappings) {
		// TODO Auto-generated method stub
		return -1; // Allow an unlimited number of rows to be extracted for this data source
	}


	public boolean setUserCredentials(String credentialsData) {
		// TODO Auto-generated method stub
		return true;
	}

//	private boolean setAuthenticatedUserCredentials(String[] userFields) { // to be called from setUserCredentials() once byte[] is decrypted
//		// TODO Auto-generated method stub
//		return true;	
//	}
		
	public boolean setLogicalTable(String logicalTableName, ResultSetMetaData logicalTableResultSetMetaData) {
		this.logicalTable = logicalTableName;
		this.logicalTableRSMD = logicalTableResultSetMetaData;
		
		System.out.println( "Preparing PolicyRowFilter for Logical Table: " + 
				logicalTable + ", columns: " + logicalTableRSMD.toString() );
		
		try {
			logicalTableColumnCount = logicalTableRSMD.getColumnCount();
		} catch (SQLException e) {
			System.out.println("Could not get logical table column count, cause: " + e);
			e.printStackTrace();
		}
		
		rowCount = 0;
		return true;
	}
	
	public boolean setQueriedColumns(int[] queriedColumns) {
		this.queriedColumns = queriedColumns;
		System.out.println("Queried columns: " + Util.intArrayAsString(this.queriedColumns));
		return true;
	}
	
//	public int nextQueriedDataSource(String dataSource, int[] columnMappings, String[] userFields) {
//		// Check if the user is authorized to query this back-end data source.
//		if ( authorizedDataSourceUsers.containsKey(dataSource) )
//		if ( ((HashSet<String>) authorizedDataSourceUsers.get(dataSource)).contains(userFields[0]) )
//			return -1;
//		
//		return 0;
//	}

	protected void applyPolicyHandlerAccessTimeDelay() {
		try { 
			Thread.sleep(10); // 10ms time delay to access the policy service for record filtering
		}
		catch (Exception e) { e.printStackTrace(); }
	}
	
	public boolean filterRow( DataValueDescriptor[] row ) {
		
		applyPolicyHandlerAccessTimeDelay();
//		System.out.println("row: " + Arrays.asList(row));
		return applyRowFilter(row);
	}
	
	protected boolean applyRowFilter( DataValueDescriptor[] row ) {
		
		rowCount++;
		
//		System.out.println("Apply row Filter - rowCount = " + rowCount);
		
		try {			
			if ( row.length < logicalTableColumnCount ) {
				System.out.println("Invalid Fetched Row: Expecting " + logicalTableColumnCount + " columns, not " + row.length);
//				System.out.println("-> " + Arrays.asList(row));
				return false;
			}
			
			if ( 0 == rowCount % 3 ) return false; // Reject every 3rd row
//			if ( row[0].toString().compareTo("II") > 0 ) return false; // Reject rows whose first column string value is > 'I'
			
			if ( MISSION_TASKS_LOGICAL_TABLE.equals(logicalTable) ) {
				
				DataValueDescriptor column = row[MISSION_TASKS_DECOY_TARGET_COLUMN-1];
				column.setValue("Commander Restricted Information");
				if ( 1 == rowCount )
				System.out.println("Successfully applied restriction policy for Decoy Target column on 1st result row");
			}
			
		} catch ( Exception e ) {
			System.out.println("Could not apply restriction policy (invalidating row " + 
					rowCount + "), cause: " + e);
			return false;
		}
		
		return true;
	}
	
	public void close() {
		rowCount = 0;
	}
}
