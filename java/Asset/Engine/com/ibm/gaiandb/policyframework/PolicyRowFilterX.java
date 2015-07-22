/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.Util;

public class PolicyRowFilterX extends SQLResultFilterX {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	private PolicyRowFilter prf = null;
	public PolicyRowFilterX() { super(); prf = new PolicyRowFilter(); }

	public DataValueDescriptor[][] filterRowsBatch(String dataSourceID, DataValueDescriptor[][] rows) {
		
		System.out.println("Entered filterRowsBatch()");
		prf.applyPolicyHandlerAccessTimeDelay();
		
		ArrayList<DataValueDescriptor[]> filteredRows = new ArrayList<DataValueDescriptor[]>();
		
		for ( DataValueDescriptor[] row : rows )
			if ( prf.applyRowFilter(row) ) filteredRows.add(row);
		
		return (DataValueDescriptor[][]) filteredRows.toArray(new DataValueDescriptor[0][]);		
	}

	public int nextQueriedDataSource(String dataSourceID, String dataSourceDescription, int[] columnMappings) {

		System.out.println("Entered nextQueriedDataSource(), args: " + 
				Arrays.asList(dataSourceID, dataSourceDescription, Util.intArrayAsString(columnMappings)));
		return nextQueriedDataSource(dataSourceDescription, columnMappings);
	}

	public int setDataSourceWrapper(String wrapperID) {
		System.out.println("Entered setDataSourceWrapper(), args: " + wrapperID);
		return Integer.MAX_VALUE;
	}

	@Override
	protected Object executeOperationImpl(String opID, Object... args) {
		System.out.println("Entered executeOperation(), opID: " + opID + ", args: " + (null == args ? null : Arrays.asList(args)) );
		return null;
	}

//	@Override
//	protected int[] getSupportedOperationsImpl(int opID) {
//		// TODO Auto-generated method stub
//		return null;
//	}

	public void close() {}
	public boolean filterRow(DataValueDescriptor[] row) { return prf.filterRow(row); }
	public int nextQueriedDataSource(String dataSource, int[] columnMappings) { return prf.nextQueriedDataSource(dataSource, columnMappings); }
	public boolean setForwardingNode(String nodeName) { return prf.setForwardingNode(nodeName); }
	public boolean setLogicalTable(String logicalTableName, ResultSetMetaData logicalTableResultSetMetaData) {
		return prf.setLogicalTable(logicalTableName, logicalTableResultSetMetaData); }
	public boolean setQueriedColumns(int[] queriedColumns) { return prf.setQueriedColumns(queriedColumns); }
	public boolean setUserCredentials(String credentialsStringBlock) { return prf.setUserCredentials(credentialsStringBlock); }
}
