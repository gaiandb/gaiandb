/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import java.sql.ResultSetMetaData;

/**
 * This interface defines the methods that will be called by GaianDB to transform the incoming SQL or the SQL propagated to another node
 * or the SQL executed against a physical data source federated by the local node.
 * For each incoming query, a query ID is generated which will be the same for every resulting propagated query or query against
 * a physical data source.
 * 
 * This interface only currently makes it possible to reduce the number of queried cols or change/remove/add predicates to the qualifiers.
 * 
 * A development on this in future would be to make it possible to transform cols in the projection using SQL function calls.
 * 
 * @author DavidVyvyan
 */

public interface SQLQueryFilter {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	// Note: originalSQL encompasses more than the query. It may describe a join on multiple logical tables including the one here.
	public boolean applyIncomingSQLFilter( String queryID, String logicalTable, ResultSetMetaData logicalTableMetaData, String originalSQL, SQLQueryElements queryElmts );
	
	public boolean applyPropagatedSQLFilter( String queryID, String nodeID, SQLQueryElements queryElmts );
	
	// Note there is no columnsMapping structure to know which of the columns in the physical data source are actually being queried if
	// a mapping is specified in the GaianDb config for the data source.. this can be added as a future extension.
	public boolean applyDataSourceSQLFilter( String queryID, String dataSourceID, SQLQueryElements queryElmts );
}
