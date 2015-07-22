/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import java.sql.ResultSetMetaData;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * 
 * @author DavidVyvyan
 *
 */

public interface SQLResultFilter {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	/**
	 * Set the forwarding
	 * 
	 * @param nodeName
	 * @return true if the query may be executed when coming form this node, false otherwise
	 */
	public boolean setForwardingNode( String nodeName );

	/**
	 * This sets the user field attributes for a user. Currently these fields are: Username, Affiliation and Clearance
	 * This should be made more generic in future.
	 * 
	 * @param userFields
	 * @return true if the user is authorised, false otherwise
	 */
//	public boolean setAuthenticatedUserCredentials( String[] userFields ); // no longer supported
	public boolean setUserCredentials( String credentialsStringBlock );
	
	/**
	 * Sets the logical table name and its column definitions for the next query to be run.
	 * The columns definitions are accessible via a JDBC ResultSetMetaData ojbect.
	 * 
	 * The purpose of this method is to inform the plugin of what logical table is being queried and of its metadata
	 * so that it can decide whether to progress with the query (else return false), and make decisions on behaviour in later
	 * stages of the query lifecycle...
	 * 
	 * @param logicalTableName This can be logical table name or a sub-query expression (if invoked via GaianQuery())
	 * @param logicalTableResultSetMetaData
	 */
	public boolean setLogicalTable( String logicalTableName, ResultSetMetaData logicalTableResultSetMetaData );
	
	/**
	 * Sets the list of logical column ids that are about to queried in the next query.
	 * 
	 * @param queriedColumns: Columns ids, numbered from 1.
	 */
	public boolean setQueriedColumns( int[] queriedColumns );
	
	/**
	 * Sets the next federated data source of this logical table which is to be queried.
	 * If the filter wishes to refuse that this data source be queried (e.g. based on user privileges) it may return 0, 
	 * in which case it will be skipped.
	 * 
	 * The dataSource argument may idenfity a filename (e.g. './datafile.dat') or an RDBMS table, relative to a database 
	 * connection URL (e.g. 'jdbc:derby:gaiandb6415;create=true::MYTABLE').
	 * 
	 * The columnMappings argument describes which logical columns map to which physical ones for this data source, e.g.
	 * If columMappings = { 2, 0, 5 }, then it means that the first 3 logical table's columns target columns 3, 1 and 6 in 
	 * the dataSource which is currently being processed. Notice the column ids are numbered from 0 in the array, but from 1
	 * externally with JDBC.
	 * 
	 * Typically, the purpose of this method is to inform the policy plugin of the column mappings so that it can later
	 * make a decision as to how it may want to filter records to be returned...
	 * 
	 * @param dataSource: String identifying the physical data source.
	 * @param columnMappings: Logical to Physical column mappings for this data source.
	 * @param user: User ID associated with this data source.
	 * @return the max number of rows that may be returned to this user for this data source, or -1 if unlimited
	 */
	public int nextQueriedDataSource( String dataSource, int[] columnMappings );
	
	/**
	 * Applies a transformation function to the columns in the row as required by the filter.
	 * A row template is passed in as an array of generic SQL columns in Derby DataValueDescriptor column wrappers.
	 * The array size and indices match the whole logical table definition, but only the queried columns will be populated.
	 * These are given when 'setQueriedColumns' is called. 
	 * 
	 * The corresponding physical table's columns can be derived using its columnMappings, which are 0-based, e.g.
	 * If columMappings = { 2, 0, 5 }, then it means that the first 3 logical table's columns target columns 3, 1 and 6 in 
	 * the dataSource which is currently being processed. Notice the column ids are numbered from 0 in the array, but from 1
	 * externally with JDBC.
	 * 
	 * Note that the only way to correlate records being extracted with the data source from which they came would be to
	 * have the GaianDB provenance columns available in the set of extracted rows. This implies that the query would itself
	 * be including them (using GaianTable options 'with_provenance' or 'explain', or logical table view suffix '_P' or '_X' or '_XF').
	 * The extended interface SQLResultFilterX method filterRowsBatch() addresses this limitation by including a dataSourceID argument. 
	 * 
	 * @param row: An array holding all logical table columns of which a subset only will be populated.
	 * @return false if the row is not to be returned, true if it should be returned (with filters applied on the columns).
	 */
	public boolean filterRow( DataValueDescriptor[] row );
	
	/**
	 * Release in-memory resources
	 */
	public void close();
}
