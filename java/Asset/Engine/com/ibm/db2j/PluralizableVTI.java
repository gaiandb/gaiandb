/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.util.Set;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * When GaianDB loads a VTI which implements this interface, it will know to lookup its "pluralized instances", so as to run as many threads as
 * required at execution time, each of which will pass a different 'dsInstanceID' String from this array to a new invocation of the VTI (or to a pooled VTI).
 * When a VTI implements 'PluralizableVTI', the dsInstanceID is always passed as first argument to the setArgs() method of the VTI (even if it is null).
 * 
 * When executing a query against the logical table which federates your VTI, where-clause predicates against the inbuilt GDB_LEAF constant data source 
 * provenance column (or against a user-defined constant end-point column) will be applied *BEFORE* the threads are spawned, such that you may end up
 * with fewer invocations of your VTI, based on which of your pluralizable instances were filtered out by the predicate conditions.
 * 
 * To define your own constant end-point columns and associate them with logical table ones, you need to supplement the 'PLURALIZED' config option 
 * with a "WITH ENDPOINT CONSTANTS <list of logical table column ids>" clause and implement method "PluralizableVTI.getEndpointConstants()"
 * 
 * Note:
 * The PluralizableVTI interface can be useful even if a VTI only accesses data from a single end-point, in that it allows the VTI to specify an instance ID
 * and also optionally mark some of its columns as constants (e.g meta-data columns) - which can all benefit from early predicate filtering.
 * 
 * @author DavidVyvyan
 */
public interface PluralizableVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";
	
	/**
	 * Gets all available end-points for this VTI instance.
	 * This method is called for every SQL query referencing a logical table which federates this VTI.
	 * The VTI will subsequently be executed for every instance ID (via setArgs(), setExtractConditions() and row fetching with nextRow()).
	 * 
	 * @return List of all end-point IDs for this VTI instance.
	 */
	public Set<String> getEndpointIDs();

	/**
	 * Gets constant column values in a DataValueDescriptor[] for an end-point exposed by this VTI instance.
	 * This method is called for every SQL query referencing a logical table which federates this VTI, and for all end-points exposed by the VTI instance.
	 * The constant values will be tested against where-clause predicates to resolve targeted end-points and create threads only for them.
	 * 
	 * The array positions in the DataValueDescriptor[] structure map to logical table column positions.
	 * The mapping is specified in the gaiandb config file, against the 'PLURALIZED' keyword option for the data source wrapper.
	 * The syntax is: "PLURALIZED [WITH ENDPOINT CONSTANTS <list of logical table column ids>]"
	 * e.g, for wrapper LT1_DS1 (under logical table LT1) having constant columns mapping to index positions 5 and 3 in the logical table definition (LT1_DEF):
	 * LT1_DS1_OPTIONS=PLURALIZED WITH ENDPOINT CONSTANTS C5 C3
	 * In this example, getEndpointConstants() will return a DataValueDescriptor[] of size 2, containing column values which map to logical table columns 5 and 3.
	 * 
	 * @return An end-points' constant column values - wrapped in type instances of Derby's DataValueDescriptor interface.
	 */
	public DataValueDescriptor[] getEndpointConstants( String endpointID );
}
