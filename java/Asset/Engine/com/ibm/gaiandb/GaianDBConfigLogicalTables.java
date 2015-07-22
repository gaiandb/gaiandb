/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.util.Set;

/**
 * A Wrapper for a parsed set of properties from config, listing all defined logical tables and their
 * indirectly referenced data sources and JDBC connection ids.
 * 
 * @author DavidVyvyan
 */
public class GaianDBConfigLogicalTables {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private Set<String> logicalTables = null;
	private Set<String> allReferencedVTIDataSources = null;
	private Set<String> allReferencedJDBCConnections = null;
	
	public GaianDBConfigLogicalTables(Set<String> logicalTables, 
			Set<String> allReferencedVTIDataSources, Set<String> allReferencedJDBCConnections) {
		super();
		this.logicalTables = logicalTables;
		this.allReferencedVTIDataSources = allReferencedVTIDataSources;
		this.allReferencedJDBCConnections = allReferencedJDBCConnections;
	}
	
	public Set<String> getAllReferencedVTIDataSources() {
		return allReferencedVTIDataSources;
	}
	public Set<String> getAllReferencedJDBCConnections() {
		return allReferencedJDBCConnections;
	}
	public Set<String> getLogicalTables() {
		return logicalTables;
	}
}
