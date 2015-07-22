/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml.schema;

/**
 * The context of the query for policies to base their decisions on
 * 
 * A placeholder for now
 * 
 * @author pzerfos@us.ibm.com
 *
 */
public class QueryContext {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	String logicalTable = null;
	String forwardingNode = null;
	String requestor = null;
	String affiliation = null;
	String securityClearance = null;
	
	public QueryContext() {
		this("", "", "", "", "");
	}
	
	public String toString() {
		return "Logical Table: " + check(logicalTable) + ", user: " + check(requestor) + 
		", affiliation: " + check(affiliation) + ", clearance: " + check(securityClearance);
	}
	
	private String check( String a ) {
		return null == a || 0 == a.length() ? "<undefined>" : a;
	}
	
	public QueryContext(String user, String affiliation, String securityClearance, String logicalTable, String forwardingNode) {
		this.requestor = user;
		this.affiliation = affiliation;
		this.securityClearance = securityClearance;
		this.logicalTable = logicalTable;
		this.forwardingNode = forwardingNode;
	}
	
//	public QueryContext(String user) {
//		this(user, "", "", "");
//	}
	
	public String getRequestor() {	
		return (requestor == null ? "" : requestor);
	}
	
	public void setRequestor(String user) {
		this.requestor = user;
	}
	
	public String getAffiliation() {
		return (affiliation == null ? "" : affiliation);
	}
	
	public void setAffiliation(String affil) {
		this.affiliation = affil;
	}

	public String getSecurityClearance() {
		return securityClearance;
	}

	public void setSecurityClearance(String securityClearance) {
		this.securityClearance = securityClearance;
	}

	public String getForwardingNode() {
		return forwardingNode;
	}

	public void setForwardingNode(String forwardingNode) {
		this.forwardingNode = forwardingNode;
	}

	public String getLogicalTable() {
		return logicalTable;
	}

	public void setLogicalTable(String logicalTable) {
		this.logicalTable = logicalTable;
	}

}
