/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml.schema;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AccessLogger {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public AccessLogger() {
		super();
	}

	public void logAccess(String qcString) {
		
		System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " ******* Log Access Event: " + qcString);
		
	}
	
	public void logAccess(QueryContext qc) {
		
		System.out.println(sdf.format(new Date(System.currentTimeMillis())) + 
				" ******* Log Event: Access to logical table: " + qc.getLogicalTable() +
				" by user " + qc.getRequestor() + ", affiliation " + qc.getAffiliation() + ", clearance " + qc.getSecurityClearance());
		
	}
	
	public void logAccess(QueryContext qc, DataSource ds) {
		
		System.out.println(sdf.format(new Date(System.currentTimeMillis())) + 
				" ******* Log Event: Access to logical table: " + qc.getLogicalTable() + ", data source: " + ds.getName() +
				" by user " + qc.getRequestor() + ", affiliation " + qc.getAffiliation() + ", clearance " + qc.getSecurityClearance());
		
	}
	
	public void logAccess() {
		System.out.println("****************************************   Log Event!!!!!!!!!!");
	}
}
