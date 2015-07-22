/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import org.apache.derby.iapi.store.access.Qualifier;

public class SQLQueryElements {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private Qualifier[][] qualifiers;	// Modifiable as required by filter
	private int[] projectedColumns;		// Modifiable as required by filter
	
	public SQLQueryElements(Qualifier[][] qualifiers, int[] projectedColumns) {
		super();
		this.qualifiers = qualifiers;
		this.projectedColumns = projectedColumns;
	}
		
	public int[] getProjectedColumns() {
		return projectedColumns;
	}
	public Qualifier[][] getQualifiers() {
		return qualifiers;
	}

	public void setProjectedColumns(int[] projectedColumns) {
		this.projectedColumns = projectedColumns;
	}

	public void setQualifiers(Qualifier[][] qualifiers) {
		this.qualifiers = qualifiers;
	}
}
