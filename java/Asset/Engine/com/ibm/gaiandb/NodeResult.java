/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

/**
 * Class used as wrapper to hold node results for a GaianResult. A node result is the data assocoiated with
 * the result of the incoming SQL query being executed against one of the child VTIWrapper nodes.
 * It may also just be a wrapper holding an int indicating the number of hanging vtis that have recently been 
 * rooted out for an associated GaianResult. The association is done via the executingVTIs Set and nodeResults queue
 * which are passed as handles from the GaianResult to the DatabaseConnectionsChecker.
 *  
 * @author DavidVyvyan
 */
public final class NodeResult implements Comparable<NodeResult> {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	final private GaianChildVTI nodeRows;
	final private VTIWrapper originatingVTI;
	final private long execTime;
	
	final private int numberOfHangingVTIs;
	
	NodeResult( int numberOfHangingVTIs ) {
		nodeRows = null;
		originatingVTI = null;
		execTime = -1;
		this.numberOfHangingVTIs = numberOfHangingVTIs;
	}
	
	NodeResult( GaianChildVTI nodeRows, VTIWrapper originatingVTI, long execTime ) {
		this.nodeRows = nodeRows;
		this.originatingVTI = originatingVTI;
		this.execTime = execTime;
		numberOfHangingVTIs = 0;
	}
	
	long getExecTime() {
		return execTime;
	}
	VTIWrapper getOriginatingVTI() {
		return originatingVTI;
	}
	GaianChildVTI getNodeRows() {
		return nodeRows;
	}
	int getNumberOfHangingVTIs() {
		return numberOfHangingVTIs;
	}

	/**
	 * Order by execution time: A NodeResult is less than another if it is the slowest of the 2, i.e. if its exec time is greatest.
	 */
	public int compareTo(NodeResult o) {
		long otherExecTime = o.getExecTime();
		return execTime > otherExecTime ? -1 : execTime < otherExecTime ? 1 : 0;
	}
}
