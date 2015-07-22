/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
 
package com.ibm.gaiandb;

/**
 * GaianSubResult is a class used to hold parameters specific to sub queries of  
 * a GaianResult, particularly the Name of the query which encapsulates the target data source.
 * This class passes the necessary parameters to the subquery.
 * 
 * @author      Paul Stone
 */
public class GaianSubResult implements Runnable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private String resultName;  // The 'Name' of this subquery, used to identify the target vti.
	private GaianResult parent; // Reference to the Gaianresult which spawned this sub-result.
	
	public GaianSubResult (GaianResult parentResult, String name) {
		resultName=name;	
		parent=parentResult;
	}
	
	public String getExecutorName() {
		return resultName;
	}

	public void run() {
		try {
	        // Start the subquery, passing in the name.
			parent.run(resultName);
			
		} catch ( Error er ) { // Only catch this one - other Throwables may be recoverable?
//			System.err.println("OUT OF MEMORY DETECTED IN GaianSubResult - Running System.exit(2)");
//			System.exit(2);
			
			GaianNode.stop( "Error in GaianSubResult", er );
		}
	}
}
