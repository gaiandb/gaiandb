/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.dsm;

import java.util.HashMap;
import java.util.Map;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.watson.dsm.services.gaian.INeighborProvider;

public class GaianNeighbourProvider implements INeighborProvider {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";
	
	@Override
	public Map<String, String> getNeighborInfo() {
		String[] gdbConnections = GaianDBConfig.getGaianConnections();
		Map<String, String> neighborInfo = new HashMap<String, String>();
		
		for ( String gdbc : gdbConnections )
			try { neighborInfo.put( GaianDBConfig.getGaianNodeID(gdbc), getIPFromConnectionID(gdbc) ); }
			catch (Exception e) { System.out.println("Unable to resolve neighbour info for GaianDB connection id: " + gdbc + ", cause: " + e); return null; }
		
		return neighborInfo;
	}

	private String getIPFromConnectionID( String cid ) throws Exception {
		String cdetails = GaianDBConfig.getRDBConnectionDetailsAsString(cid); // getConnectionURL(cid);
		
//		System.out.println("cdetails for " + cid + ": " + cdetails);
		
		if ( null == cdetails ) return null;
		
		int startIndex = cdetails.indexOf("jdbc:derby://");
		if ( -1 == startIndex ) return null;
		startIndex += "jdbc:derby://".length();
		
		int ipEndIndex = cdetails.indexOf(':', startIndex );
		if ( -1 == ipEndIndex ) return null;
		
		return cdetails.substring( startIndex, ipEndIndex );
	}
}
