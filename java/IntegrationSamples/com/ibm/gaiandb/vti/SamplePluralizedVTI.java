/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.vti;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.db2j.AbstractVTI;
import com.ibm.db2j.PluralizableVTI;
import com.ibm.gaiandb.Logger;

/**
 * This is a dummy VTI example with the following characteristics:
 * 		- it *does not* make use of AbstractVTI's disk caching capabilities.
 * 		- it can expose multiple different data sources via a single GaianDB data source wrapper, because their structures are homogeneous.
 * 				-> Using a single wrapper makes configuration simpler to manage and reduces Java heap usage.
 * 				-> To enable this, the class must implement interface PluralizableVTI and method getEndpointIDs() and the "PLURALIZED" option must be set for the data source wrapper 
 * 				-> Optionally, the class may implement the method PluralizableVTI.getEndpointConstantColumns() to allow GaianDB to perform predicate/policy filtering early
 * 
 * @author DavidVyvyan
 */

public class SamplePluralizedVTI extends AbstractVTI implements PluralizableVTI {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	private static final Logger logger = new Logger( "SamplePluralizedVTI", 30 );
	
	private int usrCPU, sysCPU = 5;
	private String dsIP;
	private int agentID;
	
	private final String DS_IP1 = "1.1.1.1", DS_IP2 = "2.2.2.2";
	private final int AGENT_ID1 = 1, AGENT_ID2 = 7;
	
	private final Map<String, DataValueDescriptor[]> endpointConstants = new HashMap<String, DataValueDescriptor[]>();
	
	// The default value below will show up if the getEndpointIDs() method is not called by GaianDB (if PLURALIZED option is not set).
	private String endpointID = "UNSET_ENDPOINT_ID:0";
	private int rowCount = 0;
	
	public SamplePluralizedVTI(String vtiArgs) throws Exception {
		super(vtiArgs, "SamplePluralizedVTI");
		endpointConstants.put( DS_IP1+':'+AGENT_ID1, new DataValueDescriptor[] { new SQLChar(DS_IP1), new SQLInteger(AGENT_ID1) } );
		endpointConstants.put( DS_IP2+':'+AGENT_ID2, new DataValueDescriptor[] { new SQLChar(DS_IP2), new SQLInteger(AGENT_ID2) } );
	}

	// The end-point instance ID is passed as first argument of the String[] in this method
	@Override public void setArgs(String[] args) throws Exception {
		
		logger.logInfo("Entered setArgs(), args are: " + Arrays.asList(args) );
		if ( null != args && 0 < args.length ) { if ( null != args[0] ) endpointID = args[0]; } // only interested in args[0]
	};
	
	// Compute result in this method
	@Override public boolean executeAsFastPath() throws StandardException, SQLException {
		
		int indexOfColon = null == endpointID ? -1 : endpointID.indexOf(':');
		dsIP = -1 == indexOfColon ? endpointID : endpointID.substring(0, indexOfColon);
		agentID = -1 == indexOfColon ? -1 : Integer.parseInt( endpointID.substring(indexOfColon+1) );
		
		usrCPU = agentID * 10;
		return true;
	}
	
	// GaianDB extract rows by calling this method repeatedly
	@Override public int nextRow(DataValueDescriptor[] arg0) throws StandardException, SQLException {

		logger.logDetail("Entered nextRow(), rowCount = " + rowCount + (4 < rowCount ? ", returning SCAN_COMPLETED" : ", getting row") );
		if ( 4 < rowCount ) return IFastPath.SCAN_COMPLETED;
		
		arg0[0] = new SQLInteger( usrCPU++ );
		arg0[1] = new SQLInteger( sysCPU );
		arg0[2] = new SQLChar("Non-rectified IP"); //new SQLChar( dsIP ); <= this value is meant to be a constant endpoint value, so GaianDB should rectify it anyway
		arg0[3] = new SQLInteger( null ); //new SQLInteger( agentID ); // <= this value is meant to be a constant endpoint value, so GaianDB should rectify it anyway
		
		rowCount++;
		
		return IFastPath.GOT_ROW;
	}
	
	// PluralizableVTI methods - used to re-factor/generalise/simplify data source wrapper configurations
	// Note each endpointID will appear as a suffix in the GDB_LEAF column when querying the logical table augmented with the provenance columns.

	@Override public Set<String> getEndpointIDs() { return endpointConstants.keySet(); }
	@Override public DataValueDescriptor[] getEndpointConstants(String endpointID) { return endpointConstants.get( endpointID ); }
	
	// Qualifyable interface - used to process predicates against our columns -
	// Note that predicates on our *constant* columns (e.g. for each pluralizable instance) should ideally be processed above by GaianDB (if we passed 
	// the constant instances up with getEndpointConstantColumns()) - however it does little harm to test/filter them again here just in case.
	@Override public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException { }
	
	@Override public int getRowCount() throws Exception { return rowCount; }

	// Empty local heap resources for this instance and set them to null if possible
	@Override public void close() throws SQLException { super.close(); endpointConstants.clear(); }
	
	// VTICosting methods - used for JOIN optimisations
	@Override public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException { return false; }
	
	@Override public boolean isBeforeFirst() throws SQLException { return 0 == rowCount; }
}
