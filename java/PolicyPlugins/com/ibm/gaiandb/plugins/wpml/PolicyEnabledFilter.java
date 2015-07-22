/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.plugins.wpml.schema.AccessLogger;
import com.ibm.gaiandb.plugins.wpml.schema.DataSource;
import com.ibm.gaiandb.plugins.wpml.schema.IRow;
import com.ibm.gaiandb.plugins.wpml.schema.QueryContext;
import com.ibm.gaiandb.plugins.wpml.schema.Row;
import com.ibm.gaiandb.policyframework.SQLResultFilter;
import com.ibm.watson.pml.PMLException;
import com.ibm.watson.pml.pep.IObjectPEP;
import com.ibm.watson.pml.pep.StrictTwoStagePEP;

/**
 * Policy-enabled filter of relational schema elements.
 * Provides an implementation of the filter based on the interface definition
 * made available by GaianDB for plug-in functionality.
 * <P>
 * Instantiates a Policy Decision Point (PDP) and a Policy Enforcement Point (PEP)
 * for the policy evaluation and connects to the repository to access the policies
 * that govern the filtering of the data. For now, default file for policies
 * can be found in C:\PFGpolicies.spl. If no such file exists, then the PDP
 * attempts to retrieve policies from a jdbc repository, using connection details
 * found in the wpml.properties file.
 * <P>
 * Policy evaluation is performed in 2 steps: first, evaluation of authorization
 * policies is performed, followed by evaluation of the obligation policies.
 * <P>
 * Notes/todo list:
 * <ol>
 * <li> Proper logging using the PMLLogger facility (com.ibm.watson.pml.util package)
 * <li> Update (03/05/2009): the nextQueriedDataSource() method will
 * be deprecated and possibly replaced with verifyDataSources() or something similar. 
 * Also, the way the QueryContext is passed to policies for evaluation needs to be changed
 * accordingly
 * <li> Get configuration for connecting to policy repository a properties file.
 * </ol>
 * 
 * @author pzerfos@us.ibm.com, drvyvyan@uk.ibm.com
 *
 */
public class PolicyEnabledFilter implements SQLResultFilter {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
//	private final static String WPML_PFG_POLICIES_FILE = "C:\\PFGpolicies.spl";

	private static final Logger logger = new Logger( "PolicyEnabledFilter", 30 );
	
	private List<String> NON_RESTRICTED_LTS = Arrays.asList("LT1", "T1", "SUBQUERY", "GDB_LTNULL",
			"POLICY", "POLICY_SET", "POLICY_SET_MEMBERSHIP", "PEP", "PEP_ATTRIBUTE", "POLICY_ATTRIBUTE", 
			"POLICY_SET_ATTRIBUTE", "INSTANCE", "PEP_HAS_INSTANCE", "TIMESTAMPS");
	
	private boolean isLogicalTableRestricted = true;
	
	/**
	 * metadata on the logical table of the query
	 */
	private ResultSetMetaData logicalTableRSMD = null;
	
	/**
	 * Number of columns in a row
	 */
	private int logicalTableColumnCount = -1;
	
	/**
	 * Array of flags to identify the columns of a row that have been
	 */
	private int[] queriedColumns = null;
	private int rowCount = -1;
	
	/**
	 * Object of {@link com.ibm.gaiandb.plugins.wpml.schema#QueryContext} anchor class to be used for policy evaluation
	 * All query context information needed in policy evaluation is held in this bean-like structure.
	 */
	protected final QueryContext queryContext;
	
	/**
	 * Object of {@link com.ibm.gaiandb.plugins.wpml.schema#IRow} anchor class to be used for policy evaluation
	 */
	private IRow filterRowObject  = null;	

	private static IObjectPEP pep = null;
	private static Object pepLock = new Object();
	private static AtomicBoolean isInitialised = new AtomicBoolean(false);
	
	/**
	 * Allocate the object pep. Allow subclasses to override.
	 * @return
	 * @throws PMLException
	 */
	protected static IObjectPEP allocateObjectPEP() throws PMLException {
		 return new StrictTwoStagePEP("pfg-pep", true);
	}
	
	private static void refreshPolicyCache() {
		
		synchronized( pepLock ) {
			try {
				if ( null == pep ) {
					pep = allocateObjectPEP();
					pep.addAlias("queryContext", QueryContext.class);
					pep.addAlias("accessLogger", AccessLogger.class);
					pep.addAlias("dataSource", DataSource.class);
				} else {
					pep.endEvaluations();
					pep.clear();
				}
				
			} catch (PMLException e) {
				System.err.println("PFG: refreshPolicyCache: Error in endEvaluations/beginEvaluations: " + e);
				e.printStackTrace();
			}
		}
	}
	
	private static boolean evaluatePEP( ArrayList<Object> oa ) throws PMLException {

		synchronized( pepLock ) {
			pep.beginEvaluations();
			return pep.evaluate(oa.toArray());
		}
	}
	
	// In future for a more extensible base policy class, pass in the query context type (i.e. the object model)
	public PolicyEnabledFilter() {
		queryContext = new QueryContext();
		
		if ( isInitialised.compareAndSet(false, true) ) {
	//		synchronized( pepLock ) { if ( null != pep ) return; }
			refreshPolicyCache();
			new Thread( new Runnable() {
				public void run() {
					while( true ) { try { Thread.sleep(5000); } catch (InterruptedException e) {} refreshPolicyCache(); }
				}
			}, "PEP refresher for " + this.getClass().getSimpleName()).start();
		}
	}
	
	public boolean setForwardingNode( String forwardingNode ) {

		if ( !isLogicalTableRestricted ) return true;
		
		if ( null != forwardingNode ) queryContext.setForwardingNode(forwardingNode);
		return true;
	}
	

	public boolean setUserCredentials(String credentialsStringBlock) {
		
		if ( null == credentialsStringBlock ) {
			logger.logInfo("Unable to retrieve/authenticate user: credentials block is null");
			return false;
		}
		String user = credentialsStringBlock;
		return setAuthenticatedUserCredentials( new String[] { user, "UK", "UK_RESTRICTED" } ); // no actual authentication - simplistic user fields passed straight through
	}
	
	private boolean setAuthenticatedUserCredentials( String[] userFields ) { // to be called from setUserCredentials() once byte[] is decrypted

		if ( !isLogicalTableRestricted ) return true;
		
//		System.out.println("SET AUTHENTICATED USR CREDS: " + (null==userFields ? null : Arrays.asList(userFields)) );
		
		String user = "", affiliation = "", clearance = "";
		
		if ( null != userFields ) {
			user = userFields[0];
			affiliation = userFields[1];
			clearance = userFields[2];
		}
			
		// User info for QueryContext is set here
		queryContext.setRequestor(user);
		queryContext.setAffiliation(affiliation);
		queryContext.setSecurityClearance(clearance);
		
		if ( null == pep ) return true;
		
		try {

//			if ( null == pep ) {
//				pep = allocateObjectPEP();
//				pep.addAlias("queryContext", QueryContext.class);
//				pep.addAlias("accessLogger", AccessLogger.class);
//				pep.addAlias("dataSource", DataSource.class);
//			}
//			
//			pep.beginEvaluations();
			
			ArrayList<Object> oa = createObjectArray();
			oa.add(new AccessLogger());
						
			/*
			 * 2-step policy evaluation: first do the authorizations
			 * then apply the obligations
			 */
//			System.out.println("Evaluation of setAuthenticatedUserCredentials " + Arrays.asList(oa));
			return evaluatePEP(oa);
		} catch (PMLException e) {
			System.err.println("PFG: setLogicalTable: policy evaluation error");
			e.printStackTrace();
		}
		
		return true;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.plugin.RowFilter#setLogicalTable(java.lang.String, java.sql.ResultSetMetaData)
	 */
	public boolean setLogicalTable(String logicalTableName, ResultSetMetaData logicalTableResultSetMetaData) {
		
//		System.out.println("Access to logical table: " + logicalTableName);
		if ( NON_RESTRICTED_LTS.contains(logicalTableName.toUpperCase()) ) {
			isLogicalTableRestricted = false;
			return true;
		}
		
//		System.out.println("SET LOGICAL TABLE: " + logicalTableName);
		
		// 1. Instantiate a PEP and allocate SingletonPolicyEvaluator.instance() for PDP
//		try {
//			pep = allocateObjectPEP();
//			pep.addAlias("queryContext", QueryContext.class);
//			pep.addAlias("accessLogger", AccessLogger.class);
//			pep.addAlias("dataSource", DataSource.class);
//		} catch (PMLException e) {
//			e.printStackTrace();
//			System.err.println("ERROR: PFG: error in loading policies from file: " + e);
//		}
		
		// 2. Obtain the schema to be filtered by policies
		this.logicalTableRSMD = logicalTableResultSetMetaData;
		try {
			this.logicalTableColumnCount = logicalTableRSMD.getColumnCount();
		} catch (SQLException sqle) {
			System.err.println("ERROR: PFG: could not retrieve logical column count: " + sqle);
			sqle.printStackTrace();
		}
		
		// 3. Set a default QueryContext
//		setQueryContext("default", "default");
		
		rowCount = 0;
		
		if ( null != logicalTableName ) queryContext.setLogicalTable(logicalTableName);
		
//		try {
//			ArrayList<Object> oa = createObjectArray();
//			
//			/*
//			 * 2-step policy evaluation: first do the authorizations
//			 * then apply the obligations
//			 */
////			System.out.println("Evaluation of nextQueriedDataSource " + Arrays.asList(oa) + ": " + rc);
//			return evaluatePEP(oa);
//		} catch (PMLException e) {
//			System.err.println("PFG: setLogicalTable: policy evaluation error");
//			e.printStackTrace();
//		}
		
		return true;
	}
	
	/**
	 * Create an object array that will store the objects to be evaluated.
	 * If the QueryContext has been set, then add it as well.
	 * 
	 * @return an arraylist for the objects that will be evaluated. It potentially
	 * includes the {@link com.ibm.gaiandb.plugins.wpml.schema.QueryContext} object
	 */
	private ArrayList<Object> createObjectArray() {
		ArrayList<Object> objectArray = new ArrayList<Object>();
		if (queryContext != null) {
//			System.out.println("Adding Query Context!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: " + queryContext);
			objectArray.add(queryContext);
		}
		
		return objectArray;
	}
	
//	/**
//	 * Add the individual column objects to the array of objects that will be
//	 * used for evaluation in policies. Besides the composite Row object, 
//	 * policies can be written directly for Columns.
//	 * 
//	 * @param oa the array list with the objects that will be used for policy evaluation
//	 * 
//	 * @param filterRowObj the {@link #Row} object with the queried columns
//	 * 
//	 * @return an array list that includes the original objects for policy evaluation, as well
//	 * as new ones for the queried columns (one object for each column)
//	 */
//	private ArrayList<Object> addColumnsObjectArray(ArrayList<Object> oa, Row filterRowObj) {
//		
//		if (filterRowObj == null)
//			return oa;
//		
//		ArrayList<IColumn> ac = filterRowObj.getColumns();
//		Iterator<IColumn> iter = ac.iterator();
//		while (iter.hasNext()) {
//			IColumn col = iter.next();
//			if (col != null && oa != null)
//				oa.add(col);
//		}
//		
//		return oa;
//	}
	
	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.plugin.RowFilter#setQueriedColumns(int[])
	 */
	public boolean setQueriedColumns(int[] queriedColumns) {
		
		if ( !isLogicalTableRestricted ) return true;
		
		this.queriedColumns = queriedColumns;
		
		/*
		 *  Construct the Row object to be used for policy evaluation.
		 *  The actual DataValueDescriptors with the data of the fields of the
		 *  queried columns are populated upon every call of the filterRow()
		 */
		filterRowObject = new Row(logicalTableRSMD, this.queriedColumns);
		
		return true;
	}	
	
	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.plugin.RowFilter#nextQueriedDataSource(java.lang.String, int[], java.lang.String)
	 */
	public int nextQueriedDataSource(String dataSource, int[] columnMappings) {
		
		if ( !isLogicalTableRestricted ) return -1;
		
//		System.out.println("authenticated user: " + user + ", datasource: " + dataSource);
//		if ( null != user && user.equals("ibmuser1") && -1 != dataSource.indexOf("datafile") ) {
//			isLogicalTableRestricted = false; // no more restrictions after this one
//			return 3; // simple test case, return 3 to signify only 3 rows may be extracted from this source
//		}
		
		if ( null == pep ) return -1; // allow all rows to be extracted
		
//		return -1;
		
		/*
		 * Perform a policy-based evaluation on whether the data source should
		 * be queried or not. Default is true. If evaluation fails, then do not
		 * query this data source.
		 */
		try {
			ArrayList<Object> oa = createObjectArray();
			
			oa.add(new DataSource(dataSource));
//			oa.add(new AccessLogger());
			
			/*
			 * 2-step policy evaluation: first do the authorizations
			 * then apply the obligations
			 */
//			System.out.println("Evaluation of nextQueriedDataSource " + Arrays.asList(oa));

			return evaluatePEP(oa) ? -1 : 0;
		} catch (PMLException e) {
			System.err.println("PFG: nextQueriedDataSource: policy evaluation error");
			e.printStackTrace();
		}
		
		return 0; // don't allow any rows to be extracted
	}	
	
	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.plugin.RowFilter#filterRow(org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public boolean filterRow(DataValueDescriptor[] row) {
		
		if ( !isLogicalTableRestricted ) return true;
		
		/*
		 * Sanity: check for the number of columns in the record
		 */
		if (row.length < logicalTableColumnCount) { 
			System.err.println("ERROR: PFG: invalid fetched row: expecting " +
					logicalTableColumnCount + " columns, instead of " + row.length);
			return false;
		}

		rowCount++;
		
		/*
		 * Set the data of the queried columns in the row that will
		 * be evaluated
		 */
		if (filterRowObject != null) {
			((Row)filterRowObject).setRowData(row);
			((Row)filterRowObject).setRowIndex(rowCount);
		}

		if ( null == pep ) return true;
		
		/*
		 * Perform a policy-based filtering on the record.
		 */
		boolean decision = false;
		try {
			
			ArrayList<Object> oa = createObjectArray();
			if (filterRowObject != null)
				oa.add(filterRowObject);
			
//			System.out.println("About to apply row filter for individual row using ObjectArray: " +
//					Arrays.asList( oa.toArray(new Object[0])) );
		
			/*
			 * 2-step policy evaluation: first do the authorizations
			 * then apply the obligations
			 */
			decision = evaluatePEP(oa);
//			System.out.println("Evaluation for row filter " + Arrays.asList(oa) + ": " + decision);
			
		} catch (PMLException e) {
			System.err.println("PFG: policy evaluation error" + e.getMessage());
			e.printStackTrace();
		}
		
		return decision;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.plugin.RowFilter#close()
	 */
	public void close() {
//		try {
//			if (null != pep) {
//				pep.endEvaluations();
//				pep = null;
//			}
//		} catch (PMLException e) {
//			System.err.println("PEP call to endEvaluations() failed (ignored), cause: " + e);
//		}
		rowCount = 0;
	}
}
