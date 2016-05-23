/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianDBProcedureUtils;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.plugins.wpml.schema.AccessLogger;
import com.ibm.gaiandb.plugins.wpml.schema.DataSource;
import com.ibm.gaiandb.plugins.wpml.schema.IRow;
import com.ibm.gaiandb.plugins.wpml.schema.QueryContext;
import com.ibm.gaiandb.plugins.wpml.schema.Row;
import com.ibm.gaiandb.policyframework.SQLResultFilterX;
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
public class GenericPolicyPluginForWPML extends SQLResultFilterX {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
//	private final static String WPML_PFG_POLICIES_FILE = "C:\\PFGpolicies.spl";

	private static final Logger logger = new Logger( "PolicyEnabledFilter", 30 );
	
	private List<String> NON_RESTRICTED_LTS = Arrays.asList("LT0", "LT1", "T1", "SUBQUERY", "GDB_LTNULL",
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
	
	/**
	 * Allocate the object pep. Allow subclasses to override.
	 * @return
	 * @throws PMLException
	 */
	protected static IObjectPEP allocateObjectPEP() throws PMLException {
		 return new StrictTwoStagePEP("pfg-pep", false);
	}
	
	private static void refreshPolicyCache() {
		
		synchronized( pepLock ) {
			try {
				if ( null == pep ) {
					pep = allocateObjectPEP();
					pep.addAlias("queryContext", QueryContext.class);
					pep.addAlias("accessLogger", AccessLogger.class);
					pep.addAlias("dataSource", DataSource.class);
					pep.addAlias("filterRowObject", IRow.class);
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
	
	// Global policy watch-dog thread: Periodically refresh policy cache + monitor policy status and notify UI with any changes.
	static {
//		synchronized( pepLock ) { if ( null != pep ) return; }
		
		System.out.println("Starting policy watchdog");
		
		new Thread( new Runnable() {
			public void run() {
				ArrayList<Object> oa = new ArrayList<Object>();
				QueryContext qc = new QueryContext();
				oa.add(qc);
				
				AccessLogger al = new AccessLogger();
				IRow ir = null;
				try { ir = new Row(new GaianResultSetMetaData(), new int[0]); }
				catch (Exception e1) { e1.printStackTrace(); }
				
				while( GenericPolicyPluginForWPML.class.getName().equals( GaianDBConfig.getPolicyClassNameForSQLResultFilter() ) ) {

					refreshPolicyCache();
					try { Thread.sleep(2000); } catch (InterruptedException e) {}
					notifyWebDemoWithStatusOnAffiliationAndPolicyChoices(oa, qc, al, ir);
					try { Thread.sleep(2000); } catch (InterruptedException e) {}
					notifyWebDemoWithStatusOnAffiliationAndPolicyChoices(oa, qc, al, ir);
				}
			}
		}, "Policy refresher watch-dog for " + GenericPolicyPluginForWPML.class.getSimpleName()).start();
	}
	
	private static String getLocalAffiliation() {
		final int dashIdx = GaianDBConfig.getAccessClusters().trim().indexOf('-');
		String aff = -1 < dashIdx ? GaianDBConfig.getAccessClusters().trim().substring(0, dashIdx) : "None";
		return aff.equals("KISH") ? "Kish" : aff;
	}
	
	// In future for a more extensible base policy class, pass in the query context type (i.e. the object model)
	public GenericPolicyPluginForWPML() {
		queryContext = new QueryContext();
		queryContext.setLocalAffiliation( getLocalAffiliation() );
	}
	
	public boolean setForwardingNode( String forwardingNode ) {

		if ( !isLogicalTableRestricted ) return true;
		
		if ( null != forwardingNode ) queryContext.setForwardingNode(forwardingNode);
		return true;
	}
	

	public boolean setUserCredentials(String credentialsStringBlock) {
		
		// Only file-retrieving queries are checked to see if requester is allowed.
		if ( false == queryContext.getLogicalTable().startsWith( IDENTIFYING_PREFIX_FOR_FILE_RETRIEVING_SUBQUERY_CALLBACK ) )
			return true;
		
//		if ( null == credentialsStringBlock ) {
//			logger.logImportant("No authenticated user was resolved: credentials block is null");
//			return true;
//		}
		
		// This bit is not generic - the structure of credentialsStringBlock is variable - We should use some object model to decompose this.. ?
		return setAuthenticatedUserCredentials( new String[] { credentialsStringBlock, credentialsStringBlock, "CLEARANCE-NONE" } );
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
		
		// always allow queries from nodes of the same affiliation
		if ( queryContext.getAffiliation().equals( queryContext.getLocalAffiliation() ) ) return true;
		
		if ( null == pep ) return true;
		
		try {
			ArrayList<Object> oa = createObjectArray();
			oa.add(new AccessLogger());
						
			/*
			 * 2-step policy evaluation: first do the authorizations
			 * then apply the obligations
			 */
			final boolean decision = evaluatePEP(oa); // 1 == 1 ? true : evaluatePEP(oa);
//			System.out.println("Evaluated policy for setAuthenticatedUserCredentials: " + Arrays.asList(oa) + ", decision: " + decision);
			
			String aff = affiliation.toLowerCase(); if ( "us".equals(aff) ) aff = "usa"; // convert affiliation string to match UI spec
			webDemoEvent("policy-update", "{'affiliation':'"+aff+"','allowed':"+decision+"}", "", "");
			
			return decision;
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
		
		// Obtain the schema to be filtered by policies
		this.logicalTableRSMD = logicalTableResultSetMetaData;
		try {
			this.logicalTableColumnCount = logicalTableRSMD.getColumnCount();
		} catch (SQLException sqle) {
			System.err.println("ERROR: PFG: could not retrieve logical column count: " + sqle);
			sqle.printStackTrace();
		}
		
		rowCount = 0;
		
		if ( null != logicalTableName ) queryContext.setLogicalTable(logicalTableName);
		
		// Don't evaluate PEP here - we build queryContext to evaluate it once for the whole query state. (then once per data source and once per row returned)
		
		return true;
	}
	
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
		
		// TODO: Make 'Allow' policy be evaluated here: This will require us to keep a cache of the known affiliations of connected nodes
		// established at discovery time - we can maintain this in GaianDBConfig.java - then we can look up an affiliation for a given nodeid.
		
		// TODO: ? Issue with policy on data source being applied in precedence to predicate for targeting a node.
		
		// We don't evaluate data source policies in this demo
		if ( 1 == 1) return -1;
		
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

			final boolean decision = evaluatePEP(oa); // 1 == 1 ? true : evaluatePEP(oa);
//			System.out.println("Evaluated policy for nextQueriedDataSource: " + Arrays.asList(oa) + ", decision: " + decision);
			return decision ? -1 : 0;
			
		} catch (PMLException e) {
			System.err.println("PFG: nextQueriedDataSource: policy evaluation error");
			e.printStackTrace();
		}
		
		return 0; // don't allow any rows to be extracted
	}
	
	private static final String IDENTIFYING_PREFIX_FOR_FHE_SEARCH_SUBQUERY = "select FHE_SEARCH('"+GaianDBConfig.getGaianNodeID();
	private static final String IDENTIFYING_PREFIX_FOR_FILE_RETRIEVING_SUBQUERY_CALLBACK = "select 0 isEncrypted, getFileBZ("; // "from new com.ibm.db2j.GaianQuery('select 0 isEncrypted, getFileBZ(";
	
	public static final String WEB_UI_UPDATE_LOCK = "WEB_UI_UPDATE_LOCK";
	
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
		
		final String ltExpression = queryContext.getLogicalTable();

		final String fNode = queryContext.getForwardingNode();
		System.out.println("forwardingNode: " + fNode + ", ltExpression: " + ltExpression);
		
		if ( null == ltExpression ) return true;
		
		if ( ltExpression.startsWith( IDENTIFYING_PREFIX_FOR_FHE_SEARCH_SUBQUERY ) && (null == fNode || 1 > fNode.length()) ) {

			try {
				final String fromNode = row[1].getString();
				
				// TODO: remove this condition if/when aborting queries earlier in nextQueriedDataSource()
				if ( row[0].isNull() ) {
					System.out.println("Result blob from node '" + fromNode + "' is Null - rejecting row");
					return false; // discard this empty result - the endpoint node was maybe not allowed to receive the query
				}
				
				// returningAnalyticResult - end
				webDemoEventReceivingData( fromNode, "", "result", "end", "" );
			}
			catch (StandardException e) {
				System.out.println("Unable to get provenance nodeID (=row[1]) from outer FHE query result - cannot notify UI");
				e.printStackTrace();
			}
		}

//		int idx = ltExpression.indexOf( IDENTIFYING_FRAGMENT_FOR_FILE_RETRIEVING_QUERY );
//		if ( -1 < idx )	System.out.println("==================================================================> BEGIN INVOKING POLICY FOR A GETFILEBZ() QUERY");
		
		// Only apply record modifying policies to queries requesting files (e.g. call-back queries for the outer FHE one)
		if ( false == ltExpression.startsWith( IDENTIFYING_PREFIX_FOR_FILE_RETRIEVING_SUBQUERY_CALLBACK ) ) return true;
		
		// Also only apply policy on records that were generated at our node
		try { if ( false == GaianDBConfig.getGaianNodeID().equals( row[2].getString() ) ) return true; }
		catch (StandardException e1) { System.out.println("PFG Unable to check provenance node of data record, cause: " + e1); e1.printStackTrace(); }
		
		/*
		 * Perform a policy-based filtering on the record.
		 */
		boolean decision = true; // this is overwritten by the default value of the StrictTwoStagePEP (2nd argument of it's constructor) 
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
			
			decision = evaluatePEP(oa); // 1 == 1 ? decision : evaluatePEP(oa);
//			System.out.println("Evaluation for row filter " + Arrays.asList(oa) + ", decision: " + decision);
			
		} catch (PMLException e) {
			System.err.println("PFG: policy evaluation error" + e.getMessage());
			e.printStackTrace();
		}
		
		// None of the code below should be required for a Generic Plugin for WPML.
		// The encryption action should be re-factored into WPML resource model.
		
		final String affiliationAtOriginNode = queryContext.getAffiliation(); // queryContext.getAccessClustersAtOriginNode()
		
		// TODO: Remove this when WPML is actually making the decisions based on the queryContext and record that came through.
		// Encrypt file if the requesting node's cluster IDs is NOT one of the TRUSTED_COALLITION_CLUSTER_IDS (i.e. disjunction is empty).
//		final List<String> TRUSTED_COALLITION_CLUSTER_IDS = Arrays.asList( "Kish", "US" );
//		decision = false == Collections.disjoint( affiliationsAtOriginNode, TRUSTED_COALLITION_CLUSTER_IDS );
		
//		System.out.println("==================================================================> END INVOKING POLICY FOR A GETFILEBZ() QUERY");
		
		System.out.println("AffiliationAtOriginNode: " + affiliationAtOriginNode + ", isTrusted policy decision: " + decision);

		String aff = affiliationAtOriginNode.toLowerCase(); if ( "us".equals(aff) ) aff = "usa"; // convert affiliation string to match UI spec
		webDemoEvent("policy-update", "{'affiliation':'"+aff+"','trusted':"+decision+"}", "", "");

		int idx = ltExpression.indexOf("'", ltExpression.indexOf("getFileBZ"));
		String fPath = ltExpression.substring( idx+1, ltExpression.indexOf("'", idx+1) );
		
		// TODO: In future re-factor this to WPML, when it can do the policy action itself (i.e. execute the required system call when its decision is false)
		if ( false == decision ) {
			
			// FHE encrypt - Replace file with its encrypted counter-part.
			
			final String fPathEncrypted = fPath.substring(0, fPath.length()-3) + "ctxt";
			System.out.println("Path arg of requested file is: " + fPath );
			System.out.println("Path of pre-encrypted file is: " + fPathEncrypted );

			try { row[1].setValue( GaianDBProcedureUtils.readAndZipFileBytes( new File(fPathEncrypted) ) ); }
			catch (Exception e) { logger.logAlways("Policy plugin issue: Cannot load new byte[] from encrypted bytes in file: '" + fPathEncrypted + "': " + e); }

			fPath = fPathEncrypted;
			
//			try {
//				
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				FileInputStream fis = new FileInputStream(fPathEncrypted);
//				
//				Util.copyBinaryData(fis, new GZIPOutputStream(baos)); // must be zipped (as we unzip on the other side)
//				byte[] preEncryptedBytesToSend = baos.toByteArray();
//				
////				numbytes1 = fileBytes.length;
//				row[1].setValue( preEncryptedBytesToSend );
//				baos.close(); fis.close();
//				
////				System.out.println("Encrypted row[1] bytes. Initial bytes[] length: " + numbytes0 + ", encrypted bytes[] length: " + numbytes1);
//			}
//			catch (Exception e) { logger.logAlways("Policy plugin issue: Cannot load new byte[] from encrypted bytes in file: '" + fPathEncrypted + "': " + e); }
		
		}
		
		// sendingDataObjectForAnalysis - start
		webDemoEventSendingData( queryContext.getForwardingPath().get(0), fPath, "query", "start", "" ); // .substring(fPath.lastIndexOf('/')+1)
		
		// Also return decision in first column - this indicates whether isEncrypted is true
		System.out.println("-> Policy does " + (decision?"not ":"") + "require filter/encryption on return value from getFileBZ() function. "
				+ "Setting row[0] to value: " + (decision ? 0 : 1) ); // decision == false means not trusted, so it's encrypted (isEncrypted (row[0]) status 1)
		try { row[0].setValue( decision ? 0 : 1 ); }
		catch (StandardException e) { System.err.println("Policy.filterRow(): Unable to set 'isEncrypted' column value (exception trace below)."); e.printStackTrace(); }
		
		return true;
	}
	
	private static final String[] KNOWN_AFFILIATIONS = { "US", "UK", "Kish" };

	private static boolean[] policyAllowedDecisions = { false, false, false }; // respective initial decisions for US/UK/Kish
	private static boolean[] policyTrustedDecisions = { false, false, false }; // respective initial decisions for US/UK/Kish
	
	private static boolean isInitialPoliciesStatusCheck = true;
	
	private static final void notifyWebDemoWithStatusOnAffiliationAndPolicyChoices( ArrayList<Object> oa, QueryContext qc, AccessLogger al, IRow ir ) {
		
		final int dashIdx = GaianDBConfig.getAccessClusters().trim().indexOf('-');
		String myAffiliation = -1 < dashIdx ? GaianDBConfig.getAccessClusters().trim().substring(0, dashIdx) : "None";
		myAffiliation = myAffiliation.toLowerCase(); if ( "us".equals(myAffiliation) ) myAffiliation = "usa"; // convert affiliation string to match UI spec
		
		// state our current affiliation 
		webDemoEvent("declare-existence", "{'affiliation':'"+myAffiliation+"'}", "", "");
		
		int i = -1;
////	for each affiliation: { for policy in (allow, trust): setup structures required; evaluate; } notify of all decisions for each affiliation to the UI
		for ( String affiliation : KNOWN_AFFILIATIONS ) {
			i++; // index in affiliation decisions arrays
			
			String aff = affiliation.toLowerCase(); if ( "us".equals(aff) ) aff = "usa"; // convert affiliation string to match UI spec
			if ( myAffiliation.equals(aff) ) continue; // ignore policies about our own affiliation
			
			qc.setAffiliation(affiliation);
			qc.setLocalAffiliation(getLocalAffiliation()); // local affiliation may change any time based on node setup
			
			boolean isAllowed = false, isTrusted = false;
			try {
//				System.out.println("queryContext.getLocalAffiliation(): " + queryContext.getLocalAffiliation() 
//						+ ", queryContext.getAffiliation(): " + queryContext.getAffiliation() );
				oa.add(al);
				isAllowed = evaluatePEP(oa); // 1 == 1 ? true : evaluatePEP(oa);
				oa.remove(al);
//				System.out.println("Evaluated policy for setAuthenticatedUserCredentials: " + Arrays.asList(oa) + ", decision: " + decision);
			} catch (Exception e) {
				System.err.println("Exception in notifyWebDemoWithPolicyStatus(), evaluating isAllowed() for affiliation: " + affiliation + ", cause: " + e);
				e.printStackTrace();
			}
			try {
				oa.add(ir);
				isTrusted = evaluatePEP(oa); // 1 == 1 ? decision : evaluatePEP(oa);
				oa.remove(ir);
//				System.out.println("Evaluation for row filter " + Arrays.asList(oa) + ", decision: " + decision);
			} catch (Exception e) {
				System.err.println("Exception in notifyWebDemoWithPolicyStatus(), evaluating isTrusted() for affiliation: " + affiliation + ", cause: " + e);
				e.printStackTrace();
			}

			if ( isInitialPoliciesStatusCheck || isAllowed != policyAllowedDecisions[i] || isTrusted != policyTrustedDecisions[i] ) {
				webDemoEvent("policy-update", "{'affiliation':'"+aff+"','allowed':"+isAllowed+",'trusted':"+isTrusted+"}", "", "");
				policyAllowedDecisions[i] = isAllowed;
				policyTrustedDecisions[i] = isTrusted;
			}
		}

		isInitialPoliciesStatusCheck = false;
	}
	
	/**
	 * Emit demo event info to a Web UI to display what is going on.
	 * 
	 * @param nodeID - the node for who the event is emitted. A node may emit an event on behalf of another node.
	 * @param eventID - identifies the event that just occurred in the demo scenario.
	 * @param eventDataJSON - holds data elements relating to the event and needed to describe it.
	 * @param status - start or end. May also be an empty string if the event is immediate.
	 * @param msg - optional message to describe the event in the UI console.
	 */
	private static final void webDemoEvent( final String nodeID, final String eventID, final String eventDataJSON, final String status, final String msg ) {

		String sysCmd = System.getenv("WEB_DEMO_EVENT_CMD");
		
//		final String webDemoEventURL = System.getenv("WEB_DEMO_EVENT_URL");
		
		if ( null == sysCmd ) {
			if ( false == eventID.equals("declare-existence") )
				System.out.println("Unable to notify Web UI (eventID: " + eventID + ", from node: "
					+ nodeID + "): Environment variable 'WEB_DEMO_EVENT_CMD' is not set");
			return;
		}
		
//		final String[] sysCmd = new String[] {
//				"curl", "-H", "\"Content-Type: application/json\"", "-X", "POST", "-d",
//				"\"{'nodeid':'"+nodeID+"', 'eventid':'"+eventID+"', 'eventdata':"+eventDataJSON+", 'status':'"+status+"', 'timestamp':"+System.currentTimeMillis()+", 'message':'"+msg+"'}\"",
//				webDemoEventURL
//		};
		
		String json = ("{'nodeid':'"+nodeID+"', 'eventid':'"+eventID+"', 'eventdata':"+eventDataJSON
						+", 'status':'"+status+"', 'timestamp':"+System.currentTimeMillis()+", 'message':'"+msg+"'}").replace('\'', '"');
		
		sysCmd = sysCmd.replaceFirst("<JSON>", "'"+json+"'");
		
//		System.out.println("sysCmd: " + sysCmd);
		
		final String[] cmdElmts = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes( sysCmd, ' ');
		
		// Remove wrapping quotes that may exist around tokens
		for ( int i=0; i<cmdElmts.length; i++ ) {
			String e = cmdElmts[i];
			char c = e.charAt(0);
			if ( '\'' == c || '"' == c )
				cmdElmts[i] = e.substring(1,e.length()-1);
		}
		
		if ( false == eventID.equals("declare-existence") ) {
			StringBuilder sb = new StringBuilder();
			for ( String s : cmdElmts ) sb.append(s + " ");
			System.out.println( sb );
		}
		
		try { Util.runSystemCommand( cmdElmts, WEB_UI_UPDATE_LOCK, Logger.logLevel > Logger.LOG_LESS ); }
		catch (IOException e) {
			System.out.print("Exception in webDemoEvent. nodeID: " + nodeID + ", eventID: " + eventID + ", cause: " + e);
			e.printStackTrace();
		}
	}
	
	public static final void webDemoEvent( final String eventID, final String eventDataJSON, final String status, final String msg ) {
		webDemoEvent( GaianDBConfig.getGaianNodeID(), eventID, eventDataJSON, status, msg );
	}
	
	public static final void webDemoEventSendingData( final String toNodeID, final String fileName, final String protocolStep,
			final String status, final String msg ) {
		final String localNodeID = GaianDBConfig.getGaianNodeID();
		webDemoEvent("sending-data", "{'src':'"+localNodeID+"','dest':'"+toNodeID+"','imageurl':'"+fileName
				+"','protocol-step':'"+protocolStep+"'}", status, msg);
		// emit reciprocal event on behalf of the receiving node
		webDemoEvent(toNodeID, "receiving-data", "{'src':'"+localNodeID+"','dest':'"+toNodeID+"','imageurl':'"+fileName
				+"','protocol-step':'"+protocolStep+"'}", status, msg);
	}
	
	public static final void webDemoEventReceivingData( final String fromNodeID, final String fileName, final String protocolStep,
			final String status, final String msg ) {
		final String localNodeID = GaianDBConfig.getGaianNodeID();
		webDemoEvent("receiving-data", "{'src':'"+fromNodeID+"','dest':'"+localNodeID+"','imageurl':'"+fileName
				+"','protocol-step':'"+protocolStep+"'}", status, msg);
		// emit reciprocal event on behalf of the sending node
		webDemoEvent(fromNodeID, "sending-data", "{'src':'"+fromNodeID+"','dest':'"+localNodeID+"','imageurl':'"+fileName
				+"','protocol-step':'"+protocolStep+"'}", status, msg);
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

	@Override
	public DataValueDescriptor[][] filterRowsBatch(String dataSourceID, DataValueDescriptor[][] rows) {
		List<DataValueDescriptor[]> newRows = null;
		int rowIdx=0;
		
		// filter through rows[][] - if none are rejected then we'll just return the same array.
		for ( ; rowIdx < rows.length; rowIdx++ )
			if ( filterRow(rows[rowIdx]) ) continue;
			else {
				newRows = new ArrayList<DataValueDescriptor[]>();
				for ( int i=0; i<rowIdx; i++ ) newRows.add( rows[i] );
				rowIdx++;
				break;
			}
		
		// complete filtering rows[][] - this only applies if one of the rows was rejected
		for ( ; rowIdx < rows.length; rowIdx++ ) {
			DataValueDescriptor[] row = rows[rowIdx];
			if ( filterRow(row) ) newRows.add( row );
		}
		
		return null == newRows ? rows : (DataValueDescriptor[][]) newRows.toArray( new DataValueDescriptor[0][] );
	}

	@Override
	public int nextQueriedDataSource(String dataSourceID, String dataSourceDescription, int[] columnMappings) {
		return nextQueriedDataSource(dataSourceDescription, columnMappings);
	}

	// Only called when invoking VTI Data-source wrappers - return number of records that can be returned (or -1 if unlimited)
	@Override
	public int setDataSourceWrapper(String wrapperID) {
		return -1;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object executeOperationImpl(String opID, Object... args) {
		// TODO: Obtain authenticated derby user + forwarding path + access cluster ids of query's entry-point node.
		// These may cause the query to be aborted here, or may trigger filtering obligations when returning records
		
//		System.out.println("Entered executeOperation(), opID: " + opID + ", args: " + (null == args ? null : Arrays.asList(args)) );
		
		if ( opID.equals(SQLResultFilterX.OP_ID_SET_AUTHENTICATED_DERBY_USER_RETURN_IS_QUERY_ALLOWED) )
			queryContext.setRequestor( (String) args[0] );
		else if ( opID.equals(SQLResultFilterX.OP_ID_SET_ACCESS_CLUSTERS_RETURN_IS_QUERY_ALLOWED) )
			queryContext.setAccessClustersAtOriginNode( (List<String>) args[0] );
		else if ( opID.equals(SQLResultFilterX.OP_ID_SET_FORWARDING_PATH_RETURN_IS_QUERY_ALLOWED) ) {
			queryContext.setForwardingPath( (List<String>)  args[0] );
			System.out.println("Set forwarding path: " + Arrays.asList( queryContext.getForwardingPath() ) );
		}
		else return null; // Not recognized - not a piece of info we can use - just ignore
		
		// Don't evaluate PEP here - we build queryContext to evaluate it once for the whole query state. (then once per data source and once per row returned)
		
		return new Boolean( true );
	}
}
