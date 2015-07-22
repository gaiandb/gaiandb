/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.searchapis.SearchREST;
import com.ibm.gaiandb.searchapis.SearchSIAPI;

/**
 * @author DavidVyvyan
 */
public class DocumentFinder extends VTI60 implements VTICosting, IFastPath, GaianChildVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "DocumentFinder", 20 );

	private String searchPort = null;
	private String searchString = null;
	private String collections = null;
	// Vector of Integers
	private Vector<DataValueDescriptor[]> resultRows = null;
	private int index = 0;
	
	// search sring -> latest row count
	private static Map<String, Long> estimatedRowCounts = 
		Collections.synchronizedMap( new CachedHashMap<String, Long>( GaianTable.QRY_METADATA_CACHE_SIZE ) );

//	private static final ResultSetMetaData rsmd = new SearchMetaData();
	private ResultSetMetaData rsmd = null;

//	public DocumentFinder() {
//	}
	
	/**
	 * @param searchString
	 * @throws Exception 
	 */
	public DocumentFinder(String searchString) throws Exception {
		super();
		logger.logInfo("Entered DocumentFinder(searchString) constructor");
		rsmd = new GaianResultSetMetaData( "DNUM INT, DURL VARCHAR(100), DINFO CLOB(32K)" );
		this.searchString = searchString;
	}
	
	public DocumentFinder(String searchString, String collections) throws Exception {
		super();
		logger.logInfo("Entered DocumentFinder(searchString, collections) constructor");
		rsmd = new GaianResultSetMetaData( "DNUM INT, DURL VARCHAR(100), DINFO VARCHAR(100)" );
		this.searchString = searchString;
		this.collections = collections;
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setArgs(java.lang.String[])
	 */
	public void setArgs(String[] args) {
		
		if ( 0 < args.length )
			this.searchString = args[0];
		
		if ( 1 < args.length )
			this.searchPort = args[1];
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setExtractConditions(org.apache.derby.iapi.store.access.Qualifier[][], int[])
	 */
	public void setExtractConditions(Qualifier[][] qualifiers, int[] logicalProjection, int[] columnsMapping) {
		// No need to set qualifiers - the search string acts as a filter instead.
		// Also ignore mappedColumns as column names are expected to be the same in the logical table. 
	}
	
//	private void setSearchString( String searchString ) {
//		this.searchString = searchString;		
//	}
	
//	public DocumentFinder(final String searchString, String nodeDefName, GaianResultSetMetaData logicalTableRSMD) throws SQLException {
//		super( nodeDefName, logicalTableRSMD );
//		this.searchString = searchString;
//	}
//	
//	public IFastPath execute(Qualifier[][] qualifiers, int[] projectedColumns) {
//		
//		DocumentFinder f = new DocumentFinder(searchString);
//	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#executeAsFastPath()
	 */
	public boolean executeAsFastPath() throws StandardException, SQLException {
		
		logger.logInfo("Entered executeAsFastPath()");
		
		if ( null != resultRows ) {
			logger.logInfo("Document results already in memory");
			return true;
		}
		
		index = 0;
		
		resultRows = new Vector<DataValueDescriptor[]>();
		
		logger.logInfo("Setting up parameters");

		String hostname = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindHostname" );
//		if ( null == collections )
//			collections = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindCollections" );
		int maxResults = 100;
		try {
			String s = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindMaxResults" );
			if ( null == s ) logger.logInfo("No maxResults property defined, using default value " + maxResults);
			else maxResults = Integer.parseInt( s );
		} catch ( NumberFormatException e ) {
			logger.logWarning(GDBMessages.DSWRAPPER_MAX_RESULTS_PROP_ERROR, "Unable to read integer value from maxResults property: " + e);
		}
		
		if ( null == hostname ) hostname = "localhost";
		
		if ( null == searchPort )
			searchPort = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindRestPort" );
		if ( null == searchPort )
			searchPort = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindPort" );
		
		String applicationName = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindEEUser" );
		String applicationPassword = GaianDBConfig.getVTIProperty( DocumentFinder.class, "omnifindEEPassword" );

//		// To build doc id hash values on decoded paths, set this property to anything but 0, false or an empty string.
//		String hdp = GaianDBConfig.getVTIProperty( DocumentFinder.class, "hashDecodedPaths" );
//		if ( null != hdp ) hdp = hdp.trim();
//		boolean hashDecodedPaths = null != hdp && !"".equals(hdp) && !"0".equals(hdp) && !"false".equalsIgnoreCase(hdp);
		
		if ( null == searchPort ) {
			logger.logWarning(GDBMessages.DSWRAPPER_OMNIFINDRESTPORT_NOT_SET, "omnifindRestPort VTI property must be set in gaiandb_config.properties (aborting): e.g. " + 
					DocumentFinder.class.getName() + ".omnifindRestPort=8080");
			return true;
		}
		
		logger.logInfo("Calling document search using hostname: " + hostname + ", port: " + searchPort + 
				", collections: " + collections + ", searchString: " + searchString + ", maxResults: " + maxResults + 
				", usr: " + applicationName + ", pwd: " + (null==applicationPassword?null:"<hidden value not null>"));
				
		if ( null == applicationName || null == applicationPassword )
			SearchREST.retrieveDocumentReferences(
				resultRows, hostname, searchPort, collections, searchString + "&results=" + maxResults );
		else {
			try {
				getClass().getClassLoader().loadClass(SearchSIAPI.class.getName());
				// Referenced by reflection in SearchEnterprise - and a member of esapi.jar which must be in CLASSPATH
				getClass().getClassLoader().loadClass(com.ibm.es.api.search.RemoteSearchFactory.class.getName());
			} catch ( Throwable e ) {
				logger.logWarning(GDBMessages.DSWRAPPER_OMNIFIND_VERIFY_ERROR, "Could not query Omnifind Enterprise (install siapi.jar and esapi.jar in GaianDB install dir): " + e);
				return true;
			}

			SearchSIAPI.retrieveDocumentReferences(
				resultRows, hostname, searchPort, collections, searchString, maxResults, applicationName, applicationPassword );
		}
			
		logger.logInfo("Document DNUMs loaded in memory: " + resultRows.size());
				
		return true;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#nextRow(org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		
		if ( null == resultRows ) {
			logger.logWarning(GDBMessages.DSWRAPPER_NO_ROWS, "No rows to fetch, "+this.getClass().getSimpleName()+" did not execute - resultRows is null");
			return IFastPath.SCAN_COMPLETED;
		}
		
		if ( index >= resultRows.size() ) {
			// Keep track of row count for this search string
			Long previousCount = estimatedRowCounts.get( searchString );
			if ( null == previousCount || index > previousCount.longValue() )
				estimatedRowCounts.put( searchString, new Long( index ) );
			index = 0;
			return IFastPath.SCAN_COMPLETED;
		}
		
		DataValueDescriptor[] row = resultRows.get(index++);
		
		dvdr[0].setValue( row[0] );
		dvdr[1].setValue( row[1] );
		dvdr[2].setValue( row[2] );
		
		return IFastPath.GOT_ROW;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#currentRow(java.sql.ResultSet, org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public void currentRow(ResultSet arg0, DataValueDescriptor[] arg1) throws StandardException, SQLException {
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#rowsDone()
	 */
	public void rowsDone() throws StandardException, SQLException {
		close();
	}
	
    /**
     *  Provide the metadata for the query against the given table. Cloudscape
	 *  calls this method when it compiles the SQL-J statement that uses this
	 *  VTI class.
     *
     *   @return the result set metadata for the query
	 * 
     *   @exception SQLException thrown by JDBC calls
     */
    public ResultSetMetaData getMetaData() throws SQLException {
		
		logger.logInfo("DocumentFinder.getMetaData(): " + rsmd);
		return rsmd;
    }

	/**
	 * Explicitly closes this PreparedStatement class. (Note: Cloudscape
	 * calls this method only after compiling a SELECT statement that uses
	 * this class.)<p>
	 *
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() {
		logger.logInfo("DocumentFinder.close()");
		reinitialise();
	}
	
	@Override
	public boolean reinitialise() {
		logger.logInfo("DocumentFinder.reinitialise()");
		if ( null != resultRows ) {
			resultRows.clear();
			resultRows.trimToSize();
			resultRows = null;
		}
		index = 0;
		return true;
	}

//	/**
//	 * Return the number of columns in the user-specified table.
//	 *
//	 * @exception SQLException	Thrown if there is an error getting the
//	 *							metadata.
//	 */
//	public int getColumnCount() throws SQLException {
//
//		logger.logInfo("!!!!!!!!!!!!DocumentFinder.getColumnCount()!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//		return rsmd.getColumnCount(); //DataSourcesManager.getLogicalTableRSMD( logicalTable ).getColumnCount();
//	}
	

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedRowCount(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
	
		Long l = estimatedRowCounts.get( searchString );
		double val = null == l ? 1000 : l.doubleValue() * 10; // multiply by 10 as this is XML!
		
		logger.logInfo("getEstimatedRowCount() returning: " + val);
		return val;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedCostPerInstantiation(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException {
		int rc = 0;
		logger.logInfo("getEstimatedCostPerInstantiation() returning: " + rc);
		return rc;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#supportsMultipleInstantiations(org.apache.derby.vti.VTIEnvironment)
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException {
		// For us to return true, we must ensure that results can be fetched repeatedly.
		// i.e. that the cursor is set back to the first row every time we reach the end of it.
		boolean rc = true;
		
		logger.logInfo("supportsMultipleInstantiations() returning: " + rc);
		return rc;
	}

	public boolean fetchNextRow(DataValueDescriptor[] row) throws Exception {
		return IFastPath.GOT_ROW == nextRow(row);
	}

	public int getRowCount() throws Exception {
		return resultRows.size();
	}

	public boolean isScrollable() {
		return true;
	}
	
	@Override
	public int getResultSetType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}
	
	@Override
	public int getResultSetConcurrency() throws SQLException {
		return ResultSet.CONCUR_UPDATABLE;
	}

	public boolean isBeforeFirst() {
		return 0 == index;
	}
}
