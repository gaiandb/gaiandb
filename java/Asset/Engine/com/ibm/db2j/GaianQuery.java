/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.util.Arrays;

import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */
public class GaianQuery extends GaianTable {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "GaianQuery", 20 );
	
	private String queryArguments = null;
	private String[] targetCIDs = null; // target RDBMS connection IDs
	
	private String splitColumnRanges = null;
	
	private static final String SOURCELISTARG_KEY = "SOURCELIST";
	private static final String SPLITCOLUMN_KEY = "SPLITCOLUMN";
		
	public GaianQuery( String sqlQuery ) throws Exception {
		this( sqlQuery, null, null, null, null ); //, null, new Integer(-1)); //, new Integer(-1) );
	}
	
//	public GaianQuery( String sqlQuery, Integer maxSteps ) throws Exception {
//		this( sqlQuery, null, null, null, null, null, new Integer(-1), maxSteps);
//	}
	
	public GaianQuery( String sqlQuery, String tableArguments ) throws Exception {
		this( sqlQuery, tableArguments, null, null, null ); //, null, new Integer(-1)); //, new Integer(-1) );
	}
	
//	public GaianQuery( String sqlQuery, String tableArguments, Integer maxSteps ) throws Exception {
//		this( sqlQuery, tableArguments, null, null, null, null, new Integer(-1), maxSteps );
//	}
	
	public GaianQuery( String sqlQuery, String tableArguments, String queryArguments ) throws Exception {
		this( sqlQuery, tableArguments, queryArguments, null, null ); //, null, new Integer(-1)); //, new Integer(-1) );
	}
	
	public GaianQuery( String sqlQuery, String tableArguments, String queryArguments, String tableDef ) throws Exception {
		this( sqlQuery, tableArguments, queryArguments, tableDef, null ); //, null, new Integer(-1)); //, new Integer(-1) );
	}
	
//	public GaianQuery( String sqlQuery, String tableArguments, String queryArguments, Integer maxSteps ) throws Exception {
//		this( sqlQuery, tableArguments, queryArguments, null, null, null, new Integer(-1), maxSteps );
//	}
		
	public GaianQuery( String sqlQuery, String tableArguments, String queryArguments, String tableDef, 
			String forwardingNode) throws Exception { //, String queryID, Integer steps) throws Exception { //, Integer maxSteps) throws Exception {
		super( sqlQuery, tableArguments, tableDef, forwardingNode ); //, queryID, steps); //, maxSteps );
		this.queryArguments = null == queryArguments || 1 > queryArguments.trim().length() ? null : queryArguments.trim();
		getQueryDetails().put( QRY_IS_GAIAN_QUERY, QRY_IS_GAIAN_QUERY );
		
		ltSignature = this.logicalTableName + ( this.tableArguments + this.queryArguments + this.tableDefinition + this.forwardingNode ).replaceAll("\\s", " ");
		
//		logger.logInfo("tableDefinition: " + this.tableDefinition);
		
		if ( null == this.tableDefinition ) { // note do not use just 'tableDef', must use GaianTable's tabeDefinition
			// We have to validate that the data source exists in one of our source list sources,
			// so that we can propagate the table def - if not the Exception MUST be thrown here - so that
			// the resulting InvocationTargetException can carry our exception back.
			logger.logInfo( "Setting up sub query meta data using subquery and first data source (or local gaiandb if none was specified)" );
			targetCIDs = getQueriedDBs();
			logger.logInfo( "Gaian Query Source List = " + Arrays.asList(targetCIDs) );
			
			// NOTE: THIS CALL MAY THROW AN EXCEPTION: If the source/table is not defined or if another Exception occurs
			// This will be seen in the InvocationTargetException that is propagated back to the client.
			try { logicalTableRSMD = DataSourcesManager.deriveMetaDataFromSubQuery( sqlQuery, targetCIDs[0], targetCIDs[0]+withProvenance+""+isExplain ); }
			catch ( Exception e ) {
				final String msg = "Unable to resolve result structure for subquery \"" + sqlQuery + "\" on RDBMS db: "+targetCIDs[0]+" (aborting query)";
				logger.logWarning(GDBMessages.ENGINE_SUBQUERY_ERROR, msg + ": " + e);
				throw new Exception(IEX_PREFIX + " " + msg + ": " + e.getMessage(), e);
			}
		}
	}
	
	public String getQueryArguments() {
		if ( null==queryArguments ) return "";
		return queryArguments;
	}
	
	// The function below is used to pick out a different range of rows on different nodes, e.g. syntax wd be:
	// select * from new com.ibm.db2j.GaianQuery(
	// 		'SELECT item, price FROM stock', '', 'SPLITCOLUMN ID host1 0 10 host2 20 30 host3 40 50') Q
	public String getSplitColumnRangeSQL() {
		if ( null==splitColumnRanges ) return null;
//		String[] elmts = splitColumnRanges.split(" ");
//		return elmts[0] + " between " + elmts[1] + " and " + elmts[2];
		
		String[] elmts = splitColumnRanges.split(" ");
		
		for ( int i=1; i<elmts.length; i++ )
			if ( elmts[i].equals(GaianDBConfig.getGaianNodeID()) )
				return elmts[0] + " between " + elmts[i+1] + " and " + elmts[i+2];
		
		return null;
	}
	
	private String[] getQueriedDBs() {
		
		String sourceList = null;
		
		if ( null != queryArguments ) {
			String[] options = Util.splitByCommas( queryArguments );
			for (int i=0; i<options.length; i++) {
				String option = options[i];
				int idx = option.indexOf('=');
				if ( -1 < idx ) {
					String key = option.substring(0,idx).trim(), val = option.substring(idx+1).trim();
					// Validate key -
					// only use so far is for SOURCELIST, which is used to specify more than just the local Derby database 
					// to run the query against 
					if ( key.equals( SOURCELISTARG_KEY ) )
						sourceList =  val;
					else if ( key.equals( SPLITCOLUMN_KEY ) )
						splitColumnRanges = val;
					else logger.logInfo("Ignoring unknown GaianQuery arg: " + key);
				} else if ( option.equalsIgnoreCase("unzip") )
					unzipLobs = true;
			}
		}
		
		return GaianDBConfig.getSourceListSources( sourceList );
	}
	
	protected void setupMetaDataAndDataSourcesArray(boolean isMetaDataLookupOnly) throws Exception {
		
		if ( null == logicalTableRSMD ) {
			// We *MUST* have a table definition defined (otherwise an exception would have aborted execution in the constructor)			
			logger.logInfo( "Setting up sub query meta data using tableDefinition: " + tableDefinition );
			logicalTableRSMD = DataSourcesManager.deriveMetaDataFromTableDef( tableDefinition, null, withProvenance+""+isExplain );
		}
		
//		if ( isPropagatedQuery ) logicalTableRSMD.includeNullColumns();
//		else logicalTableRSMD.excludeNullColumns();
		
		if ( !isMetaDataLookupOnly ) {
			logger.logInfo( "Setting up sub query vti array" );

//			if ( null == dataSources ) {
				// we didnt need the data source list until now because we had the table def to give us meta-data - but now we do
				targetCIDs = getQueriedDBs();
				logger.logInfo( "Gaian Query Source List = " + Arrays.asList(targetCIDs) );
//			}
			
//			if ( null != queryArguments && splitColumnRanges != null ) {
//				
//				int start = queryArguments.indexOf( splitColumnRanges );
//				start = queryArguments.indexOf(' ', start);
//				int end = queryArguments.indexOf(' ', start+1);
//				end = queryArguments.indexOf(' ', end+1);
//				end = queryArguments.indexOf(' ', end+1);
//				
//				// Delete the first range from the query arguments, which will be forwarded on with the query.
//				// Note that we still have a splitColumnRanges argument starting with the range we just deleted, so we can use it.			
//				queryArguments = new StringBuffer(queryArguments).delete(start, end).toString();
//			}
			
			allLogicalTableVTIs = DataSourcesManager.constructDynamicDataSources(
					DataSourcesManager.SUBQUERY_PREFIX, logicalTableRSMD, targetCIDs );
			logger.logInfo( "Built subquery vtis: " + Arrays.asList(allLogicalTableVTIs) );
		}
	}
}
