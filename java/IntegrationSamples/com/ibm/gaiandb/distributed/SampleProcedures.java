package com.ibm.gaiandb.distributed;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianDBProcedureUtils;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.tools.SQLRunner;

public class SampleProcedures extends GaianDBProcedureUtils {

	private static final Logger logger = new Logger( "SampleProcedure", 30 );
	
//	static final String PROCEDURE_SQL = ""
//	+ "!DROP PROCEDURE RUNSQL;!CREATE PROCEDURE RUNSQL(sql_expression "+Util.XSTR+", rdbmsConnectionID "+Util.XSTR+")"
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.runSQL'"	
//	;
	
	public static void runSQL( String sqlOrFile, String cid, ResultSet[] rs ) throws Exception {

		Connection c = null;
		try {
			if ( null == sqlOrFile || 1 > (sqlOrFile = sqlOrFile.trim()).length() ) return;
			if ( null != cid ) { cid = cid.trim(); if ( 1 > cid.length() || "LOCALDERBY".equals(cid) ) cid = null; }
			
			final String gdbWorkspace = GaianNode.getWorkspaceDir();
			final String fPath = null == gdbWorkspace || Util.isAbsolutePath(sqlOrFile) ? sqlOrFile : gdbWorkspace+"/"+sqlOrFile;
			
			if ( 1 == Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes(sqlOrFile, ';').length
					&& false == new File(fPath).exists() ) {
				
				// Single SQL Statement
				if ( null==cid ) {
					Statement stmt = getDefaultDerbyConnection().createStatement();
					rs[0] = stmt.execute( sqlOrFile ) ? stmt.getResultSet() : getResultSetFromQueryAgainstDefaultConnection(
							"SELECT " + stmt.getUpdateCount() + " UPDATE_COUNT FROM SYSIBM.SYSDUMMY1");
				} else {
					// Use System.currentTimeMillis() so the query is always different - this avoids it being cached.. so we see Exceptions if they occur.
					rs[0] = getResultSetFromQueryAgainstDefaultConnection(
						"select * from new com.ibm.db2j.GaianQuery('"+Util.escapeSingleQuotes(sqlOrFile)+"','','SOURCELIST="+Util.escapeSingleQuotes(cid)+"') GQ");
//					DataSourcesManager.clearSubQueryMetaData(sqlOrFile, cid+"falsefalse");
				}
				
			} else { // SQL script, or multiple SQL statements
				
				if ( null == cid ) c = getDefaultDerbyConnection();
				else {
					String connectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString(cid);
					c = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getConnection();
//					c =	DataSourcesManager.getPooledJDBCConnection(connectionDetails, DataSourcesManager.getSourceHandlesPool(connectionDetails));
				}
				
				SQLRunner sqlr = new SQLRunner(c); // Use SQLRunner to process a script file or a list of statements - then return summary
				sqlr.processSQLs( "-quiet" );
				sqlr.processSQLs( "-t" ); // explicitly use semi-colon as delimiter
				String summaryInfo = sqlr.processSQLs( sqlOrFile );
				rs[0] = getResultSetFromQueryAgainstDefaultConnection("SELECT " + summaryInfo + " FROM SYSIBM.SYSDUMMY1");
			}
			
		} catch ( Throwable e ) {
			String msg = Util.getGaiandbInvocationTargetException(e);
			msg = null == msg ? Util.getStackTraceDigest(e) /*e.toString()*/ : msg.substring(msg.indexOf(GaianTable.IEX_PREFIX)+GaianTable.IEX_PREFIX.length()).trim();
			rs[0] = getResultSetFromQueryAgainstDefaultConnection("SELECT '"+Util.escapeSingleQuotes(msg)+"' SQL_FAILURE FROM SYSIBM.SYSDUMMY1");
		} finally {
			logger.logInfo("Closing connection");
			rs[0].getStatement().getConnection().close(); // must be done for derby procedure to work
			if ( null != cid && null != c ) // Return connection to pool (may get closed immediately if not referenced by a data source or sourcelist)
				DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getRDBConnectionDetailsAsString(cid) ).push(c);
		}
	}
}
