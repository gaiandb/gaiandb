package com.ibm.gaiandb.distributed;

import java.io.File;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;

import org.apache.derby.impl.jdbc.EmbedConnection;

import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianDBProcedureUtils;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;

public class SampleUDFs extends GaianDBProcedureUtils {

	private static final Logger logger = new Logger( "SampleUDFs", 30 );
	
	public final String UDF_FHE_SEARCH = ""
	+ "!DROP FUNCTION FHE_SEARCH;!CREATE FUNCTION FHE_SEARCH(BYTES_URI "+Util.XSTR+") RETURNS BLOB(2G)"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.distributed.fheSearch'";
	
	public static Blob fheSearch( final String bytesURI ) throws Exception {

		// The initial URI scheme targets a file through GaianNode using syntax: "<NodeID> <FilePath>"
		// New schemes could be used in future, e.g. to target a db table or a web location.
		
		int idx = bytesURI.indexOf(' ');
		if ( 0 > idx ) throw new Exception(
				"FHE_SEARCH() argument 'bytesURI' does not conform to syntax: '<NodeID> <FilePath>'");
		
		final String nodeID = bytesURI.substring(0,idx);
		final String filePath = bytesURI.substring(idx+1);
		
		idx = filePath.lastIndexOf('/');
		final String fileName = 0 > idx ? filePath : filePath.substring( idx+1 );
		
		Connection c = null;
		
		try {
			final String connectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString(nodeID);
			c = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getConnection();
			
			ResultSet rs = c.createStatement().executeQuery("select getFileBZ(''"+filePath+"'') fzbytes from sysibm.sysdummy1");

			byte[] bytes = rs.getBytes(1);
			
			File file = new File(fileName);
			System.out.println("Received file: " + fileName + ", size: " + file.length());
			writeToFileAfterUnzip( file, bytes );

			rs.close();
			
			/** Call to FHE code **/
			Process process = new ProcessBuilder("sleep", "3").start();
			try { process.waitFor(); }
			catch (InterruptedException e) { e.printStackTrace(); }
			/** Call to FHE code **/
			
			file = new File(fileName);
			System.out.println("Resulting FHE bytes file: " + fileName + ", size: " + file.length());
			
			bytes = readAndZipFileBytes( file );
			// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
			Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
			blob.setBytes(1, bytes);
			
			return blob;
		}
		catch (Exception e) { throw new Exception("Unable to get bytes from node: " + nodeID + ", file: " + filePath + ": " + e); }
		finally {
			logger.logInfo("Closing connection");
			if ( null != nodeID && null != c ) // Return connection to pool (may get closed immediately if not referenced by a data source or sourcelist)
				DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getRDBConnectionDetailsAsString(nodeID) ).push(c);
		}
	}
}
