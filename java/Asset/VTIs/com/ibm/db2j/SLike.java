/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.searchapis.SLikeResultsParser;


/**
 * @author Edd Biddle
 */

/*
 * The XML-RPC functionality used by this class has been commented out so that Gaian can be shipped without 
 * an external requirement for the  xmlrpc-1.1.jar file 
 * 
 *  To re-enable uncomment the three XML-RPC Code commented out blocks
 *  
 *  The following classpath line in the Gaian launchpad needs to be added
 *  
 * 		SET CLASSPATH="%CLASSPATH%;%GDBH%\xmlrpc-1.1.jar
 * 
 * The following parameters need to be set in the Gaian Config properties file
 * 
 * 		com.ibm.db2j.slike.language=<Annotator language> (default) en
 * 		com.ibm.db2j.slike.port=<RPC listening Port> 
 *		com.ibm.db2j.slike.host=<host name>
 */

public class SLike extends VTI60 implements VTICosting, IFastPath, GaianChildVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";

	private static final Logger logger = new Logger( "SLike", 20 );
	private ResultSetMetaData rsmd = null;
	private Vector<DataValueDescriptor[]> resultRows = null;
	private int index = 0;

	private String term = null;
	private String language = null;
	private Integer port = 9877;
	private String host = null;
	
	public SLike(String term) throws Exception{
		logger.logInfo("Entered SLike(term) constructor");
//		rsmd = new GaianResultSetMetaData( "HEAD VARCHAR(100), HEAD_TYPE VARCHAR(40)" );
		rsmd = new GaianResultSetMetaData( "HEAD_TYPE VARCHAR(40)" );
		this.term = term;
	}
	
	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException {
		int rc = 0;
		logger.logInfo("getEstimatedCostPerInstantiation() returning: " + rc);
		return rc;
	}

	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
		int rc = 0;
		logger.logInfo("getEstimatedCostPerInstantiation() returning: " + rc);
		return rc;
	}

	public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException {
		boolean rc = true;
		
		logger.logInfo("supportsMultipleInstantiations() returning: " + rc);
		return rc;
	}

	public void currentRow(ResultSet arg0, DataValueDescriptor[] arg1) throws StandardException, SQLException {
	}

	public boolean executeAsFastPath() throws StandardException, SQLException {
		logger.logInfo("Entered executeAsFastPath()");
		
		if ( null != resultRows ) {
			logger.logInfo("SLike results already in memory");
			return true;
		}

		index = 0;

		resultRows = new Vector<DataValueDescriptor[]>();
		
		logger.logInfo("Setting up parameters");

		this.language = GaianDBConfig.getVTIProperty( SLike.class, "language" );
		this.host = GaianDBConfig.getVTIProperty( SLike.class, "host" );
		try {
			this.port = Integer.parseInt(GaianDBConfig.getVTIProperty( SLike.class, "port" )); 
		}
		catch(Exception e){
			this.port = 9877;
		}
		if(this.language == null){
			this.language = "en";
		}
		
		if(this.host == null){
			this.host = "127.0.0.1";
		}

		logger.logInfo("Calling SLike using term: " + this.term + ", port: " + this.port + ", host: " + this.host + ", language: " + this.language);

/*
 * XML-RPC Code commented out
 * 		
		XmlRpcClientLite rpcClientLite = null;
		try {
			rpcClientLite = new XmlRpcClientLite(this.host, this.port.intValue());
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

 */
		Vector<String> params = new Vector<String>();
		params.add(term);
		params.add(language);
		
		byte[] result = null;
/*
 * XML-RPC Code commented out
 * 
 
		try {
			result = (byte[]) rpcClientLite.execute("rules.processText", params);
		} catch (XmlRpcException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

*/
		SLikeResultsParser rp = new SLikeResultsParser(result);
		rp.parseResults();
		resultRows = rp.getParsedResults();
		
		logger.logInfo("Semantic Like values loaded in memory: " + resultRows.size());
		
		return true;
	}

	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		if ( index >= resultRows.size() ) {
//			index = 0;
			this.close();
			return IFastPath.SCAN_COMPLETED;
		}
		
		DataValueDescriptor[] row = resultRows.get(index++);
		
		dvdr[0].setValue( row[0] );
//		dvdr[1].setValue( row[1] );
		
		return IFastPath.GOT_ROW;
	}

	public void rowsDone() throws StandardException, SQLException {
		close();
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

	public void setExtractConditions(Qualifier[][] qualifiers, int[] projectedColumns, int[] physicalColumnsMapping) throws Exception {
		// No need to set qualifiers - the search string acts as a filter instead.
		// Also ignore mappedColumns as column names are expected to be the same in the logical table. 
	}
	
    public ResultSetMetaData getMetaData() throws SQLException {
		logger.logInfo("SLike.getMetaData(): " + rsmd);
		return rsmd;
    }

    public void setArgs(String[] args) {
		
		if ( 0 < args.length )
			this.term = args[0];
		
		if ( 1 < args.length )
			this.language = args[1];

		if ( 2 < args.length )
			this.port = Integer.parseInt(args[2]);
		
		if ( 3 < args.length )
			this.host = args[3];

    }
    
	public void close() {
		logger.logInfo("SLike.close()");
		reinitialise();
	}

	@Override
	public boolean reinitialise() {
		logger.logInfo("SLike.reinitialise()");
		if ( null != resultRows ) {
			resultRows.clear();
			resultRows.trimToSize();
			resultRows = null;
		}
		index = 0;
		return true;
	}
	
	public boolean isBeforeFirst() {
		return 0 == index;
	}
}
