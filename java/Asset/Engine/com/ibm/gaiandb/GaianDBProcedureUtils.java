/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.ibm.gaiandb.diags.GDBMessages;

public class GaianDBProcedureUtils extends GaianDBConfig {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	private static final Logger logger = new Logger( "GaianDBProcedureUtils", 25 );
	
	// Standard VARCHAR lengths
	static final String TSTR = Util.TSTR;
	static final String SSTR = Util.SSTR;
	static final String MSTR = Util.MSTR;
	static final String LSTR = Util.LSTR;
	static final String VSTR = Util.VSTR;
	static final String XSTR = Util.XSTR;
	
	// Note new api calls must not exceed 20 chars in length as proc name alias is cast to a TSTR for readability/usability
	
	// read only functions and procedures
	static final String listspfs = "listspfs";
	static final String listapi = "listapi";
	static final String listconfig = "listconfig";
	static final String listwarnings = "listwarnings";
	static final String listwarningsx = "listwarningsx";
	static final String listrdbc = "listrdbc";
	static final String listlts = "listlts";
	static final String listltmatches = "listltmatches";	
	static final String listds = "listds";
	static final String listderbytables = "listderbytables";
	static final String listexplain = "listexplain";
	static final String listqueries = "listqueries";
	static final String listflood = "listflood";
	static final String listnodes = "listnodes";

	static final String logtail = "logtail";
	static final String gpublickey = "gpublickey";
	static final String getconfigproperty = "getconfigproperty";
	static final String getnodes = "getnodes";
	static final String gdb_node = "gdb_node";
	static final String getnodecount = "getnodecount";
	static final String ltmatch = "ltmatch";
	static final String getlts = "getlts";
	
//	General utility functions are missing below (e.g. jhash,...)
//	static final Set<String> readOnlySPFs = new HashSet<String>( Arrays.asList(
//			listspfs, listapi, listconfig, listwarnings, listwarningsx, listrdbc, listlts, listltmatches, listds, listderbytables, listexplain, 
//			listflood, listnodes, logtail, gpublickey, getnodes, getnodecount, ltmatch, gdbtrigger, getlts
//	) );
	
	// write + execute functions and procedures
	static final String setrdbc = "setrdbc";
	static final String setlt = "setlt";
	static final String setltforrdbtable = "setltforrdbtable";
	static final String setltforfile = "setltforfile";
	static final String setltforexcel = "setltforexcel"; // Denis
	static final String setltforws = "setltforws";
	static final String setltfornode = "setltfornode";
	static final String setltconstants = "setltconstants";
	static final String setnodeconstants = "setnodeconstants";
	static final String setdsrdbtable = "setdsrdbtable";
	static final String setdslocalderby = "setdslocalderby";
	static final String setdsfile = "setdsfile";
	static final String setdsvti = "setdsvti";
	static final String setdsexcel = "setdsexcel";
	static final String removerdbc = "removerdbc";
	static final String removelt = "removelt";
	static final String removeds = "removeds";
	static final String gconnect = "gconnect";
	static final String gconnectx = "gconnectx";
	static final String gdisconnect = "gdisconnect";
	static final String setminconnections = "setminconnections";
	static final String setdiscoveryhosts = "setdiscoveryhosts";
	static final String setdiscoveryip = "setdiscoveryip";
	static final String setmaxpropagation = "setmaxpropagation";
	static final String setmaxpoolsizes = "setmaxpoolsizes";
	static final String setmaxcachedrows = "setmaxcachedrows";
	static final String setloglevel = "setloglevel";
	static final String setsourcelist = "setsourcelist";
	static final String setmsgbrokerdetails = "setmsgbrokerdetails";
	static final String setconfigproperty = "setconfigproperty";
	static final String setConfigProperties = "setConfigProperties";
	static final String setaccessclusters = "setaccessclusters";
	static final String cancelquery = "cancelquery";
	static final String gkill = "gkill";
	static final String gkillnodes = "gkillnodes";
	static final String gkillall = "gkillall";
	static final String addgateway = "addgateway";
	static final String removegateway = "removegateway";
	static final String setuser = "setuser";
	static final String removeuser = "removeuser";
	
	private static final Set<String> configurationAPIs = new HashSet<String>( Arrays.asList(
			setrdbc, setlt, setltforrdbtable, setltforfile, setltforexcel, setltfornode, setltconstants, setnodeconstants, 
			setdsrdbtable, setdslocalderby, setdsfile, setdsvti, setdsexcel, removerdbc, removelt, removeds, gconnect,gconnectx, gdisconnect,
			setminconnections, setdiscoveryhosts, setdiscoveryip, setmaxpropagation, setmaxpoolsizes,
			setmaxcachedrows, setloglevel, setsourcelist, setmsgbrokerdetails, setconfigproperty, setaccessclusters, gkill, gkillnodes, gkillall,
			addgateway, removegateway, setuser, removeuser
	) );

	// Utility ResultSet request identifiers
	static final String FILESTATS = "FILESTATS";
	
//	private static void logEnteredMsg( String spf ) {
//		logEnteredMsg(spf, null);
//	}
	
	static void logEnteredMsg( String spf, List<String> args ) {
		
		if ( null == args ) args = Arrays.asList();
		StringBuffer sb = new StringBuffer("(");
		Iterator<String> i = args.iterator();
		if ( i.hasNext() ) sb.append("'" + i.next() + "'");
		while ( i.hasNext() ) sb.append(", '" + i.next() + "'");
		sb.append(')');
		
		String prefix = Logger.sdf.format(new Date(System.currentTimeMillis())) + " ---------------> ";
    	logger.logImportant( Logger.HORIZONTAL_RULE + "\n" + prefix + "API call: " + spf + sb + "\n");
	}
	
	static void apiStart( String spf ) throws Exception {
		apiStart(spf, null);
	}
	
	static void apiStart( String spf, List<String> args ) throws Exception {
		
		logEnteredMsg( spf, args );
		if ( !isAllowedAPIConfiguration() && configurationAPIs.contains(spf) ) {
			String warning = "SQL API command '"+spf+"' is not allowed. To allow configuration management via the SQL API, set this property in " + 
								getConfigFileName() + ": " + ALLOW_SQL_API_CONFIGURATION + "=TRUE" +
								". To prevent others from updating your configuration, set a different 'GAIAN_NODE_USR' property and its associated password in derby.properties";
			logger.logWarning(GDBMessages.CONFIG_SQL_API_UNAUTHORISED,  warning);
			throw new Exception(warning);
		}
	}
	
	public static Connection getDefaultDerbyConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:default:connection"); //, getGaianNodeUser(), getGaianNodePassword());
//		return DriverManager.getConnection("jdbc:derby://localhost:6414/gaiandb", getGaianNodeUser(), getGaianNodePassword());
	}
	
	public static ResultSet getResultSetFromQueryAgainstDefaultConnection( String sql ) throws SQLException {

		Connection c = getDefaultDerbyConnection();
//		System.out.println(sql);
		ResultSet rs = c.createStatement().executeQuery( sql );
		return rs;
	}
	
	static synchronized void setConfigProperties( String sqlQueryReturningPropertyKeysAndValues, Connection conn ) throws Exception {
		
    	if ( null == sqlQueryReturningPropertyKeysAndValues ) return;
    	
    	Map<String, String> updates = new LinkedHashMap<String, String>();

		ResultSet rs = conn.createStatement().executeQuery( sqlQueryReturningPropertyKeysAndValues );
		while ( rs.next() ) updates.put( rs.getString(1).trim(), rs.getString(2).trim() );
    	
		// TODO: check if property name relates to a transient data source - e.g. if it exists in transient set.
		// If so, change the transient one, update the upr as well and reload the data source (don't reload if it was a VTI Property)
		// Blocked on this for now as cant resolve ltname and dsname deterministically...
		for ( String propertyName : updates.keySet() )
			if ( inMemoryDataSourceProperties.contains(propertyName) )
				throw new Exception("Cannot update transient data source property: '" + propertyName +
					"'. Please remove the data source first with removeds().");
		
    	persistAndApplyConfigUpdates(updates);
    	DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}
	
//	private static ResultSet getDataAsResultSet( List<String[]> rows, String[] colAliases, boolean[] isQuoted ) throws SQLException {		
//		StringBuffer sb = new StringBuffer();
//		
//		for ( String[] row : rows )
//			for ( int i=0; i<row.length; i++ )
//				sb.append( ( 0<i ? " UNION ALL " : "" ) +
//						"SELECT " + (isQuoted[i]?"'":"") + row[i] + (isQuoted[i]?"' ":" \"") + colAliases[i] + "\" FROM SYSIBM.SYSDUMMY1" );
//		
//		return getResultSetFromQueryAgainstDefaultConnection( sb.toString() );
//	}
	
	
//	public static byte[] readFileBytes( File file ) is equivalent to: 		Util.getFileBytes(file);
//	public static void writeBytesToFile( byte[] bytes, File file ) is just: Util.copyBinaryData(new ByteArrayInputStream(bytes), new FileOutputStream(file));
	
	public static byte[] readAndZipFileBytes( File file ) throws Exception {	
		try {
			if ( file.isDirectory() ) throw new Exception("File is a directory");
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Util.copyBinaryData(new FileInputStream(file), new GZIPOutputStream(baos));
			byte[] bytes = baos.toByteArray();
			baos.close(); // other streams are closed
			
			return bytes;
		}
		catch (Exception e) { throw new Exception("Cannot read/zip bytes from '" + file.getName() + "': " + e); }
	}
	
	public static void writeToFileAfterUnzip( File file, byte[] zippedBytes ) throws Exception {
		
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(zippedBytes);
			Util.copyBinaryData(new GZIPInputStream(is), new FileOutputStream(file));
			is.close(); // other streams are closed
		}
		catch (Exception e) { throw new Exception("Unable to unzip and write to file " + file.getName() + ": " + e); }
	}

}
