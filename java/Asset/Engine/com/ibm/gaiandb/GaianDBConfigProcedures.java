/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;



import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBit;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLTimestamp;

import com.ibm.db2j.FileImport;
import com.ibm.db2j.GExcel;
import com.ibm.db2j.GaianTable;
import com.ibm.db2j.GenericWS;
import com.ibm.gaiandb.DataSourcesManager.RDBProvider;
import com.ibm.gaiandb.DataSourcesManager.RecallingStack;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.webservices.parser.data.GenericWsLogicalTableGenerator;
import com.ibm.gaiandb.webservices.scanner.FormatSpecifierInputStream;
import com.ibm.gaiandb.webservices.scanner.WsDataFormat;
import com.ibm.gaiandb.webservices.ws.PostRestWS;
import com.ibm.gaiandb.webservices.ws.RestWS;
import com.ibm.gaiandb.webservices.ws.SoapWS;
import com.ibm.gaiandb.webservices.ws.WebService;

/**
 * @author DavidVyvyan
 */
public class GaianDBConfigProcedures extends GaianDBProcedureUtils {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "GaianDBConfigProcedures", 30 );
	
	// Utility ResultSet request identifiers for API procs and funcs
	private static final String LTDEFS = "LTDEFS";
	private static final String RDBCONNECTIONS = "RDBCONNECTIONS";
	private static final String DATASOURCES = "DATASOURCES";
	private static final String FULLCONFIG = "FULLCONFIG";
	private static final String LISTQUERIES = "LISTQUERIES";
	private static final String USERWARNING = "USERWARNING";
	private static final String API = "API";
	
	// Config requests' ResultSet column names
	private static final String LTNAME = "LTNAME";
	private static final String LTDEF = "LTDEF";
	private static final String CID = "CID";
	private static final String CNODEID = "CNODEID";
	private static final String CPOOLSIZE = "CPOOLSIZE";
	private static final String CDRIVER = "CDRIVER";
	private static final String CURL = "CURL";
	private static final String CUSR = "CUSR";
	private static final String DSID = "DSID";
	private static final String DSTYPE = "DSTYPE";
	private static final String DSWRAPPER = "DSWRAPPER";
	private static final String DSHANDLE = "DSHANDLE";
	private static final String DSOPTIONS = "DSOPTIONS";
	private static final String DSCID = "DSCID";
	private static final String PROPID = "PROPID";
	private static final String PROPDEF = "PROPDEF";
	private static final String TSTAMP = "TSTAMP";
	private static final String QUERYID = "QUERYID";
	private static final String STATE = "STATE";
	private static final String NBROWS = "NBROWS";
	private static final String START_TS = "START_TS";
	private static final String DEPTH = "DEPTH";
	private static final String QUERYHASH = "QUERYHASH";
	private static final String GDB_WID = "GDB_WID";
	private static final String QUERY_MS = "QUERY_MS";
	private static final String WARNING = "WARNING";
	private static final String TSTAMPFIRST = "TSTAMPFIRST";
	private static final String REPEATCOUNT = "REPEATCOUNT";
	private static final String APISETUPSQL = "APISETUPSQL";
	
	// Config requests' ResultSet definitions
	private static final String[] CONFIG_REQUESTS = { LTDEFS, RDBCONNECTIONS, DATASOURCES, FULLCONFIG, LISTQUERIES, USERWARNING, API, FILESTATS };
	private static final String[] CONFIG_REQUEST_RESULT_DEFS = { 
		LTNAME + " " + TSTR + ", " + LTDEF + " " + MSTR, // use medium string as this is just the display width
		CID + " " + TSTR + ", " + CNODEID + " " + TSTR + ", " + CPOOLSIZE + " INT, " +
			CDRIVER + " " + SSTR + ", " + CURL + " " + MSTR + ", " + CUSR + " " + TSTR,
		// DSTYPE is R, V or G for: RDB, VTI and Gaian, DSWRAPPER is e.g. "FileImport" or "C1",
		// DSHANDLE is e.g. filename or rdb tablename, DSOPTIONS is e.g. "InMemory"
		DSID + " " + TSTR + ", DSTYPE CHAR, " + DSWRAPPER + " " + TSTR + ", " + DSHANDLE + " " + MSTR +
			", " + DSOPTIONS + " " + MSTR + ", " + DSCID + " " + TSTR,
		PROPID + " " + SSTR + ", " + PROPDEF + " " + MSTR,
		QUERYID + " " + SSTR + ", " + DEPTH + " INT, " + STATE + " " + TSTR + ", " + LTNAME + " " + TSTR + ", " + QUERYHASH + " " + TSTR + ", "
			+ START_TS + " TIMESTAMP, " + QUERY_MS + " BIGINT, " + NBROWS + " BIGINT, " + GDB_WID + " " + TSTR, // This TSTR is not an enforced max size
		TSTAMP + " TIMESTAMP, " + WARNING + ' ' + MSTR + ", " + TSTAMPFIRST + " TIMESTAMP, " + REPEATCOUNT + " BIGINT",
		APISETUPSQL + " " + MSTR,
		"FNAME " + MSTR + ", MODIFIED BIGINT, SIZE BIGINT, CHECKSUM CHAR(20) FOR BIT DATA"
	};
	
	// A Hashtable for looking up the resulting table defs from a config request string
	public static final Hashtable<String, GaianResultSetMetaData> configRequestResultDefs = new Hashtable<String, GaianResultSetMetaData>();
	static {
		for (int i=0; i<CONFIG_REQUESTS.length; i++)
			try { configRequestResultDefs.put(CONFIG_REQUESTS[i], new GaianResultSetMetaData(CONFIG_REQUEST_RESULT_DEFS[i])); }
			catch (Exception e) {
				String msg = "Error loading config request meta data for " + CONFIG_REQUESTS[i] + ": " + e;
				System.out.println(msg);
				GaianNode.stop(msg);
			}
	}
	
	//private static final GaianResultSetMetaData[] CR_MD = { configRequestResultDefs.get( LTDEFS ) };
		
	// In the unlikely case where it is necessary to change the system schema from the default 'gaiandb', use setconfigproperty().
	// Further, to toggle enablement of authentication and change the gaiandb pwd, a user must edit derby.properties
//	private static final String setgaiancred = "setgaiancred";
	
	static final String GAIANDB_API = ""
		+ "!DROP PROCEDURE "+listspfs+";!CREATE PROCEDURE "+listspfs+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listStoredProceduresAndFunctions'"
		+ ";"
		+ "!DROP PROCEDURE "+listapi+";!CREATE PROCEDURE "+listapi+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listAPI'"
		+ ";"
		+ "!DROP PROCEDURE "+listconfig+";!CREATE PROCEDURE "+listconfig+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listConfig'"
		+ ";"
		+ "!DROP PROCEDURE "+listwarnings+";!CREATE PROCEDURE "+listwarnings+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listWarnings'"
		+ ";"
		+ "!DROP PROCEDURE "+listwarningsx+";!CREATE PROCEDURE "+listwarningsx+"(DEPTH INT)"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listWarningsToDepth'"
		+ ";"
		+ "!DROP PROCEDURE "+listnodes+";!CREATE PROCEDURE "+listnodes+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listGaianNodes'"
		+ ";"
		+ "!DROP PROCEDURE "+listrdbc+";!CREATE PROCEDURE "+listrdbc+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listRDBConnections'"
		+ ";"
		+ "!DROP PROCEDURE "+listlts+";!CREATE PROCEDURE "+listlts+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listLogicalTables'"
		+ ";"
		+ "!DROP PROCEDURE "+listltmatches+";!CREATE PROCEDURE "+listltmatches+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listLogicalTableMatches'"
		+ ";"
		+ "!DROP PROCEDURE "+listds+";!CREATE PROCEDURE "+listds+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listDataSources'"
		+ ";"
		+ "!DROP PROCEDURE "+listqueries+";!CREATE PROCEDURE "+listqueries+"(DEPTH INT)"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listQueries'"
		+ ";"
		+ "!DROP PROCEDURE "+listderbytables+";!CREATE PROCEDURE "+listderbytables+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listDerbyTables'"
		+ ";"
		+ "!DROP PROCEDURE "+listexplain+";!CREATE PROCEDURE "+listexplain+"(LTNAME_OR_GSQL "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listExplainGaianTableOrQuery'"
		+ ";"
		+ "!DROP PROCEDURE "+listflood+";!CREATE PROCEDURE "+listflood+"()"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.listFlood'"
		
//		+ ";"
//		+ "!DROP FUNCTION GEXPLAIN;"
//		+ " !CREATE FUNCTION GEXPLAIN(GSQL "+XSTR+") RETURNS TABLE(" + EXPLAIN_COLDEFS + ")"
//		+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA READS SQL DATA"
//		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getExplainGaianTableOrQuery'"
		
		+ ";"
		+ "!DROP FUNCTION "+getconfigproperty+";!CREATE FUNCTION "+getconfigproperty+"(PROPERTY_NAME_SQLPATTERN "+XSTR+")"
		+ " RETURNS " + XSTR
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getConfigProperty'"
		+ ";"
		+ "!DROP FUNCTION "+gdb_node+";!CREATE FUNCTION "+gdb_node+"() RETURNS " + XSTR
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfig.getGaianNodeID'"
		+ ";"
		+ "!DROP FUNCTION "+getlts+";"
		+ " !CREATE FUNCTION "+getlts+"() RETURNS TABLE(NODEID " + PROVENANCE_COLS[0].split(" ")[1] +
		", " + LTNAME + " " + XSTR + ", " + LTDEF + " " + XSTR + ")"
		+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA READS SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getLogicalTables'"
		+ ";"
		+ "!DROP FUNCTION " + getnodes + ";"
		+ " !CREATE FUNCTION " + getnodes + "() RETURNS TABLE(NODEID "+SSTR+")"
		+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA READS SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getGaianNodes'"
		
//		+ ";" // Example Table Function equivalent of a GaianTable VTI call against LT0 (with limitations)
//		+ "!DROP FUNCTION LT0;!CREATE FUNCTION LT0(TABNAME "+XSTR+") RETURNS TABLE(LOCATION "+TSTR+", NUMBER INTEGER, MISC "+TSTR+", BF2_PLAYER INTEGER)"
//		+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA NOT DETERMINISTIC READS SQL DATA EXTERNAL NAME 'com.ibm.db2j.GaianTable.queryGaianTable'"
		
		+ ";" // Procedure to get the last N lines from the log files of a set of GaianDB nodes
		+ "!DROP PROCEDURE LOGTAIL;!CREATE PROCEDURE LOGTAIL(NODELIST "+XSTR+", NUMLINES INT)"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.logTail'"
		
//		+ ";"
//		+ "!DROP PROCEDURE "+setuser+";!CREATE PROCEDURE"
//		+ " "+setuser+"(usr "+XSTR+", affiliation "+XSTR+", clearance "+XSTR+", pwd "+XSTR+")"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setUser'"
//		+ ";"
//		+ "!DROP PROCEDURE "+removeuser+";!CREATE PROCEDURE "+removeuser+"(usr "+XSTR+")"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.removeUser'"
//		+ ";" // Function that gets the current public key for this server
//		+ "!DROP FUNCTION "+gpublickey+";!CREATE FUNCTION "+gpublickey+"() RETURNS VARCHAR(1024) FOR BIT DATA PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
//		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getPublicKey'"
		
		+ ";"
		+ "!DROP PROCEDURE "+addgateway+";!CREATE PROCEDURE "+addgateway+"(ipaddress "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.addGateway'"
		+ ";"
		+ "!DROP PROCEDURE "+removegateway+";!CREATE PROCEDURE "+removegateway+"(ipaddress "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.removeGateway'"
		
		+ ";"
		+ "!DROP PROCEDURE "+setrdbc+";!CREATE PROCEDURE"
		+ " "+setrdbc+"(connectionID "+XSTR+", driver "+XSTR+", url "+XSTR+", usr "+XSTR+", pwd "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setRDBConnection'"
		+ ";"
		+ "!DROP PROCEDURE "+setlt+";!CREATE PROCEDURE "+setlt+"(LTNAME "+XSTR+", DEFINITION "+XSTR+", CONSTANTS "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTable'"
		+ ";"
		+ "!DROP PROCEDURE "+setltforrdbtable+";!CREATE PROCEDURE "+setltforrdbtable+"(LTNAME "+XSTR+", CID "+XSTR+", rdbTableOrFromExpression "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTableForRDBTable'"
		+ ";"
		+ "!DROP PROCEDURE "+setltforfile+";!CREATE PROCEDURE "+setltforfile+"(LTNAME "+XSTR+", FILENAME "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTableForFile'"
		+ ";"
		// Denis : creation of the procedure for setltforexcel
		+ "!DROP PROCEDURE "+setltforexcel+";!CREATE PROCEDURE "+setltforexcel+"(LTNAME "+XSTR+", SPREADSHEETLOCATION "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTableForExcel'"
		+ ";"
//		// Creation of the procedure setltforws
//		+ "!DROP PROCEDURE "+setltforws+";!CREATE PROCEDURE "+setltforws +"(LTNAME "+XSTR+", URL "+XSTR+", WSDLLOCATION "+XSTR+", POSTDATA "+XSTR+")"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
//		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTableForWS'"
//		+ ";"
//		+ "!DROP PROCEDURE "+setltformongodb+";!CREATE PROCEDURE "+setltformongodb +"(LTNAME "+XSTR+", URL "+XSTR+", FIELDS "+XSTR+")"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
//		+ " EXTERNAL NAME 'com.ibm.db2j.MongoDB.setLogicalTableForMongoDB'"
//		+ ";"
		+ "!DROP PROCEDURE "+setltfornode+";!CREATE PROCEDURE "+setltfornode+"(LTNAME "+XSTR+", NODEID "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTableForNode'"
		+ ";"
		+ "!DROP PROCEDURE "+setltconstants+";!CREATE PROCEDURE "+setltconstants+"(LTNAME "+XSTR+", CONSTANTS "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogicalTableConstants'"
		+ ";"
		+ "!DROP PROCEDURE "+setnodeconstants+";!CREATE PROCEDURE "+setnodeconstants+"(nodeConstants "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setNodeConstants'"
		+ ";"
		+ "!DROP PROCEDURE "+setdsvti+";!CREATE PROCEDURE"
		+ " "+setdsvti+"(ltName "+XSTR+", dsName "+XSTR+", vtiClass "+XSTR+", args "+XSTR+", options "+XSTR+", columnsCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDataSourceVTI'"
		+ ";"
		+ "!DROP PROCEDURE "+setdsexcel+";!CREATE PROCEDURE"
		+ " "+setdsexcel+"(ltName "+XSTR+", dsName "+XSTR+", fileName "+XSTR+", options "+XSTR+", columns "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDataSourceExcel'"
		+ ";"
		+ "!DROP PROCEDURE "+setdsfile+";!CREATE PROCEDURE"
		+ " "+setdsfile+"(ltName "+XSTR+", dsName "+XSTR+", fileName "+XSTR+", options "+XSTR+", columnsCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDataSourceFile'"
		+ ";"
		+ "!DROP PROCEDURE "+setdsrdbtable+";!CREATE PROCEDURE"
		+ " "+setdsrdbtable+"(ltName "+XSTR+", dsName "+XSTR+", connectionID "+XSTR+", rdbTable "+XSTR+", options "+XSTR+", columnsCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDataSourceRDBTable'"
		+ ";"
		+ "!DROP PROCEDURE "+setdslocalderby+";!CREATE PROCEDURE"
		+ " "+setdslocalderby+"(ltName "+XSTR+", dsName "+XSTR+", rdbTable "+XSTR+", options "+XSTR+", columnsCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDataSourceRDBTable'"
		
		+ ";" // Function that evaluates whether 2 logical table defs match (i.e. each column name shd have the same type in both defs or not exist)
		+ "!DROP FUNCTION " + ltmatch + ";!CREATE FUNCTION " + ltmatch + "(LTDEF1 "+XSTR+", LTDEF2 "+XSTR+")"
		+ " RETURNS SMALLINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.matchTableDefinitions'"
		
		+ ";" // Get count of GaianDB nodes
		+ "!DROP FUNCTION " + getnodecount + ";!CREATE FUNCTION " + getnodecount + "() RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getNodeCount'"

		+ ";"
		+ "!DROP PROCEDURE "+removerdbc+";!CREATE PROCEDURE "+removerdbc+"(connectionID "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.removeRDBConnection'"
		+ ";"
		+ "!DROP PROCEDURE "+removelt+";!CREATE PROCEDURE "+removelt+"(LTNAME "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.removeLogicalTable'"
		+ ";"
		+ "!DROP PROCEDURE "+removeds+";!CREATE PROCEDURE "+removeds+"(dsID "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.removeDataSource'"
		+ ";"
		+ "!DROP PROCEDURE "+gconnect+";!CREATE PROCEDURE "+gconnect+"(connectionID "+XSTR+", host "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.gaianConnect'"
		+ ";"
		+ "!DROP PROCEDURE "+gconnectx+";!CREATE PROCEDURE"
		+ " "+gconnectx+"(connectionID "+XSTR+", host "+XSTR+", port int, usr "+XSTR+", pwd "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.gaianConnect'"
		+ ";"
		+ "!DROP PROCEDURE "+gdisconnect+";!CREATE PROCEDURE "+gdisconnect+"(connectionID "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.gaianDisconnect'"
//		+ ";"
//		+ "!DROP PROCEDURE "+setgaiancred+";!CREATE PROCEDURE "+setgaiancred+"(usr "+XSTR+", pwd "+XSTR+")"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setGaianCred'"
		+ ";"
		+ "!DROP PROCEDURE "+setminconnections+";!CREATE PROCEDURE "+setminconnections+"(numConnections int)"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setNumConnectionsSought'"
		+ ";"
		+ "!DROP PROCEDURE "+setaccessclusters+";!CREATE PROCEDURE "+setaccessclusters+"(clustersCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setAccessClusters'"
		+ ";"
		+ "!DROP PROCEDURE "+setdiscoveryhosts+";!CREATE PROCEDURE "+setdiscoveryhosts+"(hostsCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDiscoveryHosts'"
		+ ";"
		+ "!DROP PROCEDURE "+setdiscoveryip+";!CREATE PROCEDURE "+setdiscoveryip+"(ipAddress "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setDiscoveryIP'"
		+ ";"
		+ "!DROP PROCEDURE "+setmaxpropagation+";!CREATE PROCEDURE "+setmaxpropagation+"(depth int)"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setMaxPropagation'"
		+ ";"
		+ "!DROP PROCEDURE "+setmaxpoolsizes+";!CREATE PROCEDURE "+setmaxpoolsizes+"(size int)"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setMaxPoolsizes'"
		+ ";"
		+ "!DROP PROCEDURE "+setloglevel+";!CREATE PROCEDURE "+setloglevel+"(level "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setLogLevel'"
		+ ";"
		+ "!DROP PROCEDURE "+setsourcelist+";!CREATE PROCEDURE "+setsourcelist+"(listID "+XSTR+", sourcesCSV "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setSourceList'"
		+ ";"
		+ "!DROP PROCEDURE "+setmsgbrokerdetails+";!CREATE PROCEDURE "+setmsgbrokerdetails+"(host "+XSTR+", port int, topic "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setMsgBrokerDetails'"
		+ ";"
		+ "!DROP PROCEDURE "+setconfigproperty+";!CREATE PROCEDURE "+setconfigproperty+"(propertyName "+XSTR+", value "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setConfigProperty'"
		+ ";"
		+ "!DROP PROCEDURE "+setConfigProperties+";!CREATE PROCEDURE "+setConfigProperties+"(SQLQUERY_RETURNING_PROPERTY_KEYS_AND_VALUES "+XSTR+")"
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setConfigProperties'"
		
		+ ";"
		+ "!DROP FUNCTION INTERNALDIAGS;!CREATE FUNCTION INTERNALDIAGS(VarName "+XSTR+") RETURNS " + XSTR
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.internalDiags'"
		+ ";" // Cancel query
		+ "!DROP PROCEDURE "+cancelquery+";!CREATE PROCEDURE "+cancelquery+"(QUERYID "+XSTR+") PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.cancelQuery'"
		
		+ ";" // Kill this GaianDB node
		+ "!DROP FUNCTION "+gkill+";!CREATE FUNCTION "+gkill+"(CODE INT) RETURNS INT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianNode.stop'"
		+ ";" // Kill a set of GaianDB nodes
		+ "!DROP PROCEDURE "+gkillnodes+";!CREATE PROCEDURE "+gkillnodes+"(NODELIST " + XSTR + ") PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.killNodes'"
		+ ";" // Kill all GaianDB nodes in network
		+ "!DROP PROCEDURE "+gkillall+";!CREATE PROCEDURE "+gkillall+"(CODE INT) PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA"
		+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.killAll'"
		
		+ ";" // Get the run status of the node
		+ "!DROP FUNCTION GDBRUNSTATUS;!CREATE FUNCTION GDBRUNSTATUS() RETURNS " + SSTR
		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.getNodeRunStatus'"
		;
		
		// Incomplete batch-update implementation - Using setConfigProperties() instead for now...
//		+ ";" // Enable auto-commit of APIs
//		+ "!DROP PROCEDURE SETCOMMITAPIS_ON;!CREATE PROCEDURE SETCOMMITAPIS_ON()"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setCommitAPIsOn'"
//		+ ";" // Disable auto-commit of APIs
//		+ "!DROP PROCEDURE SETCOMMITAPIS_OFF;!CREATE PROCEDURE SETCOMMITAPIS_OFF()"
//		+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBConfigProcedures.setCommitAPIsOff'"
//		;

	/**
	 * Returns result rows for any config request string (e.g. LTDEFS), according to corresponding table def schema.
	 * 
	 * @param request
	 * @return
	 * @throws Exception 
	 */
	public static DataValueDescriptor[][] getResultSetForUtilityRequest( String request ) throws Exception {
		return getResultSetForUtilityRequest(request, null);
	}
	
	/**
	 * Returns result rows for any config request string (e.g. LTDEFS), according to corresponding table def schema.
	 * 
	 * @param request
	 * @param additionalParams - String - CSV list of additional parameters for the request. Null if none.
	 * @return
	 * @throws Exception 
	 */
	public static DataValueDescriptor[][] getResultSetForUtilityRequest( String request, String additionalParams ) throws Exception {
		
		DataValueDescriptor[][] dvdrs = null;
		
		if ( LTDEFS.equals(request) ) {
			
			/*
			 * Check the parameters to see if constant values should be included.
			 */
			boolean includeConstantValues = false;
			String[] splitParams = Util.splitByCommas(additionalParams);
			if(splitParams.length > 0) {
				includeConstantValues = Boolean.parseBoolean(splitParams[0]);
			}
			
			synchronized ( DataSourcesManager.class ) {
			
				Set<String> lts = DataSourcesManager.getLogicalTableNamesLoaded();
				int len = lts.size();
				dvdrs = new DataValueDescriptor[len][2];
				 
//				System.out.println("lt names: " + lts);
				
				Iterator<String> iter = lts.iterator();
				for ( int i=0; iter.hasNext(); i++ ) {
					String ltname = (String) iter.next();
					dvdrs[i][0] = new SQLChar( ltname );
					GaianResultSetMetaData grsmd = DataSourcesManager.getLogicalTableRSMD(ltname);
					
					if(includeConstantValues) {
						dvdrs[i][1] = new SQLChar( null==grsmd ? null : grsmd.getColumnsDefinitionExcludingHiddenOnes() );
					}
					else {
						dvdrs[i][1] = new SQLChar( null==grsmd ? null : grsmd.getColumnsDefinitionExcludingHiddenOnesAndConstantValues() );
					}
				}
			}
			
		} else if ( RDBCONNECTIONS.equals(request) ) {

			// "CID " + TSTR + ", CNODEID " + TSTR + ", POOLSIZE " + INT + ", CUSR " + TSTR + ", CURL " + MSTR + ", CDRIVER " + SSTR,
			synchronized ( DataSourcesManager.class ) {
								
//				Set<String> allReferencedJDBCConnectionIDs = new HashSet<String>( getAllSourceListsConnectionsIDs() );
//				allReferencedJDBCConnectionIDs.addAll( getGaianConnectionsAsSet() );
//				String[] allcs = getAllUserPropertyValuesForKeysWithSuffix(CONNECTION_SUFFIX);
//				for (int i=0; i<allcs.length; i++) allcs[i] = allcs[i].split(" ")[0];
//				allReferencedJDBCConnectionIDs.addAll( Arrays.asList( allcs ) );
//				allReferencedJDBCConnectionIDs.add( LOCALDERBY_CONNECTION );
//				
//				Set<String> loadedConnections = DataSourcesManager.getLoadedRDBSourceHandles();
////				System.out.println("Getting cdetails for: " + loadedConnections );
				
				Set<String> allReferencedJDBCConnectionIDs = new HashSet<String>( lookupAllRegisteredCIDs().values() );
				allReferencedJDBCConnectionIDs.add( LOCALDERBY_CONNECTION );
				
				ArrayList<DataValueDescriptor[]> rdbcs = new ArrayList<DataValueDescriptor[]>();
				
				Iterator<String> iter = allReferencedJDBCConnectionIDs.iterator();
				while ( iter.hasNext() ) {
					
					for ( String cid : Util.splitByCommas( iter.next() ) ) {
						
						String cdetails = null;
						try { cdetails = getRDBConnectionDetailsAsString(cid); }
						catch (Exception e) { continue; } // ignore/skip - warning is logged at load time
	//					if ( loadedConnections.contains(cdetails) ) {
							DataValueDescriptor[] dvdr = new DataValueDescriptor[6];
							String[] ctokens = getConnectionTokens(cdetails);
							dvdr[0] = new SQLChar( cid );
							dvdr[1] = new SQLChar( getDiscoveredNodeID(cid) );
							dvdr[2] = new SQLInteger( ((RecallingStack<Object>)DataSourcesManager.getSourceHandlesPool( cdetails )).getMaxSize() );
							dvdr[3] = new SQLChar( ctokens[0] );
							dvdr[4] = new SQLChar( ctokens[1] );
							dvdr[5] = new SQLChar( ctokens[2] );
							
	//						System.out.println("Returning row: " + Arrays.asList( dvdr ) );
							rdbcs.add(dvdr);
	//					}
					}
				}
				dvdrs = rdbcs.toArray( new DataValueDescriptor[0][] );
			}
			
		} else if ( DATASOURCES.equals(request) ) {
			
			// "DSID " + TSTR + ", DSTYPE CHAR, DSWRAPPER " + TSTR + ", DSHANDLE " + TSTR + ", DSOPTIONS " + SSTR,
			// DSTYPE is R, V or G for: RDB, VTI and Gaian, DSWRAPPER is e.g. "FileImport" or "C1", 
			// DSHANDLE is e.g. filename or rdb url and tablename, DSOPTIONS is e.g. "InMemory"
			synchronized ( DataSourcesManager.class ) {
				
				Set<String> lts = DataSourcesManager.getLogicalTableNamesLoaded();
				ArrayList<DataValueDescriptor[]> dataSources = new ArrayList<DataValueDescriptor[]>();
				
				Iterator<String> iter = lts.iterator();
				while( iter.hasNext() ) {
					
					String ltName = (String) iter.next();
					VTIWrapper[] vtis = DataSourcesManager.getDataSources(ltName);
					
					for ( int i=0; i<vtis.length; i++ ) {
						DataValueDescriptor[] dvdr = new DataValueDescriptor[6];
						VTIWrapper vti = vtis[i];
						
						if ( null == vti ) {
							logger.logWarning(GDBMessages.CONFIG_LT_DS_NULL,
									"Detected (and ignoring) null data source for LT: " + ltName);
							continue;
						}
						
						// No need to show a gaian data source for every table - these can be deduced with listrdbc()
						if ( vti.isGaianNode() ) continue;
						
						boolean isRDB = vti instanceof VTIRDBResult;
						dvdr[0] = new SQLChar( vti.nodeDefName );
						dvdr[1] = new SQLChar( isRDB ? vti.isGaianNode() ? "G" : "R" : "V" );
						dvdr[2] = new SQLChar( vti.getSourceHandlesSnapshotInfo() );
						dvdr[3] = new SQLChar( vti.getSourceDescription(null) ); // Use 'null' to report argument in config (which may contain a wildcard)
						dvdr[4] = new SQLChar( getUserProperty( vti.nodeDefName + OPTIONS_SUFFIX ) );
						dvdr[5] = new SQLChar( getNodeDefRDBMSConnectionID( vti.nodeDefName ) );
						
						dataSources.add( dvdr );
					}
				}
				
				dvdrs = dataSources.toArray( new DataValueDescriptor[0][] );
			}
			
		} else if ( FULLCONFIG.equals(request) ) {
			
			Properties allProps = new Properties();
			synchronized ( DataSourcesManager.class ) {
				allProps.putAll( upr );
				allProps.putAll( spr );
			}
			
			for ( String[] propMap : getAllDefaultProperties() )
				if ( !allProps.containsKey(propMap[0]) )
					allProps.put("#" + propMap[0], propMap[1]); // put this in as a commented property to indicate it is the default value
			
			int len = allProps.size();
			dvdrs = new DataValueDescriptor[len][2];
			
			Iterator<Object> iter = allProps.keySet().iterator();
			for ( int i=0; iter.hasNext(); i++ ) {
				String pr = (String) iter.next();
				dvdrs[i][0] = new SQLChar( pr );
				dvdrs[i][1] = new SQLChar( pr.toUpperCase().endsWith("_PWD") ? "<pwd>" : (String) allProps.get(pr) );
			}
			
		} else if ( LISTQUERIES.equals(request) ) {
//			returns: QUERYID SSTR, DEPTH INT, STATE TSTR, LTNAME TSTR, QUERYHASH TSTR, START_TS TIMESTAMP, QUERY_MS BIGINT, NBROWS BIGINT, GDB_WID SSTR
			
			Set<GaianResult> gResults = GaianTable.getGresults();
			synchronized( gResults ) {
				dvdrs = new DataValueDescriptor[gResults.size()][9];
				
				// EXECUTE, FETCH, RE-FETCH, COMPLETE
				
				// if queryTime is set, state is re-fetch or complete
				// if fetchStartTime is not set, state is execute
				// if re-fetching, re-fetch iteration is > 0 in GaianTable
				
				Iterator<GaianResult> iter = gResults.iterator();
				for ( int i=0; iter.hasNext(); i++ ) {
					GaianResult gr = iter.next();
					long startTime = gr.getQueryStartTime();
					long queryTime = gr.getQueryTime();
					boolean isActive = -1==queryTime;
					boolean isAwaitingRefetch = gr.isAwaitingRefetch();
					dvdrs[i][0] = new SQLChar( gr.getQueryID() );
					dvdrs[i][1] = new SQLInteger( gr.getQueryDepth() );
					dvdrs[i][2] = new SQLChar( isActive ?
								gr.isExecuting() ? "EXECUTE" : "FETCH" : isAwaitingRefetch ? "RE-FETCH" : "CACHED" );
					dvdrs[i][3] = new SQLChar( gr.getLTName() );
					dvdrs[i][4] = new SQLChar( gr.getQueryHash() );
					dvdrs[i][5] = new SQLTimestamp( new Timestamp(startTime) );
					dvdrs[i][6] = new SQLLongint( isActive || isAwaitingRefetch ?
							System.currentTimeMillis() - startTime : gr.getQueryTimeIncludingRefetch() );
					dvdrs[i][7] = new SQLLongint( gr.getRowCount() );
					dvdrs[i][8] = new SQLChar( gr.getWID() );
				}	
			}
			
		} else if ( USERWARNING.equals(request) ) {
//			returns: TSTAMP TIMESTAMP, WARNING MSTR, TSTAMPFIRST TIMESTAMP, REPEAT_COUNT BIGINT
			
			String[] warnings = Logger.getLatestWarnings();
			dvdrs = new DataValueDescriptor[warnings.length][4];
			
			for ( int i=0; i<warnings.length; i++ ) {
				String w = warnings[i];
				
//				int idx = w.indexOf('#');
//				dvdrs[i][0] = new SQLChar( w.substring(0, idx) );
//				dvdrs[i][1] = new SQLChar( w.substring(idx+1) );
				
				int idx1 = w.indexOf('x'), idx2 = w.indexOf('~'), idx3 = w.indexOf('#');
				final String mostRecentTs = w.substring(0, idx1);
				final int repeatCount = Integer.parseInt( w.substring(idx1+1, idx2) );
				final String firstOccurenceTs = w.substring(idx2+1, idx3);
				final String msg = w.substring(idx3+1);
				
				dvdrs[i][0] = new SQLChar( mostRecentTs );
				dvdrs[i][1] = new SQLChar( msg );
				dvdrs[i][2] = new SQLChar( firstOccurenceTs );
				dvdrs[i][3] = new SQLLongint( repeatCount );
			}
			
		} else if ( API.equals(request) ) {
			
			String[] apicmds = Util.splitByTrimmedDelimiter( GAIANDB_API, ';' );
			dvdrs = new DataValueDescriptor[apicmds.length][1];
			
			for ( int i=0; i<apicmds.length; i++ )
				dvdrs[i][0] = new SQLChar(apicmds[i]);
		
//		} else if ( PUBLICKEY.equals(request) ) {
//			
//			String[] apicmds = Util.splitByTrimmedDelimiter( GAIANDB_API, ';' );
//			dvdrs = new DataValueDescriptor[apicmds.length][1];
//			
//			for ( int i=0; i<apicmds.length; i++ )
//				dvdrs[i][0] = new SQLChar(apicmds[i]);
		
		} else if ( request.startsWith(FILESTATS) ) {

			// Returns: Name, Modified, Size, Checksum
			dvdrs = new DataValueDescriptor[1][4];
			
			String[] toks = Util.splitByCommas(request);
			if ( 1 > toks.length ) throw new Exception("File path argument not found, required syntax is: '" + FILESTATS + ",<file path>'");
			
			File f = new File( toks[1] );
			dvdrs[0][0] = new SQLChar( f.getName() );
			dvdrs[0][1] = new SQLLongint( f.lastModified() ); //new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format( new Date(f.lastModified()) ) );
			dvdrs[0][2] = new SQLLongint( f.length() );
			dvdrs[0][3] = new SQLBit( SecurityManager.getChecksumSHA1( readAndZipFileBytes(f) ) );			
		}
		
		return dvdrs;
	}
	
	/**
	 * Stored function used to check if a connection is bi-directional.
	 * If not, we try to re-establish the link.
	 * This is the old definition for backwards compatibility.
	 * It does not have the extraInfo which passes information about the foreign node and helps to indicate the expected structure 
	 * of the return message.
	 * 
	 * @param foreignNode
	 * @return null if the connection is established, string indicating reason otherwise
	 */
	public static String maintainConnection( String senderNodeID, String usr, String scrambledpwd ) {
		return maintainConnection2( senderNodeID, usr, scrambledpwd, null );
	}
	
	/**
	 * New version of maintainConnection() - for V1.04 and V2.0 and above.
	 * 
	 * @param senderNodeID
	 * @param usr
	 * @param scrambledpwd
	 * @param extraInfo
	 * @return message indicating success or error info - If the version is null then success is indicated using a null response
	 */
	public static String maintainConnection2( String senderNodeID, String usr, String scrambledpwd, String extraInfo ) {
		return GaianNodeSeeker.maintainConnection(senderNodeID, usr, scrambledpwd, extraInfo);
	}
	
	private static final Set<String> triggerEvents = new HashSet<String>();
	
	public static int setTriggerEvent( String evt ) {
		System.out.println("GDB Trigger event: " + evt);
		triggerEvents.add(evt);
		return 1;
	}
	
	public static boolean checkAndClearTriggerEvent( String evt ) {
		return triggerEvents.remove(evt);
	}
	
	private static String constructWhereClauseForNodes( String nodelist ) {
		Set<String> nodes = new HashSet<String> ( Arrays.asList( Util.splitByCommas(nodelist) ));
		if ( nodes.isEmpty() ) return "";
		StringBuffer nlist = new StringBuffer("where GDB_NODE<'!'");
		for ( String s : nodes ) nlist.append(" OR GDB_NODE='"+s+"'");
		return nlist.toString();
	}
	
	public static void cancelQuery( String queryID ) throws Exception {
		apiStart(cancelquery, Arrays.asList(queryID));
		
		if ( null == queryID || 0 == queryID.length() ) return;
		boolean isPropagated = '!' == queryID.charAt(0);
		
		if ( isPropagated ) {
			queryID = queryID.substring(1);
			boolean isFoundAndCancelled = GaianTable.cancelQuery(queryID);
			logger.logInfo("Cancel request for queryID: " + queryID + ", foundAndCancelled = " + isFoundAndCancelled);
			return;
		}		
		
		// distribute the cancel instruction
		ResultSet rs;		
		rs = getResultSetFromQueryAgainstDefaultConnection(
				"select * from new com.ibm.db2j.GaianQuery('call "+cancelquery+"(''!"+queryID+"'')') GT" );
		while( rs.next() );	// wait until all queries have been completely processed before returning // $codepro.audit.disable emptyStatement
		rs.getStatement().getConnection().close();
	}
	
	public static void killNodes( String nodelist ) throws Exception {
		apiStart(gkill, Arrays.asList(nodelist));
		ResultSet rs;
		
		rs = getResultSetFromQueryAgainstDefaultConnection(
				"select 1 from new com.ibm.db2j.GaianQuery('select GKILL() from sysibm.sysdummy1', " +
				"'with_provenance, maxDepth=-1') GT " + constructWhereClauseForNodes(nodelist) );
		
		while( rs.next() ); // wait until all Kill commands have been completely processed before returning // $codepro.audit.disable emptyStatement
		rs.getStatement().getConnection().close();
	}
	
	public static void killAll(int code) throws Exception {
		apiStart(gkillall, Arrays.asList(code+""));
		ResultSet rs;
		
		rs = getResultSetFromQueryAgainstDefaultConnection(
				"select 1 from new com.ibm.db2j.GaianQuery('select GKILL("+code+") from sysibm.sysdummy1', 'maxDepth=-1') GT");
		
		while( rs.next() );// wait until all "GKILL" queries have been completely processed before returning
		rs.getStatement().getConnection().close();
	}

	
	public static short matchTableDefinitions( String def1, String def2 ) throws Exception {
		return (short) ( new GaianResultSetMetaData(def1).matchupWithTableDefinition(def2, false) ? 1 : 0 );
	}
	
	// GAIANDB config procedures
	
	public static void listStoredProceduresAndFunctions( ResultSet[] storedProcFuncNames ) throws Exception {
    	apiStart(listspfs);
		storedProcFuncNames[0] = getResultSetFromQueryAgainstDefaultConnection(
			"select cast(alias as " + TSTR + ") alias, aliastype, jreplace( cast(aliasinfo as " + LSTR + "), ' LANGUAGE JAVA .*', '' ) aliasinfo from " +
			"sys.sysaliases a, sys.sysschemas s where a.schemaid=s.schemaid and s.schemaname = upper('" + getGaianNodeUser() + "') order by alias"
		);
		storedProcFuncNames[0].getStatement().getConnection().close();
	}
	
	public static void listExplainGaianTableOrQuery( String sql, ResultSet[] tables ) throws Exception {
    	apiStart(listexplain);
		tables[0] = getExplainGaianTableOrQuery( sql );
		tables[0].getStatement().getConnection().close();
	}
	
	public static ResultSet getExplainGaianTableOrQuery( String sql ) throws Exception {
		// Pick off the first 7 chars to see if the sql starts with 'SELECT', otherwise it is a logical table name.
		String first7chars = sql.length() > 7 ? sql.trim().substring(0, 7).toUpperCase().replaceAll("\\s", " ") : null;
		if ( null == first7chars || ( ! first7chars.equals("SELECT ") && ! first7chars.equals("VALUES ") ))
			return getResultSetFromQueryAgainstDefaultConnection(
				"select " + EXPLAIN_FROM + ", " + EXPLAIN_TO + ", " + EXPLAIN_DEPTH + ", " + EXPLAIN_PRECEDENCE + ", " + EXPLAIN_COUNT +
				" from new com.ibm.db2j.GaianTable('" + sql + "', 'explain in graph.dot') GT"
			);
		
		return getResultSetFromQueryAgainstDefaultConnection(
			"select " + EXPLAIN_FROM + ", " + EXPLAIN_TO + ", " + EXPLAIN_DEPTH + ", " + EXPLAIN_PRECEDENCE + ", " + EXPLAIN_COUNT +
			" from new com.ibm.db2j.GaianQuery('" +	Util.escapeSingleQuotes(sql) + "', 'explain in graph.dot') GQ"
		);
	}

	public static ResultSet getGaianNodes() throws Exception {
    	apiStart(getnodes);
		return getResultSetFromQueryAgainstDefaultConnection(
				"SELECT DISTINCT gdbx_to_node node FROM new com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T order by node"
		);
	}
	
	public static void listGaianNodes( ResultSet[] tables ) throws Exception {
    	apiStart(listnodes);
		tables[0] = getGaianNodes();
		tables[0].getStatement().getConnection().close();
	}
	
	public static long getNodeCount() throws Exception {
    	apiStart(getnodecount);
		ResultSet rs = getResultSetFromQueryAgainstDefaultConnection(
				"SELECT count(distinct gdbx_to_node) FROM new com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T"
		);
		
		if ( !rs.next() )
			throw new Exception("Unable to compute node count, ResultSet empty!");
		long rc = rs.getLong(1); rs.getStatement().getConnection().close(); return rc;
	}
	
	public static void listFlood( ResultSet[] tables ) throws Exception {
    	apiStart(listflood);
		tables[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + EXPLAIN_FROM + ", " + EXPLAIN_TO + ", " + EXPLAIN_DEPTH + ", " + EXPLAIN_PRECEDENCE +
				" from new com.ibm.db2j.GaianTable('gdb_ltnull', 'explain, maxDepth=-1') GT" // where " + EXPLAIN_DEPTH + ">0"
		);
		tables[0].getStatement().getConnection().close();
	}
	
	public static void logTail( String nodelist, int numlines, ResultSet[] tables  ) throws Exception {
    	apiStart(logtail);
    	String wc = "*".equals(nodelist) ? "*" : constructWhereClauseForNodes(nodelist);
		tables[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + GDB_NODE + ", line, log from new com.ibm.db2j.GaianQuery(" +
					"'select * from ( select row_number() over () line, column1 log from new com.ibm.db2j.GaianTable(''GDB_LTLOG'', ''maxDepth=0'') GT ) SQ', " +
					"'with_provenance, order by line desc fetch first " + numlines + " rows only" + (0==wc.length()?", maxDepth=0":"") + "') GQ" + 
					(wc.equals("*") ? "" : " "+wc) + " order by gdb_node, line"
		);
		
//		Alternative query:
//		select * from ( select row_number() over () line, column1 log from gdb_ltlog_0 ) S
//		where line+51 > (
//		select count(1) from new com.ibm.db2j.GaianTable('gdb_ltlog','maxDepth=0') T
//		)
		
		tables[0].getStatement().getConnection().close();
	}
	
	public static void listLogicalTables( ResultSet[] tables ) throws Exception {
    	apiStart(listlts);
		tables[0] = getLogicalTables();
		tables[0].getStatement().getConnection().close();
	}
	
	// Really what we want is listmatchinglts, with counts for all that loosely match the local ones (i.e. they may possibly have more or less cols)
	// and listdistinctlts, which show all definitions which are not *exactly* the same (apart from col ordering, which may always be different)
	public static void listLogicalTableMatches( ResultSet[] tables ) throws Exception {
    	apiStart(listltmatches);
		tables[0] = getResultSetFromQueryAgainstDefaultConnection(
//			"select ltname, '" + getGaianNodeID() + "' nodeid, ltdef from new com.ibm.db2j.GaianConfig('LTDEFS') t UNION ALL " +
//			"select distinct b.ltname, b.nodeid, b.ltdef from new com.ibm.db2j.GaianConfig('LTDEFS') a, table(getlts()) b where " +
//				"b.ltname not in ( select ltname from new com.ibm.db2j.GaianConfig('LTDEFS') t ) OR " +
//				"a.ltname=b.ltname AND LTMATCH(a.ltdef, b.ltdef) = 0 order by ltname, nodeid"
			
//			"select * from ( " +
//				"select ltname, 1 count, '" + getGaianNodeID() + "' nodeid, ltdef from new com.ibm.db2j.GaianConfig('LTDEFS') t1 UNION ALL " +
//				"select ltname, 1 count, nodeid, ltdef from table(getlts()) t2" +
//			") s1 where ltname not in ( " +
//				"select ltname from new com.ibm.db2j.GaianConfig('LTDEFS') t3 INTERSECT " +
//				"select ltname from table(getlts()) t4) " +
//			"UNION ALL " +
//			"select distinct b.ltname, 1 count, b.nodeid, b.ltdef from new com.ibm.db2j.GaianConfig('LTDEFS') a, table(getlts()) b where " +
//				"a.ltname=b.ltname AND LTMATCH(a.ltdef, b.ltdef) = 0 " +
//			"UNION ALL " +
//			"select distinct c.ltname, count(1) count, max('" + getGaianNodeID() + "') nodeid, c.ltdef from " +
//					"new com.ibm.db2j.GaianConfig('LTDEFS') c, table(getlts()) d where " +
//					"c.ltname=d.ltname AND LTMATCH(c.ltdef, d.ltdef) = 1 group by c.ltname, c.ltdef order by ltname, nodeid"
					
					
			"select a.ltname, count(1) matches, a.nodeid, a.ltdef from table(getlts()) a, table(getlts()) b " +
			"where a.ltname = b.ltname and LTMATCH( a.ltdef, b.ltdef ) = 1 " +
			"group by a.ltname, a.nodeid, a.ltdef order by ltname, matches, nodeid"
		);
		tables[0].getStatement().getConnection().close();
	}
	
	// Get the number of logical tables for this Gaian Node
//	private static int countLogicalTables() throws Exception {
//		int count = 0;
//		ResultSet rs = getResultSetFromQueryAgainstDefaultConnection("select count(*) from new com.ibm.db2j.GaianQuery(" +
//				"'select * from new com.ibm.db2j.GaianConfig(''" + LTDEFS + "'') GC') GQ ");
//		while (rs.next()) {
//			count = rs.getInt(1);
//		}
//		return count;
//	}
//	
//	private static int countLogicalTables(String nodeid) throws Exception {
//		int count = 0;
//		ResultSet rs = getResultSetFromQueryAgainstDefaultConnection("select count(*) from new com.ibm.db2j.GaianQuery(" +
//				"'select * from new com.ibm.db2j.GaianConfig(''" + LTDEFS + "'') GC') GQ where " + GDB_NODE + "='" + nodeid + "'");
//		while (rs.next()) {
//			count = rs.getInt(1);
//		}
//		return count;
//	}
	
	public static ResultSet getLogicalTables() throws Exception {		
		return getResultSetFromQueryAgainstDefaultConnection(
			"select " + GDB_NODE + ", " + LTNAME + ", " + LTDEF + " from new com.ibm.db2j.GaianQuery(" +
			"'select * from new com.ibm.db2j.GaianConfig(''" + LTDEFS + "'') GC', 'with_provenance') GQ " +
			"order by " + GDB_NODE + ", " + LTNAME
		);
	}
	
	public static ResultSet getLogicalTables( String tname ) throws Exception {		
		return getResultSetFromQueryAgainstDefaultConnection(
			"select " + GDB_NODE + ", " + LTDEF + " from new com.ibm.db2j.GaianQuery(" +
			"'select " + LTDEF + " from new com.ibm.db2j.GaianConfig(''" + LTDEFS + "'') GC where " + LTNAME + "=''" + tname + "''', 'with_provenance') GQ " +
			"order by " + GDB_NODE
		);
	}
	
	public static ResultSet getLogicalTable( String tname, String nodeid ) throws Exception {		
		return getResultSetFromQueryAgainstDefaultConnection(
			"select " + LTDEF + " from new com.ibm.db2j.GaianQuery(" +
			"'select " + LTDEF + " from new com.ibm.db2j.GaianConfig(''" + LTDEFS + "'') GC where " + LTNAME + "=''" + tname + "''', 'with_provenance') GQ " +
			"where " + GDB_NODE + "='" + nodeid + "'"
		);
	}
	
	public static ResultSet getLogicalTable( String tname, String nodeid, String additionalParams ) throws Exception {		
		return getResultSetFromQueryAgainstDefaultConnection(
			"select " + LTDEF + " from new com.ibm.db2j.GaianQuery(" +
			"'select " + LTDEF + " from new com.ibm.db2j.GaianConfig(''" + LTDEFS + "'', ''" + additionalParams + "'') GC where " + LTNAME + "=''" + tname + "''', 'with_provenance') GQ " +
			"where " + GDB_NODE + "='" + nodeid + "'"
		);
	}
	
	public static void listDerbyTables( ResultSet[] tables ) throws Exception {
    	apiStart(listderbytables);
		tables[0] = getResultSetFromQueryAgainstDefaultConnection(
//			"select tablename from sys.systables"
			"select distinct " + GDB_NODE + ", tabname derbytable from " +
			"new com.ibm.db2j.GaianTable('DERBY_TABLES', 'with_provenance') T where tabtype = 'T'" +
			"order by " + GDB_NODE + ", derbytable"
		);
		tables[0].getStatement().getConnection().close();
	}
	
	public static void listRDBConnections( ResultSet[] rs ) throws Exception {
    	apiStart(listrdbc);    	
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + GDB_NODE + ", " + CID + ", " + CNODEID + ", " + CPOOLSIZE + ", " + CDRIVER + ", " + CURL + ", " + CUSR +
				" from new com.ibm.db2j.GaianQuery(" +
				"'select * from new com.ibm.db2j.GaianConfig(''" + RDBCONNECTIONS + "'') GC', 'with_provenance') GQ " +
				" order by " + GDB_NODE + ", " + CNODEID + ", " + CID
			);
		rs[0].getStatement().getConnection().close();
	}
	
	public static void listDataSources( ResultSet[] rs ) throws Exception {
    	apiStart(listds);
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + GDB_NODE + ", " + DSID + ", " + DSTYPE + ", " + DSWRAPPER + ", " +
				DSHANDLE + ", " + DSOPTIONS + ", " + DSCID + " from new com.ibm.db2j.GaianQuery(" +
				"'select * from new com.ibm.db2j.GaianConfig(''" + DATASOURCES + "'') GC', 'with_provenance') GQ" +
				" order by " + GDB_NODE + ", " + DSID
			);
		
		rs[0].getStatement().getConnection().close();
	}
	
	public static void listQueries( int maxDepth, ResultSet[] rs ) throws Exception {
    	apiStart(listqueries, Arrays.asList(maxDepth+""));
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + GDB_NODE + ", " + QUERYID + ", " + DEPTH + ", "  + STATE + ", " + LTNAME + ", " + QUERYHASH
					+ ", " + START_TS + ", " + QUERY_MS + ", " + NBROWS + ", " + GDB_WID + " from new com.ibm.db2j.GaianQuery(" +
				"'select * from new com.ibm.db2j.GaianConfig(''" + LISTQUERIES + "'') GC', 'with_provenance, maxDepth="+maxDepth+"') GQ" +
//				" order by START_TS desc"
				" order by jreplace("+QUERYID+",'.*:([\\d]+:[\\d]+)','$1') desc, " + DEPTH
				// Note ordering by the timestamp+seqID of the queryid itself means that exec units on different nodes for the same query are grouped together
			);
		rs[0].getStatement().getConnection().close();
	}
	
	public static void listConfig( ResultSet[] rs ) throws Exception {
    	apiStart(listconfig);
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + GDB_NODE + ", " + PROPID + ", " + PROPDEF + " from new com.ibm.db2j.GaianQuery(" +
				"'select * from new com.ibm.db2j.GaianConfig(''" + FULLCONFIG + "'') GC', 'with_provenance') GQ" +
				" order by " + GDB_NODE + ", " + PROPID
			);
		rs[0].getStatement().getConnection().close();
	}
	
	public static String getConfigProperty( String propertyKey ) throws Exception {
		return GaianDBConfig.getCrossOverProperty(propertyKey);
	}
	
	public static byte[] getPublicKey() throws Exception {
    	apiStart(gpublickey);
    	return SecurityManager.getPublicKey();
	}
	
	public static Map<String, String> prepareLogicalTable( String ltName, String ltDef, String ltConstantColsDef ) throws Exception {
		
		// Don't worry about possible conflicting properties - there shouldn't be any possible conflicts...
		// (however any existing properties ending ltName+_DS... will be deleted) .. users should not be creating such properties anyway.
//		// Check if the string tableName is already a prefix of some property and is not already defined as a logical table name
//		if ( null == getUserProperty(ltName + LTDEF_SUFFIX) ) {
//			String[] prs = getAllUserPropertiesWithPrefix(ltName);
//			if ( 0 != prs.length ) {
//				String msg = "Cannot set logical table " + ltName + " due to conflicting properties: " + Arrays.asList(prs);
//				logger.logWarning(msg);
//				throw new Exception(msg);
//			}
//		}
		
		// Construct and apply the new meta data - thereby validating the syntax
		// Note that this definition won't be proper because it doesnt include node constants and provenance/explain cols, but that
		// is ok because it will never be used as the logical table won't be fully registered until the config file has been 
		// reloaded (specifically, dsArrays in DataSourcesManager only gets set then).
		new GaianResultSetMetaData( ltDef, ltConstantColsDef );
//		DataSourcesManager.setLogicalTableRSMD( ltName, new GaianResultSetMetaData( ltDef, ltConstantColsDef ) );
		
		// Clear and update logical table properties
		Map<String, String> updates = generateNulledOutUpdatesForPropertiesPrefixed(ltName+"_DS"); //new LinkedHashMap<String, String>();
		updates.put( ltName + LTDEF_SUFFIX, ltDef );
		updates.put( ltName + CONSTANTS_SUFFIX, ltConstantColsDef);
		
		return updates;
	}
	
	public static synchronized void setLogicalTable( String ltName, String ltDef, String ltConstantColsDef ) throws Exception {		
    	apiStart(setlt, Arrays.asList(ltName, ltDef, ltConstantColsDef));
    	ltName = ltName.toUpperCase();
		Map<String, String> updates = prepareLogicalTable(ltName, ltDef, ltConstantColsDef);
		try {
			persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
			DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
			logger.logInfo("Completed checkUpdateLogicalTableViewsOnAllDBs()");
		} catch ( Exception e ) { // Can't leave a bad def hanging due to the automated view creations for it
			String errmsg = "Unable to set logical table: " + ltName + ": " + Util.getStackTraceDigest(e);
			logger.logWarning(GDBMessages.CONFIG_SET_LT_ERROR, errmsg);
			// try to clean up
			try { removeLogicalTable(ltName); } catch (Exception e1) {}
			throw e;
		}
		
		logger.logInfo("Exit setLogicalTable()");
		
		// Note this new config will be reloaded again after this update. but the meta data object will 
		// not need reconstructing.
	}
	
	public static synchronized void setLogicalTableConstants( String ltName, String ltConstantColsDef ) throws Exception {
    	apiStart(setltconstants, Arrays.asList(ltName, ltConstantColsDef));
    	ltName = ltName.toUpperCase();
		// Check if the string <ltName>_DEF is defined, meaning there is a logical table definition in place.
		if ( null == getUserProperty(ltName + LTDEF_SUFFIX) ) {
			String msg = "Cannot set constants for logical table " + ltName + " as it is not defined!";
			logger.logWarning(GDBMessages.CONFIG_LT_SET_CONSTANTS_ERROR, msg);
			throw new Exception(msg);
		}
		
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put( ltName + CONSTANTS_SUFFIX, ltConstantColsDef );
		persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}
		
	public static synchronized void removeLogicalTable( String ltName ) throws Exception {		
    	apiStart(removelt, Arrays.asList(ltName));
    	ltName = ltName.toUpperCase();
		if ( null == getUserProperty(ltName + LTDEF_SUFFIX) ) {
			String msg = "Logical Table does not exist (nothing removed): " + ltName;
			logger.logWarning(GDBMessages.CONFIG_LT_REMOVE_ERROR, msg);
			throw new Exception(msg);
		}
		
		Map<String, String> updates = generateNulledOutUpdatesForPropertiesPrefixed(ltName+"_DS");
		updates.put(ltName+LTDEF_SUFFIX, null);
		if ( null != getUserProperty(ltName + CONSTANTS_SUFFIX) )
			updates.put(ltName+CONSTANTS_SUFFIX, null);
		
		persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}
	
	public static synchronized void setLogicalTableForNode( String ltName, String nodeID ) throws Exception {		
		apiStart(setltfornode, Arrays.asList(ltName, nodeID));
    	ltName = ltName.toUpperCase();
    	
    	//Get logical table definition, with constant values
		ResultSet rs = getLogicalTable( ltName, nodeID, "true" );
		
		if ( ! rs.next() ) {
			
			//Get logical table definition, withOUT constant values
			rs = getLogicalTable( ltName, nodeID);
			
			if ( ! rs.next() ) {
				String msg = "Unable to find Logical Table " + ltName + " at Node " + nodeID;
				logger.logWarning(GDBMessages.CONFIG_LT_SET_FOR_NODE_NULL_LT, msg);
				throw new Exception(msg);
			}
		}
		
		String ltDef = rs.getString(LTDEF);				
		String[] physicalAndConstantCols = getColumnsDefArray(ltDef.toUpperCase());
		
		StringBuffer physicalColsDefSB = new StringBuffer();
		
		for (int i=0; i<physicalAndConstantCols.length; i++) {
						
			String[] tokens = Util.splitByTrimmedDelimiter( physicalAndConstantCols[i], ' ' );
			logger.logInfo("Logical Table Def tokens are: " + Arrays.asList(tokens));
			
			// Is this a constant column definition ? If so it will have 3 tokens.
			if ( 2 < tokens.length ) {
				logger.logWarning(GDBMessages.CONFIG_RT_COLUMN_CONSTANT_VALUE, "From 'setltfornode(" + ltName + ", " + nodeID + ")': The remote table column '" + tokens[0] 
                   + "' was a constant column with value '" + tokens[2] + "'. To replicate this locally, use API: 'call setltconstants('<ltName>' , '<ltconstants*>')'");
			}
			
			// new physical col
			physicalColsDefSB.append( (0<i ? "," : "") + tokens[0] + " " + tokens[1]);
		}

		Map<String, String> updates = prepareLogicalTable(ltName, physicalColsDefSB.toString(), "");
		persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
		
		Statement s = rs.getStatement();
		Connection c = s.getConnection();
		s.close(); c.close();
	}
	
	public static synchronized void setLogicalTableForRDBTable( String ltName, String cid, String fromExpression ) throws Exception {    	
		apiStart(setltforrdbtable, Arrays.asList(ltName, cid, fromExpression));
		ltName = ltName.toUpperCase();
		String cdef = null;
		cid = cid.toUpperCase();
		try { cdef = getRDBConnectionDetailsAsString( cid ); }
		catch (Exception e) {
			String errmsg = "Unable to resolve name as a Connection ID: " + cid + ": " + e;
			logger.logWarning(GDBMessages.CONFIG_LT_SET_RDBT_ERROR, errmsg);
			throw new Exception(errmsg);
		}
		
		Map<String, String> updates = null;
		
		fromExpression = fromExpression.trim();
		
		// If the expression is bracketed, then assume there is no where-clause. Otherwise, take the first token only
		String ptName = fromExpression.startsWith("(") ? fromExpression : fromExpression.split("\\s")[0];
		
		try {
			RDBProvider provider = RDBProvider.fromGaianConnectionID(cdef);
			logger.logInfo("Creating logical table " + ltName + " for RDBMS provider: " + provider);
			
			ResultSetMetaData rsmd = getNewDBConnector( getConnectionTokens(cdef) ).getTableMetaData(ptName);
	
			String tableDef = new GaianResultSetMetaData(rsmd, null, provider).getColumnsDefinition().toUpperCase();
			logger.logInfo("Obtained tableDef for RDB table: " + tableDef);
			updates = prepareLogicalTable(ltName, tableDef, "");
		} catch ( Exception e ) {
			String warning = "Failed to create Logical Table '" + ltName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_RDB_ERROR, warning, e);
			throw new Exception(warning + e);
		}		
		
		String nodeDefName = ltName + "_DS0"; // + ptName.toUpperCase();
		
		Properties backup = null;
		
		try {
			// Now setup the data source - note stale properties (with ltName as their prefix) were checked for already
			// when setting up the logical table.
			
			// Validate that the jdbc connection is defined
			getRDBConnectionDetailsAsString(cid);
			
			updates.put(nodeDefName + CONNECTION_SUFFIX, cid + " " + fromExpression);
			updates.put(nodeDefName + OPTIONS_SUFFIX, MAP_COLUMNS_BY_POSITION); // this will give more flexibility to change LT column names
			
			// Apply the updates and load the data source (thus validating syntax aswell) before updating the config file
			// Config file reload will still occur (for now) but will do minimal work.
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );
				
				/*
				 * Update upr with the new values.
				 * To avoid NPEs we iterate through the values & do null checks, rather than use putAll.
				 * Note: because upr is a Properties object (and extends Hashtable), it is automatically synchronized 
				 */
				Set<Entry<String,String>> updatesEntries = updates.entrySet();
				
				for (Entry<String, String> updateEntry : updatesEntries) {
					
					if(updateEntry.getKey() != null) {
						
						if(updateEntry.getValue() == null) {
							upr.remove(updateEntry.getKey());
						}
						else {
							upr.put( updateEntry.getKey(), updateEntry.getValue() );
						}
					}	
				}
			}
//			DataSourcesManager.loadDataSource( ltName, nodeDefName );
//			DataSourcesManager.primeJDBCSourceHandlesPoolSynchronously( nodeDefName );
			persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
			
		} catch ( Exception e ) {
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to create Data Source '" + nodeDefName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_DS_RDB_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}

	public static synchronized void setLogicalTableForFile( String ltName, String filePathTagged ) throws Exception {

		if ( Util.isWindowsOS && null != filePathTagged ) filePathTagged = filePathTagged.replaceAll("\\\\", "/");
		apiStart(setltforfile, Arrays.asList(ltName, filePathTagged));
		ltName = ltName.toUpperCase();
		
		Map<String, String> updates = null;
		
		final String filePath = resolvePathTags( filePathTagged );
		
    	if ( !new File(filePath).exists() )
    		logger.logWarning(GDBMessages.CONFIG_LT_SET_FILE_NOT_FOUND, "API " + setltforfile + " warning: File not found: " + filePath);
		
		try {
			String tableDef = new GaianResultSetMetaData(new FileImport(filePath).getMetaData(), null).getColumnsDefinition();
			logger.logInfo("Obtained tableDef for file: " + tableDef);
			updates = prepareLogicalTable(ltName, tableDef, "");
		} catch ( SQLException e ) {
			System.out.println("Check File: " + filePath);
			String warning = "Error while importing " + filePath + ", cause: ";
			logger.logException(GDBMessages.CONFIG_LT_FILE_IMPORT_ERROR_SQL, warning, e);
			throw new Exception(warning + e);
		} catch ( Exception e ) {
			String warning = "Failed to create Logical Table '" + ltName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_FILE_IMPORT_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
//		String canName = new File(fileName).getName();
//		int dotIndex = canName.indexOf('.');
//		if ( -1 != dotIndex ) canName = canName.substring(0, dotIndex);
		String nodeDefName = ltName + "_DS0"; // + canName.toUpperCase();
		
		Properties backup = null;
		try {
			// Now setup the data source - note stale properties (with ltName as their prefix) were checked for already
			// when setting up the logical table.
			
			updates.put(nodeDefName + VTI_SUFFIX, FileImport.class.getName());
			updates.put(nodeDefName + ARGS_SUFFIX, filePathTagged); // keep tagged version
			updates.put(nodeDefName + OPTIONS_SUFFIX, MAP_COLUMNS_BY_POSITION); // this will give more flexibility to change LT column names
			
			// Load data source now so we can report if any errors occur in doing so (e.g. file not found)
			// properties must be set to load data source successfully
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );
				
				/*
				 * Update upr with the new values.
				 * To avoid NPEs we iterate through the values & do null checks, rather than use putAll.
				 * Note: because upr is a Properties object (and extends Hashtable), it is automatically synchronized 
				 */
				Set<Entry<String,String>> updatesEntries = updates.entrySet();
				
				for (Entry<String, String> updateEntry : updatesEntries) {
					
					if(updateEntry.getKey() != null) {
						
						if(updateEntry.getValue() == null) {
							upr.remove(updateEntry.getKey());
						}
						else {
							upr.put( updateEntry.getKey(), updateEntry.getValue() );
						}
					}	
				}
			}
//			DataSourcesManager.loadDataSource( ltName, nodeDefName );
			
			persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
			
		} catch ( Exception e ) {
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to create Data Source '" + nodeDefName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_DS_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}
		
	// Denis : setLogicalTableForExcel
	public static synchronized void setLogicalTableForExcel( String ltName, String spreadsheetLocation ) throws Exception {
		if ( Util.isWindowsOS && null != spreadsheetLocation ) spreadsheetLocation = spreadsheetLocation.replaceAll("\\\\", "/");
		apiStart(setltforexcel, Arrays.asList(ltName, spreadsheetLocation));
		ltName = ltName.toUpperCase();
		
		Map<String, String> updates = null;
		
		try {
			String tableDef = new GaianResultSetMetaData(new GExcel(spreadsheetLocation).getMetaDataByInferringTypes(), null).getColumnsDefinition().toUpperCase();		
			logger.logInfo("Obtained tableDef for file: " + tableDef);
			updates = prepareLogicalTable(ltName, tableDef, "");
		} catch ( Exception e ) {
			String warning = "Failed to create Logical Table '" + ltName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_EXCEL_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
		String nodeDefName = ltName + "_DS0";
		
		Properties backup = null;
		try {
			// Now setup the data source - note stale properties (with ltName as their prefix) were checked for already
			// when setting up the logical table.
			
			updates.put(nodeDefName + VTI_SUFFIX, GExcel.class.getName());
			updates.put(nodeDefName + ARGS_SUFFIX, spreadsheetLocation);
			
			// Load data source now so we can report if any errors occur in doing so (e.g. file not found)
			// properties must be set to load data source successfully
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );
				
				/*
				 * Update upr with the new values.
				 * To avoid NPEs we iterate through the values & do null checks, rather than use putAll.
				 * Note: because upr is a Properties object (and extends Hashtable), it is automatically synchronized 
				 */
				Set<Entry<String,String>> updatesEntries = updates.entrySet();
				
				for (Entry<String, String> updateEntry : updatesEntries) {
					
					if(updateEntry.getKey() != null) {
						
						if(updateEntry.getValue() == null) {
							upr.remove(updateEntry.getKey());
						}
						else {
							upr.put( updateEntry.getKey(), updateEntry.getValue() );
						}
					}	
				}
			}
			
			persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
			
		} catch ( Exception e ) {
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to create Data Source '" + nodeDefName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_DS_EXCEL_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}
	
	/**
	 * This method sends a request to a web service, reads the data returned by the 
	 * web service, and determines which columns  can represent the 
	 * associated logical table. It also writes the necessary properties into the 
	 * gaian proerty file in order to query the logical table later on.
	 * 
	 * @author remi - IBM Hursley 
	 * 
	 * @param ltName
	 * 			Name of the generated logical table.
	 * @param url
	 * 			Url accessing the web service.
	 * @param wsdlLocation
	 * 			FOR A SOAP WEB SERVICE, the wsdl location of the web service.
	 * @param postData
	 * 			The data to send to the web service in order to get an answer from it.
	 * @throws Exception
	 */
	public static synchronized void setLogicalTableForWS( String ltName, String url, String wsdlLocation, String postData ) throws Exception {

		apiStart(setltforws, Arrays.asList(ltName, url, wsdlLocation, postData));
		ltName = ltName.toUpperCase();
		
		logger.logInfo("Obtained tableDef for ws service: " + url);
		
		// -----------------------------------------------------------
		// Starts the web service and get its results
		WebService ws;
		
		if (wsdlLocation != null && !wsdlLocation.isEmpty()) {
			// This is a SOAP web service
			ws = new SoapWS(url, wsdlLocation, postData);
		}
		else {
			// This is a REST web service
			if (postData == null || postData.isEmpty()) {
				// no post data => REST / GET
				ws = new RestWS(url);
			}
			else {
				// Post data => REST / POST
				ws = new PostRestWS(url, postData);
			}
		}
		ws.openConnection();
		InputStream is = ws.getInputStream();

		// -----------------------------------------------------------
		// Defines returned format. -> JSON / XML / UNKNOWN
		is = new FormatSpecifierInputStream(is);
		WsDataFormat format = ((FormatSpecifierInputStream)is).defineFormat();
		
//		//-------------------------------------------------------
//		// Checks the juno package is installed
//		if (format == WsDataFormat.JSON) {
//			try { 
//				com.ibm.juno.core.json.JsonParser.class.isLocalClass();
////				GaianDBConfigProcedures.class.getClass().getClassLoader().loadClass(
////						com.ibm.juno.core.json.JsonParser.class.getName()
////				);
//			} catch ( Throwable e ) { 
//				logger.logInfo("The Juno package is not installed. GenericWS cannot parse JSON:\n" + e); 
//				format = WsDataFormat.UNKNOWN_FORMAT;
//			}
//		}
//		
//		if (format == WsDataFormat.UNKNOWN_FORMAT) {
//			return;
//		}
		
			
		// -----------------------------------------------------------
		// Defines characteristics of the logical table which is read
		GenericWsLogicalTableGenerator ltGenerator = 
				new GenericWsLogicalTableGenerator(is, format);
		

		// -----------------------------------------------------------
		// Defines logical table and VTI properties
		ltGenerator.addProperty(ltName, GenericWS.PROP_URL, url);
		ltGenerator.addGenericWsProperties(ltName, ltName);
		if (ws instanceof SoapWS) {
			ltGenerator.addProperty(ltName, GenericWS.PROP_WS_TYPE, GenericWS.PROP_WS_TYPE_VALUE_SOAP);
		}
		else { // if (ws instanceof RestWS) {
			String urlCopy = new String(url);
			if (urlCopy.toLowerCase().startsWith("file:")) {
				ltGenerator.addProperty(ltName, GenericWS.PROP_WS_TYPE, GenericWS.PROP_WS_TYPE_VALUE_LOCAL_FILE);
			}
			else {
				ltGenerator.addProperty(ltName, GenericWS.PROP_WS_TYPE, GenericWS.PROP_WS_TYPE_VALUE_REST);
			}
		}
		ltGenerator.addProperty(ltName, GenericWS.PROP_DATA_FORMAT, format.name());
		ltGenerator.addProperty(ltName, GenericWS.PROP_WSDL, wsdlLocation);
		ltGenerator.addProperty(ltName, GenericWS.PROP_POST_DATA, postData);
		
		Map<String, String> properties = ltGenerator.getProperties();
		

		// -----------------------------------------------------------
		// Updates the properties
		
		// Copied from setLogicalTableForExcel(...)
		Properties backup = null;
		try {
			
			// Load data source now so we can report if any errors occur in doing so (e.g. file not found)
			// properties must be set to load data source successfully
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );
				
				/*
				 * Update upr with the new values.
				 * To avoid NPEs we iterate through the values & do null checks, rather than use putAll.
				 * Note: because upr is a Properties object (and extends Hashtable), it is automatically synchronized 
				 */
				Set<Entry<String,String>> updatesEntries = properties.entrySet();
				
				for (Entry<String, String> updateEntry : updatesEntries) {
					
					if(updateEntry.getKey() != null) {
						
						if(updateEntry.getValue() == null) {
							upr.remove(updateEntry.getKey());
						}
						else {
							upr.put( updateEntry.getKey(), updateEntry.getValue() );
						}
					}	
				}
				upr.putAll(properties);
			}
			
			persistAndApplyConfigUpdates(properties);
			
		} catch ( Exception e ) {
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to create Data Source '" + url + "', cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_DS_EXCEL_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
		
	}
	
	/**
	 * Generic API for setting up a logical table to access a data source exposed by a VTI.
	 * 
	 * vtiConstructor should be of the form: 'MyVTI( String vtiArgsCSV )'
	 * vtiStaticProperties should contain a CSV list of property keys and values - or perhaps a SQL SELECT expression to have 
	 * the option to retrieve them from somewhere, like the setconfigproperties() API does.
	 * 
	 * @param ltName
	 * @param vtiConstructor
	 * @param vtiStaticProperties
	 */
	public static void setLogicalTableForVTI( String ltName, String vtiConstructor, String vtiStaticProperties ) {
		
//	TODO: Need good javadoc for this
//	
//	i.e.
//	dsOptions might contain SCHEMA property
//	dsArgs = vtiArgs => csv list - first string is prefix, i.e. can be used for referencing shared vti properties set
//	
//	avoids having to register the stored procedure for clients implementing their own VTIs
	
	}
	
	
	public static synchronized void setNodeConstants( String globalConstantColsDef ) throws Exception {
		
    	apiStart(setnodeconstants, Arrays.asList(globalConstantColsDef));
		// All tables will need reloading - just validate the syntax here
		new GaianResultSetMetaData( "", globalConstantColsDef );
		
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put( NODE_CONSTANTS, globalConstantColsDef);
		persistAndApplyConfigUpdates(updates);
		DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
	}
		
	public static synchronized void setRDBConnection( String connectionID, String driver, String url, String usr, String pwd ) throws Exception {

    	apiStart(setrdbc, Arrays.asList(connectionID, driver, url, usr, "<pwd>"));
		connectionID = connectionID.toUpperCase();
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(connectionID + LABEL_DRIVER, driver);
		updates.put(connectionID + LABEL_URL, url);
		updates.put(connectionID + LABEL_USR, usr);
		updates.put(connectionID + LABEL_PWD, pwd);
		persistAndApplyConfigUpdates(updates);
	}
		
	public static synchronized void removeRDBConnection( String connectionID ) throws Exception {

    	apiStart(removerdbc, Arrays.asList(connectionID));
		connectionID = connectionID.toUpperCase();
		
		String cdef = null;
		try { cdef = getRDBConnectionDetailsAsString( connectionID ); }
		catch (Exception e) {}
		
		if ( null == getUserProperty(connectionID+LABEL_DRIVER) && null == getUserProperty(connectionID+LABEL_URL) &&
			 null == getUserProperty(connectionID+LABEL_USR) && null == getUserProperty(connectionID+LABEL_PWD) ) {
			String msg = "RDB Connection does not exist (nothing removed): " + connectionID;
			logger.logWarning(GDBMessages.CONFIG_RDB_CONN_REMOVE_NOT_FOUND, msg);
			throw new Exception(msg);
		}

		Set<String> loadedConnections = DataSourcesManager.getLoadedRDBSourceHandles();
		if ( loadedConnections.contains( cdef ) ) {
			String msg = "Cannot remove active connection " + connectionID + ", dependant data sources must be removed first";
			logger.logWarning(GDBMessages.CONFIG_RDB_CONN_REMOVE_ACTIVE, msg);
			throw new Exception(msg);
		}
		
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(connectionID + LABEL_DRIVER, null);
		updates.put(connectionID + LABEL_URL, null);
		updates.put(connectionID + LABEL_USR, null);
		updates.put(connectionID + LABEL_PWD, null);
		persistAndApplyConfigUpdates(updates);
	}
	
	public final static Map<String, String> internalDiags = new LinkedHashMap<String, String>();
		
	public static synchronized String internalDiags( String cmd )  {
		if ( null != cmd ) {
			if ( cmd.startsWith("hang_on_") ) { internalDiags.put(cmd, null); return "ok_to_hang_on_polls"; }
			if ( "clear".equals(cmd) ) synchronized(GaianDBConfigProcedures.class) { internalDiags.clear(); return "clear"; }
		}
		return internalDiags.get(cmd);
	}
	
	public static String getNodeRunStatus()  {
		switch ( GaianNode.getRunStatus() ) {
			case GaianNode.RUN_STATUS_PENDING_ON: return "RUN_STATUS_PENDING_ON";
			case GaianNode.RUN_STATUS_PENDING_OFF: return "RUN_STATUS_PENDING_OFF";
			case GaianNode.RUN_STATUS_ON: return "RUN_STATUS_ON";
			case GaianNode.RUN_STATUS_OFF: default: return "RUN_STATUS_OFF";
		}
	}
	
//	public static synchronized int getTestInvocationCount( String spf ) { return internalValues.get(spf); }
	
	private static synchronized void incrementTestInvocationCount(String spf) {
		String count = internalDiags.get(spf);
		count = null == count ? "1" : Integer.parseInt(count)+1+"";
		internalDiags.put(spf, count);
	}
	
	public static void listWarningsToDepth( int depth, ResultSet[] rs ) throws Exception {
    	apiStart(listwarningsx);
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select gdb_node, tstamp, tstampfirst, repeatCount, warning from new com.ibm.db2j.GaianQuery(" +
				"'select * from new com.ibm.db2j.GaianConfig(''" + USERWARNING + "'') GC', 'with_provenance, maxDepth=" + depth
				+ "') GQ order by gdb_node, tstamp desc"
			);
		
		if ( GaianNode.isInTestMode() ) { incrementTestInvocationCount(listwarningsx); }
		
		rs[0].getStatement().getConnection().close();
	}
	
	public static void listWarnings( ResultSet[] rs ) throws Exception {
    	apiStart(listwarnings);
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select gdb_node, tstamp, tstampfirst, repeatCount, warning from new com.ibm.db2j.GaianQuery(" +
				"'select * from new com.ibm.db2j.GaianConfig(''" + USERWARNING + "'') GC', 'with_provenance') GQ order by gdb_node, tstamp desc"
			);
		rs[0].getStatement().getConnection().close();
	}
	
	public static void listAPI( ResultSet[] rs ) throws Exception {
    	apiStart(listapi);
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select * from new com.ibm.db2j.GaianConfig('" + API + "') GC"
			);
		rs[0].getStatement().getConnection().close();
	}

	public static void gaianConnect( String connectionID, String host ) throws Exception {
		gaianConnect(connectionID, host, GaianNode.DEFAULT_PORT);
	}
	
	public static void gaianConnect( String connectionID, String host, int port ) throws Exception {
		gaianConnect(connectionID, host, port, GAIAN_NODE_DEFAULT_USR, GAIAN_NODE_DEFAULT_PWD);
	}
	
	public static synchronized void gaianConnect( String connectionID, String host, int port, String usr, String pwd ) throws Exception {

    	apiStart(gconnect, Arrays.asList(connectionID, host, port+"", usr, pwd));
		connectionID = connectionID.toUpperCase();
		Map<String, String> updates = new LinkedHashMap<String, String>();
		String definedgcs = getUserProperty(DEFINED_GAIAN_CONNECTIONS);
		
		Properties backup = null;
		try {
			
			String driver = GaianNode.isLite() ? GDB_UDP_DRIVER : DERBY_CLIENT_DRIVER;
			String url = "jdbc" + (GaianNode.isLite() ? ":udp:" : ":") + "derby://" + host + ":" + port + "/" +
				(GaianNode.DEFAULT_PORT==port ? GAIANDB_NAME : GAIANDB_NAME + port) + ";create=true";			
			
			Set<String> gcs = getGaianConnectionsAsSet();
			if ( !gcs.contains( connectionID ) ) { // throw new Exception("Gaian Connection already active");
			
				updates.put(DEFINED_GAIAN_CONNECTIONS, 
						( null == definedgcs || 0 == definedgcs.trim().length() ? "" : definedgcs + ", " ) + connectionID);
				
				// Commented out check against previous def - just set properties again every time.
				
				if ( isDiscoveredConnection(connectionID) ) {
					String msg = "Connot re-assign discovered connection ID '" + connectionID + "' as a gateway connection";
					logger.logImportant(msg);
					throw new Exception(msg);
				}
				
//				String newcdef = driver+"'"+url+"'"+usr+"'"+pwd;
//				String oldcdef = getRDBConnectionDetailsAsString(connectionID, false);
//				
//				// Check conn def doesnt already exist
//				if ( null == oldcdef ) {				
//					
					updates.put(connectionID + LABEL_DRIVER, driver);
					updates.put(connectionID + LABEL_URL, url);
					updates.put(connectionID + LABEL_USR, usr);
					updates.put(connectionID + LABEL_PWD, pwd);
//					
//				} else if ( !oldcdef.equals(newcdef) ) {
//					logger.logImportant("Cdefs don't match: " + oldcdef + " != " + newcdef);
//					throw new Exception("Connection definition already exists and doesn't match");	
//				}
				
	//			System.out.println("Adding properties: " + updates);
				
				// Load the connection - thereby validating it - config reload will still occur after but do minimal work.
				backup = new Properties();
				synchronized(upr) {
					backup.putAll( upr );
					
					/*
					 * Update upr with the new values.
					 * To avoid NPEs we iterate through the values & do null checks, rather than use putAll.
					 * Note: because upr is a Properties object (and extends Hashtable), it is automatically synchronized 
					 */
					Set<Entry<String,String>> updatesEntries = updates.entrySet();
					
					for (Entry<String, String> updateEntry : updatesEntries) {
						
						if(updateEntry.getKey() != null) {
							
							if(updateEntry.getValue() == null) {
								upr.remove(updateEntry.getKey());
							}
							else {
								upr.put( updateEntry.getKey(), updateEntry.getValue() );
							}
						}	
					}
				}
//				DataSourcesManager.primeJDBCSourceHandlesPoolSynchronously( connectionID );
				
				System.out.println(new Date(System.currentTimeMillis()) + 
						": Gateway Connection defined accessing GaianDB node on " + host + ":" + port );
			}
			
			persistAndApplyConfigUpdates(updates);
						
		} catch ( Exception e ) {
//			upr.setProperty(DEFINED_GAIAN_CONNECTIONS, definedgcs); // undo main update
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to create Gaian Connection '" + connectionID + "', cause: " ;
			logger.logException(GDBMessages.CONFIG_CONNECTION_CREATE_ERROR, warning, e);
			throw new Exception(warning + e);
		}
	}
	
	public static synchronized void gaianDisconnect( String connectionID ) throws Exception {

    	apiStart(gdisconnect, Arrays.asList(connectionID));
		connectionID = connectionID.toUpperCase();
		Map<String, String> updates = new LinkedHashMap<String, String>();
		
		try {
			Set<String> dcs = new HashSet<String>( Arrays.asList( getDefinedConnections() ) );			
			if ( !dcs.remove( connectionID ) ) throw new Exception("Defined connection not found");
			
			StringBuffer list = new StringBuffer();
			if ( !dcs.isEmpty() ) {
				Iterator<String> i = dcs.iterator();
				list.append( i.next() );
				while ( i.hasNext() ) list.append( ", " + i.next() );
			}
			
			updates.put(DEFINED_GAIAN_CONNECTIONS, list.toString());
			persistAndApplyConfigUpdates(updates);
			
		} catch ( Exception e ) {
			String warning = "Cannot remove Gaian Connection '" + connectionID + "', cause: " ;
			logger.logException(GDBMessages.CONFIG_CONNECTION_REMOVE_ERROR, warning, e);
			throw new Exception(warning + e);
		}
		
//		removeRDBConnection(connectionID);
	}
	
	public static synchronized void setDataSourceExcel( String ltName, String dsName, String fileName, String options, String columns ) throws Exception {
//		if ( Util.isWindowsOS && null != fileName ) fileName = fileName.replaceAll("\\\\", "/"); // Don't do this - leave the input path as is - users may want it to stay as it was.
    	apiStart(setdsexcel, Arrays.asList(ltName, dsName, fileName, options, columns) );
    	
    	String[] fileNameSplit = Util.splitByCommas(fileName);
    	
    	if(1 > fileNameSplit.length || fileNameSplit[0] == null) {
    		logger.logWarning(GDBMessages.CONFIG_DS_EXCEL_FILE_NOT_SPECIFIED, "API " + setdsexcel + " warning: No fileName was specified.");
    	}
    	else if(!new File(fileNameSplit[0]).exists()) {
    		logger.logWarning(GDBMessages.CONFIG_DS_EXCEL_FILE_NOT_FOUND, "API " + setdsexcel + " warning: File not found: " + fileNameSplit[0]);
    	}
    	
    	setDataSourceVTIPrivate(ltName, dsName, GExcel.class.getName(), fileName, options, columns);
	}
	
	public static synchronized void setDataSourceFile( String ltName, String dsName, String filePath, String options, String columnsCSV ) throws Exception {
		
		// Note - No code in GaianDB should assume that file paths on Windows will exclusively use the '\' character as path separator.
//		if ( Util.isWindowsOS && null != filePath ) filePath = filePath.replaceAll("\\\\", "/"); // Don't do this - leave the input path as is - users may want it to stay as it was.
		apiStart(setdsfile, Arrays.asList(ltName, dsName, filePath, options, columnsCSV) );
    	
		// Do some early checks - as long as the file is not a variable expression, i.e. containing wildcards or a regex
    	if ( null == options || -1 == options.indexOf(PLURALIZED) ) {
    		if ( !new File(filePath).exists() )
    			logger.logWarning(GDBMessages.CONFIG_DS_FILE_NOT_FOUND, "API " + setdsfile + " warning: File not found: " + filePath);
        	// Attempt to resolve control file now - this will print an early warning to the logs if it can't be found -
        	FileImport.getControlFile(filePath);
    	}
    	
    	setDataSourceVTIPrivate(ltName, dsName, FileImport.class.getName(), filePath, options, columnsCSV);
	}

	public static synchronized void setDataSourceVTI( String ltName, String dsName, String vtiClass,
			String args, String options, String columnsCSV ) throws Exception {
    	apiStart(setdsvti, Arrays.asList(ltName, dsName, vtiClass, args, options, columnsCSV) );
    	setDataSourceVTIPrivate(ltName, dsName, vtiClass, args, options, columnsCSV);
	}
	
	
	// Tough associating connections with batch updates - Implemented setconfigproperties() for simpler batch-updates for now instead...
//	private static final Set<Connection> connectionsHavingCommitStatusOff = new HashSet<Connection>(); // A connection auto-commit status is always "on" initially
//	public static boolean isCommitAPIsOn() throws SQLException {
//		return connectionsHavingCommitStatusOff.isEmpty() ? true : false == connectionsHavingCommitStatusOff.contains( getDefaultDerbyConnection() );
//	}
//	
//	public static synchronized void setCommitAPIsOn() throws Exception { setCommitAPIs(true); }
//	public static synchronized void setCommitAPIsOff() throws Exception { setCommitAPIs(false); }
//	
//	private static void setCommitAPIs( boolean isCommit ) throws Exception {
//		Connection c = getDefaultDerbyConnection();
//		if ( isCommit ) connectionsHavingCommitStatusOff.add( c ); else connectionsHavingCommitStatusOff.remove( c );
//		logger.logAlways("setCommitAPIs for Connection: " + c + " => " + isCommit );
//	}
	
	private static void setDataSourceVTIPrivate( String ltName, String dsName, String vtiClass,
			String args, String options, String columnsCSV ) throws Exception {
		
		ltName = ltName.toUpperCase();
		String nodeDefName = ltName + "_DS" + dsName.toUpperCase();
    	
		Properties backup = null;
    	try {
			if ( !DataSourcesManager.isLogicalTableLoaded(ltName) ) throw new Exception("Parent logical table is not loaded: " + ltName);
			
			// Clear any trace of this data source in upr and return corresponding null mappings for persitence.
			Map<String, String> updates = generateNulledOutUpdatesForPropertiesPrefixed(nodeDefName);
    		
			try {
				if ( -1 == vtiClass.indexOf('.') && null != Class.forName("com.ibm.db2j."+vtiClass) )
					vtiClass = "com.ibm.db2j."+vtiClass;
			} catch (ClassNotFoundException e1) {}
			
			// This restriction below is no longer in Derby from version 10.5
//			if ( !vtiClass.startsWith("com.ibm.db2j.") ) throw new Exception("VTI class package name must start with: 'com.ibm.db2j.'");
			
			updates.put(nodeDefName + VTI_SUFFIX, vtiClass);
			updates.put(nodeDefName + ARGS_SUFFIX, args);

			deriveRequiredDataSourceUpdatesFromOptionsAndColumnsLists( updates, nodeDefName, options, columnsCSV );
			
			// Apply the updates and load the data source (thus validating syntax aswell) before updating the config file
			// Config file reload will still occur (for now) but will do minimal work.
			// Putting all updates in upr immediately will mean a concurrent reload of data sources will have a chance to 
			// load this data source before we next get a chance to synchronize on the full DataSourcesManager.class (when reloading the config file)
			// Note upr is a Hashtable therefore it cannot be given null values - insead we have to remove those keys
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );
				for ( String key : updates.keySet() ) {
					String val = updates.get(key);
					if ( null == val ) upr.remove(key);
					else upr.put(key, val);
				}
			}
//			DataSourcesManager.loadDataSource( ltName, nodeDefName );
			
			// load Data Source Immediately If Transient
			if ( new HashSet<String>( Arrays.asList( Util.splitByCommas(options) ) ).contains( "TRANSIENT" ) ) {
				DataSourcesManager.refreshDataSource( ltName, nodeDefName );
				logger.logInfo("Recording transient property updates: " + updates);
				
				for ( Iterator<Object> iter = inMemoryDataSourceProperties.keySet().iterator(); iter.hasNext(); )
					if ( ((String)iter.next()).startsWith(nodeDefName) ) iter.remove();
				
				for ( String key : updates.keySet() ) {
					String val = updates.get(key);
					if ( null != val ) inMemoryDataSourceProperties.put(key, val);
				}
				
//				inMemoryDataSourceProperties.putAll( updates );
				// TODO: also put any VTI properties (e.g. "cache.expires" and "schema") into
				// transient properties? probably not because they don't require a reload...
			} else
				persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
			
    	} catch (Exception e) {
//			upr.remove(nodeDefName + VTI_SUFFIX); // undo main update
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			logger.logException(GDBMessages.CONFIG_DS_SET_VTI_ERROR, "Unable to load Data Source " + nodeDefName + ": ", e);
			throw e;
		}
	}
	
	private static void deriveRequiredDataSourceUpdatesFromOptionsAndColumnsLists( 
			Map<String, String> updates, String nodeDefName, String options, String columnsCSV ) {
		
		if ( null != options ) updates.put(nodeDefName + OPTIONS_SUFFIX, options);
		
		String[] cols = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrSingleQuotes( columnsCSV, ',' );

		Pattern explicitColumnMappingPattern = Pattern.compile("[\\s]*C[1-9][0-9]*[\\s]*=.*");
		
		for ( int i=0; i<cols.length; i++ ) {
			// Note this doesn't conflict with option 'MAP_COLUMNS_BY_POSITION' - 
			// The explicitly defined mappings will override positional mapping for those cols.
			if ( explicitColumnMappingPattern.matcher(cols[i]).matches() ) {
				int idx = cols[i].indexOf('=');
				updates.put(nodeDefName + '_' + cols[i].substring(0, idx).trim(), cols[i].substring(idx+1).trim());
			} else
				// default matching by position
				updates.put(nodeDefName + COLUMN_LABEL + (i+1), cols[i]);
		}
	}
	
	public static synchronized void setDataSourceRDBTable( String ltName, String dsName, String cid, String rdbTable, 
			String options, String columnsCSV ) throws Exception {
    	apiStart(setdsrdbtable, Arrays.asList(ltName, dsName, cid, rdbTable, options, columnsCSV));
    	
    	cid = cid.toUpperCase();
		ltName = ltName.toUpperCase();
		String nodeDefName = ltName + "_DS" + dsName.toUpperCase();
		
		Properties backup = null;
		try {
			if ( !DataSourcesManager.isLogicalTableLoaded(ltName) ) throw new Exception("Parent logical table is not loaded: " + ltName);
			// Clear any trace of this data source in upr and return corresponding null mappings for persitence.
			Map<String, String> updates = generateNulledOutUpdatesForPropertiesPrefixed(nodeDefName);
			
			// Validate that the jdbc connection is defined
			getRDBConnectionDetailsAsString(cid);
			
			updates.put(nodeDefName + CONNECTION_SUFFIX, cid + " " + rdbTable);
			
			deriveRequiredDataSourceUpdatesFromOptionsAndColumnsLists( updates, nodeDefName, options, columnsCSV );
			
//			detect explicit mappings too, e.g. "c1=kks, c6=kk"
			
			// Apply the updates and load the data source (thus validating syntax aswell) before updating the config file
			// Config file reload will still occur (for now) but will do minimal work.
			// Putting all updates in upr immediately will mean a concurrent reload of data sources will have a chance to
			// load this data source before we next get a chance to synchronize on the full DataSourcesManager.class (when reloading the config file)
			// Note upr is a Hashtable therefore it cannot be given null values - insead we have to remove those keys
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );
				for ( String key : updates.keySet() ) {
					String val = updates.get(key);
					if ( null == val ) upr.remove(key);
					else upr.put(key, val);
				}
			}
//			DataSourcesManager.loadDataSource( ltName, nodeDefName );
//			DataSourcesManager.primeJDBCSourceHandlesPoolSynchronously( nodeDefName );
			
			// load Data Source Immediately If Transient
			if ( new HashSet<String>( Arrays.asList( Util.splitByCommas(options) ) ).contains( "TRANSIENT" ) ) {
				DataSourcesManager.refreshDataSource( ltName, nodeDefName );
				logger.logInfo("Recording transient property updates: " + updates);
				
				for ( Iterator<Object> iter = inMemoryDataSourceProperties.keySet().iterator(); iter.hasNext(); )
					if ( ((String)iter.next()).startsWith(nodeDefName) ) iter.remove();
				
				for ( String key : updates.keySet() ) {
					String val = updates.get(key);
					if ( null != val ) inMemoryDataSourceProperties.put(key, val);
				}
				
//				inMemoryDataSourceProperties.putAll( updates );
				// TODO: also put any VTI properties (e.g. "cache.expires" and "schema") into
				// transient properties? probably not because they don't require a reload...
			} else
				persistAndApplyConfigUpdatesForSpecificLT(updates, ltName);
			
		} catch ( Exception e ) {
//			upr.remove(nodeDefName + CONNECTION_SUFFIX); // undo main update
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to create Data Source '" + nodeDefName + "', cause: ";
			logger.logException(GDBMessages.CONFIG_DS_SET_RDB_ERROR, warning, e);
			throw new Exception(warning + e);
		}
	}
	
	public static synchronized void setDataSourceRDBTable( String ltName, String dsName, String rdbTable, String options, String columnsCSV ) throws Exception {		
		apiStart(setdslocalderby, Arrays.asList(ltName, dsName, rdbTable, options, columnsCSV));
		setDataSourceRDBTable(ltName, dsName, LOCALDERBY_CONNECTION, rdbTable, options, columnsCSV);
	}
	
	public static synchronized void removeDataSource( String dsID ) throws Exception {
    	apiStart(removeds, Arrays.asList(dsID));
		dsID = dsID.toUpperCase();
		try {
			int ltend = dsID.lastIndexOf("_DS");
			if ( -1 == ltend ) throw new Exception("This ID does not identify a Data Source (no _DS suffix)");
			
//			boolean wasTransient = false;
			synchronized( upr ) {
				for ( Iterator<Object> iter = inMemoryDataSourceProperties.keySet().iterator(); iter.hasNext(); ) {
					String key = (String) iter.next();
					if ( key.startsWith(dsID) ) {
						logger.logInfo("Removing transient ds property (from transient and user props): " + key);
						iter.remove();
						upr.remove(key);
	//					wasTransient = true;
					}
				}
			}
			
			// Let the data source be removed automatically on forced config reload...
			
//			if ( wasTransient ) {
//				while ( -1 != ltend ) {
//					String ltName = dsID.substring(0, ltend);
//					if ( DataSourcesManager.isLogicalTableLoaded(ltName) ) {
//						logger.logInfo("Removing data source: " + key);
//						DataSourcesManager.removeDataSource(ltName, dsID);
//						break;
//					}
//				}
//			}
			
			Set<String> possibleLTs = new HashSet<String>();
			int dsIdx = -1;
			
			// Determine if this might be a data source property of a defined logical table.
			while ( -1 < ( dsIdx = dsID.indexOf("_DS", dsIdx+1) ) ) {
				String possibleLT = getUserProperty( dsID.substring(0, dsIdx) + LTDEF_SUFFIX );
				if ( null != possibleLT  ) possibleLTs.add( possibleLT );
			}
			
			// We don't really need to persist/apply cfg updates if the data source consisted of transient properties
			// only. However - best be sure we remove any potentially overriding properties in the cfg file as well.
			
			Map<String, String> updates = generateNulledOutUpdatesForPropertiesPrefixed(dsID);
			persistAndApplyConfigUpdatesForSpecificLTs(updates, possibleLTs);
		} catch ( Exception e ) {
			String warning = "Failed to remove Data Source '" + dsID + "', cause: ";
			logger.logException(GDBMessages.CONFIG_DS_REMOVE_ERROR, warning, e);
			throw new Exception(warning + e);
		}
	}
	
//	private static Map<String, String> generateNulledOutPropertiesForDataSourceEntriesWithPrefix( String dsID ) throws Exception {
//		
//		int ltend = dsID.lastIndexOf("_DS");
//		if ( -1 == ltend ) throw new Exception("This ID does not identify a Data Source (no _DS suffix)");
//		// Validate logical table exists
////		String ltName = dsID.substring(0, ltend);
////		if ( null == getUserProperty(ltName + LTDEF_SUFFIX) )
////			throw new Exception("Logical Table does not exist: " + ltName);
//
//		// Not correct.. suffixes can come beyond the _DSX marker, e.g. _CONNECTION
////		try { Integer.parseInt( dsID.substring(ltend+3) ); }
////		catch ( Exception e ) {
////			throw new Exception("This ID does not identify a Data Source (cannot resolve index integer after _DS suffix)");
////		}
//
//		Map<String, String> m = generateNulledOutUpdatesForPropertiesPrefixed(dsID, null);
//		Iterator<String> i = m.keySet().iterator();
//		synchronized( upr ) {
//			while ( i.hasNext() ) upr.remove(i.next());
//		}
//		return m;
////		persistAndApplyConfigUpdates(updates);
//	}
	
	// Take care using this method...... Only use if the prefix will definitely encompass only the properties you want
	private static Map<String, String> generateNulledOutUpdatesForPropertiesPrefixed( String prefix ) throws Exception {
		
		Map<String, String> m = new LinkedHashMap<String, String>();
		
		// Now we can use wildcards...
		// This is cleaner because they get applied to the properties in the current config file, not the loaded properties of an old file anymore...!
		if ( null != prefix && !prefix.endsWith("*") )
			prefix += '*';
		
		m.put(prefix, null);
		
//		String[] props = getAllUserPropertiesWithPrefix(prefix);
//		for ( int i=0; i<props.length; i++ )
////			if ( null == allowedSuffixes || stringEndsWithOneOf( props[i], allowedSuffixes ) )
//				m.put(props[i], null);
		
		return m;
	}
	
//	private static boolean stringEndsWithOneOf( String s, String[] suffixes ) {
//		for ( String suffix : suffixes ) if ( s.endsWith(suffix) ) return true;
//		return false;
//	}
	
//	public static void setGaianCred( String usr, String pwd ) throws Exception {
//    	apiStart(setgaiancred, Arrays.asList(usr, pwd));
//		Map updates = new LinkedHashMap();
//		updates.put(GAIAN_NODE_USR, usr);
//		updates.put(GAIAN_NODE_PWD, pwd);
//		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
//	}
	
	public static synchronized void setNumConnectionsSought( int numConnections ) throws Exception {
    	apiStart(setminconnections, Arrays.asList(numConnections+""));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(MIN_DISCOVERED_CONNECTIONS, numConnections + "");
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setAccessClusters( String clustersCSV ) throws Exception {
    	apiStart(setaccessclusters, Arrays.asList(clustersCSV));
    	Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(ACCESS_CLUSTERS, clustersCSV);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setDiscoveryHosts( String columnsCSV ) throws Exception {
    	apiStart(setdiscoveryhosts, Arrays.asList(columnsCSV));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(ACCESS_HOSTS_PERMITTED, columnsCSV);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setDiscoveryIP( String ip ) throws Exception {
    	apiStart(setdiscoveryip, Arrays.asList(ip));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(DISCOVERY_IP, ip);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}

	public static synchronized void setMaxPropagation( int depth ) throws Exception {
    	apiStart(setmaxpropagation, Arrays.asList(depth+""));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(MAX_PROPAGATION, depth+"");
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setMaxPoolsizes( int size ) throws Exception {
    	apiStart(setmaxpoolsizes, Arrays.asList(size+""));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(MAX_POOLSIZES, size+"");
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setLogLevel( String level ) throws Exception {
    	apiStart(setloglevel, Arrays.asList(level));
		level = level.toUpperCase();
		if ( false == Logger.setLogLevel(level) )
			throw new Exception("Not a valid log level: " + level + " Allowed values are: " + Arrays.asList( Logger.POSSIBLE_LEVELS ));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(LOGLEVEL, level);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setSourceList( String listID, String sourcesCSV ) throws Exception {
    	apiStart(setsourcelist, Arrays.asList(listID, sourcesCSV));
		listID = listID.toUpperCase();
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(listID + SOURCELIST_SUFFIX, sourcesCSV);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setMsgBrokerDetails( String host, int port, String topic ) throws Exception {
    	apiStart(setmsgbrokerdetails, Arrays.asList(host, port+""));
		Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(MSGBROKER_HOST, host);
		updates.put(MSGBROKER_PORT, port+"");
		updates.put(MSGBROKER_TOPIC, topic);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void setConfigProperty( String propertyName, String value ) throws Exception {
    	apiStart(setconfigproperty, Arrays.asList(propertyName, value));
		
    	if ( null == propertyName ) return;
    	
		// TODO: check if property name relates to a transient data source - e.g. if it exists in transient set.
		// If so, change the transient one, update the upr as well and reload the data source (don't reload if it was a VTI Property)
		// Blocked on this for now as cant resolve ltname and dsname deterministically...
		if ( inMemoryDataSourceProperties.contains(propertyName) )
			throw new Exception("Cannot update transient data source property: '" + propertyName +
					"'. Please remove the data source first with removeds().");
    	
    	Map<String, String> updates = new LinkedHashMap<String, String>();
		updates.put(propertyName, value); //null == value || 0 == value.length() ? null : value);
		
		if ( isPropertyPotentiallyImpactingALogicalTable( propertyName ) )
			persistAndApplyConfigUpdates(updates);
		else
			persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
		
		if ( propertyName.endsWith(LTDEF_SUFFIX) || propertyName.endsWith(CONSTANTS_SUFFIX) || MANAGE_LTVIEWS_WITH_TABLE_FUNCTIONS.equals( propertyName ) )
			DataSourcesManager.checkUpdateLogicalTableViewsOnAllDBs();
		
		if ( GaianNode.isInTestMode() ) { incrementTestInvocationCount(setconfigproperty); }
	}
	
	//TBD PDS to comment this.
	public static boolean setConfigProperties (Map<String, String> properties){
		// Copied from setLogicalTableForExcel(...)
		Properties backup = null;
		try {

			// Load data source now so we can report if any errors occur in doing so (e.g. file not found)
			// properties must be set to load data source successfully
			backup = new Properties();
			synchronized(upr) {
				backup.putAll( upr );

				/*
				 * Update upr with the new values.
				 * To avoid NPEs we iterate through the values & do null checks, rather than use putAll.
				 * Note: because upr is a Properties object (and extends Hashtable), it is automatically synchronized 
				 */
				Set<Entry<String,String>> updatesEntries = properties.entrySet();

				for (Entry<String, String> updateEntry : updatesEntries) {

					if(updateEntry.getKey() != null) {

						if(updateEntry.getValue() == null) {
							upr.remove(updateEntry.getKey()); //TBD wild card null execution
						}
						else {
							upr.put( updateEntry.getKey(), updateEntry.getValue() );
						}
					}	
				}
//				upr.putAll(properties);
			}

			persistAndApplyConfigUpdates(properties);

		} catch ( Exception e ) {
			if ( null != backup ) { Properties tmp = upr; synchronized(upr) { upr = backup; } tmp.clear(); }
			String warning = "Failed to update Properties, cause: ";
			logger.logException(GDBMessages.CONFIG_LT_CREATE_DS_EXCEL_ERROR, warning, e);
			return false; //failure
		}
		return true; //success
	}

	
	public static synchronized void setConfigProperties( String sqlQueryReturningPropertyKeysAndValues ) throws Exception {
    	apiStart(setConfigProperties, Arrays.asList( sqlQueryReturningPropertyKeysAndValues ));

    	Connection c = getDefaultDerbyConnection();
    	try { setConfigProperties( sqlQueryReturningPropertyKeysAndValues, c); }
    	finally { if ( null != c ) c.close(); }
	}
	
	public static void addGateway( String ip ) throws Exception {
    	apiStart(addgateway, Arrays.asList(ip));
    	Map<String, String> updates = new LinkedHashMap<String, String>();
    	String dgprop = getDiscoveryGateways();
    	List<String> dg = Arrays.asList( Util.splitByCommas( dgprop ) );
    	if ( dg.contains(ip) )
    		throw new Exception("Gateway is already registered: " + ip);
    	if ( null != dgprop && 0 < dgprop.trim().length() ) dgprop += ", " + ip; else dgprop = ip;
		updates.put(DISCOVERY_GATEWAYS, dgprop);
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
	public static synchronized void removeGateway( String ip ) throws Exception {
    	apiStart(removegateway, Arrays.asList(ip));
    	Map<String, String> updates = new LinkedHashMap<String, String>();
    	String dgprop = getDiscoveryGateways();
    	List<String> dg = Arrays.asList( Util.splitByCommas( dgprop ) );
    	if ( false == dg.remove(ip) )
    		throw new Exception("Gateway was not registered: " + ip + ", current list is: " + dgprop);
    	String list = dg.toString();
		updates.put(DISCOVERY_GATEWAYS, list.substring(1, list.length()-1));
		persistAndApplyConfigUpdatesAffectingNoLogicalTables(updates);
	}
	
//	public static void registerUser( String usr, String pwd ) throws SQLException {
//		logEnteredMsg("registerUser", Arrays.asList(usr, pwd) );
//		SecurityManager.registerUser(usr, pwd);
//	}
	
	public static void setUser( String usr, String affiliation, String clearance, String pwd ) throws Exception {
		try {
			SecurityManager.registerUser(usr, affiliation, clearance, pwd);
		} catch ( Exception e ) {
			String warning = "Failed to set/register new user: '" + usr + "', cause: ";
			logger.logException(GDBMessages.CONFIG_USER_SET_ERROR, warning, e);
			throw new Exception(warning + e);
		}
	}
	
	public static void removeUser( String usr ) throws Exception {
		try {
			SecurityManager.removeUser(usr);
		} catch ( Exception e ) {
			String warning = "Failed to remove user: '" + usr + "', cause: ";
			logger.logException(GDBMessages.CONFIG_USER_REMOVE_ERROR, warning, e);
			throw new Exception(warning + e);
		}
	}
}
