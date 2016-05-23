/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import sun.misc.BASE64Decoder;

import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.apps.HttpQueryInterface;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.mongodb.MongoConnectionFactory;
import com.ibm.gaiandb.mongodb.MongoConnectionParams;
import com.ibm.gaiandb.plugins.wpml.GenericPolicyPluginForWPML;
import com.ibm.gaiandb.security.common.KerberosToken;
import com.ibm.gaiandb.security.server.authn.KerberosUserAuthenticator;
import com.ibm.gaiandb.tools.SQLRunner;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

/**
 * @author Dominic Harries, David Vyvyan
 */
public class GaianDBUtilityProcedures extends GaianDBProcedureUtils {
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "GaianDBUtilityProcedures", 30 );
	// Standard VARCHAR lengths
	private static final String TSTR = Util.TSTR;
	private static final String SSTR = Util.SSTR;
	private static final String MSTR = Util.MSTR;
	//private static final String LSTR = Util.LSTR;
	//private static final String VSTR = Util.VSTR;
	private static final String XSTR = Util.XSTR;
	
	private static final String addquery = "addquery";
	private static final String setqueryfieldsql = "setqueryfieldsql";
	private static final String removequery = "removequery";
	private static final String manageConfig = "manageConfig";
	private static final String runSQL = "runSQL";
	private static final String populateMongo = "populateMongo";
	private static final String getMetaDataJDBC = "getMetaDataJDBC";
	private static final String getTablesJDBC = "getTablesJDBC";
//	private static final String runquery = "runquery";
	
	//*************************************
	// GENERAL UTILITY FUNCTIONS/PROCEDURES
	//*************************************
	
	static final String PROCEDURES_SQL = 
	"!DROP PROCEDURE "+addquery+";!CREATE PROCEDURE "+addquery+"(id VARCHAR("+HttpQueryInterface.MAX_ID_LENGTH+"), " +
			"description VARCHAR("+HttpQueryInterface.MAX_DESCRIPTION_LENGTH+")," +
			"issuer VARCHAR("+HttpQueryInterface.MAX_ISSUER_LENGTH+"), query VARCHAR("+HttpQueryInterface.MAX_QUERY_LENGTH+")," +
			"fields VARCHAR("+HttpQueryInterface.MAX_QUERY_LENGTH+"))"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.addQuery'"
	+ ";"
	+ "!DROP PROCEDURE "+setqueryfieldsql+";!CREATE PROCEDURE "+setqueryfieldsql+"(query_id VARCHAR("+HttpQueryInterface.MAX_ID_LENGTH+"), " +
			"field VARCHAR("+HttpQueryInterface.MAX_ID_LENGTH+"), query VARCHAR("+HttpQueryInterface.MAX_QUERY_LENGTH+"))"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.setQueryFieldSql'"
	+ ";"
	+ "!DROP PROCEDURE "+removequery+";!CREATE PROCEDURE "+removequery+"(id VARCHAR("+HttpQueryInterface.MAX_ID_LENGTH+"))"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.removeQuery'"

//	+ ";"
//	+ "!DROP PROCEDURE "+runquery+";!CREATE PROCEDURE "+runquery+"(id VARCHAR("+HttpQueryInterface.MAX_ID_LENGTH+"), parmsCSV "+XSTR+")"
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.runQuery'"	

	+ ";"
	+ "!DROP PROCEDURE "+runSQL+";!CREATE PROCEDURE "+runSQL+"(sql_expression "+XSTR+", rdbmsConnectionID "+XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.runSQL'"	

	+ ";"
	+ "!DROP PROCEDURE "+getMetaDataJDBC+";!CREATE PROCEDURE "+getMetaDataJDBC+"(cid "+XSTR+", catalog "+XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getMetaDataJDBC'"	

	+ ";"
	+ "!DROP PROCEDURE "+getTablesJDBC+";!CREATE PROCEDURE "
	+ getTablesJDBC+"(cid "+XSTR+", catalog "+XSTR+", schemaPattern "+XSTR+", tablePattern "+XSTR+", tableTypesCSV "+XSTR+/*", requiredCols "+XSTR+*/")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getTablesJDBC'"	
	
//	+ ";"
//	+ "!DROP PROCEDURE populateMongo;!CREATE PROCEDURE populateMongo(url "+XSTR+", collection "+XSTR+", csvKeyValueAssignments "+XSTR+")"
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.populateMongo'"
	
//	+ ";"
//	+ "!DROP PROCEDURE "+manageConfig+";!CREATE PROCEDURE "+manageConfig+"(command "+XSTR+", config_entry "+XSTR+")"
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.manageConfig'"	

	
	+ ";" // Execute outer query with parameter substituted in from result of nested query
	+ "!DROP PROCEDURE NESTEXEC;!CREATE PROCEDURE NESTEXEC(SQL_QUERY "+XSTR+", SQL_NESTED "+XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.nestExecuteQuery'"
	+ ";" // Concatenate rows
	+ "!DROP FUNCTION CONCATRS;!CREATE FUNCTION CONCATRS(SQL_QUERY "+XSTR+", ROWDEL "+XSTR+", COLDEL "+XSTR+") RETURNS CLOB(2G)"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.concatResultSet'"
	
	+ ";" // Get gdb_node, filename, last_modified, size and checksum for a file path on all nodes in the network
	+ "!DROP PROCEDURE getfilestats;!CREATE PROCEDURE getfilestats(FILE_PATH "+XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getFileStats'"
	
	+ ";" // Get File as Blob
	+ "!DROP FUNCTION GETFILEB;!CREATE FUNCTION GETFILEB(FILE_PATH "+XSTR+") RETURNS BLOB(2G)"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getFileB'"

	+ ";" // Get File as GZipped Blob
	+ "!DROP FUNCTION GETFILEBZ;!CREATE FUNCTION GETFILEBZ(FILE_PATH "+XSTR+") RETURNS BLOB(2G)"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getFileBZ'"
	
	+ ";" // Extracts and copies over a file from another node in the network.
	+ "!DROP FUNCTION COPYFILE;!CREATE FUNCTION COPYFILE(FROM_NODE "+XSTR+", FROM_PATH "+XSTR+", TO_PATH "+XSTR+") RETURNS INT"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.copyFileFromNode'"
	
//	+ ";" // Deploys a file from a path location on the local node to a single node or to all nodes in the network.
//	+ "!DROP PROCEDURE DEPLOYFILE;!CREATE PROCEDURE DEPLOYFILE(FROM_LOC "+XSTR+", TO_LOC "+XSTR+") PARAMETER STYLE JAVA LANGUAGE JAVA"
//	+ " READS SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.deployFile'"
//	
// TODO: CONVERT TO PROCEDURE XRIPPLE
//	+ ";" // Deploys a file to all nodes by making each layer of nodes extract the file in turn from its sender node
//	+ "!DROP FUNCTION XRIPPLE;!CREATE FUNCTION XRIPPLE(FROM_PATH "+XSTR+", TO_PATH "+XSTR+", ARGS "+XSTR+") RETURNS INT"
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.rippleExtract'"

	+ ";"
	+ "!DROP PROCEDURE LISTENV;!CREATE PROCEDURE LISTENV(ENV_PROPERTY_OR_NULL "+XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.listEnv'"
	
	+ ";"
	+ "!DROP PROCEDURE LISTTHREADS;!CREATE PROCEDURE LISTTHREADS()"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.listThreads'"
	
	+ ";" // Net info for interface on closest matching ip
	+ "!DROP PROCEDURE LISTNET;!CREATE PROCEDURE LISTNET(IP_PREFIX "+XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.listNet'"
	
	+ ";" // Get node's PID
	+ "!DROP FUNCTION GDB_PID;!CREATE FUNCTION GDB_PID() RETURNS INT"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianNode.getPID'"
	
	+ ";" // Data Throughput
	+ "!DROP FUNCTION GDB_THROUGHPUT;!CREATE FUNCTION GDB_THROUGHPUT() RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianNode.getDataThroughput'"
	+ ";" // Query Activity
	+ "!DROP FUNCTION GDB_QRY_ACTIVITY;!CREATE FUNCTION GDB_QRY_ACTIVITY() RETURNS INT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianNode.getQueryActivity'"
	+ ";" // CPU Workload
	+ "!DROP FUNCTION GDB_NODE_CPU;!CREATE FUNCTION GDB_NODE_CPU() RETURNS INT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianNode.getNodeCPUInLastPeriod'"
	
	+ ";"
	+ "!DROP FUNCTION GETTHREADS;!CREATE FUNCTION GETTHREADS() RETURNS TABLE(ID BIGINT, GRP " + XSTR + ", NAME " + XSTR + ", PRIORITY INT, STATE " + TSTR
	+ ", CPU INT, CPUSYS INT, ISSUSPENDED BOOLEAN, ISINNATIVE BOOLEAN, BLOCKCOUNT BIGINT, BLOCKTIME BIGINT, WAITCOUNT BIGINT, WAITTIME BIGINT)"
	+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA READS SQL DATA"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getThreads'"
	
//	+ ";" // Table function for getting system environment variable - not really necessary for now - Stored procedure listenv() is enough.
//	+ "!DROP FUNCTION GETENV;!CREATE FUNCTION GETENV(PROPERTY_OR_NULL "+XSTR+") RETURNS TABLE(PROPERTY "+XSTR+", VALUE "+XSTR+")"
//	+ " PARAMETER STYLE DERBY_JDBC_RESULT_SET LANGUAGE JAVA READS SQL DATA"
//	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getEnv'"
	
	+ ";" // Web service call SCALAR UDF
	+ "!DROP FUNCTION WSGET;"
	//CREATE FUNCTION WSGETRS(URL VARCHAR(32672), options VARCHAR(32672)) RETURNS TABLE(COL1 VARCHAR(32672),COL2 VARCHAR(32672),COL3 VARCHAR(32672),COL4 VARCHAR(32672),COL5 VARCHAR(32672),COL6 VARCHAR(32672),COL7 VARCHAR(32672),COL8 VARCHAR(32672),COL9 VARCHAR(32672),COL10 VARCHAR(32672))	 LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.webServiceGetAsTable'
	+ "!CREATE FUNCTION WSGET(URL " + XSTR + ", options " + XSTR + ") RETURNS " + XSTR + " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.webServiceGetAsString';"  
	
/**unused for now when in RTC: */
//  	+ ";" // Web service call TABLE UDF.
//	//select * FROM TABLE(WSGETRS('http://www.dantressangle.com/album.htm','dbg=1'))  T
//	+ "!DROP FUNCTION WSGETRS;"
//	+ "!CREATE FUNCTION WSGETRS(URL " + XSTR + ", options " + XSTR + ") RETURNS TABLE(COL1 "+XSTR+",COL2 "+XSTR+",COL3 "+XSTR+",COL4 "+XSTR+",COL5 "+XSTR+",COL6 "+XSTR+",COL7 "+XSTR+",COL8 "+XSTR+",COL9 "+XSTR+",COL10 "+XSTR+")"
//	+ " LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA "
//	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.webServiceGetAsTable';"  
/***/	
	 
	+ ";" // JVM Heap Memory used after GC (Bytes)
	+ "!DROP FUNCTION JMEMORY;!CREATE FUNCTION JMEMORY() RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jMemory'"
	+ ";" // JVM Heap Memory Maximum
	+ "!DROP FUNCTION JMEMORYMAX;!CREATE FUNCTION JMEMORYMAX() RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jMemoryMax'"
	+ ";" // JVM Heap Memory Percentage used after GC 
	+ "!DROP FUNCTION JMEMORYPERCENT;!CREATE FUNCTION JMEMORYPERCENT() RETURNS INT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jMemoryPercent'"
	+ ";" // JVM Non Heap Memory used 
	+ "!DROP FUNCTION JMEMORYNONHEAP;!CREATE FUNCTION JMEMORYNONHEAP() RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jMemoryNonHeap'"
	+ ";" // Launch a Garbage Collection
	+ "!DROP FUNCTION GDBGC;!CREATE FUNCTION GDBGC(count int) RETURNS INT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.garbageCollect'"
	
	+ ";" // Scalar function for getting hash value of a String
	+ "!DROP FUNCTION JSLEEP;!CREATE FUNCTION JSLEEP(MILLIS INT) RETURNS SMALLINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jSleep'"
	
	// THESE DONT WORK AS BLOB/CLOB CAN'T BE PASSED IN AS ARGUMENTS TO STORED FUNCTIONS/PROCEDURES - COULD CHANGE TO VARCHAR BUT LESS WORTHWHILE...
	+ ";" // Scalar function for zipping a blob
	+ "!DROP FUNCTION JZIP;!CREATE FUNCTION JZIP(DATA BLOB(2G)) RETURNS BLOB(2G) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jzip'"

	+ ";" // Scalar function for unzipping a blob
	+ "!DROP FUNCTION JUNZIP;!CREATE FUNCTION JUNZIP(DATA BLOB(2G)) RETURNS BLOB(2G) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.junzip'"
	
//	+ ";" // Get node's underlying OS platform name
//	+ "!DROP FUNCTION JOS;!CREATE FUNCTION JOS() RETURNS "+XSTR
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.getOSName'"

	+ ";" // Scalar function for getting hash value of a String
	+ "!DROP FUNCTION JHASH;!CREATE FUNCTION JHASH(S "+XSTR+") RETURNS INT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jHash'"
//	+ ";" // Scalar function for decoding a URL - Already exists!! Functions SUSTR(col, start) and SUBSTR(col, start, end)
//	+ "!DROP FUNCTION JSUBSTR;!CREATE FUNCTION JSUBSTR(S "+XSTR+", START INT, END INT) RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
//	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jSubstring'"
	+ ";" // Scalar function for decoding a URL
	+ "!DROP FUNCTION JURLDECODE;!CREATE FUNCTION JURLDECODE(S "+XSTR+") RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jURLDecode'"
	+ ";" // Scalar function for encoding a URL
	+ "!DROP FUNCTION JURLENCODE;!CREATE FUNCTION JURLENCODE(S "+XSTR+") RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jURLEncode'"
	+ ";" // Function that converts a timestamp to the number of seconds since the Unix epoch
	+ "!DROP FUNCTION JSECS;!CREATE FUNCTION JSECS(T TIMESTAMP) RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jTimestampSeconds'"
	+ ";" // Function that converts a timestamp to the number of milliseconds since the Unix epoch
	+ "!DROP FUNCTION JMILLIS;!CREATE FUNCTION JMILLIS(T TIMESTAMP) RETURNS BIGINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jTimestampMilliseconds'"
	+ ";" // Function that converts a bigint milliseconds value to a date
	+ "!DROP FUNCTION JTSTAMP;!CREATE FUNCTION JTSTAMP(I BIGINT) RETURNS TIMESTAMP PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jMillis2Timestamp'"
	+ ";" // Function that retrieves a quoted parameter in a string holding multiple parameters each preceded by a single quote
	+ "!DROP FUNCTION JQUOTED;!CREATE FUNCTION JQUOTED(QUOTED_STRING "+XSTR+", POSITION INT) RETURNS "+MSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jGetQuotedParameter'"
	+ ";" // Function that performs java formatting based on coded input string (with % symbols) and a list of referenced arguments to substitute in
	+ "!DROP FUNCTION JFORMAT;!CREATE FUNCTION JFORMAT(FORMAT_STRING "+XSTR+", CSVARGS "+XSTR+") RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jGetFormattedString'"
	+ ";" // Function that replaces the first instance of matched regex string in an input string with a replacement string
	+ "!DROP FUNCTION JREPLACEFIRST;!CREATE FUNCTION JREPLACEFIRST(S "+XSTR+", REGEX "+XSTR+", REPLACEMENT "+XSTR+") RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jReplaceFirst'"
	+ ";" // Function that replaces instances of matched regex strings in an input string with a replacement string
	+ "!DROP FUNCTION JREPLACE;!CREATE FUNCTION JREPLACE(S "+XSTR+", REGEX "+XSTR+", REPLACEMENT "+XSTR+") RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jReplaceAll'"
	+ ";" // Function that computes whether a string matches a regular expression
	+ "!DROP FUNCTION JMATCHER;!CREATE FUNCTION JMATCHER(S "+XSTR+", REGEX "+XSTR+") RETURNS SMALLINT PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jMatchRegex'"
	+ ";" // Function that takes a security token (as part of authentication) and returns a unique session identity
	+ "!DROP FUNCTION GAIANDB.AUTHTOKEN;!CREATE FUNCTION AUTHTOKEN(ST "+XSTR+") RETURNS "+XSTR+" PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.jSetAuthToken'"
	+ ";" // Procedure used to register a new gaiandb user schema, such that procedures/functions and views are all accessible directly as synonyms under this schema	
	+ "!DROP PROCEDURE GDBINIT_USERDB;!CREATE PROCEDURE GDBINIT_USERDB() PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.initialiseGdbUserDatabase'"
	+ ";"
//	+ "!DROP PROCEDURE DEPLOY_SEARCH_FHE;!CREATE PROCEDURE DEPLOY_SEARCH_FHE(BYTES_URI "+Util.XSTR+")"
//	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1"
//	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.deploySearchFHE'"
//	+ ";"

	+ "!DROP PROCEDURE FHE_SEARCH_ALL;!CREATE PROCEDURE FHE_SEARCH_ALL(BYTES_URI "+Util.XSTR+")"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.fheSearchAll'"
	+ ";" // Scalar function to decrypt an FHE result
	+ "!DROP FUNCTION FHE_DECRYPT;!CREATE FUNCTION FHE_DECRYPT(DATA BLOB(2G), METADATA "+Util.XSTR+") RETURNS BLOB(2G) PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.fheDecrypt'"
	+ ";"
	+ "!DROP FUNCTION FHE_SEARCH;!CREATE FUNCTION FHE_SEARCH(BYTES_URI "+Util.XSTR+") RETURNS BLOB(2G)"
	+ " PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.fheSearch'"
	
	;
//	+ ";" // Procedure used to register a new gaiandb user schema, such that procedures/functions and views are all accessible directly as synonyms under this schema	
//	+ "!DROP PROCEDURE GDBTST;!CREATE PROCEDURE GDBTST() PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL"
//	+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.gdbTest'"
//	;
	

//	public static void deploySearchFHE( final String bytesURI, ResultSet[] rs ) throws Exception {
//		
//		
//	}
	
	
	// Need a procedure to receive multiple search results on the client
	public static void fheSearchAll( String filePath, ResultSet[] rs ) throws Exception {
		
		try {
			int idx = filePath.lastIndexOf('/');
			String fileName = 0 > idx ? filePath : filePath.substring( idx+1 );
			if ( 0 > idx ) filePath = System.getenv("FHE_WORKSPACE") + "/suspects/" + fileName;
			
			final int dashIdx = getAccessClusters().trim().indexOf('-');
			String affiliation = -1 < dashIdx ? getAccessClusters().trim().substring(0, dashIdx) : "None";
			if ( affiliation.equals("KISH") ) affiliation = "Kish";
			
			setFirstResultSetFromQueryAgainstDefaultConnection( rs,
					"select '" + fileName + ".' || "+ gdb_node + " || '.jpg' result_filename, "
				+	"FHE_DECRYPT(res, gdb_node) result_data from new com.ibm.db2j.GaianQuery('select FHE_SEARCH(''"
				+	GaianDBConfig.getGaianNodeID()+" "+new File(filePath).getCanonicalPath()+"'') res from sysibm.sysdummy1', 'with_provenance') GQ" // ,maxDepth=1
				+   " where gdb_node != '" + GaianDBConfig.getGaianNodeID() + "' -- GDB_CREDENTIALS=" + affiliation, "" );
			
		} catch ( Exception e ) { e.printStackTrace(); }
		
		// Query below can't work because the inner-query "select 0 isEncrypted..." is going to Derby (not Gaian) so we can't change the isEncrypted value.
//		return getResultSetFromQueryAgainstDefaultConnection(
//					"select '" + fileName + ".' || "+ gdb_node + " || '.jpg' result_filename, "
//				+	"CASE WHEN isEncrypted > 0 THEN FHE_DECRYPT(res) ELSE res END result_data"
//				+	" from new com.ibm.db2j.GaianQuery('select 0 isEncrypted, FHE_SEARCH(''"
//				+	GaianDBConfig.getGaianNodeID()+" "+new File(filePath).getCanonicalPath()+"'') res from sysibm.sysdummy1', 'with_provenance') GQ"
//				+   " where gdb_node != '" + GaianDBConfig.getGaianNodeID() + "' -- GDB_CREDENTIALS=" + affiliation );
	}
	
	// Need a decryption function to decrypt results on the fly in SQL without having to re-create a new ResultSet ourselves
	public static Blob fheDecrypt( Blob data, String metaData ) throws Exception {
		
		if ( null == data ) return null; // nothing to do
		
		final String fileName = "suspect." + System.currentTimeMillis() + "." + System.nanoTime() + ".fhe.ctxt"; // alice2.sh requires extn to be .ctxt
		File file = new File(fileName);
		
		Util.copyBinaryData( data.getBinaryStream(), new FileOutputStream(file) );
		
		if ( 100000 > file.length() ) { // TODO: Need a better way to determine whether data is encrypted - e.g. using header bytes..
			// assume data is not encrypted - i.e. the node returning this data was trusted
			file.delete();
			return data;
		}
		
		Util.runSystemCommand( new String[] { DIR_FHE_SCRIPTS + "/alice2.sh", "-e", GaianDBConfig.getGaianNodeID(), "-f", metaData, file.getPath(), fileName + ".jpg" }, true );
		
		file.delete(); // delete input file (encrypted)
		byte[] bytes = Util.getFileBytes( file = new File(fileName + ".jpg") );
		file.delete(); // delete output file (decrypted)
		
		// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
		Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
		blob.setBytes(1, bytes);
		
		return blob;
	}
	
	public static final String DIR_FHE_SCRIPTS = System.getenv("FHE_SERVICES") + "/capes/demo";
	
	// Need a local search function that just returns a Blob because the is no simple way of creating a result set from scratch that contains Blobs.
	public static Blob fheSearch( String bytesURI ) throws Exception {
		
		// The URI scheme targets a file on a node using syntax: "<NodeID> <FilePath>"
		// New schemes could be used in future, e.g. to target a db table or a web location.
		
		bytesURI = bytesURI.trim();
		int idx = bytesURI.indexOf(' ');
		final boolean isDistributeMode = 0 > idx; // Bootstrap mechanism: Distribute the query to other nodes if a nodeID was not passed in

//		throw new Exception("FHE_SEARCH() argument 'bytesURI' does not conform to syntax: '<NodeID> <FilePath>'");

		final String originatorNodeID = isDistributeMode ? "" : bytesURI.substring(0,idx);
		final String filePath = bytesURI.substring(idx+1);
		
		idx = filePath.lastIndexOf('/');
		String fileName = 0 > idx ? filePath : filePath.substring( idx+1 );
		
		idx = fileName.lastIndexOf('.');
		final String fileNameMinusExtension = 0 > idx ? fileName : fileName.substring(0, idx);
		
		File file = new File(fileName);

		final String localNodeID = GaianDBConfig.getGaianNodeID();
		
//		System.out.println("idx =  " + idx);
		
		if ( isDistributeMode ) { // Distribution mode: propagate deployment query to all nodes in network
			
			// DRV 26/02/2016 - THIS CODE PATH IS ONLY USED IF YOU WANT THE RESULT FILES TO APPEAR IN THE WORKSPACE OF THE ENTRY-POINT NODE
			//					TO MAKE THEM APPEAR IN THE WORKING DIR OF THE CLIENT INSTEAD, USE PROCEDURE FHE_SEARCH_ALL.
			
			final int dashIdx = getAccessClusters().trim().indexOf('-');
			String affiliation = -1 < dashIdx ? getAccessClusters().trim().substring(0, dashIdx) : "None";
			if ( affiliation.equals("KISH") ) affiliation = "Kish";
			
			ResultSet rs = getDefaultDerbyConnection().createStatement().executeQuery(
						"select res, gdb_node from new com.ibm.db2j.GaianQuery('select FHE_SEARCH(''"
					+	GaianDBConfig.getGaianNodeID()+" "+new File(filePath).getCanonicalPath()+"'') res from sysibm.sysdummy1', 'with_provenance') GQ"
					+   " where gdb_node != '" + GaianDBConfig.getGaianNodeID() + "' -- GDB_CREDENTIALS=" + affiliation );
			
			int numResults = 0;
			
			while (rs.next()) {
				numResults++;
				byte[] resultBytes = rs.getBytes(1);
				if ( null == resultBytes || 1 > resultBytes.length ) continue;
				String provenanceNode = rs.getString(2);
				
				boolean isEncrypted = 100000 < resultBytes.length;
				
//				idx = 0 < fileName.lastIndexOf('.') ? filePath.lastIndexOf('.') : -1;
//				String extn = 0 > idx ? "" : filePath.substring(idx);
				file = new File(fileName + "." + provenanceNode + "." + (isEncrypted?"fhe":"jpg")); // + extn);
				writeToFileAfterUnzip( file, resultBytes );
				System.out.println("Entry-point node received result Blob written to file: " + file.getPath() + ", size: " + file.length());
				
				// FHE decrypt
				if ( isEncrypted ) {
					Util.runSystemCommand( new String[] { DIR_FHE_SCRIPTS + "/alice2.sh", 
							"-e", localNodeID, file.getPath(), fileName + "." + provenanceNode + ".jpg" }, true );
//					Thread.sleep(4000);
//					Util.runSystemCommand( new String[] { GenericPolicyPluginForWPML.DIR_WEB_EVENTING + "/webDemoEvent.sh", "31", "", "start" } );
//					Thread.sleep(2000);
//					Util.runSystemCommand( new String[] { GenericPolicyPluginForWPML.DIR_WEB_EVENTING + "/webDemoEvent.sh", "32", "", "start" } );
//					Thread.sleep(2000);
//					Util.runSystemCommand( new String[] { GenericPolicyPluginForWPML.DIR_WEB_EVENTING + "/webDemoEvent.sh", "33", "", "start" } );
				}
			}
			
			System.out.println("Received all results - numNodes having returned a result: " + numResults);
			rs.getStatement().getConnection().close();
			return null;
		}
		
		Connection c = null;
		final String cid = GaianDBConfig.getDiscoveredConnectionID( originatorNodeID );
		
		boolean isEncrypted = false;
		
		// Only need to load file if we are not on the entry-point node... (which is where the file originates!) 
		if ( false == originatorNodeID.equals( localNodeID ) ) {
			
			try {
				System.out.println("Looked up cid for origin node: " + originatorNodeID + ", as being: " + cid);
				
//				final String connectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString(cid);
//				c = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getConnection();

				c = getDefaultDerbyConnection();
				
				final int dashIdx = getAccessClusters().trim().indexOf('-');
				String affiliation = -1 < dashIdx ? getAccessClusters().trim().substring(0, dashIdx) : "None";
				if ( affiliation.equals("KISH") ) affiliation = "Kish";
				
//				GenericPolicyPluginForWPML.webDemoEvent("declaring-capability", ); TODO: <----
				
//				ResultSet rs = c.createStatement().executeQuery("select getFileBZ('"+filePath+"') fzbytes from sysibm.sysdummy1");
				ResultSet rs = c.createStatement().executeQuery(
						"select * from new com.ibm.db2j.GaianQuery("
							+ "'select 0 isEncrypted, getFileBZ(''"+filePath+"'') fzbytes from sysibm.sysdummy1', 'with_provenance') GQ where GDB_NODE = '" // ,maxDepth=1
							+ originatorNodeID + "' -- GDB_CREDENTIALS=" + affiliation
//							+ "'select 0 isEncrypted, getFileBZ(''"+filePath+"'') fzbytes from sysibm.sysdummy1', 'with_provenance, maxDepth=0') GQ"
//							+ " -- GDB_CREDENTIALS=" + affiliation
						);

				if ( false == rs.next() ) {
					System.err.println("-----> Gaian Query referencing GETFILEBZ() (to access a remote file) returned 0 records - possibly due to Policy - returning with no result");
					rs.close();
					return null;
				}

				isEncrypted = 0 < rs.getInt(1); // Trusted/Non-encrypted = 0, Untrusted/Encrypted = 1 (policy plugin would set this value)
				byte[] bytes = rs.getBytes(2);
				System.out.println("Received record, isEncrypted? " + isEncrypted 
						+ ", bytes.length: " + bytes.length + " for file: '" + filePath + "' from node: " + originatorNodeID);
				
				if ( isEncrypted ) file = new File( fileName = fileNameMinusExtension + ".ctxt" );
				
				// sendingDataObjectForAnalysis - end
				GenericPolicyPluginForWPML.webDemoEventReceivingData( originatorNodeID, filePath, "query", "end", "" );
				
				writeToFileAfterUnzip( file, bytes );
				System.out.println("Wrote byte[] to local file: " + file.getCanonicalPath() + ", length: " + file.length());

				rs.close();	
			}
			catch (Exception e) {
				throw new Exception("Unable to get fhe search result from node: " + originatorNodeID + ": " + Util.getStackTraceDigest(e));
			}
			finally {
//				System.out.println("Recycling connection");
//				if ( null != originatorNodeID && null != c ) // Return connection to pool (may get closed immediately if not referenced by a data source or sourcelist)
//					DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getRDBConnectionDetailsAsString(cid) ).push(c);
				if ( null != c ) c.close();
			}
		}
		
//		Thread.sleep(2000); // allow time for UI visualisation of message being sent
		
		// executingAnalytic - start
		GenericPolicyPluginForWPML.webDemoEvent("executing-analytic", "{}", "start", "");
		
		
		// IF file is not encrypted => Do standard feature vector comparisons and return image ; otherwise => Do FHE comparisons
		if ( isEncrypted ) {
			file = new File( fileNameMinusExtension + ".fheSearchResult.ctxt" ); // best to end with .ctxt (some scripts require this extension)
			Util.runSystemCommand( new String[] { DIR_FHE_SCRIPTS + "/bob.sh", "-e", localNodeID, fileName, file.getName() }, true );
//			Thread.sleep(7000);
		} else {
			file = new File( fileNameMinusExtension + ".plainSearchResult.jpg" );
			Util.runSystemCommand( new String[] { DIR_FHE_SCRIPTS + "/plainBob.pl", "-e", localNodeID, fileName, file.getName() }, true );
//			Thread.sleep(8000);
		}

		// executingAnalytic - end
		GenericPolicyPluginForWPML.webDemoEvent("executing-analytic", "{}", "end", "");
		
		byte[] bytes = Util.getFileBytes( file ); // readAndZipFileBytes( file );
		
		System.out.println("Returning comparison result file: " + file.getCanonicalPath() + ", as Blob... numBytes: " + bytes.length);
		
		file.delete();
		
		// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
		Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
		blob.setBytes(1, bytes);
		
		// returningAnalyticResult - start
		GenericPolicyPluginForWPML.webDemoEventSendingData( originatorNodeID, "", "result", "start", "" );
		
		return blob;
	}
	
	public static void runSQL( String sqlOrFile, String cid, ResultSet[] rs ) throws Exception {
		apiStart(runSQL, Arrays.asList(cid, sqlOrFile));

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
	
	public static void getMetaDataJDBC( String cid, String catalog, ResultSet[] rs ) throws Exception {
		apiStart(getMetaDataJDBC, Arrays.asList(cid, catalog));
		
		if ( null != catalog && 1 > catalog.trim().length() ) catalog = null;
		else catalog = catalog.trim();
		
		Connection c = null;
		
		try {
			if ( null == cid ) c = getDefaultDerbyConnection();
			else {
				String connectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString(cid, false);
				if ( null == connectionDetails ) return; // undefined cid - no result. TODO: return this error in the resultset... ?
				c = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getConnection();
//				c =	DataSourcesManager.getPooledJDBCConnection(connectionDetails, DataSourcesManager.getSourceHandlesPool(connectionDetails));
			}
			
			Set<String> csvStringsResult = new HashSet<String>();
			ResultSet res = null;
			DatabaseMetaData dmd = c.getMetaData();
			
			if ( null == catalog ) {
				res = dmd.getCatalogs(); while (res.next()) csvStringsResult.add( "'CATALOG', '"+res.getString(1)+"'" ); res.close();
			}
			res = dmd.getSchemas(catalog, null); while (res.next()) csvStringsResult.add( "'SCHEMA', '"+res.getString(1)+"'" ); res.close();
			res = dmd.getTableTypes(); while (res.next()) csvStringsResult.add( "'TABLETYPE', '"+res.getString(1)+"'" ); res.close();

			final String sql = "select * from " + transformCollectionOfCsvToSqlTableExpression(csvStringsResult, "MDTYPE, MDVALUE");
//			System.out.println("Processing sql: " + sql);
			
			setFirstResultSetFromQueryAgainstDefaultConnection( rs, sql, "ORDER BY MDTYPE, MDVALUE" );
			
		} catch ( Exception e ) { System.out.println("Unable to get database meta-data: " + e); e.printStackTrace();
		} finally {
			if ( null != cid && null != c ) // Return connection to pool (may get closed immediately if not referenced by a data source or sourcelist)
				DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getRDBConnectionDetailsAsString(cid) ).push(c);
		}
	}
	
	public static void getTablesJDBC( String cid, String catalog, String schemaPattern, String tablePattern, String tableTypesCSV,
			/*String requiredCols,*/ ResultSet[] rs ) throws Exception {		
		apiStart(getTablesJDBC, Arrays.asList(cid, catalog, schemaPattern, tablePattern, tableTypesCSV));
		
		if ( null != catalog && 1 > catalog.trim().length() ) catalog = null; else catalog = catalog.trim();
		if ( null != schemaPattern && 1 > schemaPattern.trim().length() ) schemaPattern = null; else schemaPattern = schemaPattern.trim();
		if ( null != tablePattern && 1 > tablePattern.trim().length() ) tablePattern = "%"; else tablePattern = tablePattern.trim();
		final String[] tableTypes = null != tableTypesCSV && 1 > tableTypesCSV.trim().length() ? null : Util.splitByCommas(tableTypesCSV);
		
		Connection c = null;
		int rowLimit = 100;
		
		try {
			if ( null == cid ) c = getDefaultDerbyConnection();
			else {
				int idx =  cid.indexOf(' ');
				if ( 0 < idx ) {
					try { rowLimit = Integer.parseInt(cid.substring(idx+1)); cid = cid.substring(0, idx); }
					catch (Exception e) { throw new Exception( "Invalid cid. Must have a row limit value after optional first space: " + e); }
				}
				String connectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString(cid, false);
				if ( null == connectionDetails ) return; // undefined cid - no result. TODO: return this error in the resultset... ?
				c = GaianDBConfig.getNewDBConnector( GaianDBConfig.getConnectionTokens(connectionDetails) ).getConnection();
//				c =	DataSourcesManager.getPooledJDBCConnection(connectionDetails, DataSourcesManager.getSourceHandlesPool(connectionDetails));
			}
			
			Set<String> csvStrings = new HashSet<String>();
			DatabaseMetaData dmd = c.getMetaData();
			ResultSet res = dmd.getTables(catalog, schemaPattern, tablePattern, tableTypes);
			
//			TABLE_CAT String => table catalog (may be null)
//			TABLE_SCHEM String => table schema (may be null)
//			TABLE_NAME String => table name
//			TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
//			REMARKS String => explanatory comment on the table
//			TYPE_CAT String => the types catalog (may be null)
//			TYPE_SCHEM String => the types schema (may be null)
//			TYPE_NAME String => type name (may be null)
//			SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
//			REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)

			// Resolve col names..
			ResultSetMetaData rsmd = res.getMetaData();
			StringBuilder colNames = new StringBuilder("\""+Util.escapeDoubleQuotes(rsmd.getColumnLabel(1))+"\"");
			for ( int i=2; i<=rsmd.getColumnCount(); i++ ) colNames.append (", \""+Util.escapeDoubleQuotes(rsmd.getColumnLabel(i)) + "\"");

			int numRows = 0;
			while ( res.next() && ( numRows++ < rowLimit || 0 > rowLimit ) ) {
				StringBuilder csvRow = new StringBuilder();
				csvRow.append( "'"+Util.escapeSingleQuotes(res.getString(1))+"'" ); // Note: All columns of the getTables() result are strings
				for ( int i=2; i<=rsmd.getColumnCount(); i++ ) csvRow.append( ", '"+Util.escapeSingleQuotes(res.getString(i))+"'" );
				csvStrings.add(csvRow.toString());
			}
			res.close();
			
			final String sql = "select *"
//				+ (null!=requiredCols && 0<requiredCols.length() ? requiredCols : "*")
				+ " from " + transformCollectionOfCsvToSqlTableExpression(csvStrings, colNames.toString());

			System.out.println("Processing sql: " + sql + " " + "ORDER BY " + colNames);
			
			setFirstResultSetFromQueryAgainstDefaultConnection( rs, sql, "ORDER BY " + colNames );
			
		} catch ( Exception e ) { System.out.println("Unable to get db tables: " + e); e.printStackTrace();
		} finally {
			if ( null != cid && null != c ) // Return connection to pool (may get closed immediately if not referenced by a data source or sourcelist)
				DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getRDBConnectionDetailsAsString(cid) ).push(c);
		}
	}
	
	public static void populateMongo( String url, String collection, String csvKeyValueAssignments ) throws Exception {
		apiStart(populateMongo, Arrays.asList(url, collection, csvKeyValueAssignments)); // mongo collection ~ rdbms table ; mongo document ~ rdbms record

		MongoConnectionParams connDetails = new MongoConnectionParams (url);
		DB mongoDb = MongoConnectionFactory.getMongoDB(connDetails);
		
		if ( null == csvKeyValueAssignments ) {
			if ( mongoDb.collectionExists(collection) ) mongoDb.getCollection(collection).drop();
			return;
		}
		
		DBCollection dbcollection =
			mongoDb.collectionExists(collection) ? mongoDb.getCollection(collection) : mongoDb.createCollection(collection, null);
			
		String[] cellAssignments = Util.splitByCommas(csvKeyValueAssignments);
		
		BasicDBObject doc = new BasicDBObject();
		for ( String cellAssgnmt : cellAssignments ) {
			int idx = cellAssgnmt.indexOf('=');
			if ( 1 > idx ) continue;
			doc.put( cellAssgnmt.substring(0,idx), cellAssgnmt.substring(idx+1) );
		}
		dbcollection.insert( doc );
	}
	
	private static final String GAIAN_CONFIG = "GAIAN_CONFIG";
	private static String configurations_registry_cid;
	
//	public static void manageConfig( final String command, String config_entry, ResultSet[] rs ) throws Exception {
//		apiStart(manageConfig, Arrays.asList(command, config_entry));
//
//		final boolean isConfigEntrySpecified = null != config_entry && 0 < config_entry.length();
//		
//		Connection regConnection = null;
//		Statement regStmt = null;
//		
//		// select name, creator, ctime, card, npages, fpages, stats_time, refresh_time, last_regen_time, invalidate_time, alter_time, lastused from sysibm.sysdummy1
//		// Unnecessary: GAIAN_CONFIG: ENTRY_NAME MSTR, CREATOR MSTR, CREATED TIMESTAMP, UPDATED TIMESTAMP, NUMSAVES INT, NUMPROPS INT, NUMBYTES INT
//		// GAIAN_CONFIG_<ENTRY_NAME>: PROPERTY XSTR, VALUE XSTR
//		
//		try {
//			regConnection = DataSourcesManager.getPooledJDBCConnection(
//					connectionDetails, DataSourcesManager.getSourceHandlesPool(handleDescriptor), timeout);
//			regStmt = regConnection.createStatement();
//			ResultSet rsCfg = null;
//			
//			final boolean isLoad = "LOAD".equalsIgnoreCase(command);
//			final boolean isSave = !isLoad && "SAVE".equalsIgnoreCase(command);
//			final boolean isView = !isLoad && !isSave && "VIEW".equalsIgnoreCase(command);
//			final boolean isList = !isLoad && !isSave && !isView; // This is the default command
//			
//			if ( isList ) {
//				// Note: config_entry may contain wildcard '%'
//				rs[0] = regStmt.executeQuery("select name, creator, created, updated, size from sysibm.systables where name like '"
//						+ GAIAN_CONFIG + ( isConfigEntrySpecified ? "_" + config_entry : "%" ) + "'");
//				return;
//			}
//			
//			// Now handle LOAD, SAVE or VIEW:
//			
//			String config_owner = null;
//			
//			rsCfg = regStmt.executeQuery("select name, owner from sysibm.systables where name "
//					+ ( isConfigEntrySpecified ? "='" + GAIAN_CONFIG + '_' + config_entry : "like '" + GAIAN_CONFIG + "%" )
//					+ "' order by "+ CREATED +" desc fetch first 1 rows only");
//			
//			if ( rsCfg.next() ) {
//				config_entry = rsCfg.getString(1);
//				config_owner = rsCfg.getString(2);
//			} else
//				// Only allow missing entry if this is a SAVE and a config_entry was specified (this would be a new one to be created)
//				if ( !isSave || !isConfigEntrySpecified ) return; // No result, just return 0 records.
//			
//			if ( isLoad )
//				setConfigProperties("select property, value from " + config_owner + '.' + GAIAN_CONFIG + '_' + config_entry, regConnection);
//			else if ( isSave )
//				regStmt.execute("insert into " + config_owner + '.' + GAIAN_CONFIG + '_' + config_entry + " values " + getConfigAsValuesString());
//			else if ( isView )
//				rs[0] = regStmt.executeQuery("select * from " + owner + '.' + config_entry);
//			
//		} catch ( Exception e ) {
//			rs[0] = getResultSetFromQueryAgainstDefaultConnection(
//					"SELECT '" + e + "' SQL_FAILURE FROM SYSIBM.SYSDUMMY1");
//		} finally {
//			if ( null != regStmt ) regStmt.close();
//			// Recycle registry's pooled connection
//			if ( null != regConnection )
//				DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getSourceHandlesPool(handleDescriptor) ).push(regConnection);
//		}
//	}
	
//	public static void gdbTest() { System.out.println("GDBTEST called on node: " + GaianDBConfig.getGaianNodeID()); }
	
	// Data types to establish the baseline memory in use. We keep track of the usage of GC pools
	// immediately after GC and instantaneously determine the size of nonGCmemory pools.
	private static Map<String, MemoryUsage> GCMemoryPoolUsage = new HashMap<String, MemoryUsage>();
	//private static List<MemoryPoolMXBean> nonGCMemoryPools = new ArrayList<MemoryPoolMXBean>();
	
	public static void addQuery( String id, String description, String issuer, String query, String fields ) throws Exception {
		Connection conn = getDefaultDerbyConnection();
		try {
			// Handle variable substitutions for gaian subqueries
			GaianSubqueryFieldParser gspp = new GaianSubqueryFieldParser();
			query = gspp.extractFields( query );
			SortedMap<Short, String> substitutedFields = gspp.getExtractedFields();
			
			String dummyQuery = query;
			int delta = 0;
			for ( Short offset : substitutedFields.keySet() ) {
				dummyQuery = dummyQuery.substring( 0, offset + delta ) + "0" + dummyQuery.substring( offset + delta );
				delta++; // i.e. += "0".length();
			}
			
			String[] fieldsArr = null != fields && 0 < fields.length() ? fields.split( "," ) : new String[0];
			
			// DRV - December 2014: Added shortcut to avoid checking field count if there are clearly none.
			if (-1 < dummyQuery.indexOf('?') || 0 < fields.length() ) {
				
				logger.logInfo("Preparing dummy query: " + dummyQuery);
				
				// Prepare the query to check that the SQL is valid and find out how many fields it should have
				PreparedStatement statement = conn.prepareStatement( dummyQuery );
				try {
					ParameterMetaData pmd = statement.getParameterMetaData();
					int numParams = pmd.getParameterCount();
					
					if ( fieldsArr.length != numParams )
						throw new Exception( "Number of ?s in query different to number of fields specified." );
				} finally {
					statement.close();
				}	
			}
			
			PreparedStatement statement = conn.prepareStatement( "INSERT INTO " + HttpQueryInterface.QUERIES_TABLE_NAME +
					" (id, description, issuer, query) VALUES (?, ?, ?, ?)" );
			try {
				statement.setString( 1, id );
				statement.setString( 2, description );
				statement.setString( 3, issuer );
				statement.setString( 4, query );
				
				statement.executeUpdate();
			} finally {
				statement.close();
			}
			
			if ( fieldsArr.length > 0 || substitutedFields.size() > 0 ) {
				statement = conn.prepareStatement( "INSERT INTO " + HttpQueryInterface.QUERY_FIELDS_TABLE_NAME + 
						" (query_id, seq, offset, name) VALUES (?, ?, ?, ?)" );
				try {
					for ( int i = 0; i < fieldsArr.length; i++ ) {
						statement.setString( 1, id );
						statement.setShort( 2, (short) (i + 1) );
						statement.setNull(3, Types.SMALLINT);
						statement.setString( 4, fieldsArr[i] );
						
						statement.addBatch();
					}
					
					for ( Map.Entry<Short, String> f : substitutedFields.entrySet() ) {
						statement.setString( 1, id );
						statement.setNull(2, Types.SMALLINT);
						statement.setShort( 3, f.getKey() );
						statement.setString( 4, f.getValue() );
						
						statement.addBatch();
					}
					
					statement.executeBatch();
				} finally {
					statement.close();
				}
			}
		} finally {
			conn.close();
		}
	}
	
	public static void setQueryFieldSql( String queryId, String field, String query ) throws Exception {
		Connection conn = getDefaultDerbyConnection();
		try {
			// Prepare query to check that it is valid
			PreparedStatement statement = conn.prepareStatement(query);
			statement.close();
			
			// Add it to database
			statement = conn.prepareStatement( "UPDATE " + HttpQueryInterface.QUERY_FIELDS_TABLE_NAME + 
					" SET query = ? WHERE query_id = ? AND name = ?" );
			try {
				statement.setString( 1, query );
				statement.setString( 2, queryId );
				statement.setString( 3, field );
				
				int updateCount = statement.executeUpdate();
				if ( updateCount == 0 ) {
					throw new Exception( "Update failed: could not find field '" + field + "' for query '" + queryId + "'" );
				}
			} finally {
				statement.close();
			}
		} finally {
			conn.close();
		}
	}
	
	public static void removeQuery( String id ) throws Exception {
		Connection conn = getDefaultDerbyConnection();
		try {
			PreparedStatement statement = 
				conn.prepareStatement( "DELETE FROM " + HttpQueryInterface.QUERIES_TABLE_NAME + " WHERE id = ?" );
			try {
				statement.setString(1, id);
				int deleteCount = statement.executeUpdate();
				if ( deleteCount == 0 ) {
					throw new Exception( "Removal failed: could not find query with id '" + id + "'" );
				}
			} finally {
				statement.close();
			}
		} finally {
			conn.close();
		}
	}
	
//	private static Map<String,PreparedStatement> pstmtsOfWrapperQueries = new HashMap<String,PreparedStatement>();
	
	/**
	 * Runs queries added by addQuery() - only supports Queries having Integer and String parameter types.
	 * 
	 * @param id
	 * @param parmsCSV
	 * @param rs
	 * @throws Exception
	 */
	public static void runQuery( String id, String parmsCSV, ResultSet[] rs ) throws Exception {
		
//		PreparedStatement pstmt = pstmtsOfWrapperQueries.get(id);

//		if ( null == pstmt ) {
			Connection conn = getDefaultDerbyConnection();
			logger.logInfo("Getting query id '" + id + "' from table: " + HttpQueryInterface.QUERIES_TABLE_NAME);
			PreparedStatement queryPstmt = conn.prepareStatement( "SELECT QUERY FROM " + HttpQueryInterface.QUERIES_TABLE_NAME + " WHERE id = ?" );
			queryPstmt.setString(1, id);
			ResultSet queryRS = queryPstmt.executeQuery();
			if ( false == queryRS.next() ) return; // No query -> no result
			String savedQuery = queryRS.getString(1);
			logger.logInfo("Got/preparing query: " + savedQuery);
			PreparedStatement pstmt = conn.prepareStatement( savedQuery );
//			pstmtsOfWrapperQueries.put( id, pstmt );
			queryPstmt.close();
//		}
		
		String[] parms = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrSingleQuotes( parmsCSV, ',' );
		logger.logInfo("Setting query parms: " + Arrays.asList( parms ) );
		for( int i=0; i<parms.length; i++ ) {
			String p = parms[i];
			if ( '\'' == p.charAt(0) ) pstmt.setString(i+1, p.substring(1, p.length()-1));
			else pstmt.setInt(i+1, Integer.parseInt(p));
		}
		
		rs[0] = pstmt.executeQuery();
		rs[0].getStatement().getConnection().close();
	}
	
	private static class GaianSubqueryFieldParser {
		//private static final String GAIAN_SUBQUERY_SIGNATURE = GaianQuery.class.getName();
		
		private static final String FIELD_START = "?";
		private static final String FIELD_END = "?";
		
		private SortedMap<Short, String> fields;
		
		public String extractFields( String sql ) throws Exception {
			int pos = 0;
			//Stack<Integer> subqueryQuotes = new Stack<Integer>(); // holds number of quotes for each subquery in stack
			//boolean inSubqueryString = false;
			fields = new TreeMap<Short, String>();
			
			/*while ( pos < sql.length() ) {
				if (sql.startsWith(GAIAN_SUBQUERY_SIGNATURE, pos)) {
					pos += GAIAN_SUBQUERY_SIGNATURE.length();
					pos = sql.indexOf( '\'', pos );
					if ( pos == -1 ) {
						throw new Exception( "Could not parse query" );
					}
					int quoteDepth = countQuotes(sql, pos);
					subqueryQuotes.push(quoteDepth);
					pos += quoteDepth;
				} else if ( subqueryQuotes.size() > 0 ) { // in a subquery
					int numQuotes = countQuotes(sql, pos);
					if (numQuotes > 0) {
						if (!inSubqueryString) {
							if (numQuotes == subqueryQuotes.peek()) { // closing the subquery
								pos += numQuotes;
								subqueryQuotes.pop();
							} else if (numQuotes == subqueryQuotes.peek() * 2) { // string inside subquery
								pos += numQuotes;
								inSubqueryString = true;
							} else if (numQuotes == subqueryQuotes.peek() * 4) { // empty string
								pos += numQuotes;
							} else {
								throw new Exception("Encountered " + numQuotes + " quotes at position " + pos + " - aborting.");
							}
						} else {
							if (numQuotes < subqueryQuotes.peek() * 2) {
								throw new Exception("Encountered " + numQuotes + " quotes at position " + pos + " - aborting.");
							} else if (numQuotes == subqueryQuotes.peek() * 2) {
								pos += numQuotes;
								inSubqueryString = false;
							} else if (numQuotes == subqueryQuotes.peek() * 3) { // handle case where '' is at the end of string
								pos += subqueryQuotes.peek() * 2;
								inSubqueryString = false;
							} else {
								pos += numQuotes;
							}
						}
					} else if (sql.startsWith(FIELD_START, pos)) {
						int end = sql.indexOf( FIELD_END, pos + FIELD_START.length());
						if ( end == -1 ) {
							throw new Exception( "Non-terminated field string" );
						}
						String fieldName = sql.substring( pos + FIELD_START.length(), end );
						short offset = (short) pos;
						int length = end - pos + FIELD_END.length();
						
						fields.put( fieldName, offset );
						
						// Remove field from sql string
						sql = sql.substring( 0, offset ) + sql.substring( offset + length );
					} else {
						pos++;
					}
				} else {
					pos++;
				}
			}
			if ( subqueryQuotes.size() > 0 ) {
				throw new Exception( "Could not parse query" );
			}*/
			
			while ( pos < sql.length() ) {
				if (sql.startsWith(FIELD_START, pos)) {
					// There must only be letters and number in a field name (no spaces)
					int end = sql.indexOf( FIELD_END, pos + FIELD_START.length());
					if ( end != -1 ) {
						String fieldName = sql.substring( pos + FIELD_START.length(), end );
						if (Pattern.matches("^[a-zA-Z0-9_]+$", fieldName)) {
							short offset = (short) pos;
							int length = end - pos + FIELD_END.length();
							
							fields.put( offset, fieldName );
							
							// Remove field from sql string
							sql = sql.substring( 0, offset ) + sql.substring( offset + length );
						} else {
							pos += FIELD_START.length();
						}
					} else {
						pos += FIELD_START.length();
					}
				} else {
					pos++;
				}
			}
			
			return sql;
		}

		public SortedMap<Short, String> getExtractedFields() {
			return fields;
		}
		
//		private int countQuotes(String s, int pos) {
//			int count = 0;
//			
//			while (s.charAt(pos) == '\'') {
//				count++;
//				pos++;
//			}
//			
//			return count;
//		}
	}
	
	/* Basic unit test type thing */
//	public static void main(String[] args) throws Exception {
//		GaianSubqueryFieldParser gsfp = new GaianSubqueryFieldParser();
//		String[] queries = {
//				"SELECT * FROM LTO",
//				"SELECT * FROM LTO WHERE x = ?",
//				// field not in quotes, should not be found (no longer true)
//				"SELECT * FROM new com.ibm.db2j.GaianQuery('select * from LT0 where a = ?blah?')",
//				"SELECT * FROM new com.ibm.db2j.GaianQuery('select * from LT0 where a = ''?blah?''')",
//				"call addquery(''TestXML'',''test xml query'',''GRAHAM''," +
//				"''Values(''''<Products> <Product> <ID>597</ID>'''')" +
//				"UNION ALL Values(''''<Card><ID>598</ID><Identifier>D92871CA-D217-4124-B8FB-89B9A2CFFCB4</Identifier><SourceDateTimeModified>2004-01-01 00:00:00.0</SourceDateTimeModified><DateTimeModified>2004-01-01 00:00:00.0</DateTimeModified><Status>NEW</Status><NumberOfParts>1</NumberOfParts><SourceLibrary>CIDNE</SourceLibrary></Card>'''') " +
//				"UNION ALL Values(''''<File><ID>599</ID><Archived>0</Archived><Creator>UNKNOWN</Creator><DateTimeDeclared>2004-01-01 00:00:00.0</DateTimeDeclared><Extent>1.0</Extent><Format>.txt</Format><FormatVersion>1.0</FormatVersion><ProductURL>db2://AFGIS/GBENT.WAR_DIARY/REPORTKEY/D92871CA-D217-4124-B8FB-89B9A2CFFCB4</ProductURL><Title>VRSG DIRECT FIRE Other</Title><IsProductLocal>1</IsProductLocal></File>'''') " +
//				"UNION ALL Values(''''<MetaDataSecurity><ID>600</ID><Classification>SECRET</Classification><Policy>NATO</Policy><Releasability>NATO</Releasability></MetaDataSecurity>'''') " +
//				"UNION ALL Values(''''<Parts><PartIdentifier>0000000001</PartIdentifier><Common><ID>603</ID><DescriptionAbstract>Enemy Action Direct Fire</DescriptionAbstract><Type>DOCUMENT</Type></Common><Coverage><ID>604</ID><SpatialGeographicReferenceBox><gmlPoint srsName=\"EPSG:4326\"><gmlcoord><gmlX>32.6833191</gmlX><gmlY>69.4161072</gmlY></gmlcoord></gmlPoint></SpatialGeographicReferenceBox><TemporalStart>2009-04-06T12:54:51.000000</TemporalStart></Coverage><Security><ID>605</ID><Classification>SECRET</Classification><Policy>NATO</Policy><Releasability>NATO</Releasability></Security></Parts>'''')" +
//						"			UNION ALL Values(''''</Product> </Products>'''')'','''')",
//				"SELECT * FROM new com.ibm.db2j.GaianQuery('select * from LT0 where a = ''?blah?'' and b = ''?foo?'' ')",
//				"SELECT * FROM new FUNCTION('com.ibm.db2j.GaianQuery(''select * from LT0 where a = ''''?blah?'''' and b = ''''?foo?'''''')')",
//				
//				"values concatrs(' values(''<?xml version=\"1.0\" encoding=\"UTF-8\"?><gml:FeatureCollection xmlns:gml=\"http://www.opengis.net/gml\"><Products>'') union all SELECT CAST( ''<Product> <Card><Identifier>'' || ID || ''</Identifier><SourceDateTimeModified>'' || DATE || ''</SourceDateTimeModified><SourceLibrary>CIDNE</SourceLibrary></Card> <File><Creator>'' ||CREATOR|| ''</Creator><Format>txt</Format><ProductURL>'' || URL || ''</ProductURL><Title>'' || TITLE || ''</Title><IsProductLocal>1</IsProductLocal></File> <Parts><DescriptionAbstract>'' || TYPE ||'' ''|| CATEGORY || ''</DescriptionAbstract><Type>DOCUMENT</Type><Coverage><SpatialGeographicReferenceBox>'' || LOCATION || ''</SpatialGeographicReferenceBox><TemporalStart>'' || DATE || ''</TemporalStart></Coverage><Security><Classification>'' || CLASSIFICATION || ''</Classification><Policy>NATO</Policy><Releasability>NATO</Releasability></Security></Parts> </Product>'' AS CLOB(10M)) FROM NEW com.ibm.db2j.GaianQuery('' select VARCHAR(db2gse.ST_AsGML(g.geometry), 500) as LOCATION, d.REPORTKEY as ID, d.TYPE as TYPE, d.CATEGORY as CATEGORY, d.DATE AS DATE, d.CLASSIFICATION AS CLASSIFICATION, d.ORIGINATORGROUP as CREATOR, d.DURL as URL, d.TITLE as TITLE from gbent.war_diary d, gbent.diary_geometry g where db2gse.EnvelopesIntersect(g.geometry, 67.2, 32.3, 67.25, 32.35, 1003) = 1 and d.reportkey=g.reportkey'', '''',''SOURCELIST=DIARY'', ''LOCATION VARCHAR(255), ID VARCHAR(255), TYPE VARCHAR(255), CATEGORY VARCHAR(255), DATE VARCHAR(255), CLASSIFICATION VARCHAR(255), CREATOR VARCHAR(255), URL VARCHAR(255), TITLE VARCHAR(255)'') GQ UNION ALL VALUES (''</Products></gml:FeatureCollection>'') ')",
//				
//				"values concatrs(' values(''<?xml version=\"1.0\" encoding=\"UTF-8\"?><gml:FeatureCollection xmlns:gml=\"http://www.opengis.net/gml\"><Products>'') union all SELECT CAST( ''<Product> <Card><Identifier>'' || ID || ''</Identifier><SourceDateTimeModified>'' || DATE || ''</SourceDateTimeModified><SourceLibrary>CIDNE</SourceLibrary></Card> <File><Creator>'' ||CREATOR|| ''</Creator><Format>txt</Format><ProductURL>'' || URL || ''</ProductURL><Title>'' || TITLE || ''</Title><IsProductLocal>1</IsProductLocal></File> <Parts><DescriptionAbstract>'' || TYPE ||'' ''|| CATEGORY || ''</DescriptionAbstract><Type>DOCUMENT</Type><Coverage><SpatialGeographicReferenceBox>'' || LOCATION || ''</SpatialGeographicReferenceBox><TemporalStart>'' || DATE || ''</TemporalStart></Coverage><Security><Classification>'' || CLASSIFICATION || ''</Classification><Policy>NATO</Policy><Releasability>NATO</Releasability></Security></Parts> </Product>'' AS CLOB(10M)) FROM NEW com.ibm.db2j.GaianQuery('' select VARCHAR(db2gse.ST_AsGML(g.geometry), 500) as LOCATION, d.REPORTKEY as ID, d.TYPE as TYPE, d.CATEGORY as CATEGORY, d.DATE AS DATE, d.CLASSIFICATION AS CLASSIFICATION, d.ORIGINATORGROUP as CREATOR, d.DURL as URL, d.TITLE as TITLE from gbent.war_diary d, gbent.diary_geometry g where db2gse.EnvelopesIntersect(g.geometry, cast(''''?MIN_LAT?'''' as float), cast(''''?MIN_LONG?'''' as float), cast(''''?MAX_LAT?'''' as float), cast(''''?MAX_LONG?'''' as float), 1003) = 1 and d.reportkey=g.reportkey'', '''',''SOURCELIST=DIARY'', ''LOCATION VARCHAR(255), ID VARCHAR(255), TYPE VARCHAR(255), CATEGORY VARCHAR(255), DATE VARCHAR(255), CLASSIFICATION VARCHAR(255), CREATOR VARCHAR(255), URL VARCHAR(255), TITLE VARCHAR(255)'') GQ UNION ALL VALUES (''</Products></gml:FeatureCollection>'') ')",
//				
//				"values concatrs(' values(''<?xml version=\"1.0\" encoding=\"UTF-8\"?><gml:FeatureCollection xmlns:gml=\"http://www.opengis.net/gml\"><Products>'') union all SELECT CAST( ''<Product> <Card><Identifier>'' || ID || ''</Identifier><SourceDateTimeModified>'' || DATE || ''</SourceDateTimeModified><SourceLibrary>CIDNE</SourceLibrary></Card> <File><Creator>'' ||CREATOR|| ''</Creator><Format>txt</Format><ProductURL>'' || URL || ''</ProductURL><Title>'' || TITLE || ''</Title><IsProductLocal>1</IsProductLocal></File> <Parts><DescriptionAbstract>'' || TYPE ||'' ''|| CATEGORY || ''</DescriptionAbstract><Type>DOCUMENT</Type><Coverage><SpatialGeographicReferenceBox>'' || LOCATION || ''</SpatialGeographicReferenceBox><TemporalStart>'' || DATE || ''</TemporalStart></Coverage><Security><Classification>'' || CLASSIFICATION || ''</Classification><Policy>NATO</Policy><Releasability>NATO</Releasability></Security></Parts> </Product>'' AS CLOB(10M)) FROM NEW com.ibm.db2j.GaianQuery('' select VARCHAR(db2gse.ST_AsGML(g.geometry), 500) as LOCATION, d.REPORTKEY as ID, d.TYPE as TYPE, d.CATEGORY as CATEGORY, d.DATE AS DATE, d.CLASSIFICATION AS CLASSIFICATION, d.ORIGINATORGROUP as CREATOR, d.DURL as URL, d.TITLE as TITLE from gbent.war_diary d, gbent.diary_geometry g where db2gse.EnvelopesIntersect(g.geometry, ?MIN_LAT?, ?MIN_LONG?, ?MAX_LAT?, ?MAX_LONG?, 1003) = 1 and d.reportkey=g.reportkey'', '''',''SOURCELIST=DIARY'', ''LOCATION VARCHAR(255), ID VARCHAR(255), TYPE VARCHAR(255), CATEGORY VARCHAR(255), DATE VARCHAR(255), CLASSIFICATION VARCHAR(255), CREATOR VARCHAR(255), URL VARCHAR(255), TITLE VARCHAR(255)'') GQ UNION ALL VALUES (''</Products></gml:FeatureCollection>'') ')",
//				
//				"SELECT head,durl FROM NEW com.ibm.db2j.GaianQuery('" +
//				"		select T.head as head,T.head_type as head_type, I.durl as durl from new com.ibm.db2j.ICAREST(''search,?KEYWORDS?'') I," + 
//				"		new com.ibm.db2j.GaianTable(''triple_store_s'') T " +
//				"		where T.head_type = ''?TYPE?'' and T.dnum = I.dnum " +
//				"		', 'maxDepth=0') GQ "
//
//		};
//		
//		for (String query : queries) {
//			System.out.println("Query: " + query);
//			String strippedQuery = gsfp.extractFields(query);
//			System.out.println("\twithout fields: " + strippedQuery);
//			for (Map.Entry<Short, String> f : gsfp.getExtractedFields().entrySet()) {
//				System.out.println("\t" + f.getValue() + " at " + f.getKey() + " (remaining: " + strippedQuery.substring(f.getKey()) + ")");
//			}
//			System.out.println();
//		}
//	}
	
	public static int garbageCollect(int i) throws Exception {
		apiStart("GarbageCollect");
		while ( 0 < i-- ) {
			Thread.sleep(100);
			System.gc();
		}
		return 1;
	}	

	//return the amount of java heap memory (in bytes) used after the last garbage collection 
	public static long jMemory (){
		// go through each pool and determine the usage
		List<MemoryPoolMXBean> memPools = ManagementFactory.getMemoryPoolMXBeans();
		long PoolUsageTotal = 0;
		for (MemoryPoolMXBean memPool : memPools) {
			MemoryUsage poolUsage;
			if (memPool.isCollectionUsageThresholdSupported()&&MemoryType.HEAP==memPool.getType()&&GCMemoryPoolUsage.containsKey(memPool.getName())){
			//this pool supports Garbage collection and we have the usage after the last GC
				poolUsage = GCMemoryPoolUsage.get(memPool.getName());
				PoolUsageTotal += poolUsage.getUsed();
			}
		};
		return PoolUsageTotal;
	}
	
	/**************************************************************************
	 * Static Data Structures required for  WSGETRS() and WSGET() UDFs CACHE.
	 *************************************************************************/
	private static final int    CACHE_SIZE_FOR_WEB_SERVICES_PAGES=100;
	private static final Map<String, String> CachedWebPages=new CachedHashMap<String, String>( CACHE_SIZE_FOR_WEB_SERVICES_PAGES );
	/*************************************************************************
	 * CAlls  a web service  and return the HTML Page. options can be added to change behaviour   
	 * @param newUrlSt : the HTTP URL of the web service to call (GET at the moment )
	 * @param options  : options supported right now are ; 'dbg=1/0,cached=1/0,timeout=100,NOTFOUNDVALUE=coucou'
	 * @return         : Returns the HTML of the web service.
	 * @throws Exception
	 *     in case of an exception, we just log the exception and return the NOtFoundValue. 
	 * some usages: 
	 * VALUES( WSGET('http://www.dantressangle.com','dbg=0,cached=0,timeout=10,NOTFOUNDVALUE=coucou') )
	 * VALUES( WSGET('http://www.nsn-now.com/Indexing/ViewDetail.aspx?QString=2520015411532','<b>Description:</b><br />','<br /><br />'))  
	 * VALUES( WSGET('http://www.dantressangle.com','cached=1,timeout=100') )
	 * select  WSGET('http://www.nsn-now.com/Indexing/ViewDetail.aspx?QString='||NSC||'0'||NIIN,'<b>Description:</b><br />','<br /><br />'), M.* from MJDI_MASTIFF M WHERE NIIN IN ('15411532','15570954')
     * select  WSGET('http://www.nsn-now.com/Indexing/ViewDetail.aspx?QString='||NSN,'dbg=1,cached=1,timeout=1000') from MJDIPDA  M WHERE NIIN IN ('15411532','15570954')	
	 * select  substr( WSGET('http://www.nsn-now.com/Indexing/ViewDetail.aspx?QString='||NSN,'cached=1') , 2,1000) from MJDIPDA  M WHERE NIIN like '1588%'
	 ************************************************************************/
	public static String webServiceGetAsString(String newUrlSt, String options) //throws Exception
	{		
	    HttpURLConnection connection = null;
		BufferedReader reader = null;
		String line = null;
		String NoValueFound="Not Found";
		boolean dbg=false;
		boolean cached=false;
		int timeout = 1000;
		
		if (options.length()> 0)
		{
			String[] opts=Util.splitByCommas( options );
			for(String opt:opts )//int i=0; i<opt.length; i++)
			{
				try{
					String option=opt.toUpperCase();
					if (option.equals("DBG=1"))		               dbg=true;
					else if (option.startsWith("TIMEOUT="))        timeout=Integer.parseInt(option.substring(8));
					else if (option.equals("CACHED=1") || option.equals("ISCACHED"))            cached=true;
					else if (options.startsWith("NOTFOUNDVALUE=")) NoValueFound=opt.substring(13);
				}catch(Exception e){
					//ignore exceptions while processing options..only show error if dbg=true
					if (dbg=true)
						logger.logInfo("Exception while processing option:[" + opt+"]:"+e);
				}
			}
		}
		
		/* manage the caching, return as quickly as possible here */
		if (true==cached)
		{
			synchronized(CachedWebPages){
				String CachedPage = (String) CachedWebPages.get( newUrlSt );//never seen that URL string before ? 
				if ( null != CachedPage ) {//if yes....get the value  from cache and returns
		     		if (dbg) logger.logInfo("reusing WSGET cache. Length of page is ="+CachedPage.length());
					return CachedPage;
				}
			}
		}
		else
		{
			CachedWebPages.remove(newUrlSt);
		}
			
		try {	
	     	URL	url=new URL( newUrlSt);
			StringBuffer PageSB=null;

			connection = (HttpURLConnection)url.openConnection();
			connection.setRequestMethod("GET");
			connection.setReadTimeout(timeout);

			if (dbg){
				logger.logInfo("\t\t========================================");
				logger.logInfo("\t\tConnection properties");
				Map<String, List<String>> props = connection.getRequestProperties();
				for (Map.Entry<String, List<String>> entry : props.entrySet()) {
					logger.logInfo("\t\t" + entry.getKey() + " - " + entry.getValue());
				} 
				logger.logInfo("\t\t========================================");
				// Get the response	
				logger.logInfo("Response Code: " + connection.getResponseMessage());
			}
			
			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			while ((line = reader.readLine()) != null) {
				if (PageSB==null)
					PageSB=new StringBuffer(line);
				else
					PageSB.append(line);
				 if (dbg)
					 logger.logInfo(line);
			}
			reader.close();
			if (cached )
			{
				String Page=PageSB.toString();
				synchronized(CachedWebPages){
					CachedWebPages.put( newUrlSt, Page );
				}
				return Page;
			}
			else
			  return  PageSB.toString();
			
		} catch (Exception e){
			if(dbg)
				logger.logInfo( "Exception in WEBGET UDF : "+e); 
		} finally {
			connection.disconnect();
			reader = null;
			connection = null;
		}		
		return NoValueFound;
	}

	/*********************************************************************
	 * Returns a JAVA arrayList of ArrayLists from  a HTML page (using Jsoup right now) 
	 * from : http://stackoverflow.com/questions/5396098/how-to-parse-a-table-from-html-using-jsoup
	 * @param html : source of the HTML page
	 * @param tabletag : HTML tags used to retrieve the table  (ignored for now ) 
	 * @param ntable :  number of the table to be retrieved in the page.
	 * @param dbg :  are we in tracing mode ?
	 * @return 2D Table of Strings...
	 ********************************************************************/
	public static ArrayList<ArrayList<String>>  GetTable(String html, String tabletag, int ntable, boolean dbg){
		Document doc = null; //Jsoup.parse(html);
		ArrayList<ArrayList<String>> mytable=new ArrayList<ArrayList<String>>();// mytable=new WSCall.HTMLTable(100,100);
		int r=0;	
		int t=0;
		String rowtag="tr";
		String Celltag="td";
		if (tabletag.equalsIgnoreCase("TABLE"))
		{
			doc = Jsoup.parse(html);
		}
		else 
		if (tabletag.equalsIgnoreCase("RSS"))
		{
			tabletag="channel";
			rowtag="item";
			Celltag="title";
			doc=Parser.xmlParser().parseInput(html, "");
			 //doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(html);
		}
		else
			if (tabletag.equalsIgnoreCase("ATOM"))
			{
				tabletag="feed";
				rowtag="entry";
				Celltag="title";
				doc=Parser.xmlParser().parseInput(html, "");
			}
		//System.out.println(doc);

		for (Element table : doc.select(tabletag))//"table.tablehead")) 
		{
			if (((ntable==t) && (tabletag.equalsIgnoreCase("TABLE"))) || (!tabletag.equalsIgnoreCase("TABLE")))   
				for (Element row : table.select(rowtag)) 
				{		     
					Elements tds = row.select(Celltag);
					if (tds.size() >= 1) {
						ArrayList<String> rowcells=new ArrayList<String>();
						for(int c=0; c< tds.size(); c++){
							//if (maxrowsize< c) maxrowsize=c;
							String v=tds.get(c).text();
							rowcells.add(v);
							//if(dbg) System.out.println(v);
						}
						mytable.add(rowcells);
						r++;
					}

				}
			t++;
		}
		return mytable;
	}
	
	

	/**********************************************************************
	 * Generate a SQL statement to mimic a TABLE from static strings using VALUES(). 
	 * @param n : the arrayList of ArrayList table of values.
	 * @param maxrowsize : the number of values per row maximum.
	 * @param dbg : are we in debug/tracing mode ?
	 * @return the SQL statement as shown below...
	 * here is one example:
     * SELECT * FROM (VALUES('Value1','Value2','Value3','','',''),
                            ('Value1','Value2','Value3','','Value4','Value5'),
                            ('Value1','Value2','','Value3','Value4','Value5')
                      ) as T(col1,col2,col3,col4,col5,col6)	 
     ********************************************************************/
	public static String generateSQLFromTable(ArrayList<ArrayList<String>> n, int maxrowsize, boolean dbg) {
		String endofRow="";
		StringBuffer QuerySt= new StringBuffer("SELECT * FROM (VALUES ");
        boolean atleastone=false;
		for(ArrayList<String> p:n)
		{
			int sizeofRow=p.size();
			QuerySt.append(endofRow+"(");
			String endofcells="";
			for(int i=0;i< maxrowsize /*p.size()*/;i++)				
			{
				if (i >= sizeofRow )
					QuerySt.append(endofcells+"''");
				else {
					String v=p.get(i);
					if (v != null)
					{
						atleastone=true;
						QuerySt.append(endofcells+"'"+Util.escapeSingleQuotes(v)+"'");
						if (dbg) logger.logInfo("Table["+n.indexOf(p)+"]["+p.indexOf(p.get(i))+"]=" +v);
					}
				}
				endofcells=",";
			}
			QuerySt.append(')');
			endofRow=",\n";
		}
		if (atleastone)
		{
			QuerySt.append("\n               ) as T(");
			char del=' ';
			for (int c=1; c<= maxrowsize; c++) //concatenate the number of columns names required.
			{
				QuerySt.append(del+"col"+c);
				del=',';
			}
			QuerySt.append(')');
			if(dbg)logger.logInfo("Query="+QuerySt);
			return (QuerySt.toString());
		}
		else 
		{
			if (dbg) logger.logInfo("No Data found so cannot generate SQL statement for QUerying.");
			return ("");
		}
	}
	
	
	/*********************************************************************
	 * CAlls  a web service/RSS/ATOM Feed and returns the HTML Page. options can be added to change behaviour   
	 * @param newUrlSt : the HTTP URL of the web service to call (GET at the moment )
	 * @param options  : options supported right now are ; 'dbg=1/0,cached=1/0,timeout=100,NOTFOUNDVALUE=coucou'
	 * @param rs       : returns the result set extracted from the HTML  table in the page. 
	 * @return         : nothing
	 * @throws Exception
	 *     in case of an exception, we just log the exception and return the NOtFoundValue. 
	 * some usages: 
	 * SELECT * FROM  TABLE(WSGETRS('http://mypage.html','dbg=0,cached=0,timeout=10,NOTFOUNDVALUE=coucou') ) T
	 * select 'IBM' AS COMPANY ,COL1 AS MEASURE,COL2 as YEAR_2007,COL3 as YEAR_2008,COL4 as YEAR_2009,COL5 as YEAR_2010 FROM TABLE(WSGETRS('http://www.marketwatch.com/investing/stock/ibm/financials/balance-sheet','dbg=1,iscached,timeout=50000,table=0'))  T
	 ********************************************************************/
	public static int maxrowsize=10; //hardcoded for now. 
	public static  ResultSet webServiceGetAsTable(String newUrlSt, String options ) //throws Exception
	{	
		final Map<String, String> OldPages=new CachedHashMap<String, String>( CACHE_SIZE_FOR_WEB_SERVICES_PAGES );
 		boolean dbg =false;

		try {
 		    String PageSt = webServiceGetAsString( newUrlSt,  options);
     		String Tag="table";    
     		String option=options.toUpperCase();
			if (option.indexOf("DBG=1")>0)  dbg=true;

     		int pos=option.indexOf("TABLE=");
     		int ntable=-1;
     		if (pos>0)
     		{
     			ntable=Integer.parseInt(option.replaceAll(".*TABLE=([0-9]*).*", "$1") );     			
			}
     		else {
     			pos=option.indexOf("RSS=");
     			if (pos>0)
     			{
     				Tag="RSS";
     				ntable=Integer.parseInt(option.replaceAll(".*RSS=([0-9]*).*", "$1") );   
     			}
     			else return null;
     		}
    		//now creates the java representaiton of that HTML table. 
 			ArrayList<ArrayList<String>> n=GetTable(PageSt, Tag,ntable,true);  // border=\"\" width=\"100%\"");
 			String QuerySt=generateSQLFromTable(n,maxrowsize,dbg);
 			ResultSet rs =null;
            if (QuerySt != null)
 			{
            	Connection conn = getDefaultDerbyConnection();
            	rs = conn.createStatement().executeQuery( QuerySt );
 			}
			return rs;
			
		} catch (Exception e) {
			String msg = "Exception caught in webServiceGetAsTable():";
			//logger.logException(GDBMessages.UTILITY_DEPLOY_FILE_ERROR, msg, e);
			if (dbg) logger.logInfo(msg + e);
			//throw new Exception(msg + e);
		}
		return null;
	}
	
	
	//return the maximum amount of java heap memory (in bytes) that the JVM can use
	public static long jMemoryMax() {
		MemoryMXBean m = ManagementFactory.getMemoryMXBean();
		return m.getHeapMemoryUsage().getMax() ;
	}

	// return the percentage of java heap memory (in bytes) used after the last garbage collection.
	public static int jMemoryPercent () {
		// go through each pool and determine the usage
		List<MemoryPoolMXBean> memPools = ManagementFactory.getMemoryPoolMXBeans();

		long PoolUsageTotal = 0;
		long PoolSizeTotal = 0;
		
		for (MemoryPoolMXBean memPool : memPools) {
			MemoryUsage poolUsage;
			if (memPool.isCollectionUsageThresholdSupported()&&MemoryType.HEAP==memPool.getType()&&GCMemoryPoolUsage.containsKey(memPool.getName())){
				//this pool supports Garbage collection and we have the usage after the last GC
				poolUsage = GCMemoryPoolUsage.get(memPool.getName());
				
				long reportedMax = poolUsage.getMax();
				PoolSizeTotal += (reportedMax == -1)?poolUsage.getUsed():reportedMax;
				PoolUsageTotal += poolUsage.getUsed();
			}
		};

		if (PoolSizeTotal !=0){
 			return (int) (100.0*PoolUsageTotal/PoolSizeTotal);
		} else {
			return 0;
		}
	}

	//return the amount of non heap memory (in bytes) used after the last garbage collection 
	public static long jMemoryNonHeap () {
		MemoryMXBean m = ManagementFactory.getMemoryMXBean();
		return m.getNonHeapMemoryUsage().getUsed();
	}

//	public static String jGetEnv( String s ) { return System.getenv(s); } // superceeded by table function GETENV
	
	public static int jHash( String s ) { return s.hashCode(); }
	
//	public static String getOSName() {
//		return System.getProperty("os.name");
//	}
	
	public static short jSleep( int millis ) {
//		try {
//			System.out.println("DDC: " + getDefaultDerbyConnection().getClass().getName());
//			System.out.println("EDC: " + getEmbeddedDerbyConnection().getClass().getName());
//			System.out.println("DD dbName: " + ((EmbedConnection) getDefaultDerbyConnection()).getDBName());
//			System.out.println("ED dbName: " + ((EmbedConnection) getEmbeddedDerbyConnection()).getDBName());
//		} catch (Exception e) {}
		try { Thread.sleep(millis); } catch (InterruptedException e) { logger.logWarning(GDBMessages.CONFIG_JSLEEP_INTERRUPTED, "Interrupted in jSleep(): " + e); }
		return 1;
	}
	
	public static void initialiseGdbUserDatabase() throws SQLException {

		logEnteredMsg( "GDBINIT_USERDB", null );
		
		Connection usrdbDefaultConnection = null;
		Statement stmtOnDefaultConnection = null;
		String userDB = null, userSchema = null;
		
		try {
			// Get network connection for periodic refresh of LT views.
			// Connect using system usr/pwd, and set schema to given user for lt view config.
			// System user is always authenticated based on his usr/pwd, whereas others may be authenticated via kerberos token.
			usrdbDefaultConnection = getDefaultDerbyConnection();
			
			stmtOnDefaultConnection = usrdbDefaultConnection.createStatement();
			ResultSet rs = stmtOnDefaultConnection.executeQuery("values current schema");
			if ( false == rs.next() ) throw new Exception("Unable to resolve userdb schema name using sql: values current schema");

			userDB = ((EmbedConnection) usrdbDefaultConnection).getDBName();
			userSchema = rs.getString(1).toUpperCase();
			stmtOnDefaultConnection.close(); // Use this statement as little as possible.. if failures arise it can cause a Dead statement which makes the proc fail...

			Connection gdbEmbedConnectionToUserDB = DriverManager.getConnection("jdbc:derby:" + userDB, getGaianNodeUser(), getGaianNodePassword());
			
			String gdbDB = ((EmbedConnection) gdbEmbedConnectionToUserDB).getDBName();
			String gdbSchema = getGaianNodeUser().toUpperCase();

			logger.logInfo("Processing initialisation request for userDB/userSchema: " +
					userDB + "/" + userSchema + " relative to gdbDB/gdbSchema: " + gdbDB + "/" + gdbSchema);
			
			Statement statementOnUserDB = gdbEmbedConnectionToUserDB.createStatement();
			
//			final boolean isUserdbMoreRecentThanInstallJar = new File(userdb).lastModified() > new File("lib/GAIANDB.jar").lastModified();

			// Always create the views + spfs under the gaiandb schema of any new database that the user wishes to initialise.
			// Other schemas will reference these objects using synonyms against them.
			if ( false == DataSourcesManager.isUserdbInitialised( userDB ) ) { //&& true == isUserdbMoreRecentThanInstallJar ) {
				DataSourcesManager.registerDatabaseStatementForLogicalTableViewsLoading( userDB, statementOnUserDB );
				DataSourcesManager.initialiseUserDB( userDB );
			}

			// Commented this out for now... may be useful in future (from another procedure) if users want to make all views and spfs available under a user schema
			// .. but generally these would belong to separate schemas (protected by grants etc); or they would be accessible indirectly via the system schema (e.g. 'gaiandb')
			// Task 44843 in RTC is there for future work to 1) automate GDBINIT_USERDB from derby auth plugin, 2) allow schema membership assignment for logical tables
			
//			if ( false == userSchema.equals(gdbSchema) ) {
//				statementOnUserDB.execute("SET SCHEMA " + userSchema);
//				DataSourcesManager.initialiseAlternateUserSchemaIfNew( userDB, userSchema, statementOnUserDB );
//				statementOnUserDB.execute("SET SCHEMA " + gdbSchema);
//			}
			
		} catch ( Exception e ) {
			logger.logException(GDBMessages.DISCOVERY_USER_DB_CONNECT_ERROR, "Unable to gdb-initialise userdb/schema: " +
					userDB + "/" + userSchema + ", cause: ", e);
		}
		
		// THE DEFAULT DERBY CONNECTION MUST BE CLOSED
		if ( null != stmtOnDefaultConnection ) stmtOnDefaultConnection.close();
		if ( null != usrdbDefaultConnection ) usrdbDefaultConnection.close();
		
		// DO NOT CLOSE THE EMBEDDED DERBY CONNECTION - it needs keeping open to maintain the lt views for this userdb in case they change
	}
	
	public static void nestExecuteQuery( String sql, String nestedSQL, ResultSet[] rs ) throws Exception {
    	apiStart("nestExec", Arrays.asList(sql, nestedSQL));
    	
		ResultSet nrs = getResultSetFromQueryAgainstDefaultConnection( nestedSQL );
		
		if ( !nrs.next() )
			throw new Exception("Nested Query retrieved no results");
		
		for ( int i=1; i<nrs.getMetaData().getColumnCount()+1; i++ )
			sql = sql.replaceAll("\\$" + i + "(\\D|$)", nrs.getString(i) + "$1");
		
		rs[0] = nrs.getStatement().executeQuery( sql ); // re-use Statement
		rs[0].getStatement().getConnection().close();
	}

	public static Clob concatResultSet( String sql, String rowDelimiter, String colDelimiter ) throws Exception {
		apiStart("CONCATRS", Arrays.asList(sql, rowDelimiter, colDelimiter));
		
		if ( null == sql ) return null;
		
		ResultSet rs = getResultSetFromQueryAgainstDefaultConnection(sql);
		int colCount = rs.getMetaData().getColumnCount();

		StringBuffer sb = new StringBuffer();
		boolean isNotFirst = false;
		
		while ( rs.next() ) {
			if ( null != rowDelimiter )
				if ( isNotFirst ) sb.append(rowDelimiter); else isNotFirst = true;
			sb.append( rs.getString(1) );
			for ( int i=2; i<=colCount; i++ ) {
				if ( null != colDelimiter ) sb.append(colDelimiter);
				sb.append( rs.getString(i) );
			}
		}
		
		Clob clob = ((EmbedConnection) rs.getStatement().getConnection()).createClob();
		clob.setString(1, sb.toString());
		
//		rs.getStatement().getConnection().close(); // Don't close as caller will need the connection open to retrieve the Clob!
		
		logger.logInfo("CONCATRS returning concatenated ResultSet values in Clob object, length: " + clob.length());
		return clob;
	}
	
//	public static String jSubstring( String s, int start, int end ) throws Exception {
//		return 0 > end ? s.substring(start) : s.substring(start, end);
//	}
	
	public static String jURLDecode( String s ) throws Exception {
//		String decodedURL = URLDecoder.decode( s, Charset.defaultCharset().name() );
		//decodedURL = (String) decodedURL.subSequence(0,decodedURL.length()-1);
//		int id = s.hashCode();
//		logger.logInfo("jhash(" + s + ") -> decoded string: " + decodedURL + " -> id: " + id);
		return URLDecoder.decode( s, Charset.defaultCharset().name() );
	}
	
	public static String jURLEncode( String s ) throws Exception {
		return URLEncoder.encode(s, Charset.defaultCharset().name() );
	}
	
	public static long jTimestampSeconds( Timestamp t ) {
		return null == t ? 0 : t.getTime() / 1000;
	}
	
	public static long jTimestampMilliseconds( Timestamp t ) {
		return null == t ? 0 : t.getTime();
	}
	
	public static Timestamp jMillis2Timestamp( long t ) {
		return new Timestamp(t);
	}
	
	public static String jGetQuotedParameter( String input, int position ) throws Exception {
		
		String[] toks = Util.splitByTrimmedDelimiter(input, '"');
		
		int dpos = position*2-1;
		if ( 0 > dpos ) return "";
		
		if ( toks.length >= dpos ) return toks[ dpos ];
		return "";
	}
	
	public static String jGetFormattedString( String input, String csvargs ) throws Exception {
		
		// This method just calls the Java equivalent - note that csvargs is a CSV list, so commas cannot be part of an element of the list.
		return String.format(input, (Object[]) Util.splitByCommas(csvargs));
	}

//	public static String jPadLeft( String input, String pad, int totalLength ) throws Exception {
//		
//		int lenToPad = input.length() - totalLength;
//		
//		
//		// This method just calls the Java equivalent - note that csvargs is a CSV list, so commas cannot be part of an element of the list.
//		return String.format(input, (Object[]) Util.splitByCommas(csvargs));
//		
//		.substring(s.length() - size);
//	}
	
	public static String jReplaceFirst( String input, String regex, String replacement ) throws Exception {
		return null==input? null : input.replaceFirst(regex, replacement);
	}
	
	public static String jReplaceAll( String input, String regex, String replacement ) throws Exception {
		return null==input? null : input.replaceAll(regex, replacement);
//		return Pattern.compile(regex).matcher(input).replaceAll(replacement); // equivalent to simpler syntax above
	}
	
	public static short jMatchRegex( String input, String regex ) throws Exception {
		return (short) ( null==input ? 1 : input.matches(regex) ? 1 : 0 );
	}
	
	public static String jSetAuthToken(String pToken) throws Exception {
		// construct security token
		String sid=null;
		if (pToken!=null && pToken.length()>0) {
			byte[] st=new BASE64Decoder().decodeBuffer(pToken);
			KerberosToken gkt=new KerberosToken(st);  // TODO remove hard-coded ref, make generic for all security tokens
			sid=KerberosUserAuthenticator.setToken(gkt);  // TODO remove hard-coded ref, make generic for all security tokens
		}
		return sid;
	}
	
	public static Blob jzip( Blob data ) throws Exception {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Util.copyBinaryData(data.getBinaryStream(), new GZIPOutputStream(baos));
		byte[] bytes = baos.toByteArray();
		baos.close(); // other streams are closed

//		logger.logException("Unable to compress data blob with GZIP (returning null): ", e);
		
		// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
		Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
		blob.setBytes(1, bytes);
		
		return blob;
	}
	
	public static Blob junzip( Blob data ) throws Exception {

		InputStream is = data.getBinaryStream();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Util.copyBinaryData(new GZIPInputStream(is), baos);
		byte[] bytes = baos.toByteArray();
		is.close(); // other streams are closed

//		logger.logException("Unable to uncompress data blob with GUNZIP (returning null): ", e);

		// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
		Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
		blob.setBytes(1, bytes);
		
		return blob;
	}
	
	// Syntax is deployFile('<fromPath>[@node]','[<toDir>@]<node1> <node2>..|*') where nodes may be '*' or a node list, e.g. 'node1 node2 node3' 
	// Currently supported: deployFile('<fromPath>','[<toDir>@]<node1> <node2>..|*')

	// deployFile('gaiandb_config.properties', '*') ; speculative copy (no copy for matching sizes): deployFile('GAIANDB.jar?','*')
	public static void deployFile( String fromLoc, String toLoc, ResultSet[] rs ) throws Exception {

		apiStart("deployFile", Arrays.asList(fromLoc, toLoc));
		
		try {
			if ( null == fromLoc || null == toLoc || 1 > fromLoc.length() || 1 > toLoc.length() )
				throw new Exception("Invalid arguments to deployFile(): expecting '<fromPath>','[<toDir>@]<node1> <node2>..|*'");
			
			boolean isCheckSizeOnly = fromLoc.endsWith("?");
			String fromPath = isCheckSizeOnly ? fromLoc = fromLoc.substring(0,fromLoc.length()-1) : fromLoc;
			File fromFile = new File(fromPath);
			
			fromLoc += ":" + fromFile.length() + ( isCheckSizeOnly ? "" : "." + fromFile.lastModified() );
			
			int atIdx = toLoc.lastIndexOf('@');
			String toPath = -1 == atIdx ? null : toLoc.substring(0, atIdx);
			String targetNodes = -1 == atIdx ? toLoc.trim() : toLoc.substring(atIdx+1).trim();
			
			if ( null == toPath || 1 > toPath.length() )
				toPath = fromPath;
			else if ( new File(toPath).isDirectory() )
				toPath += "/"+new File(fromPath).getName();
			
			String myNodeID = GaianDBConfig.getGaianNodeID();			
			boolean isRipple = "*".equals(targetNodes);
			
			rs[0] = getResultSetFromQueryAgainstDefaultConnection( // TODO: call xripple
					"select xripple('"+fromLoc+"','"+toPath+"','"+(isRipple ? myNodeID : ","+targetNodes)+"') deployed from sysibm.sysdummy1");
			
			rs[0].getStatement().getConnection().close();
			
		} catch (Exception e) {
			String msg = "Exception caught in deployFile():";
			logger.logException(GDBMessages.UTILITY_DEPLOY_FILE_ERROR, msg, e);
			throw new Exception(msg + e);
		}
	}
	
	private static AtomicBoolean isRippleExtractInProgress = new AtomicBoolean(false);
	private static final Map<String, Long> rippleIDs = new CachedHashMap<String, Long>(100);

	/**
	 * Copies a file to all or targeted nodes of a Gaian network in a ripple-like fashion, and optionally gets target nodes
	 * to run a custom command afterwards.
	 * 
	 * Ripple deployment means that a starting (query entry-point) node gets its neighbours to download its file, and then each of these
	 * propagates the same operation onwards to remaining nodes that have not already received the deployment instruction.
	 * This way, each node downloads the file from an immediate neighbour rather than all having to to go to the entry point node.
	 * 
	 * Concurrent 'rippleExtract' commands throughout a network are not supported and will be rejected.
	 * 
	 * @param fromDesc
	 * @param toPath
	 * @param optArgs
	 * @return
	 * @throws Exception
	 */
	// Used to deploy files around a GaianDB network. Returns: TotalNodesUpdated
	// optArgs syntax is: '<rippleFromNode>,<rippleID>' or ',<targetNodes>' - Note <targetNodes> is a space separated list.
    public static void rippleExtract( final String fromDesc, String toPath, final String optArgs, final ResultSet[] res ) throws Exception {

		try {
			apiStart( "xripple", Arrays.asList(fromDesc, toPath, optArgs) );
	
			int idx = fromDesc.lastIndexOf(':');
			String fromPath = 0 > idx ? fromDesc : fromDesc.substring(0, idx);
			String fromFileID = 0 > idx ? "noID" : fromDesc.substring(idx+1);
			
			ResultSet rs = null;
			
			if ( !isRippleExtractInProgress.compareAndSet(false, true) ) {
				logger.logInfo("xripple() already in progress: cannot propagate through this node (returning 0)");
				return; // don't set a result set
			}
		
			// Note the customCmd argument can be nested in quotes to escape any top-level commas inside it (i.e. non-backeted).
			String[] argsList = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrQuotes(optArgs, ',');
			
			logger.logInfo("Variable Args: " + Arrays.asList(argsList));

			String myNodeID = GaianDBConfig.getGaianNodeID();	
			String fromNode = 1 > argsList.length || 1 > argsList[0].length() ? null : argsList[0]; // fromNode (or null if targetting specific nodes)
			String optArg2 = 2 > argsList.length || 1 > argsList[1].length() ? null : argsList[1];  // rippleID (or list of targetted nodes)
			String customCmd = 3 > argsList.length || 1 > argsList[2].length() ? null : argsList[2];  // command to execute on reached nodes after file upload
			
			String rippleID = null;
			Set<String> targetNodes = null;
			
			if ( myNodeID.equals(fromNode) ) fromNode = myNodeID;
			
			File fromFile = new File(fromPath);
			
			if ( null != fromNode ) {
				rippleID = optArg2;
				if ( rippleIDs.containsKey(rippleID) ) {
					logger.logInfo("xripple() already processed rippleID '" + rippleID + "' (returning 0)");
					return; // don't set a result set
				}
				
				if ( null == rippleID)
					rippleID = myNodeID + System.currentTimeMillis(); // + "." + fromFile.length()+"."+fromFile.lastModified();
				
				rippleIDs.put(rippleID, System.currentTimeMillis());
			} else {
				// No originator node - a specific list of target nodes was passed in - just copy to each of them in turn
				targetNodes = new HashSet<String> ( Arrays.asList( Util.splitByTrimmedDelimiter(optArg2, ' ') ) );
				logger.logInfo("Resolved target node IDs: " + targetNodes);
			}

//			String fromFileID = null == fromNode ? fromFile.length()+"."+fromFile.lastModified() : rippleID.substring(rippleID.indexOf('.')+1);
			
			if ( new File(toPath).isDirectory() )
				toPath += "/"+fromFile.getName();
					
			File toFile = new File(toPath);
			String toFileID = toFile.length()+"."+toFile.lastModified();
			
			// This is a deploy node if the source and destination file IDs match (i.e. they have same lengths and (unless off) modification times)
			// AND if this is a rippleDeploy or if this node is one of the target nodes.
			// No need to deploy the file on nodes/hosts that have this file already (e.g. nodes on the same host)
			int deployCount = !toFileID.startsWith( fromFileID ) && (null!=fromNode || targetNodes.remove(myNodeID)) ? 1 : 0;
			
			// Only deploy if required
			if ( 0 < deployCount ) {
				
				if ( null == fromNode || fromNode.equals(myNodeID) ) {
					// This is the originator node and the files don't match - copy locally
					// Note: fromNode is null if we are just doing straight copies from the local node
					Util.copyBinaryData( new FileInputStream(fromPath), new FileOutputStream(toPath) );
					toFile.setLastModified( fromFile.lastModified() ); // Preserve modification timestamps
					logger.logInfo("Deployed file locally from '"+fromPath+"' to '"+toPath+"'");
					
				} else {
					// Get the file as a blob from the node where the ripple came from
					rs = getDefaultDerbyConnection().createStatement().executeQuery(
							"select filebz, modified from new com.ibm.db2j.GaianQuery('"+
								"select getFileBZ(''"+fromPath+"'') filebz, modified FROM "+
								"new com.ibm.db2j.GaianConfig(''"+FILESTATS+','+fromPath+"'') GC"+
							"', 'with_provenance, maxDepth=1') GQ where GDB_NODE = '"+ fromNode + "'");
					
					if ( !rs.next() ) {
						logger.logWarning(GDBMessages.CONFIG_BLOB_EXTRACT_ERROR, "Unable to extract zipped blob for '" + fromPath + "' from node '" + fromNode + "' (empty result)");
						return; // don't set a result set
					}
					
					writeToFileAfterUnzip( toFile, rs.getBytes(1) );
					toFile.setLastModified( rs.getLong(2) ); // preserve the modified timestamp
					logger.logInfo("ripple/extracted file from node '"+fromNode+"' to '"+toPath+"'");
				}
			}
			
			if ( null != customCmd ) {
				// TODO: Run command and return Blob result
			}
			
			// Only ripple if we have an originator node
			if ( null != fromNode ) {
				
				// Re-use Statement to now ripple out the command - use same path on following nodes out
				rs = (null == rs ? getDefaultDerbyConnection().createStatement() : rs.getStatement()).executeQuery( // TODO: call xripple
						"select sum(ripple_count) deployed from new com.ibm.db2j.GaianQuery('select xripple(''"+
						toPath+":"+fromFileID+"'',''"+toPath+"'',''"+GaianDBConfig.getGaianNodeID()+","+rippleID+
						"'') ripple_count from sysibm.sysdummy1', 'with_provenance, maxDepth=1') GQ"+
						(fromNode.equals(myNodeID) ? "" : " where GDB_NODE != '" + fromNode + "'"));
		
				if ( !rs.next() ) {
					logger.logWarning(GDBMessages.CONFIG_XRIPPLE_PROPAGATE_ERROR, "Unable to propagate xripple() for '" + new File(toPath).getName() +
							(fromNode.equals(myNodeID) ? " originating here " : "' received from '"+fromNode+"'") + " (stopping here)");
					
					// Set result res[0] to just hold the deployCount
			    	setFirstResultSetFromQueryAgainstDefaultConnection( res, "select "+deployCount+" deployCount from sysibm.sysdummy1", "" );
				}
				
				deployCount += rs.getInt(1);
				rs.getStatement().getConnection().close();
			
			} else {
				
				// No originator node - a specific list of target nodes was passed in - just copy to each of them in turn				
				for ( String toNode : targetNodes ) {
					// Tell the remote node to get the file from us
					rs = (null == rs ? getDefaultDerbyConnection().createStatement() : rs.getStatement()).executeQuery(
							"SELECT deployed FROM NEW com.ibm.db2j.GaianQuery('"+
								"select COPYFILE(''"+myNodeID+"'',''"+fromDesc+"'',''"+toPath+"'') deployed FROM sysibm.sysdummy1"+
							"', 'with_provenance') GQ where gdb_node = '"+toNode+"'"
						);
					
					if ( !rs.next() ) {
						logger.logWarning(GDBMessages.CONFIG_FILE_DEPLOY_ERROR, "Unable to deploy file to '"+toPath+"' at node '"+toNode+"'" + " (ignored)");
						continue;
					}
					logger.logInfo("Deploy count to '"+toPath+"' at node '"+toNode+"': " + rs.getInt(1));
					deployCount += rs.getInt(1);
				}

				if ( null != rs )
					rs.getStatement().getConnection().close();
			}
			
			logger.logInfo("xripple() complete, ripple count: " + deployCount);
			
			return; // TODO: return Blob? or resultset.. may contain deployCount or Blob result
		
		} catch (Exception e) {
			String msg = "Exception caught in rippleExtract():";
			logger.logException(GDBMessages.UTILITY_RIPPLE_EXTRACT_ERROR, msg, e);
			throw new Exception(msg + e);
		
		} finally {
			isRippleExtractInProgress.set(false);
		}
	}
	
	public static int copyFileFromNode( String fromNode, String fromDesc, String toPath ) throws Exception {
		
		String[] toks = Util.splitByTrimmedDelimiter(fromDesc, ':');
		String fromPath = toks[0];
		String fromFileID = 2 > toks.length || 1 > toks[1].length() ? "noID" : toks[1];
		if ( new File(toPath).isDirectory() )
			toPath += "/"+new File(fromPath).getName();
		File toFile = new File(toPath);
		String toFileID = toFile.length()+"."+toFile.lastModified();
		logger.logInfo("Pre-copy check if toFileID: " + toFileID + " startsWith fromFileID: " + fromFileID);
		if ( toFileID.startsWith( fromFileID ) ) {
			logger.logInfo("Files match. No need to retrieve/copy file: " + fromPath + " from node " + fromNode);
			return 0;
		}
		
		ResultSet rs = getResultSetFromQueryAgainstDefaultConnection(
				"SELECT filebz, modified FROM NEW com.ibm.db2j.GaianQuery('"+
					"select getFileBZ(''"+fromPath+"'') filebz, modified FROM "+
					"new com.ibm.db2j.GaianConfig(''"+FILESTATS+','+fromPath+"'') GC"+
				"', 'with_provenance') GQ where gdb_node = '"+fromNode+"'"
		);
		
		if ( !rs.next() )
			throw new Exception("Unable to extract zipped blob for '" + fromPath + "' from node '" + fromNode + "' (empty result)");
		
		if ( new File(toPath).isDirectory() )
			toPath += "/"+new File(fromPath).getName();
		writeToFileAfterUnzip( new File(toPath), rs.getBytes(1) );
		new File(toPath).setLastModified(rs.getLong(2)); // preserve the modified timestamp
		logger.logInfo("Extracted/copied: '" + fromPath + "' from node '" + fromNode + " to path: '" + toPath + "'");
		
		rs.getStatement().getConnection().close();
		
		return 1;
	}
	
	// Returns: Name, Modified, Size, Checksum
	public static void getFileStats( String path, ResultSet[] rs ) throws Exception {
    	apiStart("getFileStats", Arrays.asList(path));
		rs[0] = getResultSetFromQueryAgainstDefaultConnection(
				"select " + GDB_NODE + ", fname, jtstamp(modified) modified, size, checksum " +
				"FROM new com.ibm.db2j.GaianQuery('" +
					"select * from new com.ibm.db2j.GaianConfig(''"+FILESTATS+','+path+"'') GC" +
				"', 'with_provenance') GQ ORDER BY " + GDB_NODE + ", fname"
			);
		rs[0].getStatement().getConnection().close();
	}
	
	/**
	 * Get file bytes, zip them up and return result as a Blob.
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static Blob getFileBZ( String path ) throws Exception {

    	apiStart("getFileBZ", Arrays.asList(path));
		try {
			byte[] bytes = readAndZipFileBytes( new File(path) );
			
			// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
			Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
			blob.setBytes(1, bytes);
			
			return blob;
		}
		catch (Exception e) { throw new Exception("Unable to get zipped Blob from file " + path + ": " + e); }
	}
	
	/**
	 * Get file bytes as a Blob.
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static Blob getFileB( String path ) throws Exception {

    	apiStart("getFileB", Arrays.asList(path));
		try {
			byte[] bytes = null;
			File file = new File(path);
			
			try {
				if ( file.isDirectory() ) throw new Exception("File is a directory");
				bytes = Util.getFileBytes( file );
			}
			catch (Exception e) { throw new Exception("Cannot read bytes from '" + file.getName() + "': " + e); }
			
			// Cast to EmbedConnection as we know Derby supports createBlob() regardless of Java version
			Blob blob = ((EmbedConnection) getDefaultDerbyConnection()).createBlob();
			blob.setBytes(1, bytes);
			
			return blob;
		}
		catch (Exception e) { throw new Exception("Unable to get Blob from file " + path + ": " + e); }
	}
	
	public static void listThreads( ResultSet[] tables ) throws Exception {
    	apiStart("listThreads");
    	setFirstResultSetFromQueryAgainstDefaultConnection( tables, "select * from "
    			+ transformCollectionOfCsvToSqlTableExpression( GaianNode.getJvmThreadsInfo(), GaianNode.THREADINFO_COLNAMES ), "ORDER BY GRP, CPU desc" );
	}
	
	public static ResultSet getThreads() throws Exception {
    	apiStart("getThreads");
    	return getResultSetFromQueryAgainstDefaultConnection( "select * from "
    			+ transformCollectionOfCsvToSqlTableExpression( GaianNode.getJvmThreadsInfo(), GaianNode.THREADINFO_COLNAMES ) + " ORDER BY GRP, CPU desc" );
	}
	
	public static void listEnv( final String prop, ResultSet[] tables ) throws Exception {
    	apiStart("listEnv");
    	setFirstResultSetFromQueryAgainstDefaultConnection( tables, transformCollectionOfCsvToSQL( getEnvironment(prop) ), "ORDER BY PROPERTY" );
	}
	
//	public static ResultSet getEnv( final String prop ) throws Exception {
//    	apiStart("getEnv");
//    	return getResultSetFromQueryAgainstDefaultConnectionBasedOnStaticJavaList( getEnvironment(prop), " ORDER BY PROPERTY" );
//	}
	
	private static List<String> getEnvironment( String prop ) {
		if ( null != prop && 0 < prop.length() )
			return Arrays.asList("'" + Util.escapeSingleQuotes(prop) + "' PROPERTY,'"+Util.escapeSingleQuotes(System.getenv(prop))+"' VALUE");
		
    	List<String> props = new ArrayList<String>();
    	Map<String, String> env = System.getenv();
    	for ( String key : env.keySet() )
    		props.add("'" + Util.escapeSingleQuotes(key) + "' PROPERTY,'" + Util.escapeSingleQuotes(env.get(key)) + "' VALUE");
//    	System.out.println("props: " + props);
    	return props;
	}
	
	// Format: Hostname, Interface name, Address, Broadcast, NetPrefixLength
	public static void listNet( String ipPrefix, ResultSet[] netInfo ) throws Exception {
    	apiStart("listNet", Arrays.asList(ipPrefix));
    	
    	try {
    		if ( null == ipPrefix || 0 == ipPrefix.length() )
    			ipPrefix = null;
    		
    		setFirstResultSetFromQueryAgainstDefaultConnection( netInfo,
    				transformCollectionOfCsvToSQL( new Util.NetInfo().getAllInterfaceInfoAsListOfRowsWithAliasedColumnsForIPsPrefixed( ipPrefix ) ), "" );
	    	
    	} catch ( Exception e ) { throw new Exception("Unable to get net info: " + e + ", trace: " + Util.getStackTraceDigest(e)); }
	}
	
	private static String transformCollectionOfCsvToSQL( Collection<String> csvStrings ) {
		if ( null == csvStrings || csvStrings.isEmpty() ) return null;
		StringBuilder sb = new StringBuilder();
		for ( String row : csvStrings ) sb.append( "select " + row + " from sysibm.sysdummy1 UNION ALL " );
		sb.delete(sb.length()-" UNION ALL ".length(), sb.length());
		logger.logDetail( "\n" + sb );
		return sb.toString();
	}
	
	/**
	 * Converts list of csvs to a sql query that constructs a result set holding all of them, under column names specified
	 * by colNamesCsv. NOTE: This method only works if there are the same number of columns in each list entry and as many
	 * as there are column names in colNamesCsv. You may also wish to double quote col names in colNamesCsv if they contain
	 * spaces or special chars  
	 * 
	 * @param csvStrings
	 * @param colNamesCsv
	 * @return
	 */
	private static String transformCollectionOfCsvToSqlTableExpression( Collection<String> csvStrings, String colNamesCsv ) {
		String rows = null;
		if ( null == csvStrings || 1 > csvStrings.size() ) rows = "("+colNamesCsv.replace('"', '\'')+")";
		else {
			StringBuilder sb = new StringBuilder();
			for ( String row : csvStrings ) sb.append( "("+row+"), " );
			sb.setLength(sb.length()-", ".length());
			rows = sb.toString();
		}
		final String sql = "(values "+rows+") T("+colNamesCsv+")" + ( 1 > csvStrings.size() ? " where 1!=1" : "" );
		logger.logDetail( "\n" + sql );
		return sql;
	}
	
//	private static String transformResultSetToSQL( ResultSet rs ) throws SQLException {
//		if ( false == rs.next() ) return null;
//		int numCols = rs.getMetaData().getColumnCount();
//		StringBuilder sb = new StringBuilder();
//		do {
//			sb.append( "select " + rs.getString(1) );
//			for ( int i=2; i<=numCols; i++ ) sb.append( ", "+rs.getString(i) );
//			sb.append( " from sysibm.sysdummy1 UNION ALL " );
//		} while ( rs.next() );
//		sb.delete(sb.length()-" UNION ALL ".length(), sb.length()).toString();
////		System.out.println( "\n" + sb );
//		return sb.toString();
//	}
	
	private static void setFirstResultSetFromQueryAgainstDefaultConnection( ResultSet[] tables, String sql, String sqlSuffix ) throws SQLException {
		if ( null == sql || 1 > sql.length() ) return;
		tables[0] = getResultSetFromQueryAgainstDefaultConnection( sql + " " + sqlSuffix );
		tables[0].getStatement().getConnection().close();
	}
	
	public static Object[] getNetInfoForClosestMatchingIP( String ip ) throws Exception {
		return new Util.NetInfo().getInfoForClosestMatchingIP(ip);
	}
	

	//static portion to run at startup
	static {
		
		try {
			//set up call backs and data structures to maintain the baseline memeory usage.
			MemoryMXBean membean = ManagementFactory.getMemoryMXBean();
			
			// establish callbacks after garbage collection on each pool.
			List<MemoryPoolMXBean> memPools = ManagementFactory.getMemoryPoolMXBeans();

			for (MemoryPoolMXBean memPool : memPools) {
				if (memPool.isCollectionUsageThresholdSupported()){
					//this pool supports Garbage collection so set the listening threshold
					memPool.setCollectionUsageThreshold(1);
					// this is a GC pool, report the current usage until the first GC occurs
					GCMemoryPoolUsage.put(memPool.getName(),memPool.getUsage());
				}
			}
			
			NotificationEmitter emitter = (NotificationEmitter) membean;
			GCListener listener = new GCListener();
			emitter.addNotificationListener(listener, null, null);
		}
		catch( Throwable e ) {
			logger.logWarning(GDBMessages.UTILITY_MEMORYMXBEAM_ERROR, "Unable to access/process MemoryMXBean for computing Memory utilisation (ignored): " + e);
		}
	}
	
	private static class GCListener implements NotificationListener {

		public void handleNotification(Notification notification, Object handback) {

		      String notifType = notification.getType();
		      if (notifType.equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
		          CompositeData cd = (CompositeData) notification.getUserData();
		          MemoryNotificationInfo info = MemoryNotificationInfo.from(cd);
		          GCMemoryPoolUsage.put(info.getPoolName(), info.getUsage());
		          
		          //reset the notification to be informed the next time..
				
				List<MemoryPoolMXBean> memPools = ManagementFactory.getMemoryPoolMXBeans();
				for (MemoryPoolMXBean memPool : memPools) {
					if (memPool.getName().equals(info.getPoolName())){
						//this pool supports Garbage collection so set the listening threshold
						memPool.setCollectionUsageThreshold(1);
					}
				}
		      }  
		}
	}
	
//	public class NeighborProvider extends GaianDBConfig {
//
//		public Map<String, String> getNeighbourInfo() {
//			
//			String[] gdbConnections = getGaianConnections();
//			Map<String, String> neighborInfo = new HashMap<String, String>();
//			
//			for ( String gdbc : gdbConnections )
//				neighborInfo.put( gdbc, getIPFromConnectionID(gdbc) );
//			
//			return neighborInfo;
//		}
//
//		private String getIPFromConnectionID( String cid ) {
//			String url = getCrossOverProperty( cid + "_URL" );
//			if ( null == url ) return null;
//			return url.substring( "jdbc:derby://".length(), url.lastIndexOf(':') );
//		}
//	}
}
