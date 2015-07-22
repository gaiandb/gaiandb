/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.IQualifyable;
import org.apache.derby.vti.Pushable;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianChildRSWrapper;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.VTIBasic;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.utils.ErrorBuffer;

/**
 * Abstract class for querying a web service.
 * 
 * To add caching capability to a subclass of AbstractVTI, create a primary key column combination on the returned table,
 * then add calls to isCached( <filter based on query> ) in executeAsFastPath() and nextRow(); and add a call to cacheRow() in nextRow()
 * 
 * @author Ed Jellard, David Vyvyan
 */
public abstract class AbstractVTI extends VTI60 implements IFastPath, GaianChildVTI, IQualifyable, Pushable, VTICosting {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "AbstractVTI", 20 );
	
//	private static boolean isInitialised = false;

	static final String PROP_SCHEMA = "schema";
	static final String PROP_CACHE_EXPIRES = "cache.expires"; // expiry duration for cached rows, in seconds - required parameter
	static final String PROP_CACHE_PKEY = "cache.primarykey"; // nullable
	static final String PROP_CACHE_INDEXES = "cache.indexes"; // nullable
	
	private static final String DEFAULT_EXPIRY_SECONDS = "60";
	
	final String vtiClassName = this.getClass().getName(); // the name of the class extending this one
	
	private String prefix; // = ""; // Where to find the query file
	private String extension = ""; // Query file extension (i.e. txt, sparql)
	protected int isCached = 0; // Cached status for this query: 0 means not yet, -1 means cache attempt failed, 1 means it is
	
	protected GaianChildVTI resultRows = null;
	protected ResultSet underlyingResultSet = null;

	ArrayList<String> replacements = new ArrayList<String>(); // Populated by
	// constructor
	// args
	
	protected GaianResultSetMetaData grsmd = null;
	
	Hashtable<String, String> defaultVTIProperties = null;

	private int numRowsForInsertStatement = 0;
	private PreparedStatement preparedInsertStatement = null;
	
	// Keep track of errors while processing result rows - we will log them at the end of the table processing.
	ErrorBuffer errors;
	
	/**
	 * Get Hashtable of property -> value associations for this VTI
	 * The property will relate to the prefix value.
	 * The prefix is the first of the comma separated arguments of the VTI constructor String argument - this allows for separate uses of the VTI.
	 * 
	 * It is good practice to override this method in concrete implementations of this such that the set of properties used by the
	 * VTI is well defined and easy to look up.
	 * 
	 * Users can override the default values set in this Hashtable by manually setting these properties in gaiandb_config.properties.
	 * For example, for VTI ICAREST called with prefix property 'docbytes', the property 'url' can be overriden by adding the following line 
	 * to gaiandb_config.properties:
	 *  
	 * com.ibm.db2j.ICAREST.docbytes.url=http://localhost:8393/search/ESFetchServlet?cid=$0
	 * 
	 * In this example, the property takes one argument which will be substituted for $0, which is the second comma separated value 
	 * in the String argument passed to the constructor: , e.g. ICAREST('docbytes,<url_value>').
	 * 
	 * Subsequent arguments for the 'docbytes' prefix/function should be labelled $1, $2, $3 etc...
	 * 
	 * Note that the properties table must AT THE VERY LEAST have a "schema" property defined for every possible prefix value.
	 * 
	 * @return Hasthable of <property> -> <value> associations for this VTI
	 */
    public Hashtable<String, String> getDefaultVTIProperties() {

        // NOTE THIS METHOD IS MOST LIKELY OVERRIDEN - OR SHOULD BE...
        // All default values for abstract VTI properties are defined here
        if (null == defaultVTIProperties)
            defaultVTIProperties = new Hashtable<String, String>() {
                private static final long serialVersionUID = 1L;

                {
                    // put(getPrefix() + "." + PROP_CACHE_EXPIRES, DEFAULT_EXPIRY_SECONDS);
                    put(PROP_CACHE_EXPIRES, DEFAULT_EXPIRY_SECONDS);
                }
            };

        return defaultVTIProperties;
    }
	
	/**
	 * Gets a property for the VTI. The property may not be defined, in which case the value returned is null.
	 * If the property is defined in gaiandb_config.properties, then that takes precedence.
	 *  
	 * In gaiandb_config.properties, a property must be defined as: <vtiClassName>.<prefix>.<property>=<value>
	 * 
	 * For example:
	 * 
	 * com.ibm.db2j.ICAREST.docbytes.url=http://localhost:8393/search/ESFetchServlet?cid=$0
	 * 
	 * In a SQL query the VTI is invoked as in the example:
	 * select * from new com.ibm.db2j.ICAREST('docbytes,Wight3Col&uri=file:///C:/AppData/Wight3/documents/Event%2B11/SIGINT/SIGINT%2B20100507%2B014.doc') I
	 * 
	 * If the property is not defined in gaiandb_config.properties, then it is looked up in the local hashtable,
	 * which has the mapping: <prefix>.<property> -> <value>
	 * 
	 * @param property
	 * @return
	 * @throws Exception 
	 */
	public String getVTIPropertyNullable( String property ) {
		
		// Try to get the specific property for the prefix function, failing that look one level up (irrespective of prefix)
		// Failing either of those, lookup from memory, first prefixed then non-prefixed.
		
		String prefixedProperty = getPrefix() + '.' + property;
		
		String value = GaianDBConfig.getVTIProperty( this.getClass(), prefixedProperty, false );
		if ( null == value ) value = GaianDBConfig.getVTIProperty( this.getClass(), property, false );
		if ( null == value ) value = getDefaultVTIProperties().get(prefixedProperty);
		if ( null == value ) value = getDefaultVTIProperties().get(property);
		
		logger.logInfo("Got property: " + property + " = " + value);
		
		return value;
	}
	
	/**
	 * Same as getVTIPropertyNullable(), but throws an exception if the property value is null.
	 * 
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public String getVTIProperty( String property ) throws Exception {
		
		String value = getVTIPropertyNullable(property);
		
		if ( null == value ) {
			String vtiClassSimpleName = this.getClass().getSimpleName();
			String msg = "Undefined VTI property: "
				+ vtiClassSimpleName+'.'+property + " or " + vtiClassSimpleName+'.'+getPrefix()+'.'+property + " or "
				+ vtiClassName+'.'+property + " or " + vtiClassName+'.'+getPrefix()+'.'+property;
			throw new Exception(msg);
		}
		
		return value;
	}

	public AbstractVTI() throws Exception {
		this( null, null );
	}
	
	/**
	 * @param constructor
	 *            prefix to identify the service, followed by an optional comma
	 *            separated list of arguments to be used as replacements for $x
	 *            (i.e. $0)
	 * @param extension
	 *            file extension of where to find the data to append to the URL
	 *            of the webservice, the file is "prefix." + extension
	 * @throws Exception 
	 */
	public AbstractVTI(String constructor) throws Exception {
		this( constructor, null );
	}
	
	public AbstractVTI(String constructor, String extension) throws Exception {

		this.extension = null==extension ? this.getClass().getSimpleName() : extension;
		String parts[] = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrDoubleQuotes( constructor, ',' );
		
		if ( null != parts && 0 < parts.length ) {
			// Starting at 1, because first one is the identifier (schools/crimes/etc)
//			logger.logInfo("replacements: " + Arrays.asList(replacements));
			this.prefix = parts[0];
			for (int i = 1; i < parts.length; i++)
				replacements.add(parts[i]);
		}
		
		getDefaultVTIProperties(); // initialise properties
		
		errors = new ErrorBuffer();

	}

	/**
	 * Get the contents of the query file
	 * 
	 * @return
	 * @throws IOException
	 */
	public String getConfigFileTextWithReplacements() throws IOException {
		FileReader fr = new FileReader(getPrefix() + "." + extension);
		BufferedReader br = new BufferedReader(fr);
		String query = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			query += line;
			query += "\r\n";
		}
		br.close();
		return replaceReplacements(query);
	}
	
	/**
	 * Get a VTI property (e.g. URL), with all positional parameters ($0, $1, $2, ...) substituted 
	 * with the values that were passed in as arguments to the VTI.
	 * 
	 * @return
	 * @throws Exception 
	 */
	public String getVTIPropertyWithReplacements( String arg ) throws Exception {
//		String prop = getVTIProperty(arg);
//		logger.logInfo("prop = " + prop + ", replacements = " + Arrays.asList(replacements));
		return replaceReplacements(getVTIProperty(arg));
	}

	/**
	 * Replace tokens (i.e. $0) with the matching argument from the constructor.
	 * 
	 * @param argValue
	 * @return
	 */
	private String replaceReplacements(String argValue) {
		String argIn = argValue;
		for (int i = 0; i < replacements.size(); i++)
			// Replace each marked argument (labelled $0, $1, $2, etc) with values passed to the VTI
			// Ensure the character immediately following a marked argument is a non-digit character or the end of 
			// line character and add this back to the replaced text.
			argValue = argValue.replaceAll("\\$" + i + "(\\D|$)", replacements.get(i) + "$1");
		
		logger.logInfo("Replaced property positional args: " + argIn + " => " + argValue);
		return argValue;
	}
	
	protected String getCacheSchemaAndTableName() {
		return "CACHE." + getCacheTableName();
	}
	
	protected String getCacheTableName() {
		return new String( extension + "_" + getPrefix() ).toUpperCase();
	}

	private static final Object EXPIRES_TABLE_CREATION_LOCK = new Object();
	
	/**
	 * Creates a local table to cache the results
	 * 
	 * @param stmt
	 * @return true if the table already existed, false if it had to be created
	 * @throws Exception
	 */
	protected boolean findOrCreateCacheTableAndExpiryEntry( Statement stmt ) throws Exception {
		
		if ( true == Util.isExistsDerbyTable( stmt.getConnection(), "CACHE", getCacheTableName() ) )
			return true;
		
		// Create a cache table based on the definition for it - this may have columns that need morphing to Derby syntax (e.g. LONGVARCHAR) 
		// and it also should include a CACHEID column.
		String cacheTableColumns = getCacheTableMetaData().getColumnsDefinitionMorphed2DerbySyntax();
		logger.logInfo("Creating CACHE table with columns: " + cacheTableColumns);
		
		String query = "CREATE TABLE " + getCacheSchemaAndTableName() + " (" + cacheTableColumns; // + ")";
		
		String primaryKey = getVTIPropertyNullable( PROP_CACHE_PKEY );
		if (primaryKey != null) {
//			stmt.execute("ALTER TABLE " + getCacheSchemaAndTableName() + " ADD PRIMARY KEY (" + primaryKey + ")");
			query += ", PRIMARY KEY (" + primaryKey + ")";
		}

		query += ")";
		
		logger.logInfo("Executing SQL: " + query);
		stmt.execute(query);
		
		String indexString = getVTIPropertyNullable( PROP_CACHE_INDEXES );
		if (indexString != null) {
			String[] indexes = indexString.split(";");
			for ( int i=0; i<indexes.length; i++ )
				stmt.execute("CREATE INDEX IDX_" + getCacheTableName() + "_" + i + " ON " + getCacheSchemaAndTableName() + "(" + indexes[i] + ")");
		}

		synchronized( EXPIRES_TABLE_CREATION_LOCK ) {

			if ( false == Util.isExistsDerbyTable( stmt.getConnection(), "CACHE", "EXPIRES" ) )
				stmt.execute("CREATE TABLE CACHE.EXPIRES (name VARCHAR(64), lastreset BIGINT)");
		}
		
		// Set the expiry for the newly created cache table
		stmt.execute("INSERT INTO CACHE.EXPIRES VALUES ('" + getCacheSchemaAndTableName() + "', "+System.currentTimeMillis()+")");
		
		return false;
	}
//	
//	/**
//	 * Creates the CACHE.EXPIRES table, used to determine when rows from each cached table have expired
//	 * 
//	 * @param stmt
//	 * @return true if the table already existed, false if it had to be created
//	 * @throws SQLException
//	 */	
//	private void findOrCreateExpiryTable( Statement stmt ) throws SQLException {
//		
//		if (stmt.getConnection().getMetaData().getTables(null, "CACHE", "EXPIRES", null).next())
//			return true;
//		
//		stmt.execute("CREATE TABLE CACHE.EXPIRES (name VARCHAR(64), lastreset BIGINT)");
//		stmt.execute("INSERT INTO CACHE.EXPIRES VALUES ('" + getCacheSchemaAndTableName() + "', "+System.currentTimeMillis()+")");
//		return false;
//	}
	
	// Note cache tables are dropped initially when a GaianDB node starts up
	public static void dropCacheTables( Statement stmt ) throws SQLException {
		ResultSet rs = stmt.executeQuery(
				"select tablename from sys.sysschemas s,sys.systables t where s.schemaid = t.schemaid and schemaname = 'CACHE'");
		
		ArrayList<String> tables = new ArrayList<String>();
		while (rs.next()) tables.add(rs.getString(1));
		
		for (String table : tables) {
			logger.logInfo("Dropping table CACHE." + table );
			stmt.execute("DROP TABLE CACHE."+ table );
		}
	}
	
	/**
	 * This method should only be used by the extender of this class if it is attributing a different cache table to every instance of itself.
	 * This can be engineered by ensuring that the extension+prefix combination is unique for every instance of the class...
	 * This may be preferred when cache tables need to expire quickly, to facilitate deletion.
	 * If this method is used, then the expiry duration feature will be redundant...
	 * 
	 * @param stmt
	 * @throws SQLException
	 */
	public void dropCacheTableAndDeleteExpiryEntry() {
		
		Connection c = null; Statement stmt = null;
		try {
			closeCacheStatementsAndReleaseConnections();
			logger.logInfo("Dropping cache table " + getCacheSchemaAndTableName() );
			c = getPooledLocalDerbyConnection();
			
			if ( false == Util.isExistsDerbyTable(c, "CACHE", getCacheTableName()) ) {
				logger.logInfo("Unable to drop cache table as it does not exist: " + getCacheSchemaAndTableName());
				return;
			}
			stmt = c.createStatement();
			stmt.execute("DROP TABLE " + getCacheSchemaAndTableName() );
			stmt.execute("DELETE FROM CACHE.EXPIRES WHERE name = '" + getCacheSchemaAndTableName() + "'" );
		} catch ( Exception e ) {
			logger.logWarning(GDBMessages.DSWRAPPER_DROP_CACHE_TABLES_WARNING, "Unable to drop cache table " + getCacheSchemaAndTableName() + ": " + e);
		} finally {
			try { logger.logInfo("Closing DROP stmt isNull? " + (null==stmt) + ", and recycling its connection isActive? " + (null != c && !c.isClosed()) );
				if ( null != stmt ) stmt.close(); if ( null != c && !c.isClosed() ) recyclePooledLocalDerbyConnection(c); }
			catch ( SQLException e ) { logger.logWarning(GDBMessages.DSWRAPPER_RECYCLE_CONNECTION_ERROR, "Unable to recycle connection after dropping cache table"); }
		}
	}
	
	/**
	 * Checks to see if a cache exists, and if it does, if the data in it is sufficiently new.
	 * If there's no cache, the calling VTI should call cacheRow(DataValueDescriptor[] row) with each new row.
	 * If the cache does exist, the calling VTI should use nextRowFromCache(DataValueDescriptor[] row) in its nextRow() function.
	 * 
	 * It is the extending VTI's responsibility to know when it has cached ALL rows for its prefix and the given constraints
	 * before calling this method.
	 * 
	 * However, threads targeting the same cache table will not 'see' partial results from each others' inserts because
	 * they are not committed until this method is called.
	 *  
	 * To avoid duplicate entries in a cache table, you may want to define a PROP_CACHE_PKEY property for it BEFORE calling 
	 * it for the first time (which creates the cache table).
	 * 
	 * @param constraints - This is a 'where-clause' condition to target specific rows in a shared cache table, 
	 * e.g. null or "" or "1=1" for all rows
	 * @return
	 * 	 false: If rows are not cached yet. The cache table was either empty or created. The vti is now ready to cacheRow()/cacheRows().
	 *       or If caching for this vti has been disabled due to its expiry property being < 1 or an exception occurring.
	 *   true : If rows have been cached. These are available (from now until expiry only) for extraction using nextRowFromCache().
	 */
	public boolean isCached(String constraints) {
		
		try {
			logger.logImportant("Checking caching state, isCached == " + isCached + ", rows constraint: " + constraints);
			
			long expiryDuration = getExpiryDuration();
			if ( 0 >= expiryDuration ) {
				logger.logImportant("Cache expiry duration is 0 - caching disabled");
				isCached = -1;
			}

			if (isCached > -1) {
				
				Connection conn = getPooledLocalDerbyConnection();
				Statement stmt = conn.createStatement();
				
				try {
					// Evaluate booleans independently so both tables are created if need be
					boolean cacheTableAlreadyExists = findOrCreateCacheTableAndExpiryEntry(stmt);
					
					if ( cacheTableAlreadyExists ) {
						
						// Some rows are cached - check if they have expired
						logger.logInfo("Cache table exists: " + getCacheSchemaAndTableName() );
						
						boolean isCacheReady = false;
						
						if ( null != preparedInsertStatement ) {
							// Row inserts have been done - close statement
							logger.logImportant("Closing prepared insert statement for " + getCacheSchemaAndTableName());
							Connection c = preparedInsertStatement.getConnection();
							preparedInsertStatement.close(); preparedInsertStatement = null;
							recyclePooledLocalDerbyConnection( c );
							resetCacheExpiryTime();
							isCacheReady = true;
						
						} else {
							// Inserts may still be occurring in another thread/process - check lastreset time on expiry table
							ResultSet rs = stmt.executeQuery("SELECT lastreset FROM CACHE.EXPIRES WHERE name = '" + getCacheSchemaAndTableName() + "'");
							if (!rs.next())
								throw new Exception("No expiry entry found for cache table: " + getCacheSchemaAndTableName());
							
							long lastreset = rs.getLong(1);
							rs.close();
							
							if ( System.currentTimeMillis() > lastreset + expiryDuration) {
								// Rows have been inserted in this table and the current time has gone beyond the expiry time
								logger.logImportant("Deleting stale cache rows and reseting expiry for table " + getCacheSchemaAndTableName());
								stmt.execute("DELETE FROM " + getCacheSchemaAndTableName());
//								conn.setAutoCommit(false);
//								stmt.execute("DROP TABLE " + getCacheSchemaAndTableName()); // much faster to drop and recreate - but do these as a unit
//								findOrCreateCacheTable(stmt);
//								conn.commit();
//								conn.setAutoCommit(true);
								resetCacheExpiryTime();
								
								//Set isCached to 0 to indicate it is empty now
								isCached = 0;
							} else
								isCacheReady = true;
						}
						
						if ( isCacheReady ) {
							
//							System.out.println("grsmd: " + grsmd + ", constraints: " + constraints);
							
							// Don't "select *" as it may contain cache index cols which we don't want to expose
							String sql = "SELECT " + getTableMetaData().getColumnNamesWrappedIfNotOrdinary() + " FROM " + getCacheSchemaAndTableName() +
//							String sql = "SELECT " + getTableMetaData().getColumnNames() + " FROM " + getCacheSchemaAndTableName() +
								( null != constraints && 0 < constraints.trim().length() ? " WHERE " + constraints : "" );
							logger.logInfo("Executing query: " + sql);
							underlyingResultSet = conn.createStatement(
									ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ).executeQuery( sql );
							
							if (underlyingResultSet.next()) {
								logger.logImportant("Found cached rows for constraints: "+constraints);
								isCached = 1; // Rows are cached
								underlyingResultSet.beforeFirst();
								resultRows = new GaianChildRSWrapper(underlyingResultSet);
							} else {
								logger.logImportant("No cached rows for contraints: "+constraints);
								underlyingResultSet.close();
								underlyingResultSet = null;
							}
						}
						
					} else {
						logger.logImportant("Initialised cache table: " + getCacheSchemaAndTableName());
					}
				} finally {
					if ( null == underlyingResultSet ) { // No rows cached yet - no need to keep stmt open
						stmt.close();
						recyclePooledLocalDerbyConnection(conn);
					}
				}
			}
		} catch (Exception e) {
			isCached = -1;
			logger.logException(GDBMessages.DSWRAPPER_CACHE_TABLES_INIT_ERROR, "Unable to initialise cache tables (caching disabled)", e);
		}
		logger.logImportant("isCached() status " + isCached + " for table " + getCacheSchemaAndTableName() + ", returning " + (isCached==1));
		return isCached == 1;
	}

	private static PreparedStatement updateExpiryStatement = null;
	
	public void resetCacheExpiryTime() throws SQLException {
		
		if ( null == updateExpiryStatement ) {
			updateExpiryStatement = getPooledLocalDerbyConnection().prepareStatement(
					"UPDATE CACHE.EXPIRES SET lastreset = ? WHERE name = ?" );
		}
		
		synchronized( updateExpiryStatement ) {
			updateExpiryStatement.setLong(1, System.currentTimeMillis());
			updateExpiryStatement.setString(2, getCacheSchemaAndTableName());
			updateExpiryStatement.execute();
		}
	}
	
	/**
	 * You MUST call isCached(String constraints) before checking this method.
	 * 
	 * @return
	 */
	public boolean isCached() {
		return isCached == 1;
	}
	
	protected long getExpiryDuration() throws Exception {
		return 1000 * Long.parseLong(getVTIProperty( PROP_CACHE_EXPIRES ));
	}
	
	protected Connection getPooledLocalDerbyConnection() throws SQLException {
		return DataSourcesManager.getPooledJDBCConnection(
				GaianDBConfig.getLocalDerbyConnectionID(),
				DataSourcesManager.getSourceHandlesPoolForLocalNode());
	}

	protected void recyclePooledLocalDerbyConnection(Connection c) throws SQLException {
		DataSourcesManager.getSourceHandlesPool( GaianDBConfig.getLocalDerbyConnectionID() ).push(c);
	}
	
    private PreparedStatement getPreparedInsertStatement( int numRows, int numCols ) throws Exception {
    	
    	Connection conn = null;
		if ( null != preparedInsertStatement ) {
			if ( numRowsForInsertStatement == numRows )
				return preparedInsertStatement;
			
			conn = preparedInsertStatement.getConnection();
			preparedInsertStatement.close();
		}
		
		StringBuffer query = new StringBuffer( "INSERT INTO " + getCacheSchemaAndTableName() + " VALUES " );
		for ( int r=0; r<numRows; r++, query.append( r<numRows ? ", " : "" )  ) {
			query.append( "(?" );
			for ( int i=1; i<numCols; i++ )	query.append( ", ?" );
			query.append( ')' );
		}
		
		// Note inserts are committed as they are executed - so we have to deal with:
		// 1) setting expiry until all records are written 2) other exec threads reading partial results 3) other exec threads deleting our records
		// Don't use a transaction to commit all inserts as it locks the table completely and we don't know if the cache will ever be used
		// by the calling code and therefore we wdnt necessarily know when to commit all the inserts.
		// Solution: Don't let the table expire reset expiry every time an insert is done.
		try {
			preparedInsertStatement = (null == conn ? getPooledLocalDerbyConnection() : conn).prepareStatement( query.toString() );
		} catch (SQLException e) { logger.logException(GDBMessages.DSWRAPPER_CACHE_INSERT_STATEMENT_ERROR,
				"Insert statement against cache table failed: " + query.toString(), e); throw e; }
		
		numRowsForInsertStatement = numRows;
		
		logger.logInfo("Prepared insert statement against " + getCacheSchemaAndTableName() + " for numRows/numCols: " +
				numRows + "/" + numCols);
		
		return preparedInsertStatement;
    }
    
    private String vTS_typeName;
    private int vTS_typeDisplaySize;
    private int vTS_contentLength;
    
    /**
     * Validates and, if necessary, modifies the given row to match the defined result schema for this VTI.
     * @param row - the row to validate & potentially modify.
     * @param fieldModificationCountMap - a map to fill with information about any modifications that take place. The format of the map is:
     * String -> [String, Integer]
     * Field name -> [Truncated to (length or NULL), Occurrence count (i.e. how many rows affected)]
     * This map can be repeatedly passed into this method and the occurrence count for existing fields will be incremented rather than
     * reset. It is up to you to clear the map of old data.
     * @return A boolean indicating whether any data has been modified in the row.
     * @throws Exception
     */
    public boolean validateAndModifyToSchema(DataValueDescriptor[] row, Map<String, Object []> fieldModificationCountMap) throws Exception {
    	
    	//Note: Data type conversion and numeric precision should be taken care by DVDs (I believe)
    	
    	boolean dataChanged = false;
    	
    	GaianResultSetMetaData metaData = getMetaData();
    	
    	for(int i = 0; i < row.length; i++) {
    		
    		//Get the type of the current field
    		vTS_typeName = row[i].getTypeName();    	
    		
    		//If character type
    		if(vTS_typeName.startsWith("VARCHAR") || vTS_typeName.startsWith("NVARCHAR") 
    				|| vTS_typeName.startsWith("LONGVARCHAR") || vTS_typeName.startsWith("NLONGVARCHAR") 
    				|| vTS_typeName.startsWith("CHAR") || vTS_typeName.startsWith("NCHAR") || vTS_typeName.startsWith("CLOB")) {
    			
    			//Get the current fields content length & the maximum it should be
    			vTS_contentLength = row[i].getLength();
    			vTS_typeDisplaySize = metaData.getColumnDisplaySize(i + 1);
    			
    			//If too big, truncate to maximum
    			if(vTS_contentLength > vTS_typeDisplaySize) {
    				row[i].setValue(row[i].getString().substring(0, vTS_typeDisplaySize));
    				
    				if(fieldModificationCountMap != null) {
    					Object [] value;
    					
    					if(fieldModificationCountMap.containsKey(metaData.getColumnName(i + 1))) {
    						value = fieldModificationCountMap.get(metaData.getColumnName(i + 1));
    						value[1] = ((Integer)value[1]) + 1;
    					}
    					else {
    						value = new Object[2];
    						value[0] = metaData.getColumnDisplaySize(i + 1);
    						value[1] = new Integer(1);
    					}
    					
    					fieldModificationCountMap.put(metaData.getColumnName(i + 1), value);
    				}
    				
    				//Flag that change has occurred
    				dataChanged = true;
    			}
    		}
    		//Else If binary type
    		else if(vTS_typeName.startsWith("BLOB") || vTS_typeName.startsWith("BINARY") || vTS_typeName.startsWith("VARBINARY")) {
    			//Get the current fields content length & the maximum it should be
    			vTS_contentLength = row[i].getLength();
    			vTS_typeDisplaySize = metaData.getColumnDisplaySize(i + 1);
    			
    			//If too big, set to null
    			if(vTS_contentLength > vTS_typeDisplaySize) {
    				row[i].setToNull();
    				
    				if(fieldModificationCountMap != null) {
    					Object [] value;
    					
    					if(fieldModificationCountMap.containsKey(metaData.getColumnName(i + 1))) {
    						value = fieldModificationCountMap.get(metaData.getColumnName(i + 1));
    						value[1] = ((Integer)value[1]) + 1;
    					}
    					else {
    						value = new Object[2];
    						value[0] = "NULL";
    						value[1] = new Integer(1);
    					}
    					
    					fieldModificationCountMap.put(metaData.getColumnName(i + 1), value);
    				}
    				
    				//Flag that change has occurred
    				dataChanged = true;
    			}
    		}    		
    	}
    	
    	return dataChanged;
    }
	
    // This array holds the values to lookup cached results in the cache table.
    // Necessary when the cached rows don't naturally have index columns that map directly to the queries.
    // There MUST be as many cache key values as there are columns missing in the getMetaData() compared with the PROP_SCHEMA property for this cache.
    private DataValueDescriptor[] cacheKeys = new DataValueDescriptor[0];
    public void setCacheKeys( DataValueDescriptor[] keys ) {
    	if ( null != keys ) {
    		cacheKeys = keys;
    		logger.logInfo("Set cacheKeys to: " + Arrays.asList(cacheKeys));
    	}
    }
    
	/**
	 * Stores the row in the cache.
	 * @param row
	 */
	public void cacheRow(DataValueDescriptor[] row) {
		cacheRows( new DataValueDescriptor[][] {row}, null );
	}
    
	/** 
	 * This method will only cache to disk the first N columns in the passed in row record of data.
	 * 
	 * Use this when invoking from a VTI implementation which is also configured to be a data source for a GaianDB logical table, because
	 * the number of columns passed in to the nextRow() method will include extra "hidden" logical table columns which do not need caching.
	 * 
	 * Furthermore, the VTI itself may have constant column information which it doesn't need to cache.
	 * 
	 * @param row
	 * @param numColsToCache
	 */
	public void cacheRow(DataValueDescriptor[] row, int numColsToCache) {
		cacheRows( new DataValueDescriptor[][] {row}, null, numColsToCache );
	}
	
	public void cacheRow(DataValueDescriptor[] row, Map<String, Integer> errorMap) {
		cacheRows( new DataValueDescriptor[][] {row}, errorMap );
	}
	
	/**
	 * Use this method to cache all the rows passed in. All columns of the rows will be cached.
	 * 
	 * @param rows
	 */
	public void cacheRows(DataValueDescriptor[][] rows) {
		cacheRows(rows, null);
	}
	
	/**
	 * Cache all rows passed in, but only the first N columns of each row.
	 * Use this if the row passed in by Derby for fetching data might contain more columns than you wish to cache. This will be the case
	 * if your VTI is configured to be a GaianDB logical table data source. It would also be the case if your VTI returns some information that
	 * can be deduced easily at fetch-time and therefore doesn't need to be cached.
	 * 
	 * @param rows
	 * @param numColsToCache
	 */
	public void cacheRows(DataValueDescriptor[][] rows, int numColsToCache) {
		cacheRows(rows, null, numColsToCache);
	}
	
	private Integer cR_reUsedErrorCount = new Integer(0);
	
	private void cacheRows(DataValueDescriptor[][] rows, Map<String, Integer> errorMap) {
		cacheRows(rows, errorMap, null == rows || 1 > rows.length ? 0 : rows[0].length);
	}
	
	private void cacheRows(DataValueDescriptor[][] rows, Map<String, Integer> errorMap, int numColsToCache) {
	
		if ( isCached != 0 ) return;
		
		try {
			if ( 1 > rows.length ) {
				logger.logInfo("No rows to cache (ignored)");
				return;
			}
			if ( 1 > rows[0].length || numColsToCache > rows[0].length || null == getTableMetaData() ) {
				logger.logWarning(GDBMessages.DSWRAPPER_CACHE_ROWS_ERROR, "Unable to cache rows (ignored): MetaData isNull? " + (null==grsmd) +
						" or Invalid number of columns: " + rows[0].length + " (numColsToCache is "+numColsToCache+")");
				return;
			}
			
//			int numCols = rows[0].length;

			// Note some VTIs meta-data will not include the CACHEID column. This should be in the cache keys so we can lookup the right rows later.
			// There must be as many cacheKeys as there are extra columns in the PROP_SCHEMA over the meta-data.
			PreparedStatement ps = getPreparedInsertStatement(rows.length, numColsToCache + cacheKeys.length);
			
			int totalCols = numColsToCache + cacheKeys.length;
			for ( int r=0; r<rows.length; r++ ) {
				for ( int i=0; i<totalCols; i++ ) {
					int idx = totalCols*r + (i+1);
					DataValueDescriptor col = i < numColsToCache ? rows[r][i] : cacheKeys[i-numColsToCache];
//		    		logger.logDetail("Setting col value in ps idx " + idx + ": " + col.getObject());
					if ( col instanceof SQLBlob ) {
						ps.setBytes(idx, col.getBytes());
					} else {
						if (null == col.getObject()) {
							ps.setNull(idx, grsmd.getColumnType(i + 1));
						} else {
							ps.setObject(idx, col.getObject());
						}
					}
				}
			}
		
			try {
				ps.executeUpdate();
			} catch(SQLIntegrityConstraintViolationException e) {
				// Ignore SQLIntegrityConstraintViolationException - duplicates may be written to the cache as the cache is built over time
			} catch ( SQLException e ) {
				
				//If error map passed in - then fill with any errors that occur (sqlState to count of occurrences)
				if(errorMap != null) {
					if (errorMap.containsKey(e.getSQLState())) {
						cR_reUsedErrorCount = errorMap.get(e.getSQLState());
					}
					else {
						cR_reUsedErrorCount = new Integer(0);
					}
					
					cR_reUsedErrorCount++;
					
					errorMap.put(e.getSQLState(), cR_reUsedErrorCount);
				}
				//ELSE
				//Just swallow the exception - this was the previous behaviour, which 'assumes' that the entry is already in the cache and
				//so we can ignore the exception and don't need to invalidate the cache.
				//At some point, investigation should be performed as to whether this is correct - as it may not hold true
				//for all exception types
			}
			
		} catch ( Exception e ) {
			isCached = -1;
			logger.logException(GDBMessages.DSWRAPPER_ROW_CACHE_ERROR, "Unable to cache row (cache invalidated): ", e);
		}
	}

	// Overridden from IFastPath interface
	@Override public abstract int nextRow( final DataValueDescriptor[] arg0 ) throws StandardException, SQLException; //{ return IFastPath.SCAN_COMPLETED; }
	
	// Overridden from VTI60 -> ResultSet interface - Underlying VTI should also override this
	@Override public boolean isBeforeFirst() throws SQLException { return isBeforeFirst; }
	
	public int nextRowFromCache(DataValueDescriptor[] row) {
		try {
			if (resultRows.fetchNextRow(row)) {
				return GOT_ROW;
			} else {
				if ( null != underlyingResultSet )
					underlyingResultSet.beforeFirst(); // scroll back so we can fetch rows again (if close() isn't called)
				if ( -1 == isCached ) isCached = 0;
				return SCAN_COMPLETED;
			}
		} catch (Exception e) {
			logger.logException(GDBMessages.DSWRAPPER_ROW_FETCH_ERROR, "Unable to fetch row or scroll back to begining of ResultSet", e);
			if ( -1 == isCached ) isCached = 0;
			return SCAN_COMPLETED;
		}
	}

	/**
	 * Get the SQL-like operator for a given qualifier's operator (i.e. <, >, =)
	 * 
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static String getOperatorFor(int id) throws SQLException {
		switch (id) {
		case 1:
			return " < ";
		case 2:
			return " = ";
		case 3:
			return " > ";
		default:
			throw new SQLException("Unrecognised comparison");
		}
	}

	/**
	 * Assume all implementing VTIs can be re-fetched... i.e. fetchNextRow() would extract the first one again after a SCAN_COMPLETED
	 */
	public boolean isScrollable() { return true; }

	/**
	 * Sets system arguments that may have come from the comment or positional parms of the higher level incoming query
	 * Possible argument keys populated in GaianTable or GaianResult:
	 * 
	 * 		QRY_ID								Query id
	 * 		QRY_STEPS							Propagation count, i.e. depth
	 * 		QRY_CREDENTIALS						Credentials string block - Usually a String created using B64 encoding on an encrypted byte array.
	 * 		QRY_MAX_SOURCE_ROWS 				Max rows to extract for all data sources - controlled by GaianTable argument. Policy can also set its own constraint per data source.
	 * 		QRY_PATH							Path of nodes traversed by query up to this node.
	 * 		QRY_ORDER_BY_CLAUSE					Order by clause - to be pushed to endpoint RDBMS data sources
	 * 		QRY_IS_EXPLAIN						true or false, depending on whether this is an 'explain' query - i.e. getting route and count info. 
	 * 		<VTIClass>_VTIARG					Custom argument destined to a VTI data source endpoint
	 * 		QRY_EXPOSED_COLUMNS_COUNT			Number of GaianTable columns required for this query. Varies if provenance or explain columns are included.
	 * 		QRY_INCOMING_COLUMNS_MAPPING		Mapping of columns between an original and a new logical table definition (for re-executed GaianTable objects).
	 * 		QRY_APPLICABLE_ORIGINAL_PREDICATES	Original predicates identified using shallow regex parsing in GaianTable. Provides extra push optimisations for simple queries.
	 * 		QRY_TIMEOUT							Timeout for overall query. Query is abandoned after this. Default is -1 (no timeout).
	 * 		QRY_WID								Workload ID - to be set in the query comment by the application, and may be identical for multiple queries - intended for future control/monitoring features.
	 * 		QRY_HASH							Hash value of the original SQL query received by Derby at the entry point node - Propagated by GaianDB in a comment.
	 * 
	 * 		DS_WRAPPER_DEFAULT_SCHEMA			Data source wrapper default schema - as defined in _SCHEMA property in gaiandb_config.properties, or defaulting to logical table definition - including constant cols, but not their values.
	 * 
	 * Example arguments array: {QRY_STEPS=0, EXPOSED_COLUMNS_COUNT=3, QRY_ID=IBM33MO096GJBQ:1361901504972:0, queryHash=E8003F0C}
	 */
	@Override public void setArgs(String[] args) throws Exception {
		gaianTableQueryContextArguments = args;
	};
	
	private String[] gaianTableQueryContextArguments = null;
  
    private int[] columnsMappingToPhysicalOnes = null;
    private int[] allLogicalColsInvolvedInQuery = null;
    private DataValueDescriptor[] vtiRow;
    int queriedColumnsCount;
	
    private void initialiseQueriedColumns() throws SQLException {
    	
    	GaianResultSetMetaData vtirsmd = getMetaData();
    	queriedColumnsCount = vtirsmd.getPhysicalColumnCount();
    	
		int[] vtiColumnIDs = new int[ queriedColumnsCount ]; // 0-based
		for ( int i=0; i<queriedColumnsCount; i++ ) vtiColumnIDs[i] = i;
		
		// Assume all cols will be queried - and no logical->physical id mappings
		allLogicalColsInvolvedInQuery = vtiColumnIDs; // 0-based
		columnsMappingToPhysicalOnes = vtiColumnIDs; // no mapping
		
		// vtiRow will be populated later with cells passed down in fetchNextRow() for each of the projected columns
		vtiRow = new DataValueDescriptor[ vtiColumnIDs.length ];
		
		// Populate with empty cells anyway because some may not be filled in later and we don't want to pass nulls to the underlying VTI. 
		for ( int i=0; i<vtiColumnIDs.length; i++ )
			vtiRow[i] = RowsFilter.constructDVDMatchingJDBCType( vtirsmd.getColumnType(i+1) );

    }
    
	/**
	 * Evaluates columns involved in query and registers qualifiers and current column mappings so as to apply these in fetchNextRow()
	 * when extracting rows.
	 * Underlying VTI implementation is just passed columns projection and qualifiers relative to its own physical table schema.
	 * Therefore it does not need to concern itself with the logical table schema nor with mapping its columns to it.
	 * 
	 * For the case where Derby invokes this VTI directly (rather than Gaian under a logical table), Derby will just call setQualifiers() and pushProjection().
	 * 
	 * NOTE - this method is intentionally final. subclasses must not by-pass this code.
	 */
	@Override public final void setExtractConditions(Qualifier[][] qualifiers, int[] logicalProjection, int[] columnsMappingToPhysicalOnes) throws Exception {

    	// Note if logicalProjection is null it means all cols are to be selected.
    	// If columnsMapping is null it means don't map column IDs to logical ones - just use the physical ones.
    	    	
		initialiseQueriedColumns(); // could avoid doing this for every query...?
		
		if ( null != logicalProjection ) {
			this.allLogicalColsInvolvedInQuery = RowsFilter.getAllSortedColsInvolvedInQuery( logicalProjection, qualifiers ); // 0-based column IDs
			queriedColumnsCount = allLogicalColsInvolvedInQuery.length; // columnsMapping.length
		}
    	
		if ( null != columnsMappingToPhysicalOnes )
			this.columnsMappingToPhysicalOnes = columnsMappingToPhysicalOnes; // 0-based
    	
    	logger.logThreadInfo("setExtractConditions(): all query's logical col ids: "
    			+ Util.intArrayAsString(allLogicalColsInvolvedInQuery)
    			+ ", columnsMappingToPhysicalOnes: " + Util.intArrayAsString(this.columnsMappingToPhysicalOnes) );
		
    	// Push down qualifier column filters and list of queried columns - to retrieve only the required data
    	// All the following: pushProjection(), setQualifiers() and executeAsFastPath() are implemented in the extending class
    	
		int[] mappedAndReducedProjection = resolveMappedProjectedColumnIDsApplicableToVTI( logicalProjection, columnsMappingToPhysicalOnes );
    	logger.logInfo("Mapped and reduced projection for VTI to: " + Util.intArrayAsString( mappedAndReducedProjection ) );
    	
    	Qualifier[][] mappedAndReducedQualifiers = resolveMappedQualifierColumnIDsApplicableToVTI( qualifiers, columnsMappingToPhysicalOnes );
	    logger.logInfo("Mapped and reduced qualifiers for VTI to: " + RowsFilter.reconstructSQLWhereClause(mappedAndReducedQualifiers) );
    	
    	if (	null != mappedAndReducedProjection && 0 == mappedAndReducedProjection.length
    		||	null != qualifiers && null == mappedAndReducedQualifiers ) {
    		logger.logInfo("Short-cutting this VTI query because: 1) resolved projection is empty; and/or: 2) resolved qualifiers are false");
    		queriedColumnsCount = 0; // this will short-cut fetchNextRow() as well 
    		return;
    	}

		pushProjection(null, mappedAndReducedProjection);
    	setQualifiers(null, mappedAndReducedQualifiers);
		executeAsFastPath();
    	isBeforeFirst = true;
	}
	
	/**
	 * Maps 0-based colID in qualifiers structure using columnsMappingToPhysicalOnes.
	 * Creates and returns a cloned qualifiers structure with new mapped colIDs.
	 * 
	 * NOTE! Qualifiers referencing columnIDs that are outside of the range of the columns mapping array do not exist in the VTI.
	 * Therefore - these are resolved using q.getUnknownRV(), and the qualifiers structure to be returned is re-factored appropriately.
	 * 
	 * @param qualifiers
	 * @param columnsMappingToPhysicalOnes
	 * @return A Qualifier[][] structure with remaining filters to be tested against the VTI columns of extracted records;
	 * or null if it resolves to false due to out-of-bounds columns.
	 * @throws Exception
	 */
	private Qualifier[][] resolveMappedQualifierColumnIDsApplicableToVTI( Qualifier[][] qualifiers, int[] columnsMappingToPhysicalOnes ) throws Exception {
		if ( null == qualifiers ) return null;
		
		Qualifier[][] gsqs = new GenericScanQualifier[ qualifiers.length ][];
		for ( int i=0; i<qualifiers.length; i++ ) {
			Qualifier[] qRow = qualifiers[i];
			gsqs[i] = new GenericScanQualifier[qRow.length];
			
			for ( int j=0; j<qRow.length; j++ ) {
				Qualifier q = qRow[j];
				gsqs[i][j] = new GenericScanQualifier();
				try {
					// If column ID is not in this VTI, then we will need to re-factor the gsqs[][] appropriately.
					// We use testAndPruneQualifiers() at the end of this method to prune qualifiers referencing out-of-bound columns.
					
					int colID = q.getColumnId();
					
					if ( null != columnsMappingToPhysicalOnes ) {
						if ( 0 > colID || columnsMappingToPhysicalOnes.length <= colID ) colID = -1; // colID not mapped to a VTI column
						else colID = columnsMappingToPhysicalOnes[colID]; // now converted to a VTI colID, still 0-based
					}
					
					final boolean outOfRange = 0 > colID || vtiRow.length <= colID;
					
					// Adjust orderedNulls and unknownRV if this column ID was found to be out-of-range and therefore treated as NULL.
					// In the incoming Qualifiers[][], Derby might have omitted to set the flags.
					((GenericScanQualifier) gsqs[i][j]).setQualifier( colID, q.getOrderable(),
						q.getOperator(), q.negateCompareResult(),
						outOfRange ? !q.negateCompareResult() : q.getOrderedNulls(), outOfRange ? false : q.getUnknownRV() );
					
				} catch ( ArrayIndexOutOfBoundsException e1 ) {
					throw new SQLException("Unable to map logical column ID: " + q.getColumnId() + " to VTI colID using columnsMapping: "
							+ Util.intArrayAsString( columnsMappingToPhysicalOnes ) + ": " + e1 );
				} catch (StandardException e2) {
					throw new SQLException("Unable to clone logical table Qualifier with column index " + q.getColumnId()
							+ " using columnsMapping: " + Util.intArrayAsString( columnsMappingToPhysicalOnes ) + ": " + e2);
				}
			}
		}
		
    	logger.logInfo("Mapped qualifiers for VTI to: " + RowsFilter.reconstructSQLWhereClause(gsqs) );
    	gsqs = RowsFilter.testAndPruneQualifiers(vtiRow, gsqs, true); // might resolve to null due to out-of-range NULL values.
		
		return gsqs;
	}
	
	/**
	 * Create an int[] mapping for fullProjectedColIDs using columnsMappingZeroBased, with any out-of-range columnIDs removed.
	 * If isColsArrayOneBased == false, treat fullProjectedColIDs values as 1-based, otherwise they are 0-based.
	 * This affects the index used to lookup the mapping of the column.
	 * 
	 * When columnsMapping is null, the method behaves as if the mapping is 1 to 1.
	 * Any columns out of range of a non-null columnsMapping array will not be mapped.
	 * Any resolved columnIDs that are out of range of the vtiRow's set of columns are ignored.
	 * 
	 * @param fullProjectedColIDs
	 * @param columnsMappingZeroBased
	 * @param isColsArrayOneBased
	 * @return A reduced and sorted set of mapped columnIDs for columns exposed by this VTI
	 * @throws SQLException
	 */
	private int[] resolveMappedProjectedColumnIDsApplicableToVTI( int[] fullProjectedColIDs, int[] columnsMappingZeroBased ) throws SQLException {
		
		if ( null == fullProjectedColIDs ) return null;
		
		SortedSet<Integer> reducedSet = new TreeSet<Integer>();
		
		for ( int i=0; i<fullProjectedColIDs.length; i++ ) {
			int colID = fullProjectedColIDs[i] - 1; // normalise to 0-based
			if ( null != columnsMappingZeroBased ) {
				if ( 0 > colID || columnsMappingZeroBased.length <= colID ) continue; // colID not mapped to a VTI column
				colID = columnsMappingZeroBased[colID]; // now converted to a VTI colID, still 0-based
			}
			if ( 0 > colID || vtiRow.length <= colID ) continue; // out of range, skip
			reducedSet.add( colID+1 ); // convert to 1-based
		}

		int[] mappedAndReducedProjection = new int[ reducedSet.size() ];
	    int i=0; for (Integer colIntegerID : reducedSet) mappedAndReducedProjection[i++] = colIntegerID.intValue();
		
		return mappedAndReducedProjection;
	}

//	private boolean isVTIRowTypesInitialised = false;
	
	/**
	 * This method is called through the GaianChildVTI interface, i.e. as a data source of a logical table - so it may need to have its columns mapped.
	 * It fetches a record from the underlying VTI - which is expected to satisfy the qualifiers filter we passed down previously - and then copies
	 * the typed DVD cells from the VTI record to the mapped logical table column cells.
	 * NOTE - this method is intentionally final. subclasses must not by-pass this code.
	 */
	@Override public final boolean fetchNextRow( DataValueDescriptor[] logicalTableRow ) throws Exception {
		
//		if ( false == isVTIRowTypesInitialised ) {
			if ( 1 > queriedColumnsCount ) {
				// Either: queriedColumnsCount was set to 0 to short-cut query execution (e.g. if qualifiers resolved to false or projectedColumns resolved to nothing)
				if ( null != vtiRow ) return false;
				// Or: setExtractConditions() was not called (e.g. calling code used getAllRows()) -> just initialise queried columns with no qualifiers or projection.
				initialiseQueriedColumns();	
			}
			
//			// Initialise vtiRow cells involved in query to match the mapped logical table cell types.
//	    	for (int i=0; i<queriedColumnsCount; i++) {
//	    		
//	    		// We are populating all columns involved in the query, including projected columns list and columns referenced in the qualifiers.
//	    		int lcolID = allLogicalColsInvolvedInQuery[i];
//	    		int pcolID = columnsMappingToPhysicalOnes[ lcolID ];
//	    		
//	    		if ( vtiRow.length <= pcolID ) continue; // column does not exist in this VTI.
//	    		
//	    		// Create a new cell for this column in the first vtiRow
//	    		vtiRow[pcolID] = logicalTableRow[lcolID].getNewNull();
//	    	}
//	    	isVTIRowTypesInitialised = true;
//		}
		
		if ( SCAN_COMPLETED == nextRow( vtiRow ) ) {
			//Log any errors raised by the processing of rows.
			Iterator<String> errorIterator = errors.getErrorMessageIterator();
			while (errorIterator.hasNext()){
				logger.logWarning(GDBMessages.DSWRAPPER_VALUE_CONVERSION_ERROR, "Unable to set cell value from the vtiRow, the value is set to Null. "+errorIterator.next());
			}
			errors = new ErrorBuffer();
			return false; // Qualifiers *must* be processed by underlying VTI
		}
		
		// Copy the column cells populated in the VTI to the mapped logical table column cells.
    	for (int i=0; i<queriedColumnsCount; i++) {
    		
    		// We are populating all columns involved in the query, including projected columns list and columns referenced in the qualifiers.
    		int lcolID = allLogicalColsInvolvedInQuery[i];
    		int pcolID = columnsMappingToPhysicalOnes[ lcolID ];
    		
    		// Check if column is not in the VTI -
    		// This happens when the physical column exists on other physical sources but not this one. We just return a NULL value for those columns.
			// Note that we don't distinguish between null values and non-existant columns... if the column does not exist we just consider it to have a NULL value.
    		if ( vtiRow.length <= pcolID ) {
    			logicalTableRow[lcolID].setToNull();
    			continue;
    		}
    		
    		// Set the logical table cell to the value from the physical table.
    		try {
    			logicalTableRow[lcolID].setValue( vtiRow[pcolID] );
    		} catch (Exception e) {
    			errors.add("Logical Table column id: " + lcolID + ", type: " + logicalTableRow[lcolID].getTypeName() + ", " +
						"Physical column id: " + pcolID + ", " +
						"vtiRow Value: " + vtiRow[pcolID].getString()+". " + e.getMessage());
				logicalTableRow[lcolID].setToNull();
			}
    	}
    	
    	if (isBeforeFirst) isBeforeFirst = false;
    	return true;
	}
	
	private boolean isBeforeFirst = true;

	/**
	 * Indicates the columns that must be returned by a read-write VTI's ResultSet.
	 * This method is called only during the runtime execution of the VTI, after it has been constructed and before the executeQuery() method is called.
	 * At compile time the VTI needs to describe the complete set of columns it can return.
	 * The column identifiers contained in projectedColumns map to the columns described by the VTI's PreparedStatement's ResultSetMetaData.
	 * The ResultSet returned by PreparedStatement.executeQuery() must contain these columns in the order given. Column 1 in this ResultSet
	 * maps the the column of the VTI identified by projectedColumns[0], column 2 maps to projectedColumns[1] etc. Any additional columns
	 * contained in the ResultSet are ignored by the database engine. The ResultSetMetaData returned by ResultSet.getMetaData() must match the ResultSet.
	 * PreparedStatement's ResultSetMetaData column list {"id", "desc", "price", "tax", "brand"}
	 * projectedColumns = { 2, 3, 5}
	 * results in a ResultSet containing at least these 3 columns {"desc", "price", "brand"}
	 * The JDBC column numbering scheme (1 based) is used for projectedColumns.
	 * 
	 * @throws java.sql.SQLException - Error processing the request.
	 * @return true if the upper layer should subsequently only call ResultSet getXXX() methods for the projected columns. false otherwise.
	 * NOTE! ==> The return value is ignored by Derby if the VTI implements IFastPath (because it will by-pass the ResultSet getXXX() methods).
	 */
	@Override public boolean pushProjection(VTIEnvironment vtiEnvironment, int[] projectedColumns) throws SQLException { return false; } // return value ignored
	
	/**
	 * Override this method to obtain the filter predicates applied to your VTI.
	 * You may also call this method using super.setQualifiers().
	 * In this case, AbstractVTI will also test/apply the predicates itself, thus discarding records that don't pass the tests when fetching them.
	 * 
	 * FYI
	 * Qualifiers are filter predicates for the query.
	 * Each Qualifier object holds a comparison expression, containing:
	 * 	A column index,
	 * 	A comparison operator (<, >, <=, >=, =)
	 * 	A constant value
	 * 	Three flags for handling negation and null values.
	 * 		-> When negateCompareResult is true, the result of the comparison is negated.
	 * 		-> When orderedNulls is true, the comparator will treat nulls as 'less than' anything else.
	 * 		-> When orderedNulls is false, the value of unknowRV (true or false) is used as comparison result.
	 * The double array qual[][] holds all ANDed and ORed expressions in conjunctive normal form, such that:
	 * 	- The first row holds all ANDed expressions
	 * 	- All subsequent rows hold ORed expressions
	 * 	- All rows are globally ANDed together.
	 */
	@Override public abstract void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException;
	
	@Override public abstract boolean executeAsFastPath() throws StandardException, SQLException;
	
	// WARNING: This method may be overridden - e.g. in GaianTable
	@Override public GaianResultSetMetaData getMetaData() throws SQLException {
		if (null==grsmd) grsmd = getCacheTableMetaData();
		
//		logger.logInfo("AbstractVTI getMetaData() returning: " + grsmd);
		return grsmd;
	}

	private GaianResultSetMetaData crsmd = null;	
	private GaianResultSetMetaData getCacheTableMetaData() throws SQLException {
		if (null==crsmd)
			try {
				String tableSchema = getVTIPropertyNullable(PROP_SCHEMA);
				if ( null == tableSchema && null != gaianTableQueryContextArguments )
					for ( int i=0; i<gaianTableQueryContextArguments.length; i++ ) {
						String qryContextArg = gaianTableQueryContextArguments[i];
						if ( null != qryContextArg && qryContextArg.startsWith( VTIBasic.DS_WRAPPER_DEFAULT_SCHEMA + '=' ) )
							tableSchema = qryContextArg.substring( VTIBasic.DS_WRAPPER_DEFAULT_SCHEMA.length() + 1 );
					}
					
				crsmd = new GaianResultSetMetaData( tableSchema );
			} catch (Exception e) {
				String msg = "Unable to resolve VTI metadata defintion: " + e;
				logger.logException(GDBMessages.DSWRAPPER_METADATA_RESOLVE_ERROR, msg, e);
				throw new SQLException(msg);
			}
		
//		logger.logInfo("AbstractVTI getCacheTableMetaData() returning: " + crsmd);
		return crsmd;
	}
	
	public GaianResultSetMetaData getTableMetaData() throws SQLException { return getMetaData(); } // Overridden in LiteGaianStatement
	
	void setPrefix( String prefix ) { this.prefix = prefix; }
	public String getPrefix() { return prefix; }
	
	// Note that calling close() within this class will prob call a subclass close(), best use closeCacheStatementsAndReleaseConnections()...
	@Override public void close() throws SQLException {
		closeCacheStatementsAndReleaseConnections();
//		reinitialise();
	}
	
	/**
	 * Stub method which disallows re-initialisation/re-execution of the GaianChildVTI.
	 * Override this method to take advantage of pooling/re-cycling/re-execution capability.
	 * @return "true" if the VTI class has been reinitialised and is able to be reused
	 * 	       "false" if the VTI class cannot be reused. 
	 */
	@Override public boolean reinitialise() throws Exception { /*isVTIRowTypesInitialised = false;*/ return false; } // cannot re-execute this GaianChildVTI
	
	protected void closeCacheStatementsAndReleaseConnections() throws SQLException {
		if ( null != underlyingResultSet ) {
			logger.logInfo("AbstractVTI.cleanup() - recycling connection and clearing resultRows and underlyingResultSet");
			Statement s = underlyingResultSet.getStatement();
			Connection c = s.getConnection();
			resultRows.close();
			resultRows = null;
			underlyingResultSet = null;
			s.close();
			recyclePooledLocalDerbyConnection( c );
		}
		if ( null != preparedInsertStatement ) {
			Connection c = preparedInsertStatement.getConnection();
			preparedInsertStatement.close(); preparedInsertStatement = null;
			recyclePooledLocalDerbyConnection( c );
		}
	}
	
	@Override public void rowsDone() throws StandardException, SQLException {
		closeCacheStatementsAndReleaseConnections();
	}
	
	@Override public void currentRow(ResultSet arg0, DataValueDescriptor[] arg1) throws StandardException, SQLException {}
	
	@Override
	public int getResultSetType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}
	
	@Override
	public int getResultSetConcurrency() throws SQLException {
		return ResultSet.CONCUR_UPDATABLE;
	}
	
	public void setExtension(String extension) {
		this.extension = extension;
	}
}
