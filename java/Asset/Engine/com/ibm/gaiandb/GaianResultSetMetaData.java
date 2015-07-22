/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.VTIMetaDataTemplate;

import com.ibm.gaiandb.DataSourcesManager.RDBProvider;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */

public class GaianResultSetMetaData extends VTIMetaDataTemplate implements Cloneable
{
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "GaianResultSetMetaData", 35 );
	    
    private final int numberOfColumns;
    private final int numberOfPhysicalColumns; 	// Number of non-constant and non-hidden columns. These are "real" leaf data columns
    
    private int numberOfExposedColumns = 0; 	// This number may or may not include the provenance columns
    
    // Columns beyond the exposed columns - these don't exist here but we pretend that they do and leave them as NULL
    // if they are queried. This allows a node to return partial data if its table hasn't been synched up with the rest of the network yet.
    private int numberOfNullColumns = 0;
    
//    private final int numberOfHiddenColumns; 	// The provenance columns
//    private int numberOfConstantColumns = 0;	// Number of Node or Logical Table constant columns, used for routing queries.
    
//    private String originalPhysicalColsDef = null;
//    private String originalSpecialColsDef = null;
    
    private final String columnNames[];
    private final String columnDescriptions[];
    private final int columnWidths[];
    private final int columnTypes[];
    private final int columnPrecisions[];
    private final int columnScales[];

    private final String columnTableNames[];
    private final String columnSchemaNames[];
    
    private RDBProvider provider = RDBProvider.Derby;
    
    // Hashtable of: column name -> column type definition.
    // These are used to facilitate the lookup of columns and the checking of their types against propagated table defs.
    private ConcurrentMap<String, int[]> colNameTypeMappings = new ConcurrentHashMap<String, int[]>();
    
    // The list of columns (stored as "<name> <type>") that do not have a definition here but exist in other definitions for this table
    // around the network. When a propagated definition that contains such a column reaches us, the col name is added to this vector.
    private final ArrayList<String> nullColDefs = new ArrayList<String>();
    
    // Derby defaults for DECIIMAL/NUMERIC/FLOAT types
    private static final int DEFAULT_FLOAT_PRECISION = 53;
    private static final int DEFAULT_DECIMAL_PRECISION = 5;
    private static final int DEFAULT_DECIMAL_SCALE = 0;
        
    private DataValueDescriptor[] rowTemplate = null;

    // Mapping of colName+jdbcColTypeID -> constant col value (as String)
    // Used to set local constant values when meta data is constructed from propagated table def
    private ConcurrentMap<String,String> localConstantsValues = null;
    
    // Pattern used to distinguish ordinary identifiers from identifiers needing to be delimited with double quotes
//	private static final Pattern ordinaryIdentifierPattern = Pattern.compile("[a-zA-Z]\\w*");
	private static final Pattern ordinaryIdentifierPatternFoldedToUpperCase = Pattern.compile("[A-Z][A-Z_0-9]*");
	
	public static String wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier( final String colName ) {
		return wrapColumnNameForQueryingIfNotAnOrdinaryIdentifier( colName, RDBProvider.Derby );
	}
	
	public static String wrapColumnNameForQueryingIfNotAnOrdinaryIdentifier( final String colName, final RDBProvider rdbmsProvider ) {
		if ( RDBProvider.Hive == rdbmsProvider ) return colName;
		char endDelimiterChar = RDBProvider.MySQL == rdbmsProvider ? '`' : RDBProvider.MSSQLServer == rdbmsProvider ? ']' : '"';
		// Wrap delimited identifiers (i.e. non-ordinary ones) in delimiters and escape inner ones.
		return ordinaryIdentifierPatternFoldedToUpperCase.matcher(colName).matches() ? colName :
			(RDBProvider.MSSQLServer == rdbmsProvider ? '[' : endDelimiterChar) +
			Util.escapeCharactersByDoublingThem(colName, endDelimiterChar) + endDelimiterChar;
    }
    
	@Override public int getColumnCount() {
        return numberOfExposedColumns + numberOfNullColumns;
    }
    
    private String getNullColName(int nullColsIndex) {
    	String ndef = nullColDefs.get(nullColsIndex);
    	return ndef.substring(0, ndef.indexOf(' '));
    }
    
    // Method used to extract a null column from beyond the range of normally accessible columns.. this position may change
    // based on whether provenance and/or explain columns are exposed for a given query, so we need to skip more or less of 
    // these cols to get to the null ones.
    // This is therefore needed because even though the metadata is cloned in GaianTable to add the null columns,
    // the meta-data in VTIRDBResult has the old value for numExposedColumns.
    // (this is the correct base level value but is different for every fwded qry's table def that has extra cols)
    public String getColumnName(int i, int exposedColCount) {
    	try {
    		if (logger.logLevel == Logger.LOG_ALL) logger.logDetail("getColumnName " + i + ", exposedColCount " + exposedColCount);
	    	if ( exposedColCount >= i ) return columnNames[i-1];
	    	else return getNullColName(i-1-exposedColCount);
    	} catch (ArrayIndexOutOfBoundsException e) {
    		logger.logException(GDBMessages.RESULT_GET_COL_NAME_EXPOSED_ERROR, "exposedColCount: " + exposedColCount + ", i: " + i, e);
    		throw e;
    	}
    }

    @Override public String getColumnName(int i) {
    	try {
    		if (logger.logLevel == Logger.LOG_ALL) logger.logDetail("getColumnName " + i + ", numberOfExposedColumns " + numberOfExposedColumns);
	    	if ( numberOfExposedColumns >= i ) return columnNames[i-1];
	    	else return getNullColName(i-1-numberOfExposedColumns);
    	} catch (ArrayIndexOutOfBoundsException e) {
    		logger.logException(GDBMessages.RESULT_GET_COL_NAME_ERROR, "numberOfExposedColumns: " + numberOfExposedColumns + ", i: " + i, e);
    		throw e;
    	}
    }

    @Override public int getColumnType(int i) {
    	try {
	    	if ( numberOfExposedColumns >= i ) return columnTypes[i-1];
	    	else return ((int[])colNameTypeMappings.get( getNullColName(i-1-numberOfExposedColumns) ))[0];
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.logException(GDBMessages.RESULT_GET_COL_TYPE_ERROR, "numberOfExposedColumns: " + numberOfExposedColumns + ", i: " + i, e);
			throw e;
		}
    }

    @Override public int getColumnDisplaySize(int i) {
    	try {
	        if(columnWidths == null)
	            return 0x7fffffff;
	        else
	        	if ( numberOfExposedColumns >= i ) return columnWidths[i-1];
	        	else return ((int[])colNameTypeMappings.get( getNullColName(i-1-numberOfExposedColumns) ))[1];
    	} catch (ArrayIndexOutOfBoundsException e) {
    		logger.logException(GDBMessages.RESULT_GET_COL_DISPLAY_SIZE_ERROR, "numberOfExposedColumns: " + numberOfExposedColumns + ", i: " + i, e);
    		throw e;
    	}
    }
    
    @Override public int getPrecision(int i) {
    	try {
	    	if ( numberOfExposedColumns >= i ) return columnPrecisions[i-1];
	    	else return ((int[])colNameTypeMappings.get( getNullColName(i-1-numberOfExposedColumns) ))[2];
    	} catch (ArrayIndexOutOfBoundsException e) {
    		logger.logException(GDBMessages.RESULT_GET_PRECISION_ERROR, "numberOfExposedColumns: " + numberOfExposedColumns + ", i: " + i, e);
    		throw e;
    	}
    }
    
    @Override public int getScale(int i) {
    	try {
	    	if ( numberOfExposedColumns >= i ) return columnScales[i-1];
	    	else return ((int[])colNameTypeMappings.get( getNullColName(i-1-numberOfExposedColumns) ))[3];
    	} catch (ArrayIndexOutOfBoundsException e) {
    		logger.logException(GDBMessages.RESULT_GET_SCALE_ERROR, "numberOfExposedColumns: " + numberOfExposedColumns + ", i: " + i, e);
    		throw e;
    	}
    }

    @Override public String getTableName(int i) { return -1 < i && i < columnTableNames.length ? columnTableNames[i-1] : null; }
    @Override public String getSchemaName(int i) { return -1 < i && i < columnTableNames.length ? columnSchemaNames[i-1] : null; }
    
	/**
	 * Return the column index of the specified column name (1-based), or -1 if not found.
	 * 
	 * @param columnName
	 * @return
	 */
	public int getColumnPosition(String columnName) {
		for (int i = 0; i < columnNames.length; i++) {
			if (columnNames[i].equalsIgnoreCase(columnName)) {
				return i+1;
			}
		}
		return -1;
	}
	
	public String getColumnDescription(int i) {
		return columnDescriptions[i-1];
	}
    
    public String getColumnTypeDescriptionGaianDB(int i) {
    	String descr = columnDescriptions[i-1];
    	return descr.substring( descr.indexOf(' ')+1 );
    }

    public int isNullable(int i) {
        return ResultSetMetaData.columnNullable; //columnNullableUnknown;
    }    
    
    public int getExposedColumnCount() {
    	return numberOfExposedColumns;
    }
    
    public int getPhysicalColumnCount() {
    	return numberOfPhysicalColumns;
    }
    
//    public int getConstantColumnCount() {
//    	return numberOfConstantColumns;
//    }
    
    private String getColumnsDefinition( int numCols, boolean trimConstantValues ) {
    
    	StringBuffer colDefs = new StringBuffer();
    	if ( 0<numCols ) colDefs.append( trimConstantValues ? trimConstantValueFromTypeDescription( columnDescriptions[0] ) : columnDescriptions[0] );
    	for ( int i=1; i<numCols; i++ )
    		colDefs.append( ", " + ( trimConstantValues ? trimConstantValueFromTypeDescription( columnDescriptions[i] ) : columnDescriptions[i] ) );
    	return colDefs.toString();
    }
    
    // The full table definition
    public String getColumnsDefinition() {
    	return getColumnsDefinition(numberOfColumns, false);
    }
    
    public String getColumnsDefinitionExcludingHiddenOnes() {
    	return getColumnsDefinition(numberOfColumns - GaianDBConfig.NUM_HIDDEN_COLS, false);
    }

    public String getColumnsDefinitionExcludingConstantValues() {
    	return getColumnsDefinition(numberOfColumns, true);
    }
    
    // This is the definition we show users from listlts() for example - no need for constant values here
    public String getColumnsDefinitionExcludingHiddenOnesAndConstantValues() {
    	return getColumnsDefinition(numberOfColumns - GaianDBConfig.NUM_HIDDEN_COLS, true);
    }
    
    // This is the definition given to Derby based on the cols involved in a query
    public String getColumnsDefinitionForExposedColumns() {
    	StringBuffer colDefs = new StringBuffer();
    	if ( 0 < numberOfExposedColumns ) colDefs.append(columnDescriptions[0]);
    	for ( int i=1; i<numberOfExposedColumns; i++ ) colDefs.append(", " + columnDescriptions[i]);
    	
    	if ( 0 < numberOfNullColumns ) {
    		int i = 0;
	    	if ( 0 == numberOfExposedColumns ) colDefs.append( nullColDefs.get(i++) );
	    	for ( ; i<numberOfNullColumns; i++ ) colDefs.append(", " + nullColDefs.get(i));
    	}
    	return colDefs.toString();
    }
    
//    // This is the definition for all columns that are populated - i.e. that also need caching - NO: must cache nulls too :(
//    public String getColumnsDefinitionForExposedColumnsExceptNulls() {
//    	StringBuffer colDefs = new StringBuffer();
//    	if ( 0 < numberOfExposedColumns ) colDefs.append(columnDescriptions[0]);
//    	for ( int i=1; i<numberOfExposedColumns; i++ ) colDefs.append(", " + columnDescriptions[i]);
//    	return colDefs.toString();
//    }
    
    public String getColumnNames() {
    	StringBuffer cols = new StringBuffer();
    	if ( 0 < numberOfExposedColumns ) cols.append(columnNames[0]);
    	for ( int i=1; i<numberOfExposedColumns; i++ ) cols.append(", " + columnNames[i]);    	
    	return cols.toString();
    }
    
    private static String trimConstantValueFromTypeDescription( String td ) {
    	int li = td.lastIndexOf(' ');
    	return td.indexOf(' ') != li ? td.substring(0, li) : td;
    }
    
//    public String getDefinitionForVisibleColumnsIn( HashSet colIDs ) {
//    	
//    	int numberOfVisibleColumns = numberOfColumns - GaianDBConfig.PROVENANCE_COLS.length - GaianDBConfig.EXPLAIN_COLS.length;
//    	
//    	StringBuffer colDefs = new StringBuffer();
//    	    	    	
//    	Iterator i = colIDs.iterator();
//    	if ( i.hasNext() ) {
//    		int colID = ((Integer) i.next()).intValue(); // 0-based
//    		if ( colID < numberOfVisibleColumns )
//    			colDefs.append( columnDescriptions[ colID ] );
//    	}    		
//    	
//    	while (i.hasNext()) {
//    		int colID = ((Integer) i.next()).intValue(); // 0-based
//    		if ( colID < numberOfVisibleColumns )
//    			colDefs.append( ", " + columnDescriptions[ colID ] );
//    	}
//
//    	return colDefs.toString();
//    }

//    public GaianResultSetMetaData(int numCols, String colNames[], int colWidths[], int colTypes[])
//    {
//        numberOfColumns = numCols;
//        columnNames = colNames;
//        columnWidths = colWidths;
//        columnTypes = colTypes;
//    }
    
//    public void setRowTemplate( DataValueDescriptor[] row ) {
//    	rowTemplate = row;
//    }
    
    public DataValueDescriptor[] getRowTemplate() {
    	return rowTemplate;
    }
    
    // boolean that tells us if the hidden provenance and explain cols were never actually included in the definition
    private boolean isTailColumnsInDefinition = true;
    public void excludeTailColumns( int numCols ) {
    	if ( isTailColumnsInDefinition ) {
	    	// Ensure number of exposed cols is never less than 0.. which may be the case if a null md was created...
	    	numberOfExposedColumns = Math.max( 0, numberOfColumns - numCols );
    	}
    }
    
    public void includeNullColumns() {
    	numberOfNullColumns = null != nullColDefs ? nullColDefs.size() : 0;
    }
    
    public void excludeNullColumns() {
    	numberOfNullColumns = 0;
    }
    
    public GaianResultSetMetaData() throws Exception {
    	this( null, null, (String)null ); // have to cast (String so it picks the right constructor and not the one below
    }
    
    public GaianResultSetMetaData( ResultSetMetaData rsmd, String specialColsDef ) throws Exception {
    	this(rsmd, specialColsDef, RDBProvider.Derby);
    }

	public GaianResultSetMetaData( ResultSetMetaData rsmd, String specialColsDef, RDBProvider provider ) throws Exception {
    	
    	if ( null == rsmd ) numberOfPhysicalColumns = 0;
    	else numberOfPhysicalColumns = rsmd.getColumnCount();
    	
    	if ( null == specialColsDef ) specialColsDef = "";
    	String[] specialCols = GaianDBConfig.getColumnsDefArray( specialColsDef );
//    	if ( null == specialCols ) specialCols = new String[0];
    	    	    	
//    	String[] hiddenCols = GaianDBConfig.PROVENANCE_COLS;
//    	numberOfHiddenColumns = hiddenCols.length;
    	
    	this.provider = provider;
    	
        numberOfColumns = numberOfPhysicalColumns + specialCols.length;
        numberOfExposedColumns = numberOfColumns; // this is set properly later upon GaianTable construction
        
    	logger.logInfo("Meta data column counts: physical cols: " + numberOfPhysicalColumns +
    			", special cols: " + specialCols.length);
        
        columnNames = new String[ numberOfColumns ];
        columnTypes = new int[ numberOfColumns ];
        columnWidths = new int[ numberOfColumns ];
        columnPrecisions = new int[ numberOfColumns ];
        columnScales = new int[ numberOfColumns ];
        columnDescriptions = new String[ numberOfColumns ];
        columnTableNames = new String[ numberOfColumns ];
        columnSchemaNames = new String[ numberOfColumns ];
    	
    	for ( int i=0; i<numberOfPhysicalColumns; i++ ) {
    		
    		int type = rsmd.getColumnType(i+1);

        	columnTypes[i] = type;
        	columnNames[i] = rsmd.getColumnName(i+1);
        	columnWidths[i] = rsmd.getColumnDisplaySize(i+1);
        	try { columnTableNames[i] = rsmd.getTableName(i+1); } catch( SQLException e ) {} // ignore - could be a FileImportMetaData
        	try { columnSchemaNames[i] = rsmd.getSchemaName(i+1); } catch( SQLException e ) {} // ignore - could be a FileImportMetaData
        	
        	String typeSize = "";
        	switch ( type ) {

				case Types.CHAR: case Types.BINARY: case Types.VARCHAR: case Types.VARBINARY: case Types.CLOB: case Types.BLOB:
				case Types.NCHAR: case Types.NVARCHAR: case Types.NCLOB: // double the sizes for these? (because we downcast them..)
					typeSize = "(" + columnWidths[i] + ")"; break;
				
				case Types.DECIMAL: case Types.NUMERIC:
					// Precision is the total number of stored digits (before and after the decimal point)
					// Scale is the number of digits AFTER the decimal point. The number of digits BEFORE is therefore precision-scale
					// In Oracle, scale can also be negative - which is a left-shift value for which all digits BEFORE the decimal point are 0.
					// e.g. In Oracle, A NUMERIC(5,-2) column type will have value 12345 inserted as 12300. Derby does not support this.
					
					int precision = columnPrecisions[i] = rsmd.getPrecision(i+1);
					int scale = columnScales[i] = rsmd.getScale(i+1);
					
					if ( RDBProvider.Oracle.equals(this.provider) && -127 == scale ) {
						// FLOAT, REAL or DOUBLE - exposed by Oracle driver as NUMERIC(<binary_precision>, <not_used>)
						columnTypes[i] = Types.FLOAT; // override the given type (so we also ignore precision and scale)
						break; // Do not set typeSize - ignore provider size constraints - it is the provider's job to enforce these.
					}
					
					if ( 31 < precision ) columnPrecisions[i] = 31; // truncate to maximum
					
					if ( precision < scale || 0 > scale ) {
						
						columnScales[i] = DEFAULT_DECIMAL_SCALE;
						if ( columnPrecisions[i] < columnScales[i] ) columnPrecisions[i] = DEFAULT_DECIMAL_PRECISION;
						
						logger.logWarning(GDBMessages.RESULT_PROVIDER_VALUES_INVALID, "Invalid RDBMS values for precision and scale. "
								+ provider + " column: " + columnNames[i] + ' ' + rsmd.getColumnTypeName(i+1)
								+ "("+precision+","+scale+"): Derby requirements are: precision >= scale >= 0;"
								+ "Using values: ("+columnPrecisions[i]+","+columnScales[i]+")");
					}
					
					typeSize = "(" + columnPrecisions[i] + ", " + columnScales[i] + ")";
					break;
					
				case Types.FLOAT:
					columnPrecisions[i] = rsmd.getPrecision(i+1);
					typeSize = "(" + columnPrecisions[i] + ")"; break;
        	}
        	columnDescriptions[i] =
        		columnNames[i] + " " +
        		lookupColumnTypeNameGaianDB(columnTypes[i], this.provider) + typeSize; // rsmd.getColumnTypeName(i+1) + typeSize;
        	
        	logger.logDetail("Set colDescription["+i+"]: " + columnDescriptions[i]);
    	}
    	
    	setupColumns( new String[numberOfPhysicalColumns], specialCols, numberOfPhysicalColumns );
		buildRowTemplate( specialCols );
		addQryNullColumns();
    	
    	logger.logInfo("Retrieved meta data for columns: " + Arrays.asList(columnNames));
    }

    // Used for cases where the result HAD to be computed in order to obtain the ResultSetMetaData - e.g. for procedure calls
    private ResultSet retainedResultSet = null; // Only valid once, when we need to get that initial meta-data
    private int retainedUpdateCount = -2; // When set, this is only valid once
    private String dataSourceWrapperRetainingResult = null;
    
    // Parent connection acting as container when running a stored procedure.
    // This is the one that needs recycling... NOT the 'default' connection from which the resultSet actually comes...
    private Connection parentCalledProcedureConnection = null;
    
    public GaianResultSetMetaData( ResultSet rs, String dsWrapperID, boolean excludeTailColumns, Connection parentCalledProcedureConnection ) throws Exception {
    	this( rs.getMetaData(), excludeTailColumns ? "" : GaianDBConfig.getSpecialColumnsDef(null) );
    	retainedResultSet = rs;
    	isTailColumnsInDefinition = !excludeTailColumns;
    	this.parentCalledProcedureConnection = parentCalledProcedureConnection;
    	dataSourceWrapperRetainingResult = dsWrapperID;
    }
    
    public GaianResultSetMetaData( final int updateCountToRetain, String dsWrapperID ) throws Exception {
    	this("UPDATE_COUNT INT", GaianDBConfig.getSpecialColumnsDef(null));
    	retainedUpdateCount = updateCountToRetain;
        dataSourceWrapperRetainingResult = dsWrapperID;
    }
    
    public ResultSet getRetainedResultSet(String dsWrapperID) {
    	if ( dsWrapperID.equals(dataSourceWrapperRetainingResult) ) {
	    	boolean isSet = null != retainedResultSet;
	    	logger.logThreadInfo("Looking up retained result set.. isSet? " + isSet);
	    	if ( isSet ) {
	    		ResultSet rs = retainedResultSet;
	    		retainedResultSet = null; // invalidate this once it has been retrieved
	    		dataSourceWrapperRetainingResult = null;
	    		return rs;
	    	}
    	}
    	return null;
    }
    
    public Connection getParentCalledProcedureConnection(String dsWrapperID) {
    	if ( dsWrapperID.equals(dataSourceWrapperRetainingResult) ) {
	    	boolean isSet = null != parentCalledProcedureConnection;
	    	logger.logThreadInfo("Looking up parentCalledProcedureConnection.. isSet? " + isSet);
	    	if ( isSet ) {
	    		Connection c = parentCalledProcedureConnection;
	    		parentCalledProcedureConnection = null; // invalidate this once it has been retrieved
	    		dataSourceWrapperRetainingResult = null;
	    		return c;
	    	}
    	}
    	return null;
    }
    
    public int getRetainedUpdateCount(String dsWrapperID) {
    	if ( dsWrapperID.equals(dataSourceWrapperRetainingResult) ) {
	    	boolean isSet = -2 != retainedUpdateCount;
	    	logger.logThreadInfo("Looking up retained update count.. isSet? " + (-2 != retainedUpdateCount));
	    	if ( isSet ) {
	    		int rc = retainedUpdateCount;
	    		retainedUpdateCount = -2; // invalidate this once it has been retrieved
	    		dataSourceWrapperRetainingResult = null;
	    		return rc;
	    	}
    	}
    	return -2;
    }
    
    public boolean hasRetainedResult() {
    	return null != dataSourceWrapperRetainingResult; //null != retainedResultSet || -2 != retainedUpdateCount;
    }
    
//    /**
//     * Build the original ResultSetMetaData, including hidden provenance columns by default.
//     * If the query does not request them, then it will call "excludeProvenanceColumns()"
//     * 
//     * @param colDefs
//     * @throws Exception
//     */
//    public GaianResultSetMetaData( String colDefs[] ) throws Exception {
//    	
//    	// Note colDefs include constant columns (and specialCols = constantCols + hiddenCols)
//    	
//    	String[] hiddenCols = GaianDBConfig.PROVENANCE_COLS;
//    	numberOfHiddenColumns = hiddenCols.length;
//    	    	    	
//        numberOfColumns = colDefs.length + numberOfHiddenColumns;
//        numberOfExposedColumns = numberOfColumns;
//        
//        logger.logInfo("Creating columns meta data using colDefs: " + 
//        		Arrays.asList(colDefs) + ", hiddenCols: " + Arrays.asList(hiddenCols));  
//        
//    	logger.logInfo("Column counts: numCols = " + numberOfColumns + 
//    			", physical + constant = " + colDefs.length + ", hidden = " + numberOfHiddenColumns);
//        
//        columnNames = new String[ numberOfColumns ];
//        columnTypes = new int[ numberOfColumns ];
//        columnWidths = new int[ numberOfColumns ];
//        columnDescriptions = new String[ numberOfColumns ];
//        
//        setupColumns( colDefs, hiddenCols, 0 );
//    }    
    
    public GaianResultSetMetaData( String physicalColsDef ) throws Exception {
    	this( physicalColsDef, null, null );
    }
    
    /**
     * Build the original ResultSetMetaData, including hidden provenance columns by default.
     * If the query does not request them, then it will call "excludeProvenanceColumns()"
     * 
     * @param colDefs
     * @throws Exception
     */
//    public GaianResultSetMetaData( String[] physicalCols, String[] specialCols ) throws Exception {
    public GaianResultSetMetaData( String physicalColsDef, String specialColsDef ) throws Exception {
    	this( physicalColsDef, specialColsDef, null );
//    	originalPhysicalColsDef = physicalColsDef;
//    	originalSpecialColsDef = specialColsDef;
    }
    
//    public boolean wasBuiltFrom( String physicalColsDef, String specialColsDef ) {
//    	if ( 	null == specialColsDef && null == physicalColsDef ||
//    			null == specialColsDef ^ null == originalSpecialColsDef ||
//    			null == physicalColsDef ^ null == originalPhysicalColsDef ) return false;
//    	
//    	return ( ( null == physicalColsDef || physicalColsDef.intern() == originalPhysicalColsDef.intern() ) && 
//    			 ( null == specialColsDef || specialColsDef.intern() == originalSpecialColsDef.intern() ) );
//    }
    
    /**
     * Build a meta data object based on definitions for physical columns, special columns, and local constant columns.
     * Physical columns are basic column definitions comprising of a name and type definition (with possible precision value(s)).
     * Special columns *may* include system columns such as PROVENANCE and EXPLAIN columns. It may also contain constant column 
     * definitions which have a 3rd token to specify the value of the constant.
     * The local constant column definitions are only passed in when the constant columns defined in the special columns definitions
     * contain constants for another node or table. These are replaced by the values from the local constant column defs.
     * 
     * @param physicalColsDef
     * @param specialColsDef
     * @param localConstantsDef
     * @throws Exception
     */    
    public GaianResultSetMetaData( String physicalColsDef, String specialColsDef, String localConstantsDef ) throws Exception {
    	    	
    	if ( null == physicalColsDef ) physicalColsDef = "";
    	if ( null == specialColsDef ) specialColsDef = "";
    	    	
    	String[] physicalCols = GaianDBConfig.getColumnsDefArray( physicalColsDef );
    	String[] specialCols = GaianDBConfig.getColumnsDefArray( specialColsDef );
    	
    	if ( null == physicalCols ) physicalCols = new String[0];    	
    	if ( null == specialCols ) specialCols = new String[0];
    	
    	numberOfPhysicalColumns = physicalCols.length;
    	
//    	String[] hiddenCols = GaianDBConfig.PROVENANCE_COLS;    	
//    	numberOfHiddenColumns = hiddenCols.length;
    	
        numberOfColumns = numberOfPhysicalColumns + specialCols.length;
        numberOfExposedColumns = numberOfColumns; // this is set properly later upon GaianTable construction
        
        logger.logInfo("Creating columns meta data using physical cols: " + 
        		Arrays.asList(physicalCols) + ", special cols: " + Arrays.asList(specialCols));
        
        columnNames = new String[ numberOfColumns ];
        columnTypes = new int[ numberOfColumns ];
        columnWidths = new int[ numberOfColumns ];
        columnPrecisions = new int[ numberOfColumns ];
        columnScales = new int[ numberOfColumns ];
        columnDescriptions = new String[ numberOfColumns ];
        columnTableNames = new String[ numberOfColumns ];
        columnSchemaNames = new String[ numberOfColumns ];
        
        setupColumns( physicalCols, specialCols, 0 );
        
        // Now prepare to load constant values for special columns
    	
        // First check if we have locally defined consant values to replace ones that may have 
        // been passed in specialColsDef - this would happen if we are constructing meta data for a propagated table def.        
        if ( null != localConstantsDef ) {

        	// The constant col values *must* be replaced by local ones. If they don't exist locally they will be null
			localConstantsValues = new ConcurrentHashMap<String, String>();
			
	    	String[] localConstantCols = GaianDBConfig.getColumnsDefArray( localConstantsDef );
			
	    	for ( int i=0; i<localConstantCols.length; i++ ) {
	    		
	    		String[] elmts = Util.splitByTrimmedDelimiter( localConstantCols[i], ' ' );
	    		String colName = elmts[0].toUpperCase();
	    		String typeName = elmts[1].toUpperCase();
	    		int typeDetails[] = getColumnTypeDetailsFromStringDef( colName, typeName );
	    		int colType = typeDetails[0];
	    		localConstantsValues.put( colName+colType, elmts[2] );
	    	}
        }

//        System.out.println("LT col type for col 0: " + getColumnTypeNameGaianDB(1));
//        System.out.println("derby col descr for col 0: " + getColumnTypeDescriptionDerby(1));
//        System.out.println("gaiandb col descr for col 0: " + getColumnTypeDescriptionGaianDB(1));
        
    	// Now create constant col DataValueDescriptor values -
    	// When rows are fetched that match this meta-data, they may sometimes refer to the row template
    	// to fill in constant col values on the fly... (see GaianResult.nextFastPathRow())
		buildRowTemplate( specialCols );
		addQryNullColumns();
    }
        
    /**
     * Process all columns defined by GAIANDB, i.e. that have a textual definition that hasn't yet been loaded. 
     * 'fromColumns' is the number of non-GAIANDB columns, which are already processed and can therefore be skipped.
     * 
     * @param colDefs
     * @param extraCols
     * @param fromColumn
     * @throws Exception
     */
    private void setupColumns( String[] colDefs, String[] extraCols, int fromColumn ) throws Exception {
    	
        // A set of the column names - used to prevent duplication - initialise it with all column names that have already been defined.
        HashSet<String> colNamesSet = new HashSet<String>();
        for (int i=0; i<fromColumn; i++)
        	colNamesSet.add( columnNames[i] );
        
        for (int i=fromColumn; i<numberOfColumns; i++) {
        	
        	// Remember this column as it was declared
        	columnDescriptions[i] = ( i<colDefs.length ? colDefs[i] : extraCols[i-colDefs.length] );
        	String[] nameType = GaianDBConfig.getColumnSplitBySpaces( columnDescriptions[i] );
        	        	
        	logger.logDetail("Processing column with info: " + Arrays.asList(nameType));
        	
        	if ( 2 > nameType.length )
        		throw new Exception("Unable to parse column " + (i+1) + " defined as: '" + columnDescriptions[i] + 
        				"': 2 tokens must be specified to identify the name and type of the column");
        	
        	String colName = nameType[0]; //.toUpperCase();
        	String typeName = nameType[1].toUpperCase();
        	
        	if ( colNamesSet.contains( colName ) ) {
//        		if ( Arrays.asList( GaianDBConfig.HIDDEN_COL_NAMES ).contains(colName) ) {
//        			logger.logInfo("Renaming special column: " + colName + " to " + (colName+2));
//        			colName += 2;
//        		} else
        		throw new Exception("Duplicate column name defined: " + colName +
    				". Ensure that the column names do not match one of the hidden provenance columns: "  +
					GaianDBConfig.GDB_NODE + ", " + GaianDBConfig.GDB_LEAF );
        	}
        	
        	colNamesSet.add( colName );
        	
        	int typeDetails[] = getColumnTypeDetailsFromStringDef( colName, typeName );

        	columnNames[i] = colName;
        	columnTypes[i] = typeDetails[0];
        	columnWidths[i] = typeDetails[1];
        	columnPrecisions[i] = typeDetails[2];
        	columnScales[i] = typeDetails[3];

        	columnTableNames[i] = null;
        	columnSchemaNames[i] = GaianDBConfig.getGaianNodeUser();
        	
//        	columnDescriptions[i] = colName + " " + typeName as returned in typeDetails + 
//        		( nameType.length > 2 ? nameType[2] : "" );
        	
        	colNameTypeMappings.put( columnNames[i], typeDetails );
						
//        	if ( nameType.length>2 && "DEFAULT".equalsIgnoreCase(nameType[2]) )
//        		defaultColumnDVDs
        }
    }

    private int[] getColumnTypeDetailsFromStringDef( String colName, String typeName ) throws Exception {
    	
    	int type, width=-1, precision=0, scale=0;
   	
    	// THIS IS THE LIST OF DERBY TYPES SUPPORTED IN GAIANDB LOGICAL TABLE DEFINITIONS - 
    	// WE DO NOT ATTEMPT TO TRANSLATE OTHER LOGICAL TABLE COLUMN TYPES INTO DERBY TYPES (FOR NOW)
    	
    	// NOTE: THESE DERBY TYPE DEFS ARE NOT SUPPORTED - INSTEAD WE USE THE ALTERNATIVES LISTED AGAINST THEM:
    	// LONG VARCHAR													->		LONGVARCHAR
    	// { CHAR | CHARACTER } FOR BIT DATA							->		BINARY
    	// { VARCHAR | CHAR VARYING | CHARACTER VARYING } FOR BIT DATA	->		VARBINARY
    	// LONG VARCHAR FOR BIT DATA									->		LONGVARBINARY

    	// NOTE THAT THE FOLLOWING TYPES *ARE* SUPPORTED EVEN THOUGH THEY DO NOT APPEAR EXPLICITELY BELOW
    	// DOUBLE PRECISION (== DOUBLE)
    	// CHARACTER (== CHAR)
    	// INTEGER (==INT)
    	// DECIMAL (==DEC)
    	
    	// EXTRA TYPES:
    	// BIT
    	// BOOLEAN
    	// TINYINT
    	// NULL
    	
    	if ( typeName.startsWith("CHAR") ) 				type = Types.CHAR;
		else if ( typeName.startsWith("VARCHAR") )	 	type = Types.VARCHAR;
		else if ( typeName.startsWith("DEC") )			type = Types.DECIMAL;
		else if ( typeName.startsWith("NUMERIC") )		type = Types.NUMERIC;
		else if ( typeName.startsWith("FLOAT") )		type = Types.FLOAT;
		else if ( typeName.startsWith("BINARY") )		type = Types.BINARY;
		else if ( typeName.startsWith("VARBINARY") )	type = Types.VARBINARY;
		else if ( "LONGVARCHAR".equals( typeName ) )	type = Types.LONGVARCHAR;
		else if ( "LONGVARBINARY".equals( typeName ) )	type = Types.LONGVARBINARY;
		else if ( "BIT".equals( typeName ) )			type = Types.BIT;
		else if ( "BOOLEAN".equals( typeName ) )		type = Types.BOOLEAN;
		else if ( typeName.startsWith("BLOB") )			type = Types.BLOB;
		else if ( typeName.startsWith("CLOB") )			type = Types.CLOB;
		else if ( "DATE".equals( typeName ) )			type = Types.DATE;
		else if ( "TIME".equals( typeName ) )			type = Types.TIME;
		else if ( "TIMESTAMP".equals( typeName ) )		type = Types.TIMESTAMP;
		else if ( typeName.startsWith("INT") )			type = Types.INTEGER;
		else if ( "BIGINT".equals( typeName ) ) 		type = Types.BIGINT;
		else if ( "SMALLINT".equals( typeName ) )		type = Types.SMALLINT;
		else if ( "TINYINT".equals( typeName ) )		type = Types.TINYINT;
		else if ( "DOUBLE".equals( typeName ) )			type = Types.DOUBLE;
		else if ( "REAL".equals( typeName ) )			type = Types.REAL;
		else if ( "NULL".equals( typeName ) )			type = Types.NULL;
		else throw new Exception("Invalid type definition syntax for column '" + colName + "': " + typeName);
		
    	String lendef[] = typeName.split("[(|)|,]");
    	
    	try {
			switch ( type ) {
			
				case Types.CHAR: case Types.NCHAR: case Types.BINARY:
					width = 2 > lendef.length ? 1 : Integer.parseInt( lendef[1] ); break;
					
				case Types.BLOB: case Types.CLOB: case Types.NCLOB:
					width = 2 * 1024^3; // default size
					
				case Types.VARCHAR: case Types.NVARCHAR: case Types.VARBINARY:
					if ( 2 > lendef.length ) {
						if ( -1 < width ) break; // use default BLOB or CLOB width - no exception.
						String w = "Missing type length definition for column '" + colName + "': " + typeName;
//						logger.logWarning(GDBMessages.RESULT_COLUMN_LENGTH_DEF_MISSING, w); // doesn't help to log this here as we don't know what implications are.
						throw new SQLException(w);
					}
					String wdef = lendef[1];
					int multiplier = 1;
					
					if ( Types.BLOB == type || Types.CLOB == type ) {
						// look to see if we have a K, M or G marker
						int lastCharIdx = wdef.length()-1;
						char lastChar = wdef.charAt(lastCharIdx);
						if ( 'k' == lastChar || 'K' == lastChar ) multiplier = 1024;
						else if ( 'm' == lastChar || 'M' == lastChar ) multiplier = 1024^2;
						else if ( 'g' == lastChar || 'G' == lastChar ) multiplier = 1024^3;
						if ( 1 < multiplier ) wdef = wdef.substring(0, lastCharIdx); // adjust wdef if there was a marker
					}

					width = Integer.parseInt( wdef ) * multiplier; // note this includes end of string delimiter, so max chars is 1 less.
					
					// TODO: Check that width does not extend outside of allowed [min, max] range... - also implement truncation in GaianResult.nextFastPathRow()
					
					break;
				
				case Types.LONGVARCHAR: case Types.LONGVARBINARY:
					width = 32702; break; // Max size for Derby LONG VARCHAR + 2 padding
					
				case Types.DECIMAL: case Types.NUMERIC:	
					precision = 2 > lendef.length ? DEFAULT_DECIMAL_PRECISION : Integer.parseInt( lendef[1] );
					scale = 3 > lendef.length ? DEFAULT_DECIMAL_SCALE : Integer.parseInt( lendef[2] );
					width = precision + 3; break; // Add the "." and 2 for padding.
					
				case Types.FLOAT:
					precision = 2 > lendef.length ? DEFAULT_FLOAT_PRECISION : Integer.parseInt( lendef[1] );
					width = precision + 2; break; // 2 for padding.
					
				case Types.BIT: case Types.BOOLEAN:				width = 7;  break; // 5 + 2 padding
				case Types.DATE:								width = 12; break; // 1965-01-01 -> 10 + 2 padding
				case Types.TIME:								width = 12; break; // 23:53:25.0 -> 10 + 2 padding
				case Types.TIMESTAMP:							width = 23; break; // 2006-09-23 23:53:25.0 -> 21 + 2 padding
				case Types.INTEGER:								width = 12; break; // 10 + 2 padding
				case Types.BIGINT:								width = 22; break; // 20 + 2 padding
				case Types.SMALLINT:							width = 7;  break; // 5 + 2 padding
				case Types.TINYINT:								width = 5;  break; // 3 + 2 padding
				case Types.DOUBLE:								width = 22; break; // 22 ???
				case Types.REAL:								width = 22; break; // 22 ???
				case Types.NULL:								width = 6;  break; // 4 + 2 padding
	//			case Types.ARRAY:								width = 22; break; // 22 ???
	//			case Types.JAVA_OBJECT: case Types.STRUCT:		width = 22; break; // 22 ???
	//			case Types.REF: case Types.BLOB: case Types.CLOB: case Types.ARRAY: width = 22; break; // 22 ???
	//			case Types.DATALINK:							width = 22; break; // 22 ???
	//			case Types.REF:									width = 22; break; // 22 ???
	//			case Types.DISTINCT: case Types.OTHER:			width = 22; break; // 22 ??? // No distinct type supported
				default:
					String w = "Unsupported JDBC type: " + type;
					logger.logWarning(GDBMessages.RESULT_JDBC_TYPE_UNSUPPORTED, w);
					throw new SQLException(w);
			}
    	} catch ( NumberFormatException e ) {
    		String w = "Invalid length in column specification for column '" + colName + "': " + typeName + ": " + e;
    		logger.logWarning(GDBMessages.RESULT_COLUMN_LENGTH_INVALID, w); throw new SQLException(w);
    	}
		
		return new int[] { type, width, precision, scale };
    }
    
    public String getColumnTypeNameGaianDB( int i ) throws SQLException {
    	return lookupColumnTypeNameGaianDB( getColumnType(i) );
    }
    
    private static String lookupColumnTypeNameGaianDB( int type ) throws SQLException {
    	return lookupColumnTypeNameGaianDB(type, RDBProvider.Derby);
    }
    
    private static String lookupColumnTypeNameGaianDB( final int type, RDBProvider provider ) throws SQLException {

		logger.logDetail("lookupColumnTypeNameGaianDB(): jdbcType " + type + " RDBMS Provider " + provider);
    	
		switch ( type ) {
			case Types.CHAR:			return "CHAR";
			case Types.VARCHAR:			return "VARCHAR";
			case Types.LONGVARCHAR: 	return "VARCHAR(32672)"; // Converted because comparisons on LVC fail with Derby.
			case Types.LONGVARBINARY:	return "LONGVARBINARY";
			case Types.DECIMAL:			return "DECIMAL";
			case Types.NUMERIC:			return "NUMERIC";
			case Types.FLOAT:			return "FLOAT";
			case Types.BINARY:			return "BINARY";
			case Types.VARBINARY:		return "VARBINARY";
			case Types.BIT:				return "BIT";
			case Types.BOOLEAN:			return "BOOLEAN";
			case Types.BLOB:			return "BLOB";
			case Types.CLOB:			return "CLOB";
			case Types.DATE:			return "DATE";
			case Types.TIME:			return "TIME";
			case Types.TIMESTAMP:		return "TIMESTAMP";
			case Types.INTEGER:			return "INTEGER";
			case Types.BIGINT:			return "BIGINT";
			case Types.SMALLINT:		return "SMALLINT";
			case Types.TINYINT:			return "TINYINT";
			case Types.DOUBLE:			return "DOUBLE";
			case Types.REAL:			return "REAL";
			case Types.NULL:			return "NULL";	
			case Types.NCHAR:			return "CHAR"; // With Derby 10.8: Feature not implemented: NATIONAL CHAR
			case Types.NCLOB:			return "CLOB"; // With Derby 10.8: Feature not implemented: NCLOB
			case Types.NVARCHAR:		return "VARCHAR"; // With Derby 10.8: Feature not implemented: NATIONAL CHAR VARYING 
					
//			case Types.ARRAY:			
//			case Types.JAVA_OBJECT:		
//			case Types.STRUCT:			
//			case Types.REF:						
//			case Types.DATALINK:					
//			case Types.DISTINCT:		
//			case Types.OTHER:			
			default:
				String typeName = "None";

				switch ( provider ) {
					case DB2:
						switch ( type ) {
					    	case Types.OTHER:	return "DECIMAL(31,0)"; // DB2 uses Types.OTHER for DECFLOAT
						}
						break;
					case Oracle:
						// See: http://ss64.com/ora/syntax-datatypes.html
						// See: http://docs.oracle.com/cd/E11882_01/appdev.112/e13995/constant-values.html
						switch ( type ) {
					    	case 101:			return "DOUBLE"; // BINARY_DOUBLE
						    case 100:			return "FLOAT"; // BINARY_FLOAT
						    // we convert those ones
							case -100:					// TIMESTAMPNS - deprecated to TIMESTAMP from V9.2.0
							case -102:  				// TIMESTAMP WITH LOCAL TIME ZONE
							case -101:  		return "TIMESTAMP";	// TIMESTAMP WITH TIME ZONE - normalised using: <TSTZ_COL> AT LOCAL
							case -103:  		return "INTEGER";	// INTERVAL YEAR TO MONTH - normalised to number of months
							case -104:			return "DOUBLE";	// INTERVAL DAY TO SECOND - normalised to seconds with floating point value
						    case Types.ROWID:
						    case Types.OTHER:	return "VARCHAR(4000)"; // Oracle uses Types.OTHER for UROWID
						    // we don't support the following
							case -10:			typeName = "CURSOR"; break;
						}
						break;
					case MSSQLServer:
						switch ( type ) {
							case -16:			return "VARCHAR(32672)"; // XML
						}
						break;
					case Derby:
					case MySQL:
					case Other:	
					default:
						break;
				}
				
				String w = "Unsupported JDBC type from "+provider+": " + type + "\nKnown mapping(s): " + typeName
				+ ".\nPlease review the GaianDB documentation for the list of native RDBMS Unsupported Types.";
				logger.logWarning(GDBMessages.RESULT_LOOKUP_JDBC_TYPE_UNSUPPORTED, w); 
				throw new SQLException(w);
		}
    }
    
    // not used
    public static String lookupColumnTypeNameDerby( int type ) throws SQLException {
    	return morphTypeNameGaian2Derby(lookupColumnTypeNameGaianDB(type), type);
    }

    private static String morphTypeNameGaian2Derby( String gdbd, int type ) {

		switch ( type ) {
			case Types.BINARY:			int idx = gdbd.indexOf('(');
										return "CHAR" + (-1==idx?"":gdbd.substring(idx)) + " FOR BIT DATA";
			// VARBINARY must have a length definition
			case Types.VARBINARY:		return "VARCHAR" + gdbd.substring( gdbd.indexOf('(') ) + " FOR BIT DATA";
			case Types.LONGVARBINARY:	return "LONG VARCHAR FOR BIT DATA";
			case Types.LONGVARCHAR: 	return "LONG VARCHAR";
			default: return gdbd;
		}
    }
    
    public String getColumnTypeDescriptionDerby( int colIdx ) {
    	return morphTypeNameGaian2Derby(getColumnTypeDescriptionGaianDB(colIdx), getColumnType(colIdx));
    }
    
    // This method is only used for building a definition for a physical CACHE table to be created in Derby.
    // For this purpose, this GaianResultSetMetaData should have been created without any hidden, constant or null columns.
    public String getColumnsDefinitionMorphed2DerbySyntax() {
    	StringBuilder colDefs = new StringBuilder();
    	if ( 0<numberOfColumns ) colDefs.append( morphColumnDescription2Derby( 1 ) );
    	for ( int colIdx=2; colIdx < numberOfColumns+1; colIdx++ )
    		colDefs.append( ", " + morphColumnDescription2Derby( colIdx ) );
    	return colDefs.toString();
    }
    
    private String morphColumnDescription2Derby( int colIdx ) {
    	return wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier( getColumnName(colIdx) ) + ' ' + getColumnTypeDescriptionDerby(colIdx);
    }
    
	/**
	 * Set constant values for the row's special columns that will be set.
	 * These may include: GAIAN_NODE, GAIAN_LEAF, or any constant value set in the properties file.
	 * Any of these may be used for routing queries through the network efficiently.
	 * 
	 * The value for GAIAN_LEAF is not known at this stage, as it depends on the propeties of each
	 * child-node as it is being processed. It will be a SQLVarchar with an initial value of null. 
	 * 
	 * @return
	 * @throws Exception
	 */
	private void buildRowTemplate( String[] specialCols ) throws Exception {
				
//    	numberOfPhysicalColumns = numberOfColumns - specialCols.length;
//    	numberOfConstantColumns = numberOfColumns - numberOfPhysicalColumns - numberOfHiddenColumns;
		
		int ltColCount = numberOfColumns;
		int specialColCount = specialCols.length;
		
		logger.logInfo("Building row template from special columns: " + Arrays.asList(specialCols) );    	
		logger.logInfo("ltColCount = " + ltColCount + ", specialColCount (constant + hidden cols) = " + specialColCount);
		
		rowTemplate = new DataValueDescriptor[ ltColCount ]; // each DVD is initialised to null
		
		for ( int i=0; i<ltColCount; i++ )
			if ( null == ( rowTemplate[i] = RowsFilter.constructDVDMatchingJDBCType( columnTypes[i], this.provider ) ) )
				throw new Exception("Type not supported for column: " + columnDescriptions[i]);
		
		for ( int i=0; i<specialColCount; i++ ) {
			
			String colDef = specialCols[i];
			String[] colElmts = GaianDBConfig.getColumnSplitBySpaces( colDef );
			int ltColIndex = ltColCount - specialColCount + i; // 0-based
			int colType = columnTypes[ ltColIndex ];
			
//			if ( null == ( rowTemplate[ltColIndex] = RowsFilter.constructDVDMatchingJDBCType( colType ) ) )
//				throw new Exception("Type not supported for CONSTANT column: " + columnDescriptions[ltColIndex]);
			
			String colName = colElmts[0];
			
			logger.logDetail("Setting DVD for special col: " + colName);
			
			if ( GaianDBConfig.GDB_NODE.equalsIgnoreCase( colName ) ) {
				
				rowTemplate[ltColIndex].setValue( GaianDBConfig.getGaianNodeID() );
				
			} else if ( ! colName.startsWith( GaianDBConfig.GDB_PREFIX ) ) {
				
				// Must be a constant column - expecting constant value, so 3 elements to the column definition
				if ( 3 > colElmts.length )
					throw new Exception("Missing value in definition of CONSTANT column: " + columnDescriptions[ltColIndex]);

				String colValue = null != localConstantsValues ? (String) localConstantsValues.get( colName + colType ) :
					colDef.trim().substring(colName.length()).trim().substring(colElmts[1].length()).trim();
				
				if ( null == localConstantsValues && '"' == colValue.charAt(0) ) {
					
					// This is a double quoted value, so it MUST be delimited by double quotes and we must remove these and inner escape chars too...
					
					if ( 2 > colValue.length() || '"' != colValue.charAt( colValue.length() - 1 ) )
						throw new Exception("Unable to build RSMD definition: Double quoted value in CONSTANT column definition does not end with a quote: " +
							columnDescriptions[ltColIndex]);
					
					colValue = colValue.substring(0, colValue.length()-1);
					
					for ( int k=1; k<colValue.length(); k++ )
						if ( '"' == colValue.charAt(k) && '\\' != colValue.charAt(k-1) )
							throw new Exception("Unable to build RSMD definition: Non-escaped nested quote in CONSTANT column definition (backslash required): " +
								columnDescriptions[ltColIndex]);
					
					colValue = Util.stripEscapeCharacterDownOneNestingLevel( colValue.substring(1) , '\\' );
				}
			
				// colValue might legitimately be null if we are constructing meta data for a propagated table def and 
				// the constant col doesnt exist locally
				if ( null != colValue )
					rowTemplate[ltColIndex].setValue( colValue );
			}
		}
		
		logger.logInfo("Built Logical Table DVD template: " + Arrays.asList(rowTemplate));
	}
	
//	public void addMissingColumnsAsNulls( String referenceTableDef ) throws Exception {
//		
//		String[] refColDefs = GaianDBConfig.getColumnDefArray( referenceTableDef );
//		for (int i=0; i<refColDefs.length; i++) {
//			
//			String[] nameType = GaianDBConfig.getColumnSplitBySpaces( refColDefs[i] );
//			String colName = nameType[0];
//			if ( colNameTypeMappings.containsKey( colName ) )
//				continue;
//
//			// The column from the propagated table def is not defined here - add it to NULL columns
//        	String stype = nameType[1];
//        	        	
//        	int typeDetails[] = getColumnTypeDetailsFromStringDef( colName, stype );
//        	
//        	// Remember this column as a NULL column.
//        	nullColNames.add( colName );
//        	colNameTypeMappings.put( colName, typeDetails );
//		}
//	}
	
	/**
	 * Matching up meta data with a table definition consists in:
	 * 		- Checking that type definitions that exist in both are identical.
	 * 		- Creating NULL columns of the correct type in the meta data for columns that exist in the table definition but not in the meta data.
	 */	
	public boolean matchupWithTableDefinition( String referenceTableDef, boolean fillOutWithNulls ) { //throws Exception {
		
		String[] refColDefs = GaianDBConfig.getColumnsDefArray( referenceTableDef );
		for (int i=0; i<refColDefs.length; i++) {
			
			String refColDef = refColDefs[i];
			logger.logInfo("Matching up column: " + refColDef);
			
			String[] nameType = GaianDBConfig.getColumnSplitBySpaces( refColDef );
			
			// Note nameType may be a constant col too, in which case the constant value will be in nameType[2], 
			// but we don't need to compare this, as it is just the constant value for the node that propagated the query to us.
			String colName = nameType[0];
			String stype = nameType[1];
        	
			int tdRef[];
			try {
				tdRef = getColumnTypeDetailsFromStringDef( colName, stype );
			} catch (Exception e) {
				logger.logException(GDBMessages.RESULT_MATCHUP_ERROR, "Could not match up column defs: ", e);
				return false;
			}
			
			int[] tdLoc = null;
			
			// Establish if we've seen this column as a missing one before.
			// Note that the type may be different if it has changed or if the query is coming from a different node - so we ONLY MATCH THE COLUMN NAME.
			boolean wasNotAlreadyANullColumn = true; //!nullColDefs.contains(refColDef);
			for ( String nullColDef : nullColDefs )
				if ( nullColDef.toUpperCase().startsWith(colName.toUpperCase() + ' ' )) {
					wasNotAlreadyANullColumn = false; break;
				}
			
			if ( null != ( tdLoc = (int[]) colNameTypeMappings.get( colName ) ) && wasNotAlreadyANullColumn ) {
			
				// Check type definition is identical - compare type, size, precision and scale values
				if ( tdLoc[0] != tdRef[0] || tdLoc[1] != tdRef[1] || tdLoc[2] != tdRef[2] || tdLoc[3] != tdRef[3] )
					return false;
				
				// This column matches - check the next one
				continue;
			}

			if ( fillOutWithNulls ) {
				logger.logInfo("Adding NULL column to meta data: " + colName);
				
				// Check if the column already exists as a NULL column - if not add it
				if ( wasNotAlreadyANullColumn )
					nullColDefs.add( refColDef );
				
	        	// Set the column type info
	        	colNameTypeMappings.put( colName, tdRef );
			}
		}

		logger.logInfo("Matched up all columns");
		
		// Do not make Null cols visible - this is controlled form the outside - default behaviour is for them to be hidden
//		numberOfNullColumns = nullColNames.size();
		
		return true;
	}
	
	private void addQryNullColumns() throws Exception {
    	
		String[] qryCols = GaianDBConfig.getColumnsDefArray( GaianDBConfig.QRYID_COLDEFS + ", " + GaianDBConfig.GDB_CREDENTIALS_COLDEF );
		for (int i=0; i<qryCols.length; i++) {
			String colDef = qryCols[i];
			String[] nameType = GaianDBConfig.getColumnSplitBySpaces( colDef );
			
			String colName = nameType[0];
			String stype = nameType[1];
			int tdRef[] = getColumnTypeDetailsFromStringDef( colName, stype );
			
			nullColDefs.add( colDef );		
	    	// Set the column type info
	    	colNameTypeMappings.put( colName, tdRef );
		}
		
		// Do not include these columns by default - they only need including temporarily for Derby's benefit...
//		numberOfNullColumns = nullColNames.size();
    }

//	This method doesn't really work because the numberOfExposedColumns is only changed in a cloned copy of this meta data.
//	GAIANDB does not (currently) modify the original copies of the meta data objects which are held by the VTIWrapper objects...
//
//	public boolean isExplain() {
//		return numberOfColumns == numberOfExposedColumns;
//	}
	
	public void setExplainTemplateColumns( String from, String to, int depth, int precedence ) throws StandardException {
		
		int offset = getExplainColumnsOffset();
		
		rowTemplate[ offset ].setValue( from );
		rowTemplate[ offset+1 ].setValue( to );
		rowTemplate[ offset+2 ].setValue( depth );
		rowTemplate[ offset+3 ].setValue( precedence );
		// Initial explain count is unknown but cannot be left to be null as it gets confused with the other null constant col which is gaian leaf...
//		rowTemplate[ offset+4 ].setValue( -1 );
		
		// Don't log anything here as this method is called by GaianTable (and we don't know if this is a System LOG_EXCLUDE query which we don't want to log)
	}
	
	public int getExplainColumnsOffset() {		
		return numberOfColumns - GaianDBConfig.EXPLAIN_COLS.length;
	}
	
	/**
	 * Returns mapping of all column indices in the given rsmd -> col indices of physical cols in this GaianResultSetMetaData
	 * Columns are matched by name. We assume they all exist and we ignore types.
	 * All column indices in the returned array are relative to 0.
	 */
	public int[] derivePhysicalColumnsMapping( GaianResultSetMetaData fromRSMD ) throws SQLException {
		
		int colCount = fromRSMD.getPhysicalColumnCount(); //getColumnCount();
		int[] mapping = new int[colCount];
		int j;
		
		for ( int i=0; i<colCount; i++ ) {
			String col = fromRSMD.getColumnName(i+1).toUpperCase();
			j=0;
			for ( ; j<numberOfPhysicalColumns; j++ )
				if ( col.equals(getColumnName(j+1)) ) {
					mapping[i] = j;
					break;
				}
			if ( j == numberOfPhysicalColumns ) mapping[i] = -1; // no mapping for this column
		}
		
		return mapping;
	}	
    
    public Object clone() {
        try {
        	// No need to deep clone the Hashtable as it is not used by cloned instances
            return super.clone();
        } catch (CloneNotSupportedException e) {
            logger.logException( GDBMessages.RESULT_CLONE_ERROR, "Error: Unexpected Exception caught whilst cloning meta data: ", e );
        }
        return null;
    }
    
//    public String getColumnNames( int exposedColsCount ) {
//    	StringBuffer cols = new StringBuffer();
//    	if ( 0 < numberOfExposedColumns ) cols.append(columnNames[0]);
//    	for ( int i=1; i<numberOfExposedColumns; i++ ) cols.append(", " + columnNames[i]);    	
//    	return cols.toString();
//    }

    public String getPhysicalOrConstantColumnNames() {
    	StringBuffer cols = new StringBuffer();    	
    	for ( int i=0; i < numberOfColumns - GaianDBConfig.NUM_HIDDEN_COLS ; i++ )
    		cols.append((0==i?"":", ") + ( wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier(columnNames[i]) ) );
    	return cols.toString();
    }
    
    public String getColumnNamesWrappedIfNotOrdinary() {
    	StringBuffer cols = new StringBuffer();
    	if ( 0 < numberOfExposedColumns ) cols.append( wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier(columnNames[0]) );
    	for ( int i=1; i<numberOfExposedColumns; i++ ) cols.append(", " + wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier(columnNames[i]));    	
    	return cols.toString();
    }
    
    public String getColumnNamesIncludingNullOnes( final int exposedColsCount, final boolean doubleQuoteNonOrdinaryIdentifiers ) {
    	
    	final boolean dq = doubleQuoteNonOrdinaryIdentifiers;
    	
    	StringBuffer cols = new StringBuffer();    	
    	for ( int i=0; i<exposedColsCount; i++ )
    		cols.append((0==i?"":", ") + ( dq ? wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier(columnNames[i]) : columnNames[i] ) );
    	
    	for ( int i=0; i<nullColDefs.size(); i++ )
    		cols.append((0==exposedColsCount && 0==i?"":", ") + 
    				( dq ? wrapDerbyColumnNameForQueryingIfNotAnOrdinaryIdentifier(getNullColName(i)) : getNullColName(i) ) );
    	
    	return cols.toString();
    }
    
    // Should only be used for logging...
    public String toString() { return '[' + getColumnNamesIncludingNullOnes(numberOfExposedColumns, false) + ']'; }

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	public boolean isAutoIncrement(int column) throws SQLException { return false; } // Is the column automatically numbered, and thus read-only?
	public boolean isCaseSensitive(int column) throws SQLException { return false; } // Does a column's case matter?
	public boolean isSearchable(int column) throws SQLException{ return true; } // Can the column be used in a WHERE clause?
}
