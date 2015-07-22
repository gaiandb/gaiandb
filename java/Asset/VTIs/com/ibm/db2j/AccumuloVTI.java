/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;
import org.apache.hadoop.io.Text;

import com.ibm.db2j.AbstractVTI;
import com.ibm.gaiandb.GaianDBConfigProcedures;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * This VTI provides access to an Accumulo Database Table.
 * It reads configuration parameters from gaiandb_config.properties (e.g. instance name, zookeeper ips, user, password, table, visibility) - see configuration example below.
 * It supports use of all fields in the Accumulo table structure:
 * 
 * 		Row ID, Column Family, Column Qualifier, Column Visibility, Timestamp, Value
 * 
 * The first 5 fields represent a Key. All fields are byte arrays except for the Timestamp which is a long.
 * Row ID is the equivalent of a "primary key". All Accumulo table rows having a same Row ID represent a "record" returned by this VTI - i.e. a set of related column family values.
 * Column Family effectively holds the column name, and column values returned by this VTI can be configured to contain the Column Qualifier or Value field.
 * Column Visibility holds a logical expression allowing restrictions on cell-level access by a table scan.
 * Timestamp allows ordering by time of the records. If the field is not provided at ingestion-time, it is automatically generated instead.
 * 
 * Configuration example:
 * ======================
 * 
 * # Logical table 'LTBIKES' definition:
 * LTBIKES_DEF=ROWID INT, AREA VARCHAR(50), INSTALLED BOOLEAN, LAT DOUBLE, LOCKED BOOLEAN, LON DOUBLE, NUMBIKES INT, NUMEMPTYDOCKS INT, STATION VARCHAR(50), NOT_FIXED BOOLEAN
 * 
 * # Data-source wrapper 'LTBIKES_DS0' properties - federated under logical table LTBIKES:
 * LTBIKES_DS0_VTI=com.ibm.db2j.AccumuloVTI
 * LTBIKES_DS0_ARGS=AccumuloUserInstance1, mytable qualifiers [vis1&vis2], DERIVE_SCHEMA_FROM_FIRST_ROW
 * LTBIKES_DS0_OPTIONS=MAP_COLUMNS_BY_POSITION
 * 
 * # NOTE: The property ending with "_ARGS" specifies the arguments that are passed to the VTI constructor:
 * #	1) A reference to the static Accumulo connection properties
 * #	2) The physical accumulo table name, an identifier specifying the field to extract for table values (either "values" or "qualifiers"), and a visibility expression
 * #	3) An optional 'DERIVE_SCHEMA_FROM_FIRST_ROW' argument to tell the VTI to derive it's schema based on the first RowID row.
 * #		=> Default would be to use property: LTBIKES_DS0_SCHEMA, or failing that: LT_BIKES_DEF
 * 
 * # Static VTI properties:
 * AccumuloVTI.AccumuloUserInstance1.INSTANCE=instance1
 * AccumuloVTI.AccumuloUserInstance1.ZOOKEEPERS=9.71.39.154
 * AccumuloVTI.AccumuloUserInstance1.USR=root
 * AccumuloVTI.AccumuloUserInstance1.PWD=<pwd>
 * 
 * @author DavidVyvyan
 */

public class AccumuloVTI extends AbstractVTI { //implements PluralizableVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	private static final Logger logger = new Logger( "AccumuloVTI", 30 );
	
	private final static Map<String, ZooKeeperInstance> zooKeeperInstances = new Hashtable<String, ZooKeeperInstance>();
	
	private final Connector accumuloConnector;

	private final String accumuloTable;
	private final boolean isDeriveSchemaFromFirstRow;
	private static final String DERIVE_SCHEMA_FROM_FIRST_ROW = "DERIVE_SCHEMA_FROM_FIRST_ROW";
	
	// Table meta-data, i.e. schema/shape including column indexes, names, types and sizes
	private GaianResultSetMetaData accumuloTableRSMD = null;

	private final Scanner standardScanner;
	private final BatchScanner batchScanner;
	private ScannerBase tableScanner = null; // Can switch between the 2 scanner types above
	
	private List<Range> rowidRangesPredicates = null;
	private List<IteratorSetting> preScanIterators = new ArrayList<IteratorSetting>(); // iterators based on regex filters built from predicates
	
	private Iterator<Map.Entry<Key,Value>> rowScanIterator = null;
	
	private Map.Entry<Key,Value> currentAccumuloRow = null;
	
    private int[] projectedColumns = null;
    private Qualifier[][] qualifiers = null;

	Map<String, Integer> projectedColumnsNameToIndexMap = new HashMap<String, Integer>();
	
	private final boolean isExtractAccumuloColumnQualifiersInPlaceOfValues;
	private final Authorizations tableScannerAuthorizations;
	
	// Fields used for building meta-data
	private static final String ROWID = "ROWID";
	private static final String VC256 = "VARCHAR(256)";
	private static final boolean isRowidInSchema = true; // Include Accumulo RowID as a column in this VTI's table shape?
	private static int rowidColShift = 0;
	
	private static final String PROPERTY_INSTANCE = "INSTANCE";
	private static final String PROPERTY_ZOOKEEPERS = "ZOOKEEPERS";
	private static final String PROPERTY_USR = "USR";
	private static final String PROPERTY_PWD = "PWD";
	
	private int rowCount = 0;
	private int numRowsReceivedFromAccumulo;
	
	private static final String VTI_ARGS_SYNTAX_HELP =
		"AccumuloVTI args syntax (LTX_DSY_ARGS): <vtiPropertiesID>, <accumuloTable [fieldToExtractExpression] [visibilityExpressionInSquareBrackets]>[, "
		+DERIVE_SCHEMA_FROM_FIRST_ROW+"]";
	
	/**
	 * Build an AccumuloVTI based on passed in CSV String holding:
	 *  
	 * 	1) A reference to the static Accumulo connection properties
	 * 	2) The physical accumulo table name, an identifier specifying the field to extract for table values (either "values" or "qualifiers"), and a visibility expression
 	 * 	3) An optional 'DERIVE_SCHEMA_FROM_FIRST_ROW' argument to tell the VTI to derive it's schema based on the first RowID row.
 	 * 		=> Default would be to use property: LTBIKES_DS0_SCHEMA, or failing that: LT_BIKES_DEF
 	 * 
	 * @param vtiArgs
	 * @throws Exception
	 */
	public AccumuloVTI(String vtiArgs) throws Exception {
		super(vtiArgs, "AccumuloVTI");
		
		String[] locatorArgs = replacements.toArray( new String[0] );
		if ( 1 > locatorArgs.length ) throw new Exception("Missing AccumuloVTI table argument: " + vtiArgs + ". " + VTI_ARGS_SYNTAX_HELP);
		
		final String accumuloTableAndConstraints = locatorArgs[0];
		isDeriveSchemaFromFirstRow = 1 < locatorArgs.length && DERIVE_SCHEMA_FROM_FIRST_ROW.equals(locatorArgs[1]);
		
		String[] toks = Util.splitByTrimmedDelimiterNonNestedInSquareBracketsOrDoubleQuotes(accumuloTableAndConstraints, ' ');
		
		accumuloTable = toks[0];
		final boolean isFirstConstraintFieldToExtract = 1 < toks.length && '[' != toks[1].charAt(0);
		final int visFieldIdx = 1 < toks.length && '[' == toks[1].charAt(0) ? 1 : ( 2 < toks.length ? 2 : -1 );
		isExtractAccumuloColumnQualifiersInPlaceOfValues = isFirstConstraintFieldToExtract && toks[1].equalsIgnoreCase("qualifiers");
		
		String visibilityExpression = null; // default null = not specified
		
		if ( 0 < visFieldIdx ) {
			final String vis = toks[visFieldIdx];
			final int vlen = vis.length();
			if ( '[' != vis.charAt(0) || ']' != vis.charAt(vlen-1) )
				throw new Exception("Missing AccumuloVTI table argument: " + vtiArgs + ". " + VTI_ARGS_SYNTAX_HELP);
			visibilityExpression = vis.substring(1, vlen-1);
		}
		
		tableScannerAuthorizations = null == visibilityExpression ? new Authorizations() : new Authorizations( visibilityExpression );
		
		String instanceName = getVTIProperty(PROPERTY_INSTANCE);
		String zooKeeperHostPortLocationsCSV = getVTIProperty(PROPERTY_ZOOKEEPERS);
		String usr = getVTIProperty(PROPERTY_USR);
		String pwd = getVTIProperty(PROPERTY_PWD);
		
		ZooKeeperInstance instance;
		if ( zooKeeperInstances.containsKey( instanceName ) ) instance = zooKeeperInstances.get( instanceName );
		else zooKeeperInstances.put( instanceName, instance = new ZooKeeperInstance( instanceName, zooKeeperHostPortLocationsCSV ) );
		
		// All properties are referenced relative to: AccumuloVTI.<vtiArgs prefix argument>, e.g. AccumuloVTI.MyUserInstanceTable1
		accumuloConnector = instance.getConnector( usr, pwd );
		
		try { standardScanner = accumuloConnector.createScanner( accumuloTable, tableScannerAuthorizations ); }
		catch ( TableNotFoundException e ) { throw new SQLException("Unable to construct Accumulo Scanner. table = " + accumuloTable + ", cause: " + e); }
		
		try { batchScanner = accumuloConnector.createBatchScanner(accumuloTable, tableScannerAuthorizations, 10); }
		catch ( TableNotFoundException e ) { throw new SQLException("Unable to construct Accumulo BatchScanner. table = " + accumuloTable + ", cause: " + e); }
		
		tableScanner = standardScanner; // default
	}
	
	/**
	 * Gives VTI's table schema, i.e. number of columns, their types, names, sizes etc.
	 * Deduces this from the first row of data in the targeted table (whose name should be specified in gaiandb_config.properties).
	 * This method is always called by the querying engine (Gaian or Derby) *before* query execution.
	 */
	@Override public GaianResultSetMetaData getMetaData() throws SQLException {
		
		if ( false == isDeriveSchemaFromFirstRow ) accumuloTableRSMD = super.getMetaData();
		else if ( null == accumuloTableRSMD ) {
			
			// Get table shape from first accumulo record
			
			rowScanIterator = standardScanner.iterator();
			if ( false == rowScanIterator.hasNext() )
				throw new SQLException("Table has no data to derive it's schema. Table name = " + accumuloTable);
			
			Key key = rowScanIterator.next().getKey();
			Text rowID = key.getRow(), previousRowID = null;
			
			StringBuilder tableDefSB = new StringBuilder(
					( isRowidInSchema ? ROWID + ' ' + VC256 + ',' : "" ) + key.getColumnFamily() + ' ' + VC256 );
			
			while ( rowScanIterator.hasNext() ) {
				
				key = rowScanIterator.next().getKey();
				previousRowID = rowID;
				rowID = key.getRow();
				
				if ( false == rowID.equals(previousRowID) ) break; // stop when a full record has been read.
				
				tableDefSB.append( ',' + key.getColumnFamily().toString() + ' ' + VC256 );
			}
			
			reinitialise(); // clear scanner for re-use
			
			try { accumuloTableRSMD = new GaianResultSetMetaData( tableDefSB.toString() ); }
			catch (Exception e) {
				throw new SQLException("Unable to build AccumuloVTI RSMD table schema from definition: "
						+ tableDefSB + " (returning null), cause: " + e);
			}
		}
		
		// ROWID must always be included... if missing then hardly no qualifiers can be pushed down at all.
		// The logical table could still be configured to cut out the ROWID if this was really necessary (but performance could not longer be optimised).
//		isIncludeRowID = null != accumuloTableRSMD && "ROWID".equalsIgnoreCase( accumuloTableRSMD.getColumnName(1) );
		
		return accumuloTableRSMD;
	}
	
	/**
	 * The args passed here include:
	 * 		1) End-point instance ID (if VTI implements Pluralizable),
	 * 		2) All arguments passed to or created in the invoking parent GaianTable().
	 * The end-point instance ID is passed as first argument of the String[] in this method
	 */
	@Override public void setArgs(String[] args) throws Exception {
		super.setArgs(args); // allow super class to extract arguments it may need, e.g. QRY_CONTEXT_ARG_LT_DEF, to deduce default table schema.
		// Note we don't use default table schema based on logical table definition because we need to give exact column family names 
		// to accumulo when specifying projected columns - there is no room for using differently named columns or different character case 
		// for them in the logical table. 
		logger.logInfo("Entered setArgs(), args are: " + Arrays.asList(args) );
//		if ( null != args && 0 < args.length ) { if ( null != args[0] ) pluralizationInstance = args[0]; } // Not interested in pluralization yet
	};
	
	@Override public boolean pushProjection(VTIEnvironment arg0, int[] arg1) throws SQLException {
		logger.logThreadDetail("Entered AccumuloVTI.pushProjection(), projection: " + Util.intArrayAsString(arg1));
		if ( null != arg1) projectedColumns = arg1;
		return true;
	}
	
	/**
	 *  Qualifyable interface - used to process predicates against our columns -
	 *  Note that predicates on our *constant* columns (e.g. for each pluralizable instance) should ideally be processed above by GaianDB (if we passed 
	 *	the constant instances up with getEndpointConstantColumns()) - however it does little harm to test/filter them again here just in case.
	 */
	@Override public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException {
		
		logger.logThreadDetail("Entered AccumuloVTI.setQualifiers(), qualifiers: " + RowsFilter.reconstructSQLWhereClause(qual));
		qualifiers = qual;
		
		try {
			preScanIterators.clear();
			
			// Prepare range predicates based on given column qualifiers.
			QualifiersToAccumuloRangesConverter predicatesConverter = new QualifiersToAccumuloRangesConverter(qualifiers);
			
			if ( predicatesConverter.isQualifiersResolveToFalse() ) {
				logger.logInfo("Resolved column ranges - one of which is empty - ABORTING QUERY");
				projectedColumns = new int[0];
				return;
			}
			
			rowidRangesPredicates = predicatesConverter.getAccumuloRanges();
			logger.logInfo("Qualifiers resolved to "+ (null==rowidRangesPredicates?"0 Accumulo Ranges on ROWID":
				rowidRangesPredicates.size() + " Accumulo Ranges: " + rowidRangesPredicates) );
			qualifiers = predicatesConverter.getPrunedQualifiers();
			logger.logInfo("Remaining Qualifiers: " + RowsFilter.reconstructSQLWhereClause(qualifiers, accumuloTableRSMD));
			
			for ( int i=0; i<projectedColumns.length; i++ ) {
				
				final int pColID = projectedColumns[i];
				if ( 1 == pColID ) continue;
				final String colName = accumuloTableRSMD.getColumnName( pColID );
				String regex = predicatesConverter.getPredicatesRegexForColID( pColID-1 ); // passed in colID must be 0-based
				
				if ( null == regex || ".*".equals(regex) ) continue; // null case should not happen
				
				logger.logInfo("Built REGEX filter for column '" + colName + "' (colID " + pColID + "): " + regex);
				
				IteratorSetting regexIteratorSetting = new IteratorSetting(1, "FilterForQualsOrValues_" + pColID, RegExFilter.class);
				
//				RegExFilter.setRegexs( regexIteratorSetting, rowTerm, cfTerm, cqTerm, valueTerm, orFields? );
				if ( isExtractAccumuloColumnQualifiersInPlaceOfValues )
					 RegExFilter.setRegexs( regexIteratorSetting, null, "^(?:"+colName+")$", regex, null, false );
				else RegExFilter.setRegexs( regexIteratorSetting, null, "^(?:"+colName+")$", null, regex, false );
				
				// Add 1 regex scan iterator per column having predicates on it - doesn't work the way we want... (only the column with the predicate is retrieved)
//				tableScanner.addScanIterator( regexIteratorSetting );
				
				preScanIterators.add( regexIteratorSetting );
			}
			
		} catch ( Exception e ) {
			logger.logImportant("Exception whilst resolving Accumulo Ranges from qualifiers (using empty Ranges list), cause " + Util.getStackTraceDigest(e));
		}
	}
	
	/**
	 * Prepare table scanner and its iterators.
	 * Use scanner.fetchColumnFamily() to specify requested columns.
	 * Use scanner.setRanges(), scanner.fetchColumn() and custom scanner iterators to specify column filters.
	 */
	@Override public boolean executeAsFastPath() throws SQLException {
		
		try {
			final int columnCount = accumuloTableRSMD.getColumnCount();
			
			// Initialise array of queried columns if not set.
			if ( null == projectedColumns ) {
				projectedColumns = new int[ columnCount ];
				for ( int i=0; i<columnCount; i++ ) projectedColumns[i] = i+1; // 1-based
			}
			
			if ( null != rowidRangesPredicates ) {
				int numRanges = rowidRangesPredicates.size();
				if ( 1 < numRanges ) ((BatchScanner) (tableScanner = batchScanner)).setRanges( rowidRangesPredicates );
				else if ( 1 == numRanges ) ((Scanner) (tableScanner = standardScanner)).setRange( rowidRangesPredicates.get(0) );
			}
			
			if ( false == preScanIterators.isEmpty() ) {
				
				if ( 1 == preScanIterators.size() && ( 1 == projectedColumns.length || ( 2 == projectedColumns.length && 1 == projectedColumns[0] ) ) )
					// No need for pre-scans as there is only 1 column with predicates and no other columns are being extracted.					
					tableScanner.addScanIterator( preScanIterators.get(0) );
				else {
					for ( IteratorSetting it : preScanIterators ) {

						tableScanner.addScanIterator( it );
						
						long millis = System.currentTimeMillis();
						rowidRangesPredicates = iterateAndExtractRowRanges( tableScanner.iterator() );
						logger.logInfo("Scanned rowID ranges matching a column's predicates in " + (System.currentTimeMillis() - millis)
								+ "ms. Remaining RowIDs ranges count = " + rowidRangesPredicates.size());

						if ( rowidRangesPredicates.isEmpty() ) { projectedColumns = new int[0]; break; }
						
						tableScanner.clearScanIterators();
						int numRanges = rowidRangesPredicates.size();
						if ( 1 < numRanges ) ((BatchScanner) (tableScanner = batchScanner)).setRanges( rowidRangesPredicates );
						else if ( 1 == numRanges ) ((Scanner) (tableScanner = standardScanner)).setRange( rowidRangesPredicates.get(0) );
					}
				}
			}
			
			// Initialise a shift value to 1 if ROWID needs setting in nextRow()
			if ( 0 == projectedColumns.length ) return true;
			rowidColShift = 1 == projectedColumns[0] ? 1 : 0;
			
			// Initialise mapping of column names to column indexes
			for ( int i=0; i<projectedColumns.length; i++ ) {
				int pColID = projectedColumns[i];
				projectedColumnsNameToIndexMap.put( accumuloTableRSMD.getColumnName(pColID), pColID );
			}
			
			// Specify all columns required to be extracted for this query
			for ( int i=0; i<projectedColumns.length; i++ ) {
				final int pColID = projectedColumns[i];
				if ( 1 == pColID ) continue;
				final String colName = accumuloTableRSMD.getColumnName( pColID );
				logger.logInfo("Adding projected column (column family): " + colName);
				tableScanner.fetchColumnFamily( new Text(colName) );
				// NOTE - Could make use of the following if the column name were to be built from family+qualifier fields:
//				tableScanner.fetchColumn( new Text("<colFamily>"), new Text("<colQualifier>") );
			}
			
			rowScanIterator = tableScanner.iterator();
			
		} catch ( Exception e ) {
			throw new SQLException("Exception in AccumuloVTI.executeAsFastPath(), cause: " + Util.getStackTraceDigest(e));
		}
		
		return true;
	}
	
	/**
	 * The purpose of this method is to retrieve a list of rowIDs that satisfy predicates set on an IteratorSetting.
	 * This method must only be called with an iterator that will retrieve 1 Accumulo record per rowID.
	 * 
	 * @param scanIterator
	 * @return
	 */
	private List<Range> iterateAndExtractRowRanges( Iterator<Map.Entry<Key, Value>> scanIterator ) {
		
		List<Range> rowRanges = new ArrayList<Range>();
		while( scanIterator.hasNext() )
			rowRanges.add( new Range( scanIterator.next().getKey().getRow() ) );
		return Range.mergeOverlapping( rowRanges );
	}
	
	/**
	 * GaianDB extract rows by calling this method repeatedly.
	 * 'dvdRecord' contains the number of columns resolved in tableShapeRSMD.
	 * However we only need to populate the projected columns indexes.
	 */
	@Override public int nextRow( final DataValueDescriptor[] dvdRecord ) throws StandardException, SQLException {
		
//		logger.logDetail("Getting new relational record based on set of Accumulo rows. rowCount = " + rowCount +
//				", currenAccumuloRow: " + currentAccumuloRow );
		
		if ( 0 == rowCount ) {
			numRowsReceivedFromAccumulo = 0;
			if ( 0 == projectedColumns.length || false == rowScanIterator.hasNext() ) return IFastPath.SCAN_COMPLETED; // empty table
			else currentAccumuloRow = rowScanIterator.next(); // kick-start row extraction
		}
		
		// Check if there are any Accumulo records left...
		if ( null == currentAccumuloRow ) return IFastPath.SCAN_COMPLETED;
		
		Key key = currentAccumuloRow.getKey(); // lots of info available off the Key: rowID, col name/family, col qualifier, visibility, timestamp
		Text rowID = key.getRow();
		
		// Look for a new record... until one is found that meets qualifiers, or until none are left
		do {
			// Check if there are any Accumulo records left...
			if ( null == currentAccumuloRow ) return IFastPath.SCAN_COMPLETED;
			
			numRowsReceivedFromAccumulo++;
			
			// Set rowID column before extracting others associated with it in the while loop
			if ( 1 == rowidColShift ) dvdRecord[0].setValue( rowID.toString() );
			
			// Initialise column cells to NULL value.
			for ( int i=rowidColShift; i<projectedColumns.length; i++ )
				dvdRecord[ projectedColumns[i]-1 ].setToNull();

			// Extract columns from Accumulo records for this rowID - note: Accumulo rows don't have to be complete
			Text previousRowID = rowID;
			while ( rowID.equals( previousRowID ) ) {
				
				final String colName = key.getColumnFamily().toString();
				final Integer pColID = projectedColumnsNameToIndexMap.get(colName);
				if ( null == pColID ) {
					logger.logImportant("Encountered Accumulo column which was not requested as column family (skipped): " + colName);
					continue; // this column was not requested - should not happen
				}
				
				// Log info about the newly found column
				final String cellStringValue = isExtractAccumuloColumnQualifiersInPlaceOfValues ?
						currentAccumuloRow.getKey().getColumnQualifier().toString() : currentAccumuloRow.getValue().toString();
//				logger.logDetail("Setting ProjectedColID: " + pColID +
//						", from record with Key: " + key + " ==> ColFamily: " + key.getColumnFamily()
//						+ ( isExtractAccumuloColumnQualifiersInPlaceOfValues ? ", ColQualifier: " : ", Value: " ) + cellStringValue );
				
				// Set column value for the row - this also does type conversion.
				dvdRecord[ pColID-1 ].setValue( cellStringValue ); // normalise to 0-based
				
				// Scroll to the next column - break if we run out of records (rows don't have to be complete)
				if ( false == rowScanIterator.hasNext() ) {
					currentAccumuloRow = null;
					break;
				}
				currentAccumuloRow = rowScanIterator.next();
				key = currentAccumuloRow.getKey();
				previousRowID = rowID;
				rowID = key.getRow();
			}
			
		} while ( null != qualifiers && false == RowsFilter.testQualifiers( dvdRecord, qualifiers ) );
		
		rowCount++;
		return IFastPath.GOT_ROW;
	}
	
	// PluralizableVTI methods - used to re-factor/generalise/simplify data source wrapper configurations
	// Note each endpointID will appear as a suffix in the GDB_LEAF column when querying the logical table augmented with the provenance columns.
//	@Override public Set<String> getEndpointIDs() { return endpointConstants.keySet(); }
//	@Override public DataValueDescriptor[] getEndpointConstants(String endpointID) { return endpointConstants.get( endpointID ); }
	
	@Override public int getRowCount() throws Exception { return rowCount; }

	// Empty local heap resources for this instance and set them to null if possible
	@Override public void close() throws SQLException { super.close(); reinitialise(); } //endpointConstants.clear(); }
	
	private static final Range RANGE_INFINITY = new Range();
	private static final List<Range> RANGES_INFINITY = Arrays.asList( RANGE_INFINITY );
	/**
	 * Clears the query objects and returns true
	 */
	@Override public boolean reinitialise() {
		if ( 0 < rowCount )
			logger.logImportant("Re-initialising AccumuloVTI. Row Counts for last query - filtered locally/remotely: "
				+ rowCount + '/' + numRowsReceivedFromAccumulo);
		tableScanner.clearColumns();
		tableScanner.clearScanIterators();
		if ( tableScanner == standardScanner ) standardScanner.setRange( RANGE_INFINITY );
		else batchScanner.setRanges( RANGES_INFINITY );
		tableScanner = standardScanner; // default
		rowCount = 0;
		return true;
	}
	@Override public boolean isBeforeFirst() throws SQLException { return 0 == rowCount; }
	
	// VTICosting methods - used for JOIN optimisations
	@Override public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException { return false; }

	private class QualifiersToAccumuloRangesConverter {
		
		private final static int ROWID_COLID = 0; // This VTI always exposes the rowID as the first column of the result.
		private final Qualifier[][] originalQualifiers;
		private Qualifier[][] prunedQualifiers;

		// Mapping from column ID to Lists of Range objects converted from qualifiers on the columnID
		private final Map<Integer, List<Range>> accumuloColIDs2RangesMap; // Integer colID is 0-based
		
		private QualifiersToAccumuloRangesConverter( Qualifier[][] queryColumnPredicates ) throws StandardException, SQLException {
			originalQualifiers = queryColumnPredicates;
			accumuloColIDs2RangesMap = deriveAccumuloRangesAndPruneQualifiers(); // might be null if a range resolves to empty
		}
		
		private boolean isQualifiersResolveToFalse() { return null==accumuloColIDs2RangesMap; }
		
		private List<Range> getAccumuloRanges() { return null==accumuloColIDs2RangesMap ? null : accumuloColIDs2RangesMap.get( ROWID_COLID ); }
		public Qualifier[][] getPrunedQualifiers() { return prunedQualifiers; }
		
		/**
		 * Converts List of ranges for a column to a regular expression - expects the type to be String (UTF8)
		 * Passed in colID must be 0-based. 
		 * 
		 * Example String regex:
		 * 	Original predicates/range: 		(C>="c" AND C<"i") OR C="bob"
		 * 	In conjunctive normal form:  	C<"i" AND (C="bob" OR C>="c")
		 * 	Equivalent REGEX:				(?:[c-h].*|bob)
		 * 
		 * We wanted a solution that would work with Accumulo 1.4.4
		 * However a better solution in future (1.5+) is to use full server-side Filter, e.g. AccumuloPredicateHandler
		 * 		https://github.com/bfemiano/accumulo-hive-storage-manager/wiki/Iterator%20Predicate%20pushdown
		 * 		http://storage-handler-docs.s3.amazonaws.com/javadocs/org/apache/accumulo/storagehandler/predicate/AccumuloPredicateHandler.html
		 * 
		 * @param colID - 0-based column ID
		 * @return a regular expression equivalent to the list of Ranges converted from the qualifiers; or ".*" if all records pass, and null if none do (empty range).
		 */
		public String getPredicatesRegexForColID( int colID ) { // Passed in colID must be 0-based

			if ( null == accumuloColIDs2RangesMap ) return null; // it has been determined that no records can match the query
			List<Range> columnRanges = accumuloColIDs2RangesMap.get(colID);
			if ( null == columnRanges ) return ".*"; // null means no range restrictions (i.e. no filter) so all records match..
			if ( 1 > columnRanges.size() ) return null; // an empty range means that no records can match the query
			
			// Ranges list is already ordered...
			
			StringBuilder sb = new StringBuilder( "^(?:");

			for ( Range colRange : columnRanges )
				sb.append( constructRegexForRange( colRange ) + "|" );
			
			int len = sb.length();
			sb.replace( len-1, len, ")$" ); // replace the last '|' with the closing bracket for the regex
			
			return sb.toString();
		}
		
		/**
		 * Example ranges to consider for conversion to regex (Note Accumulo Ranges methods will have already eliminated impossible ranges like >="aaa" and <"aa"):
		 * 
		 * v>"a"	and v<max		=>		a..*  | [b-max].*
		 * 
		 * v>"aaa"	and v<max		=>		aaa..*| aa[b-max].* | a[b-max].* | [b-max].*
		 * v>="aaa"					=> 		aaa.* | aa[b-max].* | a[b-max].* | [b-max].*
		 * v>="aaa" and v<="ad"		=> 		aaa.* | aa[b-max].* | a[b-c].* | ad
		 * v>="aaa" and v<"ad"		=> 		aaa.* | aa[b-max].* | a[b-c].*
		 * v>="aaa" and v<"ac"		=> 		aaa.* | aa[b-max].* | ab.*
		 * v>="aaa" and v<"ab"		=> 		aaa.* | aa[b-max].*
		 * 
		 * v>="ab" and v<"azz"		=> 		ab.* | a[c-y].* | az | az[min-y].*
		 * 
		 * v>="aaa" and v<"acc"		=> 		aaa.* | aa[b-max].* | ab.* | ac | ac[min-b].*
		 * v>="aaa" and v<"aad"		=> 		aaa.* | aa[b-c].*
		 * v>="aaa" and v<"aadd"	=> 		aaa.* | aa[b-c].* 	| aad[e-max].*
		 * 
		 * v>="aaa" and v<"aaad"	=> 		aaa   | 			  aaa[min-c].*
		 * v>"aaa" and v<"aaadd"	=> 						      aaa[min-c].* | aaad | aaad[min-c].*
		 * 
		 * v>="aaa" and v<="aaa"	=> 		aaa
		 * 
		 * v>="aaa" and v<"bbb"		=> 		aaa.* | aa[b-max].* | a[b-max].*           | b | b[min-a].* | bb | bb[min-a].*
		 * v>="aaa" and v<"cbb"		=> 		aaa.* | aa[b-max].* | a[b-max].* | b.*     | c | c[min-a].* | cb | cb[min-a].*
		 * v>="aaa" and v<"dbb"		=> 		aaa.* | aa[b-max].* | a[b-max].* | [b-c].* | d | d[min-a].* | db | db[min-a].*
		 * 
		 * v>min    and v<"bbb"		=>										 [min-a].* | b | b[min-a].* | bb | bb[min-a].*
		 * v>min    and v<"b"		=>										 [min-a].*
		 * 
		 * v>="a"	and v<"b"		=>		a.*
		 * 
		 * Basic algorithm:
		 * 1. If keys are equal and included, quote the value and return it.
		 * 2. If the start key is negative infinitity skip to step 7.
		 * 3. If the start key is a sub-string of the end key, skip to step 7
		 * 4. Create a starting regex matching the whole left bound string. If the start key is not included, then enforce existence of a trailing char
		 * 5. Create a new ORed regex for every sub-string of it, until it matches the beginning of the end key
		 * 		-> build every new substring by removing right-most chars 1 at a time, and adding 1 to the rightmost char
		 * 6. If/when the last sub-string matches the beginning of the end key, create an ORed regex range bounded on both sides (unless left bound+1 > right bound-1)
		 * 7. Add an ORed regex for every remaining super-string of the end key (adding chars left to right and substracting 1 from the rightmost char).
		 * 		-> For each of these, also pre-pend another ORed value for the exact new super-string (i.e. with no suffix)
		 * 8. Add a final regex matching the whole right-bound string if it is included.
		 * 
		 * @param range
		 * @return
		 */
		private String constructRegexForRange( Range range ) {
			
			final Key k1 = range.getStartKey(), k2 = range.getEndKey();
			final boolean inf1 = range.isInfiniteStartKey(), inf2 = range.isInfiniteStopKey();

			if ( true == inf1 && true == inf2 ) return ".*"; // infinite range
			
			final String v1 = true == inf1 ? null : k1.getRow().toString(), v2 = true == inf2 ? null : k2.getRow().toString();
			
			logger.logDetail( "startKey: " + (null == v1 ? "-inf" : "'" + v1 + "' (incl? " + range.isStartKeyInclusive() + ")") );
			logger.logDetail( "endKey: " + (null == v2 ? "+inf" : "'" + v2 + "' (incl? " + range.isEndKeyInclusive() + ")") );
			
			// 1. If keys are equal and included, quote the value and return it.
			if ( null != v1 && v1.equals(v2) && range.isStartKeyInclusive() ) return rQuote(v1); // Quote/wrap in case v1 contains special characters
			
			// Keys are not equal, so get ready for an actual range...
			StringBuilder regex = new StringBuilder();
			
			int prefixLen = 0; // the prefix which may be common to v1 and v2, e.g: "aa" is a common prefix for v1="aaaa" and v2="aadd", and used to form expressions like: "aa[b-c].*"
			final int len1 = inf1 ? 0 : v1.length(), len2 = inf2 ? 0 : v2.length();
			
			// 2. If the start key is infinite skip to step 7.
			if ( false == inf1 ) {
				
				// 3. If the start key is a sub-string of the end key, skip to step 7
				if ( false == inf2 && v2.startsWith(v1) )
					prefixLen = v1.length();
				else {
				
					// 4. Create a starting regex matching the whole left bound string. If the start key is not included, then enforce existence of a trailing char
					regex.append( rQuote(v1) + (range.isStartKeyInclusive()?"":".") + ".*" + '|' );

					logger.logDetail("Char values in '" + v1 + "': " + unicodeValuesAsString(v1) );
					
					// 5. Create a new ORed regex for every sub-string of it, until it matches the beginning of the end key
					for ( int i=1; i<=len1; i++ ) {
						prefixLen = len1-i; // length of the next prefix
						String prefix = v1.substring(0, prefixLen);
						if ( '\u00FF' == v1.charAt(prefixLen) ) continue;
						char c1 = (char) (v1.charAt(prefixLen)+1); // character just beyond the prefix, shifted up 1 in the utf8 range.
//						logger.logDetail("len2 " + len2 + ", prefixLen " + prefixLen + ", prefix " + prefix);
						if ( false == inf2 && len2 > prefixLen && v2.startsWith(prefix) ) {
							
							// 6. If/when the last sub-string matches the beginning of the end key, create an ORed regex range bounded on both sides (unless left bound+1 > right bound-1)
							char c2 = (char) (v2.charAt(prefixLen)-1); // character just beyond the prefix in the upper bound, shifted down 1 in the utf8 character set.
							if ( c1 < c2 )			regex.append( rQuote(prefix) + '[' + toUnicode(c1) + '-' + toUnicode(c2) + "].*" + '|' );
							else if ( c1 == c2 )	regex.append( rQuote(prefix) + toUnicode(c1) + ".*" + '|' );
							prefixLen++;
							break;
						}
						if ( '\u00FF' > c1 ) regex.append( rQuote(prefix) + '[' + toUnicode(c1) + '-' + UMAX + "].*" + '|' );
						else if ( '\u00FF' == c1 ) regex.append( rQuote(prefix) + UMAX + ".*" + '|' );
					}
				}
			}
			
			// 7. Add an ORed regex for every remaining super-string of the end key (adding chars left to right and substracting 1 from the rightmost char).
			if ( false == inf2 ) {
				
				logger.logDetail("Char values in '" + v2 + "': " + unicodeValuesAsString(v2) );

				for ( int i=prefixLen; i<len2; i++ ) {
					String prefix = v2.substring(0, i);
					regex.append( rQuote(prefix) + '|' );
					if ( '\u0000' == v2.charAt(i) ) continue;
//					if ( '\u0000' == v2.charAt(i) ) { regex.append( rQuote(prefix) + '|' ); continue; }
					char c2 = (char) (v2.charAt(i)-1); // character just beyond the prefix, shifted down 1 in the utf8 character set.
					if ( '\u0000' < c2 ) regex.append( rQuote(prefix) + '[' + UMIN + '-' + toUnicode(c2) + "].*" + '|' );
					else if ( '\u0000' == c2 ) regex.append( rQuote(prefix) + UMIN + ".*" + '|' );
				}
			}
			
			// 8. Add a final regex matching the whole right-bound string if it is included.
			if ( range.isEndKeyInclusive() ) regex.append( rQuote(v2) );
			else regex.deleteCharAt( regex.length()-1 ); // remove last '|' symbol
			
			return regex.toString();
		}
		
		public String unicodeValuesAsString( String s ) {
			if ( null==s ) return null; int len = s.length();
			StringBuffer pcs = new StringBuffer( 0<len ? "[" + toUnicode(s.charAt(0)) : "[" );
			for (int i=1; i<len; i++) pcs.append( ", " + toUnicode(s.charAt(i)) ); pcs.append(']');
			return pcs.toString();
		}
		
		private String rQuote( String s ) { return 0 == s.length() ? "" : "\\Q" + s + "\\E"; }
		private String toUnicode( char c ) { return c+""; }
//		private String toUnicode( char c ) { return ((16>(int)c) ? "\\u000" : "\\u00" ) + Integer.toHexString(c); }
		
		private static final String UMIN = "\\u0000";
		private static final String UMAX = "\\u00FF";
		
		/**
		 * Create a list of ranges for each column, based on conditional expressions in the Qualifier[][] structure, which holds predicates in conjunctive normal form.
		 * Each list of ranges represents conditions for 1 column.
		 * The ranges are only built for columns that are compared to strings.
		 * All comparison expressions ORed with comparisons involving other columnIDs are excluded entirely from all derived lists of ranges.
		 * 
		 * Finally, this method also creates the prunedQualifiers field, which holds the original qualifiers minus those that were extracted to build the ranges lists.
		 */
		private Map<Integer, List<Range>> deriveAccumuloRangesAndPruneQualifiers() throws SQLException, StandardException {

			// Mapping from column ID to Lists of Range objects converted from qualifiers on the columnID
			Map<Integer, List<Range>> colIDs2RangesMap = new HashMap<Integer, List<Range>>(); // Integer colID is 0-based
			
			prunedQualifiers = originalQualifiers;
			if ( null == originalQualifiers || 0 == originalQualifiers.length ) return colIDs2RangesMap;

			Set<Integer> excludedColIDs = new HashSet<Integer>();
			
			// Find all column IDs that are compared to values that are not strings.
			// We will not push predicates down to Accumulo for these columns.
			for ( int i=0; i<originalQualifiers.length; i++ )
				for ( int j=0; j<originalQualifiers[i].length; j++ )
					if ( false == originalQualifiers[i][j].getOrderable() instanceof SQLChar )
						excludedColIDs.add( originalQualifiers[i][j].getColumnId() );
			
			// Process 1st row, which contains ANDed expressions.
			Qualifier[] qRow = originalQualifiers[0];
			for ( int j=0; j<qRow.length; j++ ) {
				Qualifier qCell = qRow[j];
				int colID = qCell.getColumnId();
				
				// Create new Ranges for this qCell.
				// Example: ROWID!=3 becomes ranges: [null,3[ ; ]3,null]
				List<Range> newRanges = constructRangesFromSingleQualifier( qCell ); // can't be empty
				
				List<Range> accumuloRanges = colIDs2RangesMap.get( colID );
				if ( null == accumuloRanges ) { colIDs2RangesMap.put( colID, newRanges ); continue; }
				
				// Clip new ranges (i.e. intersect) with current ones.
				// Example for ROWID!=3 AND ROWID!=5 we end up with: [null,3[ ; ]3,5[ ; ]5,null]
				List<Range> combinedRanges = new ArrayList<Range>();
				for ( Range r1 : accumuloRanges ) for ( Range r2 : newRanges ) {
					Range r = r1.clip(r2, true);
					if ( null != r ) combinedRanges.add( r );
				}
				
				if ( (newRanges = Range.mergeOverlapping( combinedRanges )).isEmpty() ) return null; // empty range - no records can match
				// Reduce current set before moving on to the next ANDed cell in this first row.
				colIDs2RangesMap.put( colID, newRanges );
			}

			List<Qualifier[]> remainingQualifiers = new ArrayList<Qualifier[]>();
			remainingQualifiers.add( new Qualifier[0] ); // The first row contains anded predicates - we have pruned them all...
			
			// Process all subsequent rows, which now contain ORed expressions - also combine these with previous rows
			// ORed rows that reference columns other than the ROW ID column are skipped/ignored because they may just be true.
			// Rows are combined with previous ones by intersecting them
			for ( int i=1; i<originalQualifiers.length; i++ ) {
				qRow = originalQualifiers[i];
				boolean isOnlyOneColIDReferencedInThisORedQrow = true;
				int colID = qRow[0].getColumnId();
				
				for ( int j=1; j<qRow.length; j++ )					
					if ( colID != qRow[j].getColumnId() ) { isOnlyOneColIDReferencedInThisORedQrow = false; break; }
				// Ignore ORed expressions containing more than 1 column ID - they can't enforce constrainst on the Ranges list because the other column may resolve to true.
				if ( false == isOnlyOneColIDReferencedInThisORedQrow ) {
					remainingQualifiers.add( qRow );
					continue;
				}
				
				// Create ranges for this ORed row
				List<Range> oredRanges = new ArrayList<Range>();
				for ( int j=0; j<qRow.length; j++ )
					oredRanges.addAll( constructRangesFromSingleQualifier(qRow[j]) ); // can't become empty
				
				List<Range> accumuloRanges = colIDs2RangesMap.get( colID );
				if ( null == accumuloRanges ) { colIDs2RangesMap.put( colID, Range.mergeOverlapping( oredRanges ) ); continue; } // can't resolve to empty here
				
				// Now clip (i.e. intersect) all current ranges with ranges built in this row.
				List<Range> combinedRanges = new ArrayList<Range>();
				for ( Range r1 : accumuloRanges ) for ( Range r2 : oredRanges ) {
					Range r = r1.clip(r2, true);
					if ( null != r ) combinedRanges.add( r );
				}
				
				if ( (accumuloRanges = Range.mergeOverlapping( combinedRanges )).isEmpty() ); // return null; // empty range - no records can match
				// Finally, try to reduce the current set before moving on to the next ORed row
				colIDs2RangesMap.put( colID, accumuloRanges );
			}
			
			logger.logDetail("Built Map <colID -> List<Range>> (size " + colIDs2RangesMap.size() + "): " + colIDs2RangesMap);
			
			prunedQualifiers = 2 > remainingQualifiers.size() ? null : (Qualifier[][]) remainingQualifiers.toArray( new Qualifier[0][] );
			
			return colIDs2RangesMap;
		}
	}
	
	private static List<Range> constructRangesFromSingleQualifier( final Qualifier q ) throws SQLException, StandardException {
		
		List<Range> ranges = new ArrayList<Range>();
		final String val = q.getOrderable().getString();
		final int operator = q.getOperator();
		boolean negate = q.negateCompareResult();
		
		switch ( operator ) {
		
			case Orderable.ORDER_OP_EQUALS:
				if (negate) { ranges.add(new Range(null, false, val, false)); ranges.add(new Range(val, false, null, false)); }
				else ranges.add(new Range(val));
				break;
				
			case Orderable.ORDER_OP_GREATEROREQUALS: negate = !negate;
			case Orderable.ORDER_OP_LESSTHAN:
				ranges.add( negate ? new Range(val, null) : new Range(null, false, val, false) ); break;
				
			case Orderable.ORDER_OP_LESSOREQUALS: negate = !negate;
			case Orderable.ORDER_OP_GREATERTHAN:
				ranges.add( negate ? new Range(null, val) : new Range(val, false, null, false) ); break;
			
			default:
				String errmsg = "Invalid operator detected (not one of the Orderable interface): " + operator;
				logger.logThreadWarning(GDBMessages.ENGINE_OPERATOR_INVALID, "DERBY ERROR: " + errmsg);
				throw new SQLException( errmsg );
		}
		
		return ranges;
	}
	
//	/**
//	 * Automatically creates a logical table based on an Accumulo one.
//	 * Logical table column types can later be changed manually by the user in gaiandb_config.properties if they want 
//	 * to expose them via more useful types for searching.
//	 * 
//	 * @param ltName
//	 * @param instanceID
//	 * @param zookeepersCSV
//	 * @param usr
//	 * @param pwd
//	 * @param accumuloTable
//	 */
//	public static void setLogicalTableForAccumulo( final String ltName,
//			final String instanceID, final String zookeepersCSV, final String usr, final String pwd, final String accumuloTable ) {
//		
//		// Use a hash of the connector properties to derive a unique vtiPropertiesID for looking them up.
//		// Note we need to order the Zookeepers list so it doesn't affect the hash.
//		final String accumuloInstancePropertiesHash = Util.getFileMD5(file)( instanceID + usr + pwd
//				+ new TreeSet<String>( Arrays.asList( Util.splitByCommas(zookeepersCSV) ) ) ).hashCode();
//		
//		final String vtiPropertiesID = AccumuloVTI.class.getSimpleName() + '.' + ;
//		
//		GaianDBConfigProcedures.setConfigProperties(
//				"values ('" + vtiInstancePrefix + ".INSTANCE', '" + instanceID)
//		
//	}
	
	/**
	 * Automatically creates a logical table based on an Accumulo one.
	 * Logical table column types can later be changed manually by the user if they want to expose them via more useful types for searching.
	 * 
	 * This is the registration SQL for this procedure:
	 * CREATE PROCEDURE setltforaccumulo( ltname varchar(32672), connectorID varchar(32672), tablexpr varchar(32672) )
	 * PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME 'com.ibm.db2j.AccumuloVTI.setLogicalTableForAccumulo'
	 * 
	 * Example use:
	 * call setltforaccumulo('LTBIKES', 'connectAcc:root:password@ets03.hursley.ibm.com/instance1', 'mytable')
	 * 
	 * @param ltName
	 * @param connectorInfo - Either just a connectorID (to point to an existing one), or
	 * 	 	set a new connectorID, using format: connectorID:user:password@zookeepersCSV/instance;
	 * 		Example: "Connector1:user:password@host1,9.12.34.56,host3/MyAccumuloInstance"
	 * @param accumuloTableExpression - This may contain a simple accumulo table name, or a more complex expression to allow:
	 * 			- Access to different combinations of Accumulo table fields for each column family (e.g. extract column qualifier instead of the value field).
	 * 			- Specification of the "Visibility" logical expression in square brackets. This visibility will be attributed 
	 * 				to the Scanner that will run against the Accumulo table - this needs to be a subset of the User's Visbility.
	 * 		Example: "myAccumuloTable qualifiers [A&B]"
	 */
	public static void setLogicalTableForAccumulo( String ltName, String connectorID, String accumuloTableExpression ) throws Exception {
		
		final String HELP_SYNTAX =
			"Syntax: call setLogicalTableForAccumulo('<ltName>', '<connectorID>', '<accumuloTable>'), where "
			+ "connectorID may also set itself: <connectorID:user:password@zookeepersCSV/instance>";
		
		int idx = connectorID.indexOf(':');
		String connectorInfo = null;
		
		if ( -1 < idx ) {
			connectorInfo = connectorID.substring(idx+1);
			connectorID = connectorID.substring(0, idx);
		}
		
		final String vtiPropertiesPrefix = AccumuloVTI.class.getSimpleName() + '.' + connectorID;
		
		String instance, zookeepersList, usr, pwd;
		
		if ( null != connectorInfo ) {
			// Create new connector properties
//			int idx1 = connectorInfo.indexOf('@');
//			int idx2 = connectorInfo.indexOf(';');
//			int idx3 = connectorInfo.indexOf(':');
			int idx1 = connectorInfo.indexOf(':');
			int idx2 = connectorInfo.indexOf('@');
			int idx3 = connectorInfo.lastIndexOf('/');
			
			if ( 0 > idx1 || 0 > idx2 || 0 > idx3 )
				throw new Exception("Incorrect syntax for Accumulo connectorID argument. " + HELP_SYNTAX);
			
//			instance = connectorInfo.substring(0, idx1);
//			zookeepersList = connectorInfo.substring(idx1+1, idx2);
//			usr = connectorInfo.substring(idx2+1, idx3);
//			pwd = connectorInfo.substring(idx3+1);
			usr = connectorInfo.substring(0, idx1);
			pwd = connectorInfo.substring(idx1+1, idx2);
			zookeepersList = connectorInfo.substring(idx2+1, idx3);
			instance = connectorInfo.substring(idx3+1);
			
			GaianDBConfigProcedures.setConfigProperties(
					"values ('" + vtiPropertiesPrefix + '.' + PROPERTY_INSTANCE + "', '" + instance + "')"
					+ ", ('" + vtiPropertiesPrefix + '.' + PROPERTY_ZOOKEEPERS + "', '" + zookeepersList + "')"
					+ ", ('" + vtiPropertiesPrefix + '.' + PROPERTY_USR + "', '" + usr + "')"
					+ ", ('" + vtiPropertiesPrefix + '.' + PROPERTY_PWD + "', '" + pwd + "')"
			);
		}
		
		// Now build VTI instance to derive schema, then set logical table and data source properties
		final String vtiArgs = connectorID + ',' + accumuloTableExpression + ',' + DERIVE_SCHEMA_FROM_FIRST_ROW;
		AccumuloVTI vtiInstance = new AccumuloVTI( vtiArgs );
		String tableDef = vtiInstance.getMetaData().getColumnsDefinition();
		
		logger.logInfo("Derived schema (from 1st row) for Accumulo table '" + accumuloTableExpression + "': " + tableDef);
		
//		Map<String, String> ltProperties = GaianDBConfigProcedures.prepareLogicalTable(ltName, tableDef, "");
//		ltProperties.put( ltName + "_DS0_VTI", AccumuloVTI.class.getName() );
//		ltProperties.put( ltName + "_DS0_ARGS", vtiArgs );
//		ltProperties.put( ltName + "_DS0_OPTIONS", "MAP_COLUMNS_BY_POSITION" );
//		ltProperties.put( ltName + "_DS0_SCHEMA", tableDef );
//		// apply all properties...
		
		GaianDBConfigProcedures.setLogicalTable(ltName, tableDef, "");
		GaianDBConfigProcedures.setDataSourceVTI(ltName, "0", AccumuloVTI.class.getName(), vtiArgs, "MAP_COLUMNS_BY_POSITION", "");
		
		// TODO: add _SCHEMA property in deriveRequiredDataSourceUpdatesFromOptionsAndColumnsLists()
	}
	

	
}

