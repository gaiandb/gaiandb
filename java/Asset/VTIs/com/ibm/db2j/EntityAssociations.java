/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;



import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.gaiandb.CachedHashMap;
import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.EntityMatrixJoiner;
import com.ibm.gaiandb.GaianChildRSWrapper;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */
public class EntityAssociations extends VTI60 implements VTICosting, IFastPath, GaianChildVTI { /*Pushable, IQualifyable, */
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "EntityAssociations", 25 );
	
	public static final String PROPERTY_ENTITY_ASSOCIATIONS_INPUT_FILE = "fileName";
	public static final String PROPERTY_ENTITY_ASSOCIATIONS_OUTPUT_FILE = "outputFile";
	public static final String PROPERTY_ENTITY_ASSOCIATIONS_NUM_ENTITIES = "numEntities";
	public static final String PROPERTY_ENTITY_ASSOCIATIONS_GROUP_SIZE = "maxGroupSize";
	
	private static final String GROUPZISE_COLNAME = "GROUPSIZE";
	private static final String ENTITY_COLNAME_PREFIX = "ENTITY";
	
	private ResultSetMetaData rsmd;
	private String inputFile;
	private String logicalTable;
	private int restrictedGroupSize;
	
	private EntityMatrixJoiner emj = null;
	private int index = 0;
	
	// filename -> latest row count
	private static Map<String, Long> estimatedRowCounts = 
		Collections.synchronizedMap( new CachedHashMap<String, Long>( GaianTable.QRY_METADATA_CACHE_SIZE ) );
	
	// filename -> matrix computed at initialisation
	private static ConcurrentMap<String, EntityMatrixJoiner> matrices = new ConcurrentHashMap<String, EntityMatrixJoiner>();
	private static ConcurrentMap<String, Long> loadTimes = new ConcurrentHashMap<String, Long>();
	
	public static boolean loadMatrix( String inputFileName, int numEntities, int maxGroupSize, String outputFileName ) {
		
		try {
			
			EntityMatrixJoiner emj = new EntityMatrixJoiner(inputFileName, numEntities, maxGroupSize);
			emj.processJoins();
			
			if ( null != outputFileName ) {
				emj.writeNonOverflowedRowsToFile( outputFileName );
				emj.releaseMatrixFromMemory();
			} else {
				matrices.put( new File( inputFileName ).getCanonicalPath().intern(), emj );
				loadTimes.put( new File( inputFileName ).getCanonicalPath().intern(), new Long( System.currentTimeMillis() ) );
			}
			
		} catch ( Exception e ) {
			logger.logException( GDBMessages.DSWRAPPER_MATRIX_JOINER_LOAD_WRITE_ERROR, "Unable to load Entity Matrix Joiner associations or write them to output file (inputFile: " +
					inputFileName + ", numEntities: " + numEntities + ", maxGroupSize: " + maxGroupSize + 
					", outputFile: " + outputFileName + ")", e);
			return false;
		}
		
		return true;
	}
	
	public static void unloadMatrix( String inputFileName ) throws IOException {
		
		if ( null == inputFileName ) return;
		EntityMatrixJoiner emj = (EntityMatrixJoiner) matrices.remove( new File( inputFileName ).getCanonicalPath().intern() );
		loadTimes.remove( new File( inputFileName ).getCanonicalPath().intern() );
		if ( null != emj ) emj.releaseMatrixFromMemory();
	}
	
	public static long getLoadTime( String inputFileName ) {
		Long l = null;
		try { l = (Long) loadTimes.get( new File( inputFileName ).getCanonicalPath().intern() ); } catch (IOException e) {}
		if ( null == l ) return 0;
		return l.longValue();
	}

	// No logical table involved - straight access to local data only (in-mem)
	public EntityAssociations() throws Exception {
		this( new Integer(-1) );
	}
	
	public EntityAssociations( Integer inputGroupSize ) throws Exception {
		this( inputGroupSize, null );
	}
	
	public EntityAssociations( String logicalTable ) throws Exception {
		this( null, logicalTable );
	}
	
	public EntityAssociations( String logicalTable, Integer inputGroupSize ) throws Exception {
		this( inputGroupSize, logicalTable );
	}
	
	public EntityAssociations( Integer inputGroupSize, String logicalTable ) throws Exception {
		
		super();
		
		logger.logInfo("Constructor: new EntityAssociations(" + logicalTable + ", " + inputGroupSize + ")" );
		
		setArgs( logicalTable, null == inputGroupSize ? -1 : inputGroupSize.intValue() );
	}
	
	private void setArgs( String logicalTable, int inputGroupSize ) throws Exception {
		
		this.inputFile = GaianDBConfig.getVTIProperty( EntityAssociations.class, PROPERTY_ENTITY_ASSOCIATIONS_INPUT_FILE );
		this.logicalTable = logicalTable;	
				
//		if ( null == inputGroupSize ) {
//			EntityMatrixJoiner emj = (EntityMatrixJoiner) matrices.get( new File( outputFile ).getCanonicalPath().intern() );
//			if ( null != emj ) restrictedGroupSize = emj.getNumGroups();
//			else restrictedGroupSize = -1; // Not defined
//		} else
//			restrictedGroupSize = inputGroupSize.intValue();
		
		restrictedGroupSize = inputGroupSize;		
		
		int numEntityColumns = 1 < restrictedGroupSize ? restrictedGroupSize :
			Integer.parseInt(GaianDBConfig.getVTIProperty( EntityAssociations.class, PROPERTY_ENTITY_ASSOCIATIONS_GROUP_SIZE ));
		
		StringBuffer cols = new StringBuffer( GROUPZISE_COLNAME + " INT" );	
		for ( int i=1; i<=numEntityColumns; i++ )
			cols.append( ", " + ENTITY_COLNAME_PREFIX + i + " INT" );
		
//		try {		
//
//					
//		} catch ( Exception e ) {
//			logger.logException("Error constructing EntityAssociations()", e);
//		}
		
		rsmd = new GaianResultSetMetaData( cols.toString() );
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setArgs(java.lang.String[])
	 */
	public void setArgs(String[] args) throws Exception {
		
//		if ( 1 < args.length )
//			setArgs( args[0], Integer.parseInt( args[1] ) );
		
		if ( 0 < args.length ) {
			String[] elmts = args[0].split(" ");
			if ( 1 < elmts.length )
				setArgs( elmts[0], Integer.parseInt( elmts[1] ) );
			else if ( 0 < elmts.length )
				setArgs( elmts[0], -1 );
		}
		else
			setArgs( null, -1 );
	}
	
	/* (non-Javadoc)
	 * @see com.ibm.gaiandb.GaianChildVTI#setExtractConditions(org.apache.derby.iapi.store.access.Qualifier[][], int[])
	 */
	public void setExtractConditions(Qualifier[][] qualifiers, int[] logicalProjection, int[] columnsMapping) {
		// No need to set qualifiers - the search string acts as a filter instead.
		// Also ignore mappedColumns as column names are expected to be the same in the logical table. 
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#executeAsFastPath()
	 */
	public boolean executeAsFastPath() throws StandardException, SQLException {
		
		logger.logInfo("EntityAssociations.executeAsFastPath()");
		
		try {
			emj = (EntityMatrixJoiner) matrices.get( new File( inputFile ).getCanonicalPath().intern() );
		} catch (IOException e1) {
			logger.logWarning(GDBMessages.DSWRAPPER_ENTITYMATRIXJOINER_GET_WARNING, "Unable to get EntityMatrixJoiner from fileName, cause: " + e1);
		}
		
		if ( null == emj ) {
			if ( null == logicalTable ) {
				String msg = "NO LOADED ENTITY ASSOCIATIONS FOR: " + inputFile;
				logger.logWarning(GDBMessages.DSWRAPPER_NO_ENTITIY_ASSOC, msg );
				return true;
			}
			logger.logInfo("No in-mem associations for local file " + inputFile + " - they will be loaded as part of distributed qry");
		}
		
		try {
			// If this is a Logical Table query, we want to query more than just the local file.
			// We want the local matrix plus all matrices from other connected nodes.
			if ( null != logicalTable ) {
				
				String query = "select * from new com.ibm.db2j.GaianTable('" +
					logicalTable + ( 1<restrictedGroupSize ? "', 'EntityAssociationsVTIARGS=" + restrictedGroupSize + "'" : "" ) + "') GT";
//					"', '', '', '" + ( null == emj ? "" : GaianDBConfig.getGaianNodeID() ) + "') GT";
				
				// Run a distributed query to get the rows from other nodes - pass in our table definition so that
				// the local node can just act as a gateway to the others (as it needs the meta-data for this).
				// The database driver for the local derby is already loaded
				
				String cdetails = GaianDBConfig.getLocalDerbyConnectionID();
				Stack<Object> connectionPool = DataSourcesManager.getSourceHandlesPool( cdetails );
				
				GaianChildVTI nodesRows = new GaianChildRSWrapper(
//					DriverManager.getConnection( "jdbc:derby://localhost:" + 
//						GaianDBConfig.getDerbyServerListenerPort() + "/" + GaianDBConfig.getGaianNodeDatabasePath(),
//						GaianDBConfig.getGaianNodeUser(), GaianDBConfig.getGaianNodePassword())
						DataSourcesManager.getPooledJDBCConnection( cdetails, connectionPool )
						.createStatement().executeQuery( query ) );
				
				
				
	//			emj.clone();
				if ( null == emj ) {
					int numEntities = Integer.parseInt(GaianDBConfig.getVTIProperty( EntityAssociations.class, PROPERTY_ENTITY_ASSOCIATIONS_NUM_ENTITIES ));
					int maxGroupSize = Integer.parseInt(GaianDBConfig.getVTIProperty( EntityAssociations.class, PROPERTY_ENTITY_ASSOCIATIONS_GROUP_SIZE ));
					emj = new EntityMatrixJoiner( nodesRows, numEntities, maxGroupSize );
				} else
					emj.mergeGaianChildRows( nodesRows );
				
				emj.processJoins();
			}
		} catch ( Exception e) {
			logger.logException(GDBMessages.DSWRAPPER_DIST_QUERY_PROCESS_ERROR, "Unable to process distributed query and/or its results", e);
		}
		
		// restrictedGroupSize must be >= 2 - Ignore it if it isn't
		if ( 1 < restrictedGroupSize )
		try { emj.setGroupSizeRestriction( restrictedGroupSize ); } 
		catch ( Exception e ) {	logger.logException(GDBMessages.DSWRAPPER_RESTRICTED_GPSIZE_SET_ERROR, "Unexpected Exception whilst setting restricted group size", e); };
		
		index = 0;
		
		return true;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#nextRow(org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		
		if ( null == emj ) return IFastPath.SCAN_COMPLETED; // emj wasn't set because no corresponding rows were loaded
		
		try { 
			if ( index >= emj.getNumGroups() ) {
				// Keep track of row count for this search string
				Long previousCount = estimatedRowCounts.get( logicalTable );
				if ( null == previousCount || index > previousCount.longValue() )
					estimatedRowCounts.put( logicalTable, new Long( index ) );
				if ( !matrices.containsKey( new File( inputFile ).getCanonicalPath().intern() ) ) {
					// Clear mem ASAP before a new load (dont reset index) - this means this VTI does not support multiple instantiations
					emj.releaseMatrixFromMemory();
					emj = null;
				} else
					index = 0;
				return IFastPath.SCAN_COMPLETED;
			}
			
			int groupHead = emj.getGroupHead( index++ );
			int[] row = emj.getGroupRow( groupHead );
			
			dvdr[0].setValue( row[0]+1 ); // the count
			dvdr[1].setValue( groupHead ); // the head entity
			
			for ( int i=2; i<dvdr.length; i++ ) // ...and all its connected entities
				dvdr[i].setValue( row[i-1] );
			
		} catch ( Exception e ) { logger.logException(GDBMessages.DSWRAPPER_NEXT_ROW_GET_ERROR, "Unexpected Exception whilst getting next row", e); return IFastPath.SCAN_COMPLETED; };
		
		return IFastPath.GOT_ROW;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#currentRow(java.sql.ResultSet, org.apache.derby.iapi.types.DataValueDescriptor[])
	 */
	public void currentRow(ResultSet arg0, DataValueDescriptor[] arg1) throws StandardException, SQLException {
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.IFastPath#rowsDone()
	 */
	public void rowsDone() throws StandardException, SQLException {
		close();
	}
	
    /**
     *  Provide the metadata for the query against the given table. Cloudscape
	 *  calls this method when it compiles the SQL-J statement that uses this
	 *  VTI class.
     *
     *   @return the result set metadata for the query
	 * 
     *   @exception SQLException thrown by JDBC calls
     */
    public ResultSetMetaData getMetaData() throws SQLException {
		
		logger.logInfo("EntityAssociations.getMetaData()");
		return rsmd;
    }

	/**
	 * Explicitly closes this PreparedStatement class. (Note: Cloudscape
	 * calls this method only after compiling a SELECT statement that uses
	 * this class.)<p>
	 *
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() {
		logger.logInfo("EntityAssociations.close()");
		reinitialise();
	}
	
	@Override
	public boolean reinitialise() {
		logger.logInfo("EntityAssociations.reinitialise()");
		index = 0;
		return true;
	}
	
	public boolean isBeforeFirst() {
		return 0 == index;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedRowCount(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
	
		Long l = estimatedRowCounts.get( logicalTable );
		double val = null == l ? 100 : l.doubleValue();
		
		logger.logInfo("getEstimatedRowCount() returning: " + val);
		return val;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#getEstimatedCostPerInstantiation(org.apache.derby.vti.VTIEnvironment)
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException {
		int rc = 0;
		logger.logInfo("getEstimatedCostPerInstantiation() returning: " + rc);
		return rc;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.vti.VTICosting#supportsMultipleInstantiations(org.apache.derby.vti.VTIEnvironment)
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException {
		// For us to return true, we must ensure that results can be fetched repeatedly.
		// i.e. that the cursor is set back to the first row every time we reach the end of it.
		boolean rc = true;
		
		logger.logInfo("supportsMultipleInstantiations() returning: " + rc);
		return rc;
	}

	public boolean pushProjection(VTIEnvironment arg0, int[] arg1) throws SQLException {
		return false;
	}

	public void setQualifiers(VTIEnvironment arg0, Qualifier[][] arg1) throws SQLException {
	}

	public boolean fetchNextRow(DataValueDescriptor[] row) throws Exception {
		return false;
	}

	public int getRowCount() throws Exception {
		return emj.getNumGroups();
	}

	public boolean isScrollable() {
		return false;
	}
}
