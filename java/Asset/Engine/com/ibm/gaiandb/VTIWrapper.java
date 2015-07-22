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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.TypeId;

import com.ibm.gaiandb.DataSourcesManager.RecallingStack;
import com.ibm.gaiandb.diags.GDBMessages;


/**
 * @author DavidVyvyan
 */
public abstract class VTIWrapper implements Runnable { //implements Cloneable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "VTIWrapper", 35 );
	
	private static final int IN_MEMORY_ROWS_INITIAL_SIZE = 1000000; // 1 million rows, will be trimmed after load.
	
	protected final String nodeDefName;
	protected final String sourceID;
	
	protected int numExecutingThreads = 0;
//	private boolean isQuiesceExecs = false;
	protected final Object reinitLock = new Object();
	
	protected GaianResultSetMetaData logicalTableRSMD;
	
	// pColTypes is currently used only for RDBMS sources - in case casting is required when reconstructing SQL where clause predicates.
	// Reset when the config file has changed or potentially the physical source has changed.
	// It represents the types of the physical columns mapped against the logical table column ids.
	// It is not required for files, in-memory rows or other non-rdbms sources because predicate comparisons in these cases are
	// handled by GAIANDB which uses the DataValueDescriptor classes to do type casting implicitly.
	private int[] pColTypes = null;
	
	// Mapping of logical column id (e.g. 0, 1, 2) -> physical column name.
	// Only reset when the config file is changed.
	// This mapping obviously only includes columns that exist (under their mapped name) on this physical node.
	// Therefore this represents all column names dealt with by this VTIWrapper.
	// When there is no logical to physical name mapping, the physical col name defaults to that of the logical col definition.
	// Where the physical col name does not exist on the source, the columnsMapping variable below will set the mapped physical col
	// id to the last column+1 which is set to the DataValueDescriptor value: new SQLChar().getNewNull()... meaning the column will
	// look as if it was just a null, even though in fact it doesn't even exist.
	private String[] colNames = null;
	
	// Mapping of logical column id -> physical source col id - needed by a VTI resource when extracting a column
	// Reset when either the config file or the physical source have changed.
	// If there is no physical col id for the logical one, it is set to last column id + 1, which holds a SQLChar DataValueDescriptor
	// type with a null value, so that the column will always return a null when queried or compared against.
	private int[] columnsMapping = null;
	
	// Used when the vti manages the columns mapping, e.g. for FileImport or InMemoryRows
	// Represents *all* column names of the physical source only.
	// Only used as a cache for refreshing column mappings when the config properties file has changed.
	private String[] physicalColNames = null;
		
	// State variable used to update all variable config values together
	protected NodeState safeExecNodeState = null;
	
	public int[] getColumnsMappingCurrent() { return null == safeExecNodeState ? null : safeExecNodeState.getColumnsMapping(); }
	
	protected final class NodeState {
		
		private GaianResultSetMetaData logicalTableRSMD = null; // currently only used at exec time by VTIRDBResult - only modified at config re-init
		private int[] pColTypes = null; // Only used by VTIRDBResult
		private String[] colNames = null; // currently only used at exec time by VTIRDBResult - only modified at config re-init
		private int[] columnsMapping = null; // Currently only used at exec time by VTIFile - now also used to build RDBResult queried columns list
		
		public NodeState( GaianResultSetMetaData logicalTableRSMD, 
				int[] pColTypes, String[] colNames, int[] columnsMapping ) {
			this.logicalTableRSMD = logicalTableRSMD;
			this.pColTypes = pColTypes;
			this.colNames = colNames;
			this.columnsMapping = columnsMapping;
		}
		
		String[] getColNames() {
			return colNames;
		}
		
		int[] getPhysicalColTypes() {
			return pColTypes; // when this method returns null, the alternative getColTypes must be called
		}
		
		// If the column is explicitly mapped in config, then it may be a composite expression, e.g. function call.
		// VTIRDBResult uses this to decide whether it can double quote a column name - in cases where it does not match standard identifier string pattern.
		boolean isColumnMappingExplicitlyDefinedInConfig( int ltPcolIdx ) {
			// For VTIRDBResult, columnsMapping holds -1 for all columns that are explicitly mapped and therefore might hold an expression like a function call.
			// Missing columns have mapping value == numCols (i.e. out of range)
			return null != columnsMapping && -1 == columnsMapping[ltPcolIdx];
		}
		
		int[] getPhysicalColTypes( Connection c, String tableWithNoWhereClause ) throws SQLException {
			
			// colTypes is set to null when the config file has changed, therefore immediately after re-initialisation -
			// So no need to worry about currently executing threads in the following initialisation code.
			// This must be done here rather than during re-initialisation because we don't know when the connection will
			// be available (as it is obtained in a separate thread) -
			if ( null == pColTypes ) {
				
				logger.logThreadInfo( "VTIWrapper Synchronising on Re-init lock (LT reload was triggered)" );
				
				// If some other thread is already re-initialising the structures then wait here.
				synchronized( reinitLock ) {
					// Check pColTypes again - if another thread got here first it will have reset it.
					if ( null == pColTypes ) {
						ResultSetMetaData physicalTableRSMD = null;
						try {
//							 Do not use getMetaData() on a non executed prepared statement - Oracle does not allow it
//							physicalTableRSMD = c.prepareStatement( "select * from " + table ).getMetaData();
							
							logger.logInfo("Finding columns md for expression: " + tableWithNoWhereClause);
							ResultSet rs = c.createStatement().executeQuery(
									"select * from " + tableWithNoWhereClause + " where 0=1");
							physicalTableRSMD = rs.getMetaData();
							
						} catch ( SQLException e ) {
							String msg = "Unable to get physical meta data for table: " + tableWithNoWhereClause + ": " + e;
							throw new SQLException(msg);
						}
						if ( null != physicalTableRSMD ) {
							VTIWrapper.this.refreshColumnsMappingSetPhysicalColTypes( physicalTableRSMD );
							columnsMapping = VTIWrapper.this.columnsMapping; // needed in case in-mem rows is set from the start
							pColTypes = VTIWrapper.this.pColTypes;
//							logger.logThreadDetail("getPhysicalColTypes(): columnsMapping: " + Util.intArrayAsString(columnsMapping) +
//									", pColTypes: " + Util.intArrayAsString(pColTypes));
						}
					}
				}
			}
			
			return pColTypes;
		}
		
		int[] getColumnsMapping() {
			// On startup, columnsMapping may not have been set if rows were being loaded in memory
			if ( null == columnsMapping ) columnsMapping = VTIWrapper.this.columnsMapping;
			return columnsMapping;
		}
		
		int[] getColumnsMapping( int[] incomingColsMapping ) {
			
			if ( null==incomingColsMapping ) return getColumnsMapping();
			
			logger.logThreadInfo("Mappings: ltold->ltnew: " + Util.intArrayAsString(incomingColsMapping) +
					"; ltnew->pt: " + Util.intArrayAsString(columnsMapping));
			
//			int[] newmap = new int[ columnsMapping.length ];
			int[] newmap = new int[ incomingColsMapping.length ];
			
			for ( int i=0; i<incomingColsMapping.length; i++ )
//				if ( incomingColsMapping.length != incomingColsMapping[i] && incomingColsMapping[i] < columnsMapping.length )
				newmap[i] = -1 == incomingColsMapping[i] ? physicalColNames.length : columnsMapping[ incomingColsMapping[i] ];
			
//			for ( int i=0; i<incomingColsMapping.length; i++ )
//				if ( -1 != incomingColsMapping[i] )
//					incomingColsMapping[i] = columnsMapping[ incomingColsMapping[i] ];

			logger.logThreadInfo("Combined mapping ltold->pt: " + Util.intArrayAsString(newmap));

			return newmap;
//			return incomingColsMapping;
		}
		
		GaianResultSetMetaData getLogicalTableRSMD() {
			return logicalTableRSMD;
		}
		
		void refreshColumnsMapping( ResultSetMetaData ptrsmd ) throws SQLException {
			VTIWrapper.this.refreshColumnsMapping( ptrsmd );
			columnsMapping = VTIWrapper.this.columnsMapping;
		}
		
		boolean isColumnMissingInPhysicalSource( int ltColIndex ) {
//			if ( null != physicalColNames ) {
//				logger.logThreadDetail("isColumnMissingInPhysicalSource(): colsMapping["+ltColIndex+"]=" + columnsMapping[ltColIndex] +
//						", physicalColNames " + Arrays.asList(physicalColNames) + ", logicalTableRSMD: " + logicalTableRSMD);
//			}
			return null == physicalColNames ? false : physicalColNames.length <= columnsMapping[ltColIndex];
		}
		
		String[] getPhysicalColumnNames() { return physicalColNames; }
	}

	// Does config specify this node to have its rows kept in memory ?
	protected boolean isRowsInMemory = false;
	
	// Rows, each is a DataValueDescriptor[]
	protected ArrayList<DataValueDescriptor[]> inMemoryRows = null;

	// Hashtable of physical source col id -> logical col type
	// Used to order the tree map index appropriately
	private ConcurrentMap<Integer, Integer> inMemIndexColTypes = null;
//	protected int[] inMemoryRowsLatestIndexCols = null; // Reset to null immediately after load.

	// Hashtable of: Physical source columnID -> TreeMap of ordered rows
	protected ConcurrentMap<Integer, SortedMap<DataValueDescriptor, Object>> inMemoryRowsIndexes = new ConcurrentHashMap<Integer, SortedMap<DataValueDescriptor, Object>>();
	
		
	/**
	 * @param s
	 * @throws SQLException
	 * @throws Exception
	 */
	public VTIWrapper( String sourceID, String nodeDefName ) throws SQLException {
		
		this.sourceID = sourceID;
		this.nodeDefName = nodeDefName;
		
//		genericReinitialise();
	}
	
	public String getSourceID() { return sourceID; }
	
//	/**
//	 * If the meta data was not prevoiously set, this method must be called before a call to execute()
//	 * 
//	 * @param rsmd
//	 */
//	public void setMetaData( GaianResultSetMetaData rsmd ) {
//		logicalTableRSMD = rsmd;
//	}
	
//    public Object clone() {
//        try {
//        	// The clone method is only used for building dynamic nodes (gateway or subquery nodes) efficiently
//        	// The clones must be reinitialised to ensure the columns mapping info is current.
//        	// Later improvement on this would be to cache vtis so immediate lookups can be made when using the same meta-data
//        	// repeatedly (i.e. for previously seen subqueries or propagated table definitions)
//            VTIWrapper vti = (VTIWrapper) super.clone();
//            
//            // Keep current state - all that is required is the physicalColNames to avoid reloaded
//            
//            // pColTypes only applies to VTIRDBResult wrappers.
//        	vti.pColTypes = null; // This will trigger a columns meta data refresh via refreshColumnsMappingSetColTypes()
//        	        	
//        	// colNames are reloaded by reinitialise method, and setting physicalColNames here will allow columnsMapping 
//        	// to be loaded as well. If physical source has changed then physicalColNames will be reloaded too.
//        	vti.physicalColNames = new String[ physicalColNames.length ];
//        	System.arraycopy( physicalColNames, 0, vti.physicalColNames, 0, physicalColNames.length );
//
////        	vti.colNames = new String[ colNames.length ];
////        	System.arraycopy( colNames, 0, vti.colNames, 0, colNames.length );
////        	vti.columnsMapping = new int[ columnsMapping.length ];
////        	System.arraycopy( columnsMapping, 0, vti.columnsMapping, 0, columnsMapping.length );
////        	ArrayList inMemoryRows = null; // DataValueDescriptor[][]
////        	Hashtable columnIndexTypes = null;
////        	Hashtable inMemoryRowsIndexes = new Hashtable();
//            
//            return vti;
//            
//        } catch (CloneNotSupportedException e) {
//            logger.logException( "Error: Unexpected Exception caught whilst cloning VTIWrapper: ", e );
//        }
//        return null;
//    }
//	
//	private static String[] getColNamesFromResultSetMetaData( ResultSetMetaData rsmd ) throws SQLException {
//		
//		int ptColCount = rsmd.getColumnCount();
//		String[] colNames = new String[ ptColCount ];
//		for ( int i=0; i<ptColCount; i++ )
//			colNames[i] = rsmd.getColumnName(i+1);
//		
//		return colNames;
//	}
	
//	protected int[] getPhysicalProjection( int[] logicalProjection ) {
//		
//		if ( null == logicalProjection ) return null;
//		
//		int len = logicalProjection.length;		
//		int[] physicalProjection = new int[len];
//		for ( int i=0; i<len; i++ )
//			physicalProjection[i] = columnsMapping[ logicalProjection[i] ];
//		
//		return physicalProjection;			
//	}	

	protected void refreshColumnsMappingSetPhysicalColTypes( ResultSetMetaData ptrsmd ) throws SQLException {
		
		if ( null == ptrsmd ) return; // null;
		
		logger.logInfo(nodeDefName + " Getting colmappings to derive LT's physical col types for poss casts required later");
		refreshColumnsMapping(ptrsmd);
		
		final int colCount = columnsMapping.length;
		final int ptColCount = physicalColNames.length; // all the physical source columns
		pColTypes = new int[colCount];
		try {
			for( int i=0; i<colCount; i++ ) {
				int pcol = columnsMapping[i];
				// no need to honour logical table type when physical source column does not exist - Use Types.NULL instead.
				pColTypes[i] = ptColCount == pcol ? Types.NULL : ptrsmd.getColumnType( pcol+1 );
//				pColTypes[i] = ptColCount == pcol ? logicalTableRSMD.getColumnType(i+1) : ptrsmd.getColumnType( pcol+1 );
			}
			logger.logInfo(nodeDefName + " Got logical table's physical column types: " + Util.intArrayAsString(pColTypes));
			
		} catch ( SQLException e ) {
			logger.logWarning(GDBMessages.ENGINE_COLUMN_TYPES_GET_ERROR, nodeDefName + " Failed to get all column types (ignoring), cause: " + e);
		}
	}
	
	protected void refreshColumnsMapping( final ResultSetMetaData ptrsmd ) throws SQLException {
		if ( null==ptrsmd )
			physicalColNames = null;
		else {
			
			int ptColCount = ptrsmd.getColumnCount();
			physicalColNames = new String[ ptColCount ];
			for ( int i=0; i<ptColCount; i++ )
				physicalColNames[i] = ptrsmd.getColumnName(i+1);
			
			refreshColumnsMapping();
		}
	}
	
	private void refreshColumnsMapping() { //throws SQLException {
		
		if ( null==physicalColNames ) return;
		
		// The column count - map all physical columns of the logical table wrapping this node.
		final int ltPhyscialColCount = logicalTableRSMD.getPhysicalColumnCount(); // just the physical cols present in the logical table
		final int ptColCount = physicalColNames.length; // all the physical source columns
		columnsMapping = new int[ ltPhyscialColCount ];
		int j=0;
		// Determine if the logical column names should be used for the physical ones (i.e. ignore physical ones)
		boolean isMapByPosition = GaianDBConfig.isColumnNamesMirroredFromLTDef( nodeDefName );

		logger.logThreadInfo( nodeDefName + " Mapping logical to physical col ids. Resolving " +
				ltPhyscialColCount + " logical col names for " + ptColCount + " cols of the physical source. isMapByPosition ? " + isMapByPosition);
		
		// For every required configured physical column name in colNames[] (which is ordered by logical col id),
		// find if the physical column actually exists in the physical source meta data. If so, map its index to the logical one in columnsMapping[].
		for ( int i=0; i<ltPhyscialColCount; i++ ) {
			
			String columnName = colNames[i]; // getColumnName( nodeDefName, i, ltrsmd );
			
			if ( 0 == columnName.length() )
				j = ptColCount; // Column name was mapped to an empty physical col name. This means the user wants to return null for this column
//			else if ( Util.stringsContainCommonChars( columnName, "\"(+-*/%&!<>?|{[" ) )
			else if ( this instanceof VTIRDBResult && GaianDBConfig.isColumnMappingExplicitlyDefined(nodeDefName, i) ) {
				// Override column name lookup for explicitly defined column mappings - so we can allow functions etc. to be specified as well...
				columnsMapping[i] = -1; // for VTIRDBResult, this means there might be mapping to a physical column expression - e.g. function call - but not seen as "missing" in GaianChildRSWrapper
				continue;
			}
			else if ( isMapByPosition ) {
				// Mirrored LT Map (i.e. physical schema is ignored because 'MAP_COLUMNS_BY_POSITION' is set in config)
				// Note that any columns explicitly defined will take precedence over the positional mapping (i.e. previous 'if' condition above)
				columnsMapping[i] = i; // if ( i >= ptColCount ) then the column will be ignored (i.e. nulled out)
				if ( this instanceof VTIRDBResult && i < ptColCount ) {					// For a mirrored map, column names to be extracted from the node's source are chosen by position.
					// This must be a back end data source and a straight column name (not function)
					// DRV 13/11/2012: No need to wrap in double quotes here because getColumnNamesAsCSV() will do this when necessary
					// in VTIRDBResult. It is best to keep the column name in its original format until the last minute when we build the select expression. 
					colNames[i] = physicalColNames[i];
					logger.logDetail(nodeDefName + " Mapped logical column " + columnName + " to RDBMS column " + colNames[i]);
				}
				continue;
			}
			else {
				logger.logDetail( nodeDefName + " Searching for column name " + columnName + " in " + ptColCount + " pcols");// of " + ptrsmd.getColumnCount() + " pcols" );

				// Find the index j of the mapped logical column name in the physical table
				for ( j=0; j<ptColCount; j++ ) {
					
					logger.logDetail( nodeDefName + " Checking physical col " + (j+1) + "... -> physicalColName = " + physicalColNames[j] );
					
					if ( physicalColNames[j].equalsIgnoreCase( columnName ) ) {
						logger.logThreadInfo( nodeDefName + " Found that mapped column " +
								logicalTableRSMD.getColumnName(i+1) + "->" + columnName + " is physical colindex " + (j+1) );
						columnsMapping[i] = j; break;
					}
				}
			}
			
			if ( j == ptColCount ) {
				// Column not found in physical table
				logger.logThreadInfo( nodeDefName + " Note: Undefined column name '" + columnName + "' for leaf node: " +
						nodeDefName + ". The Data Source for this node will return a null in this column");
//						nodeDefName + ". This node will return null ResultSet for queries that reference this column");
				// Use negative col index to signify this column is not referenced in the physical table
//				columnsMapping[i] = -1;
				columnsMapping[i] = j; // use last column which is reserved to contain a null DataValueDescriptor column
			}
		}
	}
	
	public String getNodeDefName() {
		return nodeDefName;
	}
	
	public String toString() {
		return nodeDefName;
	}
	
	/**
	 * Determine whether this VTIWrapper was constructed using the same initial info.
	 * If so it can be re-used (after modification of the properties file).
	 * 
	 * @param s Descriptor String which determines what info the VTIWrapper was built with
	 * @return true if the VTIWrapper's resource properties are equal to s. e.g. if s is the original file name used
	 */
	public abstract boolean isBasedOn( String s );
	
	/**
	 * Get a meaningful String description of the resource that this VTIWrapper is based on.
	 * For DB connections, this returns: <URL> :: <table>, or just <URL> if the table is null.
	 * 
	 * @param 	instanceID - Used for pluralizable data sources - designates instance ID;
	 * 			Otherwise, use null to designate the original config setting (which may have a wildcard in it).
	 * @return
	 */
	public abstract String getSourceDescription( String instanceID );
	
	public String getSourceHandlesSnapshotInfo() {
		Stack<Object> pool = DataSourcesManager.getSourceHandlesPool(sourceID, isRowsInMemory);
		synchronized (pool) {

//			logger.logInfo("Retrieving info for pool attached to: " + sourceID + ", isRowsInMemory: " + isRowsInMemory + ", size: " + pool.size() );
			
//			System.out.println("Pool: " + (pool.isEmpty() ? "null" : pool.peek().getClass().getSimpleName()) + ", sourceid: " + sourceID);
//			Stack pool2 = DataSourcesManager.getSourceHandlesPool(sourceID, !isRowsInMemory);
//			System.out.println("Pool2: " + (pool2.isEmpty() ? "null" : pool2.peek().getClass().getSimpleName()) );
			
//			return pool.isEmpty() ? null : pool.peek().getClass().getSimpleName() + ":" + pool.size();
			
			RecallingStack<Object> rstk = (RecallingStack<Object>) pool;
			Object firstPushedObject = rstk.getFirstPushedObject();
			return null == firstPushedObject ? null : firstPushedObject.getClass().getSimpleName() + ":" + pool.size();
		}
	}
	
	/**
	 * This method is thread safe wrt to the reinitialise method.
	 * This ensures that info such as column mappings and in-memory rows are consistent from start to end of execution.
	 * 
	 * This is achieved by using a safe set of state variables for executions which are all set in one operation
	 * by updating the NodeState wrapper for them.
	 * 
	 * @param arguments
	 * @param qualifiers
	 * @param projectedColumns
	 * @return
	 * @throws Exception
	 */
	protected abstract GaianChildVTI execute( ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns, String table ) throws Exception;

//	public ResultSet execute( Hashtable arguments, Qualifier[][] qualifiers, int[] projectedColumns, String table ) throws Exception {
//	
//		ResultSet rs = null;
//		
////		numExecutingThreads++;
////		
////		if ( isQuiesceExecs ) {
////			logger.logThreadWarning("Node is Re-initialising - execute returning null");
////		} else
//			rs = doExecute(arguments, qualifiers, projectedColumns, table);
//		
////		numExecutingThreads--;
////		
////		logger.logThreadInfo("Checking whether to notify Re-init, NumExecThread " + numExecutingThreads);
////		
////		if ( 1 > numExecutingThreads && isQuiesceExecs ) {
////			logger.logThreadInfo( "VTIWrapper Synchronising on Re-init lock");
////			synchronized( reinitLock ) { reinitLock.notify();	}
////		}
//		
//		return rs;
//	}
//		
	/**
	 * This method is thread safe wrt to the reinitialise method.
	 * This ensures that info such as column mappings and in-memory rows are consistent from start to end of execution.
	 * 
	 * This is achieved by keeping a count of executing threads, and making new threads wait when the reinitialise()
	 * method has been called. When all current executing threads have completed, the reinitilise method
	 * is released from its own wait(), completes and then notifies all threads waiting to execute.
	 *  
	 * @param arguments
	 * @param qualifiers
	 * @param projectedColumns
	 * @return
	 * @throws Exception
	 */	
//	public ResultSet executeWrapper( Hashtable arguments, Qualifier[][] qualifiers, int[] projectedColumns ) throws Exception {
//		
//		ResultSet rs = null;
//		
//		numExecutingThreads++;
//		
////		if ( isReinitPending )
////		synchronized( executorsLock ) { executorsLock.wait(); }
////		numExecutingThreads++;
//		
//		if ( isReinitPending ) {
//			logger.logThreadWarning("Node is re-initialising - execute returning null");
//		} else
//			rs = execute(arguments, qualifiers, projectedColumns);
//		
//		numExecutingThreads--;
//		
//		if ( 1 > numExecutingThreads && isReinitPending )
//			synchronized( reinitLock ) { reinitLock.notify();	}
//		
//		return rs;
//	}
	
	/**
	 * Execute a query against a pre-established table.
	 * The qualifiers describe the predicates - currently only comparison predicates are supported by Derby's VTI interface.
	 * The projected columns designate the columns to be queried.
	 * No sql functions or constructs are currently pushed down by Derby, however we can still pass some function
	 * through the argument list of the GaianTable VTI, such as 'order by' or 'explain', which translate into required
	 * input args here.
	 * 
	 * @param arguments
	 * @param qualifiers
	 * @param projectedColumns
	 * @return
	 * @throws Exception
	 */
	public abstract GaianChildVTI execute( ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns ) throws Exception;
	
	public void reinitialise( GaianResultSetMetaData ltrsmd ) throws Exception {
		
//		!!!!!!!!!!what if this happens whilst we are refreshing columns mapping in execution code ? -> both synchronize on reinitLock so its ok.
		
//		isQuiesceExecs = true;
//		if ( 0 < numExecutingThreads ) {
//			logger.logInfo( nodeDefName + " VTIWrapper Synchronising on Re-init lock, numExecThreads " + numExecutingThreads);
//			synchronized( reinitLock ) {
//				logger.logInfo( nodeDefName + " VTIWrapper Waiting on Re-init lock");
//				try { reinitLock.wait(); }
//				catch (InterruptedException e) { logger.logException( nodeDefName + " Unexpected exception whilst in Re-init wait():", e); }
//			}
//		}
		
		logger.logDetail( nodeDefName + " VTIWrapper Synchronising on Re-init lock in reinitialise()");
		synchronized ( reinitLock ) {
			logicalTableRSMD = ltrsmd;
			genericReinitialise();
			customReinitialise();
			safeExecNodeState = new NodeState( logicalTableRSMD, pColTypes, colNames, columnsMapping );
		}
		
//		isQuiesceExecs = false;
	}

	// Synchronize to make sure no executors are working on old in memory rows.
	// Reinitialise - if InMemoryRows have to be unloaded/reloaded, do it now!
	/**
	 * Reinitialises a VTI node.
	 * Returns whether or not large deep structures were unloaded or reinitialised due to them having been changed.
	 * Large deep structures for now are just the InMemoryRows and indexes on them.
	 */
	protected abstract void customReinitialise() throws Exception;
	
	protected void genericReinitialise() throws SQLException {
		
		if ( null==logicalTableRSMD ) return; // If this VTIWrapper has no column mappings (i.e. GAIAN Node) then return
		
		int numColumns = logicalTableRSMD.getPhysicalColumnCount(); // + logicalTableRSMD.getConstantColumnCount();
		colNames = new String[ numColumns ];
		
		for ( int i=0; i<numColumns; i++ )
			colNames[i] = GaianDBConfig.getColumnName( nodeDefName, i, logicalTableRSMD );

		// Avoid logging columns for discovered nodes as 1) they can get added/removed often, and 2) column names will be the same as LT ones anyway. 
		if ( GaianDBConfig.isNodeDefExists(nodeDefName) )
			logger.logInfo(nodeDefName + " Set physical column names to: " + Arrays.asList( colNames ));
		
		// physicalColNames is only known if the physical source has been loaded - i.e. not on the initial call...
		if ( null != physicalColNames ) {
			
			refreshColumnsMapping(); // refresh due to table definition or col mapping definition change
			reloadInMemoryRowsIndexesIfRowsInMemory(); // Also sets isRowsInMemory
			
		} else
			// Set the flag here so that the customReinitialise() methods kick off the loading process
			isRowsInMemory = GaianDBConfig.isNodeInMemoryOptionSet( nodeDefName ) && false == GaianDBConfig.isNodePluralizedOptionSet( nodeDefName );
		
		pColTypes = null; // Trigger reload of physical source meta-data
	}
	
//	protected abstract void close() throws SQLException;
	public final void close() throws SQLException {
		
//		isQuiesceExecs = true;
//		if ( 0 < numExecutingThreads ) {
//			logger.logThreadInfo( "VTIWrapper close() Synchronising on Re-init lock");
//			synchronized( reinitLock ) {
//				logger.logThreadInfo( "VTIWrapper close() Waiting on Re-init lock");
//				try { reinitLock.wait(); }
//				catch (InterruptedException e) { logger.logException("Unexpected exception whilst in close() Re-init wait():", e); }
//			}
//		}
		
		genericClose();
		
//		isQuiesceExecs = false; // not needed
	}
	
	private boolean isClosed = false;
	private void genericClose() { isClosed = true; clearInMemoryRowsAndIndexes(); dataSourceWrappersExpectedToHaveCachedRows.remove(this); }
	
	// Override these as appropriate in sub-classes
	public boolean isGaianNode() { return false; }
	public boolean isSubQuery() { return false; }
	
	/**
	 * Generic pooling method for data sources.
	 * This retrieval method gets a pooled Object (e.g. JDBC Connection or VTI created with a given prefix argument) based on the sourceID.
	 * GaianDB includes the pooled handles with it's own when reporting them with API: SQL> call listds() 
	 * 
	 * @return pooled handle - this may be created if the pool was empty, which will be the case on first invocation.
	 * @throws Exception
	 */
	protected Object getPooledSourceHandle() throws Exception {
		
		Stack<Object> pool = DataSourcesManager.getSourceHandlesPool( sourceID );
		synchronized( pool ) { if ( !pool.empty() ) return pool.pop(); }
		
		// Note - instead of just waiting indefinitely for creation to succeed below, we may need to wrap this with logic to impose a time-limit on the creation, 
		// beyond which the method returns null but continues to create the handle in the background and eventually pushes it to the pool if successful.
		// This multi-threaded logic is already done for RDBMS connections in DatabaseConnector.java
		return getNewSourceHandleWithinTimeoutOrToSourcesPoolAsynchronously();
	}
	
	/**
	 * To be overridden.
	 * Implementing multi-threaded VTIWrappers should override this method to return a *new instance* of the source handle.
	 * Examples: For an RDBMS this would be a JDBC Connection, for VTIs it is just an instance of the VTI for a given prefix argument.
	 * 
	 * @return new object for accessing the data source, e.g. JDBC Connection or VTI created with a given prefix argument
	 */
	abstract protected Object getNewSourceHandleWithinTimeoutOrToSourcesPoolAsynchronously() throws Exception;
	
	
	
	/**
	 * Re-use a VTIWrapper resource handle once it has been finished with - this gets put back in a pool.
	 * If the handle cannot be reinitialised (e.g. VTI cannot be re-executed), then this method should just close it.
	 *  
	 * @param rows
	 */
	public abstract void recycleOrCloseResultWrapper( GaianChildVTI rows ) throws Exception;
	
	/**
	 * Generic pooling method for data sources.
	 * This recycling method acts on a pooled handle object (e.g. JDBC Connection or VTI created with a given prefix argument).
	 * GaianDB can report pooled handles with API: SQL> call listds()
	 * 
	 * @param sourceHandle
	 * @return
	 * @throws SQLException
	 */
	protected boolean recycleSourceHandleToPool( Object sourceHandle ) {
		
		String rName = null;
		if ( Logger.LOG_LESS < Logger.logLevel )
			rName = sourceHandle.getClass().getSimpleName();
//			rName = rName.substring( rName.lastIndexOf('.')+1 );
		
		Stack<Object> appropriateSourceHandles = DataSourcesManager.getSourceHandlesPool( sourceID, sourceHandle instanceof InMemoryRows );
		
		// Attempt to recycle object - this will fail and return null if pool is maxed out.
		if ( null == appropriateSourceHandles.push( sourceHandle ) ) return false;
		
		if ( Logger.LOG_LESS < Logger.logLevel ) {
			int idx = sourceID.lastIndexOf("'"); // find index of pwd to then exclude it from the logs
			logger.logThreadInfo( nodeDefName +
					" recycleResult(): Stored " + rName +
					" object for " + ( 0 < idx ? sourceID.substring(0, idx) + "'<pwd>" : sourceID ) + " in Pool for re-use. Pool current size/max = " +
					appropriateSourceHandles.size() + "/" + GaianDBConfig.getMaxPoolsizes());
		}
		
		return true;
	}
	
	public boolean isPluralized() { return GaianDBConfig.isNodePluralizedOptionSet(nodeDefName); } // && false == GaianDBConfig.isNodeInMemoryOptionSet(nodeDefName);
	public abstract String[] getPluralizedInstances();
	public boolean supportsEndpointConstants() { return false; }
	public abstract DataValueDescriptor[] getPluralizedInstanceConstants( String dsInstanceID );
	
	abstract GaianChildVTI getAllRows() throws Exception;
	
	protected void clearInMemoryRowsAndIndexes() {
		
		isRowsInMemory = false;
//		System.out.println(nodeDefName + " isRowsInMemory xxx false " + 4 + " " + Thread.currentThread().getName());
//		try {
//			throw new Exception("just printing stack trace");
//		} catch (Exception e) {
//			if ( "LTDAT1_DS0".equals(nodeDefName) )
//				e.printStackTrace();
//		}
		
		if ( null != inMemoryRows ) {
		
			int arraySize = inMemoryRows.size();
			
			inMemoryRows.clear();
			inMemoryRows.trimToSize();
			inMemoryRows = null;
			
			GaianNode.notifyArrayElementsCleared( arraySize );
	
			logger.logThreadInfo(nodeDefName + " Cleared in-memory rows: " + arraySize);
		}

		Set<Integer> indexes = inMemoryRowsIndexes.keySet();
		logger.logThreadInfo( nodeDefName + " Clearing indexes, count " + indexes.size() );
		Iterator<Integer> iter = indexes.iterator();
		while ( iter.hasNext() ) {
			Integer colID = iter.next();
			SortedMap<DataValueDescriptor, Object> index = inMemoryRowsIndexes.remove( colID );
			index.clear();
			index = null;
			logger.logThreadInfo( nodeDefName + " Dropped index on col id " + (colID.intValue()+1) );
		}
		
		if ( null != inMemIndexColTypes ) {
			inMemIndexColTypes.clear();
			inMemIndexColTypes = null;
		}		
	}
	
	private void reloadInMemoryRowsIndexesIfRowsInMemory() {
		
		ConcurrentMap<Integer, Integer> newColumnIndexTypes = 
			GaianDBConfig.getInMemoryIndexes( nodeDefName, colNames, columnsMapping, logicalTableRSMD );
		
		boolean wasRowsInMemory = isRowsInMemory;
		isRowsInMemory = null != newColumnIndexTypes;
		
		if ( !isRowsInMemory )
			dataSourceWrappersExpectedToHaveCachedRows.remove(this);
		
		logger.logThreadInfo( nodeDefName + " INMEMORY setting: " + ( isRowsInMemory ? "ON" : "OFF " ) );
		
		logger.logThreadInfo( nodeDefName + " wasRowsInMemory: " + wasRowsInMemory + 
				", isRowsInMemory: " + isRowsInMemory + ", existing in-mem coltypes: " + (inMemIndexColTypes != null) );

		// Check if there are already rows in memory - (and potentially indexes)
		if ( ! wasRowsInMemory )
			return; // no in-memory rows yet, so can't build indexes (they might be loaded later), and no indexes to reinitialise
		
		// If the INMEMORY setting was removed in the config file, just clear the indexes and return
		if ( ! isRowsInMemory ) {
//			clearInMemoryIndexes();
			return;
		}
		
		// The INMEMORY setting is defined - and rows are in memory
		// If there are previous index defs, then drop whichever ones relate to columns that are not specified in 
		// the index definition anymore, or whose logical type def is different
		if ( null != inMemIndexColTypes ) {

			// Drop any column indexes which aren't configured anymore, or whose column types have changed
			Iterator<Integer> iter = inMemIndexColTypes.keySet().iterator();		
			while ( iter.hasNext() ) {	
				
				Integer colID = (Integer) iter.next();
				
				logger.logThreadInfo( nodeDefName + " Checking if index def still has column with same type, col: " + 
						(colID.intValue()+1) );
				
				if ( ! inMemIndexColTypes.get(colID).equals(newColumnIndexTypes.get(colID)) ) {
					
					// Clear and drop index
					SortedMap<DataValueDescriptor, Object> index = inMemoryRowsIndexes.remove( colID );
					if ( null != index ) { // check if we actually had built the index!
						index.clear();
						index = null;
						logger.logThreadInfo( nodeDefName + " Dropped index on column: " + (colID.intValue()+1) );
					}
				} else {
					logger.logThreadInfo( nodeDefName + " Index already exists for same col/type on col: " +
							(colID.intValue()+1) );
				}
			}
			
			inMemIndexColTypes.clear();
		}

		// Now we can acknowledge the value for the new column index types - and build the new indexes
		inMemIndexColTypes = newColumnIndexTypes;

		Iterator<Integer> iter = inMemIndexColTypes.keySet().iterator();		
		while ( iter.hasNext() ) {
			
			Integer colID = (Integer) iter.next();			
			if ( ! inMemoryRowsIndexes.containsKey(colID) )
				buildNewInMemoryRowsIndex(colID);
		}
	}
	
	private void buildNewInMemoryRowsIndex( Integer colID ) {
		// sort rows according to indexes
		// use TreeMap in setExtractConditions() to filter rows efficiently

		int cid = colID.intValue();
		int keyType = (inMemIndexColTypes.get(colID)).intValue();
		
		logger.logThreadInfo( nodeDefName + " Building new index for physical source column " + (cid+1) + 
				", type " + TypeId.getBuiltInTypeId( keyType ).getSQLTypeName() );
		
		TreeMap<DataValueDescriptor, Object> map = new TreeMap<DataValueDescriptor, Object>();
		
		int len = inMemoryRows.size();
		for ( int i=0; i<len; i++ ) {
			DataValueDescriptor[] row = inMemoryRows.get(i);
			DataValueDescriptor inMemDVD = row[cid];
//			Object inMemCol = row[cid];
			
//			if ( null == inMemCol ) {
			if ( inMemDVD.isNull() ) {
				logger.logThreadWarning( GDBMessages.ENGINE_PHYSICAL_SRC_COLUMN_VALUES_NULL, nodeDefName + " Physical source column " + (cid+1) + 
						" contains null values - skipping this column for indexing" );
				return;
			}
			
			DataValueDescriptor dvd = RowsFilter.constructDVDMatchingJDBCType( keyType );
//			Object ltcol = RowsFilter.convertToType(inMemCol, keyType);
			
			try {
				dvd.setValue( inMemDVD );
			} catch (StandardException e) {
				logger.logThreadWarning( GDBMessages.ENGINE_PHYSICAL_SRC_COLUMN_CONVERT_ERROR, nodeDefName + " Physical source column " + (cid+1) + 
						" cannot be converted to logical table col type (skipping this column for indexing): " + e );
				return;
			}
			
			Object keyRows = map.get(dvd);
			
			if ( null == keyRows ) {
				// No value for this key yet - just make it a single value entry for now
				map.put( dvd, row );
				continue;
			}
			
			logger.logThreadInfo( nodeDefName + " Duplicate key " + dvd + " detected for column " + (cid+1) + ", creating a linked row entry" );
			
			ArrayList<Object> newRowsEntry = null;
			
			if ( keyRows instanceof DataValueDescriptor[] ) {
				// keyRows is just one row
				newRowsEntry = new ArrayList<Object>();
				newRowsEntry.add( keyRows ); // add the single row previsouly stored for this key
			}
			else {
				newRowsEntry = (ArrayList<Object>)keyRows;
			}
			
			newRowsEntry.add( row );
			
//			if ( map.containsKey(dvd) ) {
//				logger.logWarning( nodeDefName + " Physical source column " + (cid+1) + 
//						" does not contain unique values in each row - skipping this column for indexing" );
//				return;
//			}
			
			map.put( dvd, newRowsEntry );
		}
		
		inMemoryRowsIndexes.put( colID, map );
		
		logger.logThreadInfo( nodeDefName + " Indexes count " + inMemoryRowsIndexes.keySet().size() );

		logger.logThreadInfo( nodeDefName + " Successfully built new index for physical source column: " +
				(cid+1) + ", index size: " + map.size());
	}	
	
	private void loadRowsInMemory() throws Exception {
				
		GaianChildVTI rows = getAllRows();
		ResultSetMetaData rsmd = rows.getMetaData();
//		int numCols = logicalTableRSMD.getColumnCount(); //rsmd.getColumnCount();
//		int numHiddenCols = GaianDBConfig.HIDDEN_COLS.length;

//		int numPhysicalCols = logicalTableRSMD.getPhysicalColumnCount(); //numCols-numHiddenCols;		
		int numPhysicalCols = rsmd.getColumnCount();
		
		// The physical cols may have changed (e.g. if the underling source is a file and was modified) - so refresh the col mappings.
		logger.logThreadInfo( nodeDefName + " Got rows and meta-data from data source. Refreshing physical mappings against logical defs");
		refreshColumnsMapping( rsmd );
		
		DataValueDescriptor[] dvdRow = new DataValueDescriptor[numPhysicalCols+1];
		
		for ( int i=0; i<numPhysicalCols; i++ ) {
			dvdRow[i] = RowsFilter.constructDVDMatchingJDBCType( rsmd.getColumnType(i+1) );
			if ( null == dvdRow[i] ) {
				logger.logWarning( GDBMessages.ENGINE_ROWS_LOAD_COLUMN_ID_ERROR, nodeDefName + " Unable to build InMemoryRows because of the type of physical column id: " + (i+1) );
				isRowsInMemory = false; // unset the in-memory setting to fall back on the source.
				return;
			}
		}
		
		dvdRow[numPhysicalCols] = new SQLChar();  // used as null col for comparisons when col does not exist

		logger.logThreadInfo( nodeDefName + " Clearing and re-loading in-memory rows");
		
		clearInMemoryRowsAndIndexes();
		inMemoryRows = new ArrayList<DataValueDescriptor[]>( IN_MEMORY_ROWS_INITIAL_SIZE ); // ArrayList imrows
		
		// Rows are extracted in their physical form (any different logical tables may federate them)
		
		while ( true ) {
			// Create each row with one extra column left as null, used later for comparisons when a col does not exist
						
			DataValueDescriptor[] nextRow = new DataValueDescriptor[numPhysicalCols+1];
			for ( int i=0; i<numPhysicalCols+1; i++ ) nextRow[i] = dvdRow[i].getNewNull();
			if ( false == rows.fetchNextRow( nextRow ) ) break;
							
			// Quick fix to store objects instead of dvds
//			Object[] nextRow = new Object[numPhysicalCols+1];
//			for ( int i=0; i<numPhysicalCols; i++ )
//				nextRow[i] = nextDvdRow[i].getObject();
			
			// Better fix to just get objects out of the data source in the first place
//			Object[] nextRow = new Object[numPhysicalCols+1];
//			for ( int i=0; i<numPhysicalCols+1; i++ ) nextRow[i] = new Object();
//			if ( false == rows.fetchNextRow( nextRow ) ) break;
			
//			if ( Logger.LOG_ALL == Logger.logLevel )
//			logger.logDetail("Loading in-mem row: " + Arrays.asList(nextRow) );
		    
		    inMemoryRows.add( nextRow );
		}

//		inMemoryRows = (DataValueDescriptor[][]) imrows.toArray( new DataValueDescriptor[imrows.size()][] );
		
		inMemoryRows.trimToSize();
		
		// Do some housekeeping
		GaianNode.notifyArrayElementsAdded( inMemoryRows.size() );
		recycleOrCloseResultWrapper( rows );

		cachedRowsStartTime = System.currentTimeMillis();
		
		if ( Logger.LOG_LESS < Logger.logLevel )
		logger.logThreadInfo( nodeDefName + " Loaded all " + inMemoryRows.size() + " rows in memory for Resource: " + getSourceDescription(null) );
	}
	
	private long cachedRowsStartTime = -1;
	private static final List<VTIWrapper> dataSourceWrappersExpectedToHaveCachedRows = new Vector<VTIWrapper>(); // use Vector as its methods are synchronized
	
	public static final void reloadCachedRowsForAllDataSourceWrappersRequiringIt() {
		long timeNow = System.currentTimeMillis();
		synchronized( dataSourceWrappersExpectedToHaveCachedRows ) {
			for ( VTIWrapper dsw : dataSourceWrappersExpectedToHaveCachedRows ) {
				long expiryDuration = GaianDBConfig.getDataSourceCacheExpirySeconds(dsw.nodeDefName);
				// RELOAD ROWS: 1) if the rows failed to load (e.g. lost network) OR 2) if the expiry duration is set and has passed
				if ( !dsw.isRowsInMemory || -1 < expiryDuration && dsw.cachedRowsStartTime + expiryDuration*1000 < timeNow ) {
//				if ( -1 < expiryDuration && dsw.cachedRowsStartTime + expiryDuration < timeNow ) {
					logger.logInfo( dsw.nodeDefName + " RE-LOADING cached rows " + ( dsw.isRowsInMemory ? "as expiry duration " +
							expiryDuration + "ms has passed since cache start time: " + Logger.sdf.format( new Date(dsw.cachedRowsStartTime) ) :
								"after a previous failed attempt" ) );
					dsw.loadRowsInMemoryAsynchronously(false);
				}
			}
		}
	}
	
	protected final void loadRowsInMemoryAsynchronously() { loadRowsInMemoryAsynchronously(true); }
	
	private final void loadRowsInMemoryAsynchronously( boolean printLog ) {
		isRowsInMemory = false; // rows are not in memory YET!
		if ( printLog )
			logger.logInfo( nodeDefName + " Loading rows in memory asynchronously (using physical source for queries meanwhile)" );
		new Thread( this, (printLog ? "" : Logger.LOG_EXCLUDE) + nodeDefName + " InMemLoader" ).start();
	}
	
	public void run() {
		
		dataSourceWrappersExpectedToHaveCachedRows.remove(this); // No need for other threads to try loading rows while we are...
		
		logger.logThreadInfo( "Synchronizing on Re-init lock" );
		synchronized( reinitLock ) {

			logger.logThreadInfo( "Starting to load in-memory rows" );
			try {
				loadRowsInMemory();
				isRowsInMemory = true;
//				System.out.println(nodeDefName + " isRowsInMemory xxx true " + 1);
				// Reload the indexes
				logger.logThreadInfo( nodeDefName + " Rows loaded in memory. Reloading indexes, count " + inMemoryRowsIndexes.keySet().size() );
				reloadInMemoryRowsIndexesIfRowsInMemory();
				
			} catch (Exception e) {
				logger.logThreadWarning( GDBMessages.ENGINE_ROWS_LOAD_ERROR, "Failed to load rows in memory (disable this source if not reachable), cause: " + e );
//				logger.logThreadException( GDBMessages.ENGINE_ROWS_LOAD_ERROR, "Failed to load rows in memory", e );
				clearInMemoryRowsAndIndexes();
			} finally {
				// Record the fact caching is EXPECTED for watchdog's benefit - even if a failure occured
				if ( false == isClosed )
					dataSourceWrappersExpectedToHaveCachedRows.add(this);
			}
		}
	}
}
