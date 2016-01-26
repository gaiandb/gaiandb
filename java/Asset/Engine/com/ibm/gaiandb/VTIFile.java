/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.Stack;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.db2j.FileImport;
import com.ibm.db2j.GaianTable;
import com.ibm.db2j.tools.ImportExportSQLException;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */
public class VTIFile extends VTIWrapper {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "VTIFile", 30 );
	
	private final String filePathID;
	private final File fileHandle;
	private long fileLastModified;
	
	long timeOfLastPluralizedInstancesResolution = 0L;
	
	/**
	 * @param s
	 * @throws Exception
	 */
	public VTIFile( String fileID, String nodeDefName, GaianResultSetMetaData logicalTableRSMD ) throws Exception {
		
		super( fileID, nodeDefName );
		
		logger.logInfo( nodeDefName + " Building new VTIWrapper File based on: " + fileID );
		
		this.filePathID = fileID;
		this.fileHandle = new File( getPrimaryFileInstancePath() ); // can only run this once fileID has been set
		fileHandle.createNewFile(); // only creates the file if it doesnt already exist
		fileLastModified = 0;
		
//		sourceHandles = DataSourcesManager.getSourceHandlesStackPool( filePath, false );
		
		reinitialise( logicalTableRSMD );
	}

	public GaianChildVTI execute( ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns ) throws Exception {
		return execute( arguments, qualifiers, projectedColumns, null ); // no table arg - not used in the context of files
	}
	
	private void obtainExecLock() {
		numExecutingThreads++;
	}
	
	private void releaseExecLock() {
		if ( 1 > --numExecutingThreads )
			synchronized ( reinitLock ) { reinitLock.notify(); }
	}
	
	private String[] pluralizedInstances = null;
	
	/**
	 * This method computes the new set of instances matching the arguments
	 * expression, which may contain a wildcard, regex, range expression, etc.
	 * The set is cached for a period of time so we don't re-compute the list more often than is necessary.
	 */
	@Override
	public String[] getPluralizedInstances() {
		
		long timeNow = System.currentTimeMillis();
		if ( 5000 < timeNow - timeOfLastPluralizedInstancesResolution ) {

			timeOfLastPluralizedInstancesResolution = timeNow;
			
			pluralizedInstances = Util.findFilesTreeMatchingMask( filePathID, GaianDBConfig.isPluralizedOptionUsingRegex(nodeDefName) ); // this could be time-consuming

			logger.logInfo("Derived pluralized VTIFile instances: " + ( null == pluralizedInstances ? null : Arrays.asList( pluralizedInstances ) ));
		}
		return pluralizedInstances;
	}
	
	private String getPrimaryFileInstancePath() {
		if ( false == isPluralized() ) return filePathID;
		getPluralizedInstances();
		if ( null == pluralizedInstances || 1 > pluralizedInstances.length )
			return filePathID;
		
		return pluralizedInstances[0];
	}
	
	@Override
	public DataValueDescriptor[] getPluralizedInstanceConstants(String dsInstanceID) { return null; }

	@SuppressWarnings("unchecked")
	protected GaianChildVTI execute( ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns,
			String dsInstanceID ) throws Exception {
		
		logger.logInfo("executing VTIFile.. isRowsInMemory? " + isRowsInMemory);
		
		GaianChildVTI result = null;
		
		String filePath = null;
		
		if ( true == isPluralized() && null != dsInstanceID ) {

			filePath = dsInstanceID;
			
		} else {
			
			filePath = filePathID;
			
			long latestLastModifiedTime = fileHandle.lastModified();
			boolean fileWasModified = latestLastModifiedTime != fileLastModified;
			
			// The following code must be safeguarded against mid-reinitialisation
			// Threads that pass here are expected to be using critical resources (in setRowsAndIndexes and setExtractConditions)
			obtainExecLock();
			
			if ( fileWasModified ) {
				
				logger.logThreadInfo( "VTIFile Synchronising on Re-init lock (reload triggered by file modification)");
				
				// A re-initialisation needs doing, or is already in progress but hasn't completed yet.
				// This thread needs to go out of execution mode temporarily to see if it needs to refresh the 
				// in-memory rows and/or the columns mapping
				releaseExecLock();
				
				synchronized( reinitLock ) {
					
					logger.logThreadInfo( "VTIFile WAITING on Re-init lock for execs to complete, numExecThread " + numExecutingThreads);

					// One of threads to wait here will be released on his own to reinit the code below before releasing the others.
					if ( 0 < numExecutingThreads ) reinitLock.wait();
					
					// Only run this for the first thread that comes through here
					// Check if our global time var has been updated since we locked on reinitLock
					latestLastModifiedTime = fileHandle.lastModified();
					if ( latestLastModifiedTime != fileLastModified ) {
					
						if ( isRowsInMemory )
							loadRowsInMemoryAsynchronously();
						
						Stack<Object> sourceHandles = DataSourcesManager.getSourceHandlesPool( filePath );
						synchronized( sourceHandles ) {
							while ( !sourceHandles.empty() ) ((GaianChildVTI) sourceHandles.pop()).close();
						}
						
						if ( null == result ) {
							try { result = new FileImport(filePath); }
							catch ( ImportExportSQLException e ) {
//								System.out.println("Check File: " + filePath);
								logger.logThreadWarning(GDBMessages.ENGINE_FILE_IMPORT_INIT_ERROR_SQL, "Error on reload while importing " + filePath + ": " + e);
								return null;
							}
							logger.logThreadInfo( "Created new FileImport() instance as Pool was empty");
						} else {
							logger.logThreadInfo( "Extracted existing FileImport() instance from Pool");
						}
						
						safeExecNodeState.refreshColumnsMapping( result.getMetaData() );
						
						fileLastModified = latestLastModifiedTime;
						reinitLock.notifyAll(); // There are no other waits for this instance of reinitLock other than above- we notify all these. 
					}
					
					// re-obtain the exec lock as we're about to actually use the critical resource now
					obtainExecLock();
				}
			}	
		}
		
		// Use a try/catch/finally block to ensure the exec lock is always released
		try {
			if ( null == result ) {

				// Take a local snapshot of this value in case it changes mid-execution.
				boolean isInMemRows = isRowsInMemory;
				
				if ( false == isPluralized() ) {
					
					logger.logInfo("Getting handle for file data.. isRowsInMemory? " + isRowsInMemory);
				
					Stack<Object> sourceHandles = DataSourcesManager.getSourceHandlesPool( filePath, isInMemRows );
					
					synchronized( sourceHandles ) {
						if ( ! sourceHandles.empty() ) result = (GaianChildVTI) sourceHandles.pop();
					}
				}
				
				if ( null == result ) {
					
					if ( isInMemRows )
						result = new InMemoryRows();
					else
						try { result = new FileImport(filePath); }
						catch ( ImportExportSQLException e ) {
//							System.out.println("Check File: " + filePath);
							logger.logThreadWarning(GDBMessages.ENGINE_FILE_IMPORT_ERROR_SQL, "Error in exec while importing " + filePath + ": " + e);
							return null;
						}
					
					if ( Logger.LOG_LESS < Logger.logLevel ) {
						String rName = result.getClass().getSimpleName();
//						rName = rName.substring( rName.lastIndexOf('.')+1 );
						logger.logThreadInfo( "Created a new " + rName + "() instance as the Pool was empty");
					}
					
				} else {
					
					if ( Logger.LOG_LESS < Logger.logLevel ) {
						String rName = result.getClass().getSimpleName();
//						rName = rName.substring( rName.lastIndexOf('.')+1 );
						logger.logThreadInfo( "Extracted an existing " + rName + "() instance from the Pool");
					}
				}
			}
			
			if ( result instanceof InMemoryRows )
				((InMemoryRows) result).setRowsAndIndexes( inMemoryRows, inMemoryRowsIndexes );
						
			result.setExtractConditions( qualifiers, projectedColumns,
					safeExecNodeState.getColumnsMapping( (int[]) arguments.get(GaianTable.QRY_INCOMING_COLUMNS_MAPPING) ) );

		} catch ( Exception e ) {
			throw e;
		} finally {
			releaseExecLock();
		}
		
		if ( Logger.LOG_NONE < Logger.logLevel ) {
			String rName = result.getClass().getSimpleName();
//			rName = rName.substring( rName.lastIndexOf('.')+1 );
	    	String prefix = Logger.sdf.format(new Date(System.currentTimeMillis())) + " ---------------> OBTAINED rows from: ";
			logger.logThreadImportant( nodeDefName + " OBTAINED ROWS USING:\n\n" + prefix + rName + "('" + filePath + "')\n" );
		}
		
		return result;
	}
	
//	int getRowCount() throws Exception {
//		logger.logInfo( nodeDefName + " Counting rows in file: " + filePath );
//		Stack sourceHandles = DataSourcesManager.getSourceHandlesStackPool( filePath, false );
//		GaianChildVTI gc = sourceHandles.empty() ? new FileImport(filePath) : (FileImport) sourceHandles.pop();
//		int count = 0;
//		while ( gc.next() ) count++;
//		recycleResult(gc);
//		return count;
//	}
	
	GaianChildVTI getAllRows() throws Exception {
		logger.logThreadInfo( nodeDefName + " Getting all rows from file: " + filePathID );
//		return new com.ibm.db2j.tools.FileImport( filePath );
		Stack<Object> sourceHandles = DataSourcesManager.getSourceHandlesPool( filePathID );
		GaianChildVTI gc = null;
		
		synchronized(sourceHandles) {
			if ( !sourceHandles.empty() ) gc = (FileImport) sourceHandles.pop();
		}
		
		if ( null == gc ) 
			try { gc = new FileImport(filePathID); }
			catch ( ImportExportSQLException e ) {
//				System.out.println("Check File: " + filePathID);
				throw new Exception("Error in getAllRows() while importing " + filePathID + ": " + e);
			}
		
//		int ptColCount = gc.getMetaData().getColumnCount();
//		
//		int[] allColIDs = new int[ ptColCount ];
//		for ( int i=0; i<ptColCount; i++ ) allColIDs[i] = i+1;
//		
//		int[] identityMap = new int[ ptColCount ];
//		for ( int i=0; i<ptColCount; i++ ) identityMap[i] = i;
//		
//		gc.setExtractConditions( null, allColIDs, identityMap );
		
		return gc;
	}
	
	public boolean isBasedOn( String s ) {
		return s.equals(filePathID);
	}
	
	public String getSourceDescription( String dsInstanceID ) {
		return null == dsInstanceID || false == isPluralized() ? filePathID : dsInstanceID;
	}
	
	public void recycleOrCloseResultWrapper( GaianChildVTI result ) throws Exception {		
		if ( result.reinitialise() && false == isPluralized() )
			recycleSourceHandleToPool( result );
	}
	
	/**
	 * This method is called when the user modifies the table configurations.
	 * It re-initialises the max pool size and the state of whether the file's rows
	 * are kept in memory or not.
	 */
	protected void customReinitialise() throws Exception {
		
		timeOfLastPluralizedInstancesResolution = 0L;
		
		if ( null == inMemoryRows && isRowsInMemory ) {

			loadRowsInMemoryAsynchronously();
			
//			try {
//				loadRowsInMemory();
//			} catch (Exception e) {
//				logger.logWarning( nodeDefName + " Unable to load rows in memory: " + e );
//				isRowsInMemory = false;
//				clearInMemoryRowsAndIndexes();
//			}
			
		} else if ( !isRowsInMemory && null != inMemoryRows ) {
			
//			// First swap source handles to the real file handles - to ensure queries can keep being satisfied
//			sourceHandles = DataSourcesManager.getSourceHandlesStackPool( filePath, false );
			
			logger.logInfo( nodeDefName + " Clearing in-memory rows and indexes" );
			clearInMemoryRowsAndIndexes();
		}

		final String primaryFileInstancePath = getPrimaryFileInstancePath();
		Stack<Object> sourceHandles = DataSourcesManager.getSourceHandlesPool( primaryFileInstancePath, isRowsInMemory );
		
		if ( isRowsInMemory ) {
//			loadInMemory();
			if ( sourceHandles.empty() ) {
				sourceHandles.push( new InMemoryRows() );
				logger.logInfo( nodeDefName + " Created initial InMemory rows for Stack Pool");
			} else
				logger.logInfo( nodeDefName + " Initial InMemory rows already exist in Stack Pool");
		} else {
//			 Get a File now
			try {
				if ( sourceHandles.empty() ) {
					sourceHandles.push( new FileImport( primaryFileInstancePath ) );
					logger.logInfo( nodeDefName + " Created initial File handle for Stack Pool");
				} else {
					logger.logInfo( nodeDefName + " Initial File already exists in Stack Pool");
				}
				// Check if file has changed.. if so refresh column mappings
				long latestLastModifiedTime = fileHandle.lastModified();
				if ( latestLastModifiedTime != fileLastModified ) {
					FileImport fi = null;
					synchronized(sourceHandles) {
						if ( !sourceHandles.empty() ) fi = (FileImport) sourceHandles.peek();
					}
					if ( null == fi ) fi = new FileImport( primaryFileInstancePath );
					refreshColumnsMapping( fi.getMetaData() );
					fileLastModified = latestLastModifiedTime;
				}
			} catch ( ImportExportSQLException e ) {
//				System.out.println("Check File: " + primaryFileInstancePath);
				throw new Exception("Error on init while importing " + primaryFileInstancePath + ": " + e);
			}
		}		
	}

	/**
	 * TODO
	 * This method should create a FileImport object within a max time (FileImport instances are usually very quick to create anyway...)
	 * We should re-factor code above to get pooled FileImport objects using the method: getPooledSourceHandle(), which calls this one.
	 */
	@Override
	protected Object getNewSourceHandleWithinTimeoutOrToSourcesPoolAsynchronously() throws Exception {
		return null; // currently not used - we just get connections from the pool using getConnection()
	}
}
