/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;

import com.ibm.db2j.PluralizableVTI;
import com.ibm.gaiandb.diags.GDBMessages;


/**
 * @author DavidVyvyan
 *
 * This VTIWrapper type:
 * 		- *does* now support stack pool management of data handles (e.g. db connection),
 * 		  and therefore provides a recycling method for them.
 * 
 * 		- is based on data source identifier: vtiClassName + prefix argument to the VTI (others use filename or rdbms table),
 * 
 * 		- does not support in memory loading of rows,
 * 
 * 		- does require processing for re-initialisation (after config file updates)
 * 
 * This VTIWrapper just takes a free-form string argument which is passed to the actual physical
 * underlying VTI. Instances of VTIBasic must be disposed of independently after use.
 * 
 */
public class VTIBasic extends VTIWrapper {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "VTIBasic", 30 );
	
	public static final String EXEC_ARG_CUSTOM_VTI_ARGS = "VTIARG";
	
	private final Class<GaianChildVTI> vtiClass;
	private final boolean isSupportsEndpointConstants;
	private final String vtiClassSimpleName;
	private final Object reinitLock = new Object(); // Use dedicated object for locking around columns-mapping reload
	
	private ResultSetMetaData vtiMetaDataMapped = null;
	private String vtiArgs;
	
	@Override
	public boolean supportsEndpointConstants() { return isSupportsEndpointConstants; }
	
	/**
	 * @param s
	 * @throws Exception
	 */
	public VTIBasic( String className, String nodeDefName, GaianResultSetMetaData logicalTableRSMD ) throws Exception {
		
//		super( className + ':' + derivePrefixArgFromCSVArgs( GaianDBConfig.getVTIArguments(nodeDefName) ), nodeDefName );
		super( className + ':' + GaianDBConfig.getVTIArguments(nodeDefName), nodeDefName );
		
		// arguments is the underlying vti class name
		vtiClass = (Class<GaianChildVTI>) GaianNode.getClassUsingGaianClassLoader(className);
//		vtiClass = (Class<GaianChildVTI>) Class.forName(className);
		isSupportsEndpointConstants = PluralizableVTI.class.isAssignableFrom( vtiClass );
		logger.logInfo( nodeDefName + " is a " + vtiClass.getName() );
		
		vtiClassSimpleName = vtiClass.getSimpleName();		
		reinitialise( logicalTableRSMD );
	}
	
	private Map<String, DataValueDescriptor[]> dsInstanceConstants = null;
	
	@Override
	public String[] getPluralizedInstances() {

		String[] dsInstanceIDs = null;
		
		if ( supportsEndpointConstants() ) {
			try {
				GaianChildVTI vtiInstance = vtiClass.getDeclaredConstructor(String.class).newInstance( vtiArgs );
				dsInstanceIDs = ((PluralizableVTI) vtiInstance).getEndpointIDs().toArray(new String[0]);
				dsInstanceConstants = new HashMap<String, DataValueDescriptor[]>( dsInstanceIDs.length );
				for ( String id : dsInstanceIDs ) dsInstanceConstants.put(id, ((PluralizableVTI) vtiInstance).getEndpointConstants(id) );
				vtiInstance.close();
			} catch ( Exception e ) { logger.logException(GDBMessages.ENGINE_RESOLVING_PLURALIZED_VTI_ATTRIBUTES, "Unable to resolve VTI's pluralized instances: ", e); }
		}
		
		return dsInstanceIDs;
	}
	
	@Override
	public DataValueDescriptor[] getPluralizedInstanceConstants(String dsInstanceID) { return null == dsInstanceConstants ? null : dsInstanceConstants.get( dsInstanceID ); }	
	
	public GaianChildVTI execute(ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns) throws Exception {
		return execute( arguments, qualifiers, projectedColumns, null ); // 4th arg table is not used in this context
	}
	
	/**
	 * Instantiates and invokes a VTI data source endpoint.
	 * First, the VTI is instantiated with a 'vtiArgs' String object.
	 * Later, the setArgs() method is called and given a CSV string representation of the 'arguments' map.
	 * Finally the setExtractConditions() method is called with qualifiers (predicates), projected columns and resolved column mappings.
	 * vtiArgs is normally just the static _ARGS property defined in the config file for the data source. However it can also be overridden in 
	 * the SQL by passing them via the GaianTable() arguments list. This is generally redundant though, because all arguments from this list are also passed 
	 * via the VTI's setArgs() method, which also includes more general query arguments from the GaianTable() invocation.
	 * In future, we might pool instances of the VTI, meaning we would only be passing arguments through the setArgs() method for repeated query invocations.
	 * 
	 * The dsInstanceID identifies a "pluralized instance" where each of them (resolved via getPluralizedInstances()) use the same gaiandb data source wrapper,
	 * exercising a same instance of this VTIBasic class concurrently. This reduces configuration required in the config file and reduces memory required too.
	 * The dsInstanceID is only passed to the setArgs() method.
	 * 
	 * @see com.ibm.gaiandb.VTIWrapper#execute(org.apache.derby.iapi.store.access.Qualifier[][], int[])
	 */
	public GaianChildVTI execute(ConcurrentMap<String,Object> arguments, Qualifier[][] qualifiers, int[] projectedColumns, String dsInstanceID) throws Exception {
		
		GaianChildVTI childVTI = null;
		
//		if ( null == sourceHandles )
//			sourceHandles = DataSourcesManager.getSourceHandlesStackPool( arguments, false );
//		
//		if ( sourceHandles.empty() ) {
		
		// If a dynamic arguments value was passed in to GaianTable's arguments against special key: vtiClassSimpleName + EXEC_ARG_CUSTOM_VTI_ARGS, then use it instead
		// of the static _ARGS value taken from the config file. 
		if ( null != arguments ) {
			String dynamicArgs = (String) arguments.get(vtiClassSimpleName + EXEC_ARG_CUSTOM_VTI_ARGS);
			if ( null != dynamicArgs ) {
				// TODO: To support this, the calling code should be the one looking up the appropriate VTIBasic which has a matching prefix argument.
				// We do not want to be changing the prefix argument once this VTIBasic has been constructed - The model assumes this is a "static" property
				// for this VTIBasic and pool of VTI objects.
				// Methods isBasedOn() and reinitialise() provide capability to pool VTIs which can be reinitialised without changing the core "connection"
				// handle described by the prefix argument.
				throw new Exception("Unsupported VTI mode: Cannot use dynamic args for VTIs yet...");
//				vtiArgs = dynamicArgs;
			}
		}
		
		logger.logInfo("Instantiating " + vtiClassSimpleName + " with argument: " + vtiArgs );
		
		childVTI = (GaianChildVTI) getPooledSourceHandle();
		
		if ( null == childVTI ) return null; // Unable to get a pooled handle - reason should have been logged - this data source will be skipped.
		
//		try {
//			// Use getDeclaredConstructor() so that we can put in arguments if we want, and so we can handle instantiation exceptions
//			childVTI = vtiClass.getDeclaredConstructor(String.class).newInstance( vtiArgs );
////			childVTI = (GaianChildVTI) vtiClass.getDeclaredConstructor(String[].class).newInstance(Arrays.asList(vtiArgs)); // (GaianChildVTI) vtiClass.newInstance();
//		} catch ( ClassCastException e ) {
//			logger.logException( GDBMessages.ENGINE_PROCESS_IMPLEMENTATION_ERROR, "Unable to process VTIBasic as it does not implement GaianVTIChild: " + vtiClass.getName(), e );
//			return null;
//		} catch ( Exception e ) {
//			logger.logException( GDBMessages.ENGINE_PROCESS_ERROR, "Unable to process VTIBasic " + vtiClassSimpleName + ":", e );
//			return null;
//		}
		
//		if ( null != arguments ) {
//			String args = (String) arguments.get(vtiClassSimpleName + EXEC_ARG_CUSTOM_VTI_ARGS);
//			childVTI.setArgs( null == args ? new String[0] : new String[] { args } );
//		}
		
		List<String> args = new ArrayList<String>();
		
		// If the VTI is a PluralizableVTI, the 1st arg must be the instance id (even if it's null)
		if ( supportsEndpointConstants() ) args.add( dsInstanceID );
		
		// Pass all global query arguments to the underlying VTI - these arguments originate from the GaianTable() which invoked this VTI. They are different to vtiArgs. 
		if ( null != arguments ) {
			for ( Iterator<String> iter = arguments.keySet().iterator(); iter.hasNext(); ) {
				String key = iter.next();
				args.add( key + '=' + arguments.get(key).toString() );
			}
			String dsWrapperDefaultSchemaProperty = GaianDBConfig.getVTISchema(nodeDefName); // may be overridden if VTI implements getMetaData()
			args.add( DS_WRAPPER_DEFAULT_SCHEMA + '='
					+ ( null != dsWrapperDefaultSchemaProperty ? dsWrapperDefaultSchemaProperty :
						logicalTableRSMD.getColumnsDefinitionExcludingHiddenOnesAndConstantValues() ) );
		}

		childVTI.setArgs( args.toArray( new String[0]) );
		
//		} else {
//			childVTI = (GaianChildVTI) sourceHandles.pop();	
//		}
		
		ResultSetMetaData vtiMetaData = childVTI.getMetaData();
		
		if ( vtiMetaData != vtiMetaDataMapped )
			// refresh the column mappings if necessary - only let one exec thread do this
			synchronized( reinitLock ) { // synch on ds node instance
				if ( false == isColumnNamesAndPositionsIdentical(vtiMetaData, vtiMetaDataMapped) ) {
					safeExecNodeState.refreshColumnsMapping( vtiMetaData );
					vtiMetaDataMapped = vtiMetaData;
				}
			}
		
		childVTI.setExtractConditions( qualifiers, projectedColumns, safeExecNodeState.getColumnsMapping() );
		
		return childVTI;
	}
	
	public final static String DS_WRAPPER_DEFAULT_SCHEMA = "DS_WRAPPER_DEFAULT_SCHEMA";
	
	private static final boolean isColumnNamesAndPositionsIdentical( ResultSetMetaData rsmd1, ResultSetMetaData rsmd2 ) throws SQLException {
		if ( rsmd1 == rsmd2 ) return true;
		if ( null == rsmd1 || null == rsmd2 ) return false;
		int colCount = rsmd1.getColumnCount();
		if ( colCount != rsmd2.getColumnCount() ) return false;
		
		for ( int i=1; i<=colCount; i++ )
			if ( false == rsmd1.getColumnName(i).equals(rsmd2.getColumnName(i)) )
				return false;
		
		return true;
	}

	public boolean isBasedOn( final String vtiClassNameAndArgs ) {
		// can only be re-initialized if the vti class is the same and _ARGS prefix value is the same.
		return null != vtiClassNameAndArgs && vtiClassNameAndArgs.equals( sourceID ); // == vtiClass.getName() + ':' + prefixArg
	}
	
//	public boolean isBasedOn( final String vtiClassNameAndPrefixArgument ) {
//		// can only be re-initialized if the vti class is the same and _ARGS prefix value is the same.
//		return null != vtiClassNameAndPrefixArgument && vtiClassNameAndPrefixArgument.equals( sourceID ); // == vtiClass.getName() + ':' + prefixArg
//	}

	public String getSourceDescription( String dsInstanceID ) {
		
		String vtiArgs = GaianDBConfig.getVTIArguments(nodeDefName);
		if ( null != vtiArgs ) return vtiArgs + ( null==dsInstanceID ? "" : "::" + dsInstanceID );
		
		String kname = vtiClass.getName();
		return kname.substring( kname.lastIndexOf('.')+1 );
	}

	/**
	 * TODO
	 * This method should create a VTI object within a max time, beyond which it should return null but continue to 
	 * create the VTI in the background and then if this eventually succeeds it should put it in the source handles pool.
	 * Ideally, we would use/re-factor the same code in DatabaseConnector to handle both types of handles (Creation of JDBC Connections and VTIs)
	 * 
	 * When the code above requires a VTI, it should call getPooledSourceHandle(), which calls this one.
	 */
	@Override
	protected Object getNewSourceHandleWithinTimeoutOrToSourcesPoolAsynchronously() throws Exception {

//		To get the pool, use: DataSourcesManager.getSourceHandlesPool( sourceID ); ...

		// Use getDeclaredConstructor() so that we can put in arguments if we want, and so we can handle instantiation exceptions
		try { return vtiClass.getDeclaredConstructor(String.class).newInstance( vtiArgs ); }
		catch ( Exception e ) { logger.logException( GDBMessages.ENGINE_PROCESS_ERROR, "Unable to create new VTI: " + vtiClassSimpleName + ":", e ); }
		
		// The VTI class instantiation may take a long time (e.g. creating connectors to remote sources etc) -
		// Therefore we always want to return quickly from this method and  may not have any diagnostics.
		return null;
	}
	
//	public static String derivePrefixArgFromCSVArgs( final String argsCSV ) {
//		if ( null == argsCSV ) return null;
//		int idx = argsCSV.indexOf(',');
//		return 0 > idx ? argsCSV : argsCSV.substring(0, idx);
//	}
	
	protected void customReinitialise() throws SQLException {
		vtiArgs = GaianDBConfig.getVTIArguments(nodeDefName);
//		prefixArg = derivePrefixArgFromCSVArgs( vtiArgs ); // cannot change after construction (sourceID must be fixed - Use of isBasedOn() ensures this)
		logger.logInfo("customReinitialise() derived arguments for VTIBasic " + vtiClassSimpleName + ": " + vtiArgs );
	}

	public void recycleOrCloseResultWrapper(GaianChildVTI result) throws Exception {
		if ( result.reinitialise() && false == isPluralized() )
			recycleSourceHandleToPool( result );
	}

//	protected void doClose() throws SQLException {
//	}

	// Not used - Rows are naturally in memory - in a vector.
	GaianChildVTI getAllRows() throws Exception {
		return null;
	}
}
