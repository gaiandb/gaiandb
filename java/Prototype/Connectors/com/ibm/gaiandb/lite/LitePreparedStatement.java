/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.lite;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;

import com.ibm.db2j.VTI60;
import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.DatabaseConnectionsChecker;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianDBUtilityProcedures;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * This class handles 'call' and 'values' statements respectively for GaianDB procedure and simple function invocations.
 * It also delegates queries when appropriate.
 * 
 * @author DavidVyvyan
 */
public class LitePreparedStatement extends VTI60 implements IFastPath, GaianChildVTI {
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "LitePreparedStatement", 35 );
	
	// Delegate node details, used only if query needs delegating - in which case the associated connection needs closing at the end...
	private String delegateNodeConnectionDetails = null;
		
	// Inner class for looking up a java stored procedure of function
	private class SPFOperation {
		private final Method method;
		private final Object[] args;
		private String returnDefinition;
		
		private SPFOperation(Method method, Object[] args, String returnDefinition) {
			super();
			this.method = method;
			this.args = args;
			this.returnDefinition = returnDefinition;
		}
	}
	
	private final String opName;
	private final String[] args;	
	
	private final ResultSetMetaData rsmd;

	private SPFOperation operation;
	private Object spfResult = null;

//	private static final Map<String, SPFOperation> procedureMethods = new HashMap<String, SPFOperation>();
//	private static final Map<String, SPFOperation> functionMethods = new HashMap<String, SPFOperation>();
    
//	private static final Pattern operationDeclaration = Pattern.compile(
//    		"(?i)(?:!)?CREATE[\\s]+(?:PROCEDURE|FUNCTION)[\\s]+..."
//    );
	
//	static {
//		for all function/procedure declarations, parse operation elements as specified in GAIANDB_API string, then populate Maps
//		String s;
//		Class c = Class.forName(s);
//		c.getMethod(name, parameterTypes);
//	}
	
//	public LitePreparedStatement() {
//	}
//	
//	public boolean execute( String sql ) throws SQLException {
//		if ( null != spfResult ) return false;
//		
//		LitePreparedStatement ps = (LitePreparedStatement) getConnection().prepareStatement(sql);
//		try {
//			return ps.executeAsFastPath();
//		} catch (StandardException e) {
//			logger.logThreadWarning("Exception in LitePreparedStatement.execute(): " + e);
//			throw new SQLException(e);
//		}
//	}
	
    private static final Set<String> allowedOperations = new HashSet<String>() {
        private static final long serialVersionUID = 1L;

        {
            add("maintainConnection2");
            add(DELEGATE_SQL_PROC);
            add("listNet");
        }
    };

    public static boolean isAllowedOperation(String opName) {
        for (String s : allowedOperations) {
            if (s.equalsIgnoreCase(opName)) {
                return true;
            }
        }
        
        return false;
    }

    private static ResultSetMetaData valuesIntRsmd = null;
//    private static ResultSetMetaData valuesStringRsmd = null;
    private static ResultSetMetaData listNetRsmd = null;
    
	private static Class<?> stringClass = null;
	
	public LitePreparedStatement( String opName, String[] args ) throws Exception {
		
		this.opName = opName;
		this.args = args;
		
		if ( opName.equals("values 1") ) {
			
			spfResult = 1;
			rsmd = null == valuesIntRsmd ? valuesIntRsmd = new GaianResultSetMetaData("RC INT") : valuesIntRsmd;
			
		} else if ( opName.equalsIgnoreCase("listNet") ) {
			
			rsmd = null != listNetRsmd ? listNetRsmd : ( listNetRsmd = new GaianResultSetMetaData(
					"hostname "+Util.SSTR+",interface "+Util.SSTR+",description "+Util.MSTR+
					",ipv4 "+Util.SSTR+",broadcast "+Util.SSTR+",NetPrefixLength INT" ) );
		
		} else if ( opName.equalsIgnoreCase("maintainConnection2") ) {
			
			if ( null == stringClass ) stringClass = Class.forName("java.lang.String");
			
			Class<?> c = Class.forName("com.ibm.gaiandb.GaianDBConfigProcedures");
			Method m = c.getMethod("maintainConnection2", new Class[]{ stringClass, stringClass, stringClass, stringClass });
			String rdef = "RC VARCHAR(500)";

			operation = new SPFOperation(m, args, rdef);
			rsmd = new GaianResultSetMetaData(operation.returnDefinition);
			
		} else if ( DELEGATE_SQL_PROC.equals(opName) ) {

			operation = null;
			if ( 1 != args.length ) {
				logger.logThreadWarning(GDBMessages.NETDRIVER_ARGS_NUMBER_INVALID, "Invalid number of arguments "+args.length+
						" detected for '"+DELEGATE_SQL_PROC+"(<sql>)' - ignoring call");
				rsmd = null;
			} else {
				ResultSet rs = delegateSqlToNearestCapableNode( args[0] );
				rsmd = null == rs ? null : rs.getMetaData();
				spfResult = rs;
			}
			
		} else {
			// need to execute procedures to obtain an rsmd
			
			operation = null;
			rsmd = null;
		}
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if ( isClosed ) throw new SQLException("Cannot complete getMetaData(): LitePreparedStatement is closed");
		return rsmd;
	}
	
	@Override
	public boolean execute() throws SQLException {
		return executeAsFastPath();
	}

	public boolean executeAsFastPath() throws SQLException {
		
		if ( isClosed ) throw new SQLException("Cannot complete executeAsFastPath(): LitePreparedStatement is closed");
			
		if ( null != spfResult ) // query already executed
			return true;
		
		try {
			if ( opName.equalsIgnoreCase("listNet") )
				spfResult = GaianDBUtilityProcedures.getNetInfoForClosestMatchingIP(args[0]);	
			else
				spfResult = operation.method.invoke(null, operation.args);
			
		} catch (Exception e) {
			throw new SQLException("Unable to executeAsFastPath(): " + e);
		}
//		logger.logThreadInfo("executeAsFastPath() successfully invoked method: " + operation.method.getName() + ", result: " + spfResult );
		return true;
	}

	public int nextRow(DataValueDescriptor[] dvdr) throws StandardException, SQLException {
		
		if ( isClosed ) throw new SQLException("Cannot complete nextRow(): LitePreparedStatement is closed");
		
		if ( null == spfResult ) return IFastPath.SCAN_COMPLETED;
		
		if ( spfResult instanceof ResultSet ) {
			
			logger.logThreadInfo("nextRow() handling ResultSet...");
			ResultSet rs = (ResultSet) spfResult;
			if ( !rs.next() ) {
				rs.close();
				spfResult = null;
				return IFastPath.SCAN_COMPLETED;
			}
			
			for ( int i=0; i<dvdr.length; i++) {
				DataValueDescriptor dvd = dvdr[i];
				dvd.setValueFromResultSet( rs, i+1, rsmd.isNullable(i+1) != ResultSetMetaData.columnNoNulls );
			}
			
		} else {

			if ( spfResult instanceof Object[] ) {
				
				Object[] result = (Object[]) spfResult;
				for ( int i=0; i<result.length; i++ ) {
//					logger.logDetail("Getting column " + (i+1) + ", value: " + result[i] + ", type: " + result[i].getClass().getName());
					dvdr[i].setObjectForCast(result[i], true, ( null==result[i] ? null : result[i].getClass().getName() ));
				}
			} else {
				dvdr[0].setObjectForCast(spfResult, true, ( null==spfResult ? null : spfResult.getClass().getName() ));
//				logger.logThreadDetail("nextRow() got operation value: " + dvdr[0].toString());
			}

			spfResult = null; // single result is fetched
		}
		
		return IFastPath.GOT_ROW;
	}

	public void currentRow(ResultSet arg0, DataValueDescriptor[] arg1) throws StandardException, SQLException {}
	public void rowsDone() throws StandardException, SQLException {}
	
	private boolean isClosed = false;

	@Override
	public void close() throws SQLException {
		// Recycle the connection used for a delegated query - other call/values invoked queries have their connections closed implicitly 
		if ( null != spfResult && spfResult instanceof ResultSet && null != delegateNodeConnectionDetails )
			DataSourcesManager.getSourceHandlesPool( delegateNodeConnectionDetails )
				.push( ((ResultSet) spfResult).getStatement().getConnection() );
		isClosed = true;
	}
	
	public boolean reinitialise() { return false; } // cannot re-execute this GaianChildVTI

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}
		
	static final String DELEGATE_SQL_PROC = "DELEGATESQL"; // Not a Derby registered procedure - this will only work in lite mode
	
	private ResultSet delegateSqlToNearestCapableNode( String sql ) throws Exception {		
		int dist = DatabaseConnectionsChecker.getDistanceToServerNode();
		
		if ( 1 > dist ) {
			logger.logThreadWarning(GDBMessages.NETDRIVER_ERRONEOUS_REQUEST, "Erroneous request for SQL delegation: distance to server node: "
					+ dist + " should be greater than 0 - returning null for query");
			return null;
		}

		logger.logInfo("Delegating SQL (distance to capable node: " + dist + "): " + sql);
				
		String nodeID = DatabaseConnectionsChecker.getBestPathToServer();
		String gc = GaianDBConfig.getDiscoveredConnectionID(nodeID);
		if ( null == nodeID || null == gc ) {
			logger.logThreadWarning(GDBMessages.NETDRIVER_SQL_DELEGATE_NODE_ID_NULL, "Unable to delegate SQL: Unknown node path " + nodeID);
			return null;
		}
				
		Connection c = null;
		
		try {
			delegateNodeConnectionDetails = GaianDBConfig.getRDBConnectionDetailsAsString( gc );
			c = DataSourcesManager.getPooledJDBCConnection(
				delegateNodeConnectionDetails, DataSourcesManager.getSourceHandlesPool( delegateNodeConnectionDetails ) );
			
			if ( 1 < dist ) // Propagate further unless distance is 1
				sql = "call " + DELEGATE_SQL_PROC + "('" + Util.escapeSingleQuotes(sql) + "')";
			
			return c.createStatement().executeQuery( sql );
			
		} catch (SQLException e) { 
			logger.logThreadWarning(GDBMessages.NETDRIVER_SQL_DELEGATE_ERROR_SQL, "Unable to delegate SQL, cause: " + e);
		}
		
		return null;
	}

	public boolean fetchNextRow(DataValueDescriptor[] row) throws Exception {
		return IFastPath.GOT_ROW == nextRow(row);
	}

	public int getRowCount() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isBeforeFirst() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isScrollable() {
		// TODO Auto-generated method stub
		return false;
	}

	public void setArgs(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
	}

	public void setExtractConditions(Qualifier[][] qualifiers,
			int[] projectedColumns, int[] physicalColumnsMapping)
			throws Exception {
		// TODO Auto-generated method stub
		
	}
}
