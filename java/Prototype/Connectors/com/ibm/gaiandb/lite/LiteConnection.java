/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.lite;

import java.io.Serializable;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import org.apache.derby.vti.IQualifyable;
import org.apache.derby.vti.Pushable;
import org.apache.derby.vti.VTIEnvironment;

import com.ibm.db2j.GaianTable;
import com.ibm.db2j.VTI60;
import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author DavidVyvyan
 */

public class LiteConnection implements Connection {
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "LiteConnection", 30 );
	
	private boolean isClosed = false;
	
	public void close() throws SQLException {
		isClosed = true;
	}

	public boolean isClosed() throws SQLException {
		return isClosed;
	}
	
    private static final Hashtable<String, String> viewTypes = new Hashtable<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            put("0", "maxDepth=0");
            put("1", "maxDepth=1");
            put("P", "with_provenance");
            put("X", "explain");
            put("XF", "explain in graph.dot");
        }
    };
	
	private final static String GDB_PROPAGATION_SQL = GaianDBConfig.GDB_QRYID+"=? AND "+GaianDBConfig.GDB_QRYSTEPS+"=?";
	private final static String GDB_CREDENTIALS_SQL = " AND "+GaianDBConfig.GDB_CREDENTIALS+"=?";

    private static final Pattern selectExpressionVTI = Pattern.compile(    		
    		"(?i).*['\"\\s]FROM[\"\\s][\\s]*NEW[\\s]+COM.IBM.DB2J.([\\w]+)[\\s]*\\((.*)\\)[\\s]*(?:T|Q|GQ|GT)[0-9]*[\\s]*" +
    		"(WHERE['\"\\(\\s].*)?([\\s]+(?:GROUP|ORDER|FETCH|FOR|OFFSET|WITH)[\\s]+.*|,.*|JOIN[\\s].*)?"
    );
    
    private static final Pattern selectExpression = Pattern.compile(
    		"(?i).*['\"\\s]FROM[\"\\s][\\s]*([\\w]+)[\"\\s]?[\\s]*()"+
    		"(WHERE['\"\\(\\s].*)?([\\s]+(?:GROUP|ORDER|FETCH|FOR|OFFSET|WITH)[\\s]+.*|,.*|JOIN[\\s].*)?"
    );
    	
    static final Pattern callOrValuesExpression = Pattern.compile(
    		"(?i)(?:call|values)[\\s]*([\\w]*)[\\s]*\\((.*)\\)(.*)(.*)"
    );
    
    // There may be a single or double quote before the 'for' token, and a double one after - but ignore this possibility here
    private static final Pattern fromPattern = Pattern.compile( "(?i)[\\s]+FROM[\\s]+" );
    
    private static final String SUBQUERY_HINT = " -- " + DataSourcesManager.SUBQUERY_PREFIX;
    
    
	public PreparedStatement prepareStatement( final String sql ) throws SQLException {
		
		if ( isClosed ) throw new SQLException("Unable to prepareStatement(sql): LiteConnection is closed");
		
		logger.logThreadInfo("prepareStatement on sql: " + sql);
		
		boolean isSubQuery = sql.endsWith(SUBQUERY_HINT);
		String sql2 = isSubQuery ? sql.substring(0, sql.length() - SUBQUERY_HINT.length()) : sql;
		
		int propagationSQLIndex = sql2.indexOf( GDB_PROPAGATION_SQL );
		boolean isPropagatedSQL = -1 != propagationSQLIndex;
		boolean isPropagatedWithCredentials = !isPropagatedSQL ? false :
			GDB_CREDENTIALS_SQL.equals( sql2.substring( propagationSQLIndex + GDB_PROPAGATION_SQL.length() ) );
		
		sql2 = sql2.trim().replaceFirst("\\s", " ");
		int idx = sql2.indexOf(' ');
		
		if ( -1 == idx ) throw new SQLException("SQL parsing error (no verb detected): " + sql);
		
		String verb = sql2.substring(0, idx).toUpperCase();
		boolean isSimpleOperationInvocation = verb.equals("CALL") || verb.equals("VALUES");
		boolean isExplicitVTIinstanciation = -1 != sql2.toUpperCase().indexOf("COM.IBM.DB2J");
		
		Pattern sqlPattern = isSimpleOperationInvocation ? callOrValuesExpression :	
			isExplicitVTIinstanciation ? selectExpressionVTI : selectExpression;
		
		String opInfo = sqlPattern.matcher(sql2).replaceFirst("$1 $2GDB_WHERE_XPR$3GDB_END_XPR$4");
		String opName = null;
		String[] args = new String[0];
		
		opInfo = opInfo.replaceFirst("GDB_WHERE_XPRnull", "GDB_WHERE_XPR").replaceFirst("GDB_END_XPRnull", "GDB_END_XPR");
		
		logger.logThreadInfo("Resulting opInfo from replace op: " + opInfo);
		int endIdx = opInfo.lastIndexOf("GDB_END_XPR");
		
		boolean isSQLUnmatched = sql2.equals(opInfo) || opInfo.substring(endIdx).length() > "GDB_END_XPR".length();
		
		int whereIdx = opInfo.lastIndexOf("GDB_WHERE_XPR");
		
		// Only allow predicates specified in conjunctive normal form (in order to facilitate parsing)
		Predicate[][] predicates = null;
		if ( !isSQLUnmatched ) {
			int endIdx2 = opInfo.lastIndexOf("GDB_QRYID=? AND GDB_QRYSTEPS=?", endIdx);
			if ( -1 != endIdx2 ) endIdx = endIdx2;
			String whereClause = opInfo.substring(whereIdx + "GDB_WHERE_XPR".length(), endIdx).trim();
			if ( "WHERE".length() <= whereClause.length() ) whereClause = whereClause.substring("WHERE".length()).trim();
			if ( whereClause.startsWith("(") && whereClause.endsWith(") AND") )
				// strip outer enclosing brackets for propagated queries
				whereClause = whereClause.substring(1, whereClause.length()-5);
			
			logger.logThreadInfo("Extracting predicates from WHERE sub-clause: " + whereClause);
			
			try {
				predicates = extractPredicates( whereClause );
			} catch (Exception e) {
				logger.logThreadWarning(GDBMessages.NETDRIVER_PREDICATES_EXTRACT_ERROR, "Unable to extract predicates (delegating query): " + e + ", stack: " + Util.getStackTraceDigest(e));
				isSQLUnmatched = true;
			}
		}
		
//		if ( isSQLUnmatched ) {
//			// Unmatched sql - delegate
//			opName = LitePreparedStatement.DELEGATE_SQL_PROC;
//			args = new String[] { sql };
//			isSimpleOperationInvocation = true;
//		} else {
//			idx = opInfo.indexOf(' ');
//			opName = opInfo.substring(0, idx);
//			args = Util.splitByDelimiterNonNestedInBracketsOrSingleQuotes( opInfo.substring(idx+1, whereIdx), ',' );
//	
//			for ( int i=0; i<args.length; i++ )
//				args[i] = Util.stripSingleQuotesDownOneNestingLevel(args[i]);
//		}
		
		if ( !isSQLUnmatched ) {
			idx = opInfo.indexOf(' ');
			opName = opInfo.substring(0, idx);
			if ( isSimpleOperationInvocation && !LitePreparedStatement.isAllowedOperation(opName) ) {
				isSQLUnmatched = true;
			} else {
//				System.out.println("is allowed: " + opName);
				args = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrSingleQuotes( opInfo.substring(idx+1, whereIdx), ',' );
				
				for ( int i=0; i<args.length; i++ ) {
					String s = args[i].trim();
					if ( 1 < s.length() ) s = s.substring(1, s.length()-1);
					args[i] = Util.stripSingleQuotesDownOneNestingLevel( s );
				}
			}
		}
		
		if ( isSQLUnmatched ) {
			// Unmatched sql - delegate (as long as it is not a sub-query - as it was purposefully sent to us for execution here)
			// Note: There is no envisaged purpose that a node may have for sending a sub-query around if it is not to be executed
			// on the targeted nodes in the network.. other than perhaps: "select * from GaianQuery('call findNearestFullNode') Q" !!
			if ( isSubQuery )
				throw new SQLException("Unsupported SUB-QUERY syntax (and no sense in delegating): " + sql);
			
			if ( "values 1".equals( opInfo ) )
				opName = opInfo; // special case used by GaianDB for testing a connection - we just allow this simple values stmt for now
			else {
				opName = LitePreparedStatement.DELEGATE_SQL_PROC;
				args = new String[] { sql };
			}
			isSimpleOperationInvocation = true;
		}
		
		logger.logThreadInfo("opName: " + opName + ", arguments["+args.length+"]: " + Arrays.asList(args));

		PreparedStatement pstmt = null;
		
		try {
			if ( isSimpleOperationInvocation ) {
				pstmt = new LitePreparedStatement(opName, args);
			} else {
				
				if ( !isExplicitVTIinstanciation ) {
					
					String ltName = opName.toUpperCase();
					idx = opName.lastIndexOf('_');
					String arg = -1 == idx ? null : viewTypes.get( ltName.substring(idx+1) );
					if ( -1 != idx && null != arg ) ltName = ltName.substring(0, idx);
					
					pstmt = new LiteGaianStatement( ltName, arg );
					
				} else if ( "GaianTable".equalsIgnoreCase(opName) && 5 > args.length ) {
					
					String[] p = new String[4]; // an array of fixed size - so we only have to invoke 1 GaianTable constructor
					System.arraycopy(args, 0, p, 0, args.length);
					pstmt = new LiteGaianStatement( p[0], p[1], p[2], p[3] );
					
				} else if ( "GaianQuery".equalsIgnoreCase(opName) && 6 > args.length ) {
										
					String[] p = new String[5]; // an array of fixed size - so we only have to invoke 1 GaianTable constructor
					System.arraycopy(args, 0, p, 0, args.length);
					try {
						pstmt = (new LiteGaianStatement()).new LiteGaianQueryStatement( p[0], p[1], p[2], p[3], p[4] );
					} catch ( SQLException e ) {
						String iex = Util.getGaiandbInvocationTargetException(e);
						if ( null == iex || 0 == iex.length() ) {
							throw new Exception(e);
						} else {
							// Log message and delegate query
							logger.logThreadWarning(GDBMessages.NETDRIVER_SUBQUERY_EXEC_ERROR, "Unable to execute subquery (delegated): " + sql);
							opName = LitePreparedStatement.DELEGATE_SQL_PROC;
							args = new String[] { sql };
							pstmt = new LitePreparedStatement(opName, args);
						}
					}
					
				} else {
					throw new SQLException("Unable to prepare SQL statement: Invalid VTI class name: " + opName +
							", or number of arguments: " + args.length + " (should be max 4 for GaianTable and max 5 for GaianQuery)");
				}
				
				if ( !opName.equals( LitePreparedStatement.DELEGATE_SQL_PROC ) ) {
				
					VTIEnvironment vtie = new VTIEnvironment() {
						public void setSharedState(String arg0, Serializable arg1) {}
						public Object getSharedState(String arg0) { return null; }
						public boolean isCompileTime() { return false; }
						public int getStatementIsolationLevel() { return 0; }
						public String getOriginalSQL() { return sql; }
					};
					
					String[] sqlSplitAroundFromToken = fromPattern.split(sql2); // inefficient extraneous operation - later we could pick this out above...
					String selectedColumns = sqlSplitAroundFromToken[0].substring("select ".length()).trim();
					String[] selectedCols = Util.splitByCommas( selectedColumns );
					
					logger.logThreadInfo("Selected columns: " + Arrays.asList(selectedCols));
					
					int[] resultCols = null, sortedResultCols = null;
					
					if ( ! "*".equals(selectedColumns) ) {
						resultCols =  new int[selectedCols.length];
						GaianResultSetMetaData tableMetaData = ((TableResultExtractor) pstmt).getTableMetaData();
						for ( int i=0; i<selectedCols.length; i++ ) {
							int colID = tableMetaData.getColumnPosition( selectedCols[i] );
							if ( -1 == colID ) {
								String msg = "Unresolved queried column name: '" + selectedCols[i] +
									"' for GaianTable: " + ((GaianTable) pstmt).getLogicalTableName(true);
								logger.logThreadWarning(GDBMessages.NETDRIVER_COLUMN_UNRESOLVED_FOR_GT, msg);
								throw new SQLException(msg);
							}
							resultCols[i] = colID;
						}
						
						// Prepare the LiteGaianStatement to return result columns rather than table columns
						((TableResultExtractor) pstmt).setResultColumns(resultCols);
						
						sortedResultCols = new int[resultCols.length];
						System.arraycopy(resultCols, 0, sortedResultCols, 0, resultCols.length);
						Arrays.sort(sortedResultCols);
					}
					
					((Pushable) pstmt).pushProjection(vtie, sortedResultCols); // Tell the vti which columns to query from underlying sources
					
					Qualifier[][] qualifiers = null;
					if ( null != predicates ) {
						GaianResultSetMetaData tableMetaData = ((TableResultExtractor) pstmt).getTableMetaData();
						qualifiers = new Qualifier[predicates.length][predicates[0].length];
						for ( int x=0; x<predicates.length; x++)
							for ( int y=0; y<predicates[x].length; y++ ) {
								
								Predicate p = predicates[x][y];
								int colIndex = tableMetaData.getColumnPosition(p.colName); // 1-based
									
								DataValueDescriptor value = p.orderable.startsWith("'") ?
									new SQLChar( Util.stripSingleQuotesDownOneNestingLevel( p.orderable ) ) :
									java.sql.Types.TIMESTAMP == tableMetaData.getColumnType(colIndex) ?
										new SQLTimestamp( new Timestamp( new Double(Double.parseDouble( p.orderable )).longValue() ) ) :
										new SQLDouble( Double.parseDouble( p.orderable ) );
								
								// Derby refuses to flip operators '>' and '>=' when necessary (e.g. when comparing int 1 > double 0.0)
								// - therefore we normalise down to 3 operators too, and instruct Derby to negate results when appropriate 
								boolean isNegateCompareResult = '>' == p.operator.charAt(0);
								
								int operator =
									p.operator.equals("=") ? Orderable.ORDER_OP_EQUALS :
									p.operator.equals("<") || p.operator.equals(">=") ? 
											Orderable.ORDER_OP_LESSTHAN : Orderable.ORDER_OP_LESSOREQUALS ;
									
								// this didn't always work due to the flipping issue described above..
//									p.operator.equals("<") ? Orderable.ORDER_OP_LESSTHAN :
//									p.operator.equals("<=") ? Orderable.ORDER_OP_LESSOREQUALS :
//									p.operator.equals(">=") ? Orderable.ORDER_OP_GREATEROREQUALS :
//									p.operator.equals(">") ? Orderable.ORDER_OP_GREATERTHAN :
//									Orderable.ORDER_OP_EQUALS ;
								
								GenericScanQualifier gsq = new GenericScanQualifier();
								gsq.setQualifier( colIndex-1, value, operator, isNegateCompareResult, false, false );
								qualifiers[x][y] = gsq;
							}
					}
					
					((IQualifyable) pstmt).setQualifiers(vtie, qualifiers); //new Qualifier[0][]); // no qualifier predicate filters for now
				}
			}
			
		} catch ( Exception e ) {
			throw new SQLException(e);
		}
		
		((VTI60) pstmt).setConnection(this);
		
		if ( isPropagatedSQL )
			((LiteParameterMetaData) pstmt.getParameterMetaData()).setParameterCount( isPropagatedWithCredentials ? 3 : 2 );
		
		return pstmt;
	}
	
//	String orderable = Util.stripSingleQuotesDownOneNestingLevel( tailExpression );

	private static final Pattern arithmeticExpression = Pattern.compile( "([0-9\\.]+|current_millis)(.*)" );
	
	private class Predicate {
		private final String colName, operator, orderable;
		public Predicate(String colName, String operator, String orderable) {
			super(); this.colName = colName; this.operator = operator;
			if ( orderable.startsWith("'") ) {
				this.orderable = orderable;
			} else {
				String s = new String(orderable).replaceAll(" ", "");
				double total = 0;
				char op = '+';
				while ( true ) {
					String[] ts = arithmeticExpression.matcher(s).replaceFirst("$1 $2").split(" ");
					double v = "current_millis".equalsIgnoreCase(ts[0]) ? System.currentTimeMillis() : Double.parseDouble(ts[0]);
					
//					logger.logThreadInfo("Predicate Orderable Running Total: " + total + ", op: " + op + ", v: " + v);
//					logger.logThreadInfo("ts[]: " + Arrays.asList(ts));
					
					switch(op) { 	case '+': total += v; break; case '-': total -= v; break; case '*': total *= v; break; 
									case '/': total /= v; break; case '%': total %= v; break; case '^': total = Math.pow(total, v); }
					if ( 2 > ts.length ) break;
					String x = ts[1];
					if ( null == x || 0 == x.length() || x.equals("null") ) break;
					op = x.charAt(0);
					s = x.substring(1);
				}
				this.orderable = total + "";
			}

			logger.logThreadInfo("Resolved predicate to: " + this.colName+this.operator+this.orderable );
		}
	}
	
	private static final Pattern predicateExpression = Pattern.compile( "(?i)([\\w]+)[\\s]*(<=|>=|=|<|>)[\\s]*(.*)" );
	
	// This method needs re-writing... to cater for possibility of having embedded " AND " or " OR " strings - and to support 'OR' conditions
	private Predicate[][] extractPredicates( String whereClause ) throws Exception {
		
		List<Predicate> rowPredicates = new ArrayList<Predicate>();
		
		whereClause = whereClause.replaceAll(" or ", " OR ").replaceAll(" and ", " AND ");
		
		if ( null != whereClause && 0 < whereClause.length()) {
			if ( -1 != whereClause.indexOf(" OR ") )
				throw new Exception("Unsupported where clause ('OR' condition not supported): " + whereClause);
			
			String[] andedConditions = whereClause.split(" AND ");
			for ( String condition : andedConditions ) {
				condition = condition.trim();

				String info = predicateExpression.matcher(condition).replaceFirst(">$1 $2 $3");
				if ( info.equals(whereClause) ) {
					throw new Exception("Unsupported condition: [" + condition + "] in where clause: " + whereClause);
				} else {
					int idx = info.indexOf(' ');
					int idx2 = info.indexOf(' ', idx+1);
					String colName = info.substring(1, idx).trim(); // skip the '>', used to confirm the string was parsed successfully
					String operator = info.substring(idx+1, idx2).trim();
					String orderable = info.substring(idx2+1).trim();
					
					logger.logThreadInfo("Adding validated predicate: " + colName + operator + orderable );
					rowPredicates.add(new Predicate( colName, operator, orderable ));
				}
			}
		}
		
		Predicate[][] predicates = new Predicate[1][];
		predicates[0] = rowPredicates.toArray( new Predicate[0] );
		
		return predicates;
	}
	

	
	
	
	
	
	
	
	
	
	
		

	// NOT USED CURRENTLY - As only the UDP driver server uses the LiteDriver
	public Statement createStatement() throws SQLException {
		
		if ( isClosed ) throw new SQLException("Unable to createStatement(): LiteConnection is closed");
		return null; //new LitePreparedStatement();
	}

	
	// NOT USED - referenced for Caching and SpatialQuery
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return null;
	}
	
	// NOT USED
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	// SHOULD NOT BE CALLED IN LITE MODE (Most initialisation code using this is disabled in GaianNode)
	public DatabaseMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	// NOT USED
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	// NOT USED
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	// NOT USED
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	// NOT USED
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	// NOT USED
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}	

	

	
	
	// NOT USED.....
	
	
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// TODO Auto-generated method stub
	}

	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void commit() throws SQLException {
		// TODO Auto-generated method stub

	}	

	public boolean isValid(int timeout) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	
	
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public String getCatalog() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getClientInfo(String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public String nativeSQL(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void rollback() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void rollback(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub

	}
	
	public void setCatalog(String catalog) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	public void setHoldability(int holdability) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		// TODO Auto-generated method stub

	}

	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setTransactionIsolation(int level) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub
		
	}
}
