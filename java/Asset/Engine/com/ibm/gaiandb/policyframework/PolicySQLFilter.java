/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.policyframework;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.TypeId;

public class PolicySQLFilter implements SQLQueryFilter {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	public boolean applyIncomingSQLFilter(String queryID, String logicalTable, ResultSetMetaData logicalTableMetaData, String originalSQL, SQLQueryElements queryElmts) {

		// Column "LOCATION" of logical table "LT0" is not to be returned...
		
		System.out.println("Applying Incoming SQL Filter.. projectedCols: " + intArrayAsString(queryElmts.getProjectedColumns()) + 
				", where-clause: " + reconstructSQLWhereClause(queryElmts.getQualifiers(), logicalTableMetaData));
		
		int colid = -1;
		try {
			if ( "LT0".equalsIgnoreCase(logicalTable) ) {
				int i;
				for ( i=0; i<logicalTableMetaData.getColumnCount(); i++ )
					if ( "LOCATION".equalsIgnoreCase(logicalTableMetaData.getColumnName(i+1)) ) {
						colid = i+1;
						break;
					}
				
				if ( i == logicalTableMetaData.getColumnCount() ) {
					// Column not in table def, nothing to remove
					return true;
				}
			}
			
			if ( -1 == colid ) throw new Exception("Internal error: colid is not set");
			
			int[] projectedColumns = queryElmts.getProjectedColumns();
			for ( int i=0; i<projectedColumns.length; i++ ) {
				if ( colid == projectedColumns[i] ) {
					int[] newProjection = new int[projectedColumns.length-1];
					
					// copy projection, ommiting targetted column
					System.arraycopy(projectedColumns, 0, newProjection, 0, i);
					System.arraycopy(projectedColumns, i+1, newProjection, 0, projectedColumns.length-i-1);
					
					System.out.println("Policy altering incoming SQL's projected columns: " + 
							intArrayAsString(projectedColumns) + " -> " + intArrayAsString(newProjection) );
					
					queryElmts.setProjectedColumns(newProjection);
				}
			}
			
		} catch ( Exception e ) {
			System.out.println("Unable to apply policy filter on incoming SQL (cancelling query): " + e);
			e.printStackTrace();
			return false;
		}
				
		return true;
	}

	public boolean applyPropagatedSQLFilter(String queryID, String nodeID, SQLQueryElements queryElmts) {
		
		System.out.println("Policy allowing query " + queryID + " to be forwarded without alteration to node: " + nodeID);
		return true; // accept all queries against any node
	}
	
	public boolean applyDataSourceSQLFilter(String queryID, String dataSourceID, SQLQueryElements queryElmts) {
		
		System.out.println("Policy allowing query " + queryID + " to be executed without alteration against data source: " + dataSourceID);
		return true; // accept all queries against any physical data source on this node
	}
	
	
	public static String intArrayAsString(int[] a) {
		if ( null==a ) return null; int len = a.length;
		String pcs = new String( 0<len ? "[" + a[0] : "[" );
		for (int i=1; i<len; i++) pcs += ", " + a[i]; pcs += "]";
		return pcs;
	}
	
	
	/**
	 * Parses the qualifiers predicates structure and returns a SQL string "where-clause" representation of them.
	 * 
	 * The next 2 arguments are used to find column names to populate the string with. When only the logical table result set meta data
	 * is provided, the logical column names will be used. When not even the meta data is provided, columns are simply represented
	 * by logical table index position, i.e. as C1, C2, C3, ...
	 * 
	 * @param qualifiers - The predicates structure to be parsed - contains CNF representation of comparison predicates.
	 * @param logicalTableRSMD - The logical table's result set meta data - used to find column names.
	 * @return
	 */		
	public static String reconstructSQLWhereClause( Qualifier[][] qualifiers, ResultSetMetaData logicalTableRSMD ) {
		
		StringBuffer sqlWhereClause = new StringBuffer("");
		try {
//			System.out.println("qualifiers is null ? " + (null == qualifiers ? "true" : "false, len " + qualifiers.length));
			if ( null != qualifiers ) {
				
				for (int x=0; x<qualifiers.length; x++) {
					
					// Note Derby passes qualifiers in conjunctive normal form:
					// e.g: "a and b and c and (d or e) and (g or h) and (j or k or l)"...
					
//					// Start with a "WHERE " if this is the first qualifier row and if it is not empty or there are other rows.
//					if ( 0 == x && ( 1 < qualifiers.length || 0 < qualifiers[x].length ) )
//						sqlWhereClause.append("WHERE ");
//					else
						
					if ( 0 < x ) {
						if ( 1 == x && 0 == qualifiers[0].length ) {
							// No need for an 'AND'... and we only need an opening bracket if there are further qualifier[][] rows
							// e.g: "(a or b) and (c or d)..." rather than just: "a or b"
							if ( 1 < qualifiers[1].length && 2 < qualifiers.length ) sqlWhereClause.append('(');
						
						} else {
							if ( 1 < qualifiers[x].length ) {
								// e.g: "a and b and (c or d)"
								sqlWhereClause.append(" AND (");
							} else {
								sqlWhereClause.append(" AND ");
							}
						}
					}
					
					for (int y=0; y<qualifiers[x].length; y++) {
						
						if ( 0 < y ) {
							if ( 0 == x ) sqlWhereClause.append(" AND ");
							else sqlWhereClause.append(" OR ");
						}
						
						Qualifier q = qualifiers[x][y];
						int colID = q.getColumnId(); // 0-based
						String colName = ( null != logicalTableRSMD ? logicalTableRSMD.getColumnName( colID+1 ) : "C" + (colID+1) );
													
						sqlWhereClause.append( colName );
						
						
						String orderable = getFormattedValueOfOrderable( q );
						
						if ( null == orderable )
							sqlWhereClause.append ( q.negateCompareResult() ? " IS NOT NULL" : " IS NULL" );
						else {
							sqlWhereClause.append( getOrderingOperatorString( q ) );
							sqlWhereClause.append( orderable );
						}
					}
					
					// Consider adding a closing bracket if this is the second row or above and it had more than one element 
					if ( 0 < x && 1 < qualifiers[x].length ) {
						// Add a closing bracket as long as:
						//		   - this is the 3rd row or above
						//		or - the 1st row (of ANDs) had some elements
						//		or - there are more than 2 rows
						if ( 1 < x || 0 < qualifiers[0].length || 2 < qualifiers.length ) sqlWhereClause.append(')');
					}
				}
			}
		} catch ( Exception e ) {
			System.err.println("Exception building WHERE clause: " + e);
		}
		
		return sqlWhereClause.toString();
	}
    
	private static String getOrderingOperatorString( Qualifier q ) throws SQLException {
		int operator = q.getOperator();
		boolean negate = q.negateCompareResult();
		switch ( operator ) {
			case Orderable.ORDER_OP_EQUALS: return negate ? "!=" : "=";
			case Orderable.ORDER_OP_GREATEROREQUALS: return negate ? "<" : ">=";
			case Orderable.ORDER_OP_GREATERTHAN: return negate ? "<=" : ">";
			case Orderable.ORDER_OP_LESSOREQUALS: return negate ? ">" : "<=";
			case Orderable.ORDER_OP_LESSTHAN: return negate ? ">=" : "<";
		}
		String errmsg = "Invalid operator detected (not one of the Orderable interface): " + operator;
		System.err.println("DERBY ERROR: " + errmsg);
		throw new SQLException( errmsg );
	}
	
	private static String getFormattedValueOfOrderable( Qualifier q ) throws SQLException {

		try {
			DataValueDescriptor dvd = q.getOrderable();
			if ( dvd.isNull() ) return null;
			String value = dvd.getString();
			int jdbcType = TypeId.getBuiltInTypeId( dvd.getTypeName() ).getJDBCTypeId();
//			logInfo("Getting value for JDBC type: " + jdbcType);
			switch ( jdbcType ) {
				case Types.CHAR: case Types.VARCHAR: case Types.LONGVARCHAR: case Types.CLOB: 
				case Types.DATE: case Types.TIME: case Types.TIMESTAMP: return "'" + value + "'";
				default: return value;
			}
		} catch (StandardException e) {
			String errmsg = "Could not get Orderable value from Qualifier: " + e;
			System.err.println("DERBY ERROR: " + errmsg);
			throw new SQLException( errmsg );
		}
	}
}
