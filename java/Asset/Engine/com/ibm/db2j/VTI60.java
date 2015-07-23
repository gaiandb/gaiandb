/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.IQualifyable;
import org.apache.derby.vti.Pushable;
import org.apache.derby.vti.RestrictedVTI;
import org.apache.derby.vti.Restriction;
import org.apache.derby.vti.UpdatableVTITemplate;
import org.apache.derby.vti.VTIEnvironment;
import org.apache.derby.vti.Restriction.ColumnQualifier;

import com.ibm.gaiandb.GaianDBProcedureUtils;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.lite.LiteParameterMetaData;

public class VTI60 extends UpdatableVTITemplate implements ResultSet, RestrictedVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final Logger logger = new Logger( "VTI60", 20 );
	
	private void debugStack() { debugStack( false ); }
	
	private void debugStack( boolean isIgnored ) {
		try { throw new Exception("VTI method not implemented in " + this.getClass().getSimpleName() + (isIgnored ? " (no-op)" : "") ); }
		catch( Exception e ) {
			String msg = Util.getStackTraceDigest(e, 5, 3, true);
			logger.logWarning(GDBMessages.ENGINE_VTI60_EXCEPTION, msg);
//			System.out.println( msg );
		}
	}
	
	
	// Methods for UpdatableVTITemplate abstract class ( incl. PreparedStatement interface )

	@Override public void addBatch() throws SQLException { debugStack(); super.addBatch(); }
	@Override public void addBatch(String arg0) throws SQLException { debugStack(); super.addBatch(arg0); }
	@Override public void cancel() throws SQLException { debugStack(); super.cancel(); }
	@Override public void clearBatch() throws SQLException { debugStack(); super.clearBatch(); }
	@Override public void clearParameters() throws SQLException { debugStack(); super.clearParameters(); }
	@Override public void clearWarnings() throws SQLException { debugStack(); super.clearWarnings(); }
	@Override public void close() throws SQLException { debugStack(); super.close(); }
	@Override public boolean execute() throws SQLException { debugStack(); return super.execute(); }
	@Override public boolean execute(String arg0, int arg1) throws SQLException { debugStack(); return super.execute(arg0, arg1); }
	@Override public boolean execute(String arg0, int[] arg1) throws SQLException { debugStack(); return super.execute(arg0, arg1); }
	@Override public boolean execute(String arg0, String[] arg1) throws SQLException { debugStack(); return super.execute(arg0, arg1); }
	@Override public boolean execute(String arg0) throws SQLException { debugStack(); return super.execute(arg0); }
	@Override public int[] executeBatch() throws SQLException { debugStack(); return super.executeBatch(); }
	@Override public ResultSet executeQuery() throws SQLException { debugStack(); return super.executeQuery(); }
	@Override public ResultSet executeQuery(String arg0) throws SQLException { debugStack(); return super.executeQuery(arg0); }
	@Override public int executeUpdate() throws SQLException { debugStack(); return super.executeUpdate(); }
	@Override public int executeUpdate(String arg0, int arg1) throws SQLException { debugStack(); return super.executeUpdate(arg0, arg1); }
	@Override public int executeUpdate(String arg0, int[] arg1) throws SQLException { debugStack(); return super.executeUpdate(arg0, arg1); }
	@Override public int executeUpdate(String arg0, String[] arg1) throws SQLException { debugStack(); return super.executeUpdate(arg0, arg1); }
	@Override public int executeUpdate(String arg0) throws SQLException { debugStack(); return super.executeUpdate(arg0); }	
	
	private Connection connection = null;
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	@Override public Connection getConnection() throws SQLException {
		return connection;
	} // debugStack(); return super.getConnection(); }
	
	@Override public int getFetchDirection() throws SQLException { debugStack(); return super.getFetchDirection(); }
	@Override public int getFetchSize() throws SQLException { debugStack(); return super.getFetchSize(); }
	@Override public ResultSet getGeneratedKeys() throws SQLException { debugStack(); return super.getGeneratedKeys(); }
	@Override public int getMaxFieldSize() throws SQLException { debugStack(); return super.getMaxFieldSize(); }
	@Override public int getMaxRows() throws SQLException { debugStack(); return super.getMaxRows(); }
	@Override public ResultSetMetaData getMetaData() throws SQLException { debugStack(); return super.getMetaData(); }
	@Override public boolean getMoreResults() throws SQLException { debugStack(); return super.getMoreResults(); }
	@Override public boolean getMoreResults(int arg0) throws SQLException { debugStack(); return super.getMoreResults(arg0); }	
	private ParameterMetaData pmd = null;
	
	@Override public ParameterMetaData getParameterMetaData() throws SQLException {
		if ( GaianNode.isLite() )
			return GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE ? null : null == pmd ? pmd = new LiteParameterMetaData() : pmd; debugStack(); return super.getParameterMetaData();
	}
	
	@Override public int getQueryTimeout() throws SQLException { debugStack(); return super.getQueryTimeout(); }
	@Override public ResultSet getResultSet() throws SQLException { debugStack(); return super.getResultSet(); }
	@Override public int getResultSetConcurrency() throws SQLException { debugStack(); return super.getResultSetConcurrency(); }
	@Override public int getResultSetHoldability() throws SQLException { debugStack(); return super.getResultSetHoldability(); }
	@Override public int getResultSetType() throws SQLException { debugStack(); return super.getResultSetType(); }
	@Override public int getUpdateCount() throws SQLException { debugStack(); return super.getUpdateCount(); }
	@Override public SQLWarning getWarnings() throws SQLException { debugStack(); return super.getWarnings(); }
	@Override public void setArray(int arg0, Array arg1) throws SQLException { debugStack(); super.setArray(arg0, arg1); }
	@Override public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException { debugStack(); super.setAsciiStream(arg0, arg1, arg2); }
	@Override public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException { debugStack(); super.setBigDecimal(arg0, arg1); }
	@Override public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException { debugStack(); super.setBinaryStream(arg0, arg1, arg2); }
	@Override public void setBlob(int arg0, Blob arg1) throws SQLException { debugStack(); super.setBlob(arg0, arg1); }
	@Override public void setBoolean(int arg0, boolean arg1) throws SQLException { debugStack(); super.setBoolean(arg0, arg1); }
	@Override public void setByte(int arg0, byte arg1) throws SQLException { debugStack(); super.setByte(arg0, arg1); }
	@Override public void setBytes(int arg0, byte[] arg1) throws SQLException { debugStack(); super.setBytes(arg0, arg1); }
	@Override public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException { debugStack(); super.setCharacterStream(arg0, arg1, arg2); }
	@Override public void setClob(int arg0, Clob arg1) throws SQLException { debugStack(); super.setClob(arg0, arg1); }
	@Override public void setCursorName(String arg0) throws SQLException { debugStack(); super.setCursorName(arg0); }
	@Override public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException { debugStack(); super.setDate(arg0, arg1, arg2); }
	@Override public void setDate(int arg0, Date arg1) throws SQLException { debugStack(); super.setDate(arg0, arg1); }
	@Override public void setDouble(int arg0, double arg1) throws SQLException { debugStack(); super.setDouble(arg0, arg1); }
	@Override public void setEscapeProcessing(boolean arg0) throws SQLException { debugStack(); super.setEscapeProcessing(arg0); }
	@Override public void setFetchDirection(int arg0) throws SQLException { debugStack(); super.setFetchDirection(arg0); }
	@Override public void setFetchSize(int arg0) throws SQLException { debugStack(); super.setFetchSize(arg0); }
	@Override public void setFloat(int arg0, float arg1) throws SQLException { debugStack(); super.setFloat(arg0, arg1); }
	@Override public void setInt(int arg0, int arg1) throws SQLException { debugStack(); super.setInt(arg0, arg1); }
	@Override public void setLong(int arg0, long arg1) throws SQLException { debugStack(); super.setLong(arg0, arg1); }
	@Override public void setMaxFieldSize(int arg0) throws SQLException { debugStack(); super.setMaxFieldSize(arg0); }
	@Override public void setMaxRows(int arg0) throws SQLException { debugStack(); super.setMaxRows(arg0); }
	@Override public void setNull(int arg0, int arg1, String arg2) throws SQLException { debugStack(); super.setNull(arg0, arg1, arg2); }
	@Override public void setNull(int arg0, int arg1) throws SQLException { debugStack(); super.setNull(arg0, arg1); }
	@Override public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException { debugStack(); super.setObject(arg0, arg1, arg2, arg3); }
	@Override public void setObject(int arg0, Object arg1, int arg2) throws SQLException { debugStack(); super.setObject(arg0, arg1, arg2); }
	@Override public void setObject(int arg0, Object arg1) throws SQLException { debugStack(); super.setObject(arg0, arg1); }
	
	@Override public void setQueryTimeout(int arg0) throws SQLException {
		debugStack(true);
//		super.setQueryTimeout(arg0); // Commented - i.e. no-op for now
	}

	@Override public void setRef(int arg0, Ref arg1) throws SQLException { debugStack(); super.setRef(arg0, arg1); }
	@Override public void setShort(int arg0, short arg1) throws SQLException { debugStack(); super.setShort(arg0, arg1); }
	@Override public void setString(int arg0, String arg1) throws SQLException { debugStack(); super.setString(arg0, arg1); }
	@Override public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException { debugStack(); super.setTime(arg0, arg1, arg2); }
	@Override public void setTime(int arg0, Time arg1) throws SQLException { debugStack(); super.setTime(arg0, arg1); }
	@Override public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException { debugStack(); super.setTimestamp(arg0, arg1, arg2); }
	@Override public void setTimestamp(int arg0, Timestamp arg1) throws SQLException { debugStack(); super.setTimestamp(arg0, arg1); }
	@Override public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException { debugStack(); super.setUnicodeStream(arg0, arg1, arg2); }
	@Override public void setURL(int arg0, URL arg1) throws SQLException { debugStack(); super.setURL(arg0, arg1);
	}	
	
	
	/************************************ Unimplemented Methods by Superclass ************************************/
	
	
	public void setNClob(int parameterIndex, NClob value) throws SQLException { debugStack(); }	
	public void setRowId(int parameterIndex, RowId x) throws SQLException { debugStack(); }
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException { debugStack(); }	
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException { debugStack(); }
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException { debugStack(); }
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException { debugStack(); }
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException { debugStack(); }
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException { debugStack(); }
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException { debugStack(); }
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException { debugStack(); }
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException { debugStack(); }
	public void setClob(int parameterIndex, Reader reader) throws SQLException { debugStack(); }
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException { debugStack(); }
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException { debugStack(); }
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException { debugStack(); }
	public void setNClob(int parameterIndex, Reader reader) throws SQLException { debugStack(); }
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException { debugStack(); }
	public void setNString(int parameterIndex, String value) throws SQLException { debugStack(); }
	public boolean isClosed() throws SQLException { debugStack(); return false; }
	public boolean isPoolable() throws SQLException { debugStack(); return false; }
	public void setPoolable(boolean poolable) throws SQLException { debugStack(); }
	public boolean isWrapperFor(Class<?> iface) throws SQLException { debugStack(); return false; }
	public <T> T unwrap(Class<T> iface) throws SQLException { debugStack(); return null; }
	public void closeOnCompletion() throws SQLException { debugStack(); }
	public boolean isCloseOnCompletion() throws SQLException { debugStack(); return false; }

	
//    Restriction moveAndsToTop( Restriction rt ) {
//    	
//    }
//    
//    Qualifier[][] convertRestrictionToQualifiers( Restriction rt ) {
//    	
//    	Restriction rt2 = moveAndsToTop( rt );
//    	
//    	
//    }
    
//    private static Qualifier[][] convertRestrictionTreeToCNFQualifiers( Restriction rst ) {
//    	
//		if ( null != node && node instanceof Restriction.ColumnQualifier ) {
//			
//			Restriction.ColumnQualifier cq = (Restriction.ColumnQualifier) node;
//			cq.
//			GenericScanQualifier gsq = new GenericScanQualifier();
//    		try {
//    			ResultSetMetaData rsmd = getMetaData();
//    			int colId = ((GaianResultSetMetaData)rsmd).getColumnPosition(cq.getColumnName());
//    			int jdbcType = rsmd.getColumnType(colId);
//    			DataValueDescriptor operandAsDVD = RowsFilter.constructDVDMatchingJDBCType(jdbcType);
//    			Object val = cq.getConstantOperand();
//    			operandAsDVD.setObjectForCast(val, true, ( null==val ? null : val.getClass().getName() ));
////    			operandAsDVD.setValue(cq.getConstantOperand());
//    			
//				gsq.setQualifier( colId-1, operandAsDVD, cq.getComparisonOperator(), true, false, false );
//			} catch (StandardException e) {
//				throw new SQLException("Unable to convert Restriction to Qualifier: " + e);
//			}
//			qualifiers = new Qualifier[][] { new Qualifier[] { gsq } };
//		}
//    }
	
	
	// RestrictedVTI interface
	
    private static final Qualifier convertToInternalQualifier( ColumnQualifier cq, GaianResultSetMetaData grsmd ) throws SQLException {
    	
    	// Internal qualifier attributes used by Derby
    	boolean negateCR = false; // (negate compare-result) allows you to express that the operator should be switched around.
    	boolean orderedNulls = false; // if true, then Derby orders Nulls high by default/convention (can be over-ridden at "compare()" time)
    	boolean unknownRV = false; // (unknown return value) result used when orderedNulls is false
    	
		int colId = grsmd.getColumnPosition(cq.getColumnName());
		DataValueDescriptor operandAsDVD = RowsFilter.constructDVDMatchingJDBCType( grsmd.getColumnType(colId) );

		Object cqOperand = cq.getConstantOperand();
    	int cqOperator = cq.getComparisonOperator(), orderableOperator;
    	
		switch ( cqOperator ) {
			case Restriction.ColumnQualifier.ORDER_OP_LESSTHAN: 		orderableOperator = Orderable.ORDER_OP_LESSTHAN; break;
			case Restriction.ColumnQualifier.ORDER_OP_LESSOREQUALS: 	orderableOperator = Orderable.ORDER_OP_LESSOREQUALS; break;
			case Restriction.ColumnQualifier.ORDER_OP_GREATERTHAN: 		orderableOperator = Orderable.ORDER_OP_GREATERTHAN; break;
			case Restriction.ColumnQualifier.ORDER_OP_GREATEROREQUALS: 	orderableOperator = Orderable.ORDER_OP_GREATEROREQUALS; break;
			
			case Restriction.ColumnQualifier.ORDER_OP_NOT_EQUALS:		negateCR = true;
			case Restriction.ColumnQualifier.ORDER_OP_EQUALS: 			orderableOperator = Orderable.ORDER_OP_EQUALS;
																		break;
			
			case Restriction.ColumnQualifier.ORDER_OP_ISNOTNULL:		negateCR = true;
			case Restriction.ColumnQualifier.ORDER_OP_ISNULL:			orderableOperator = Orderable.ORDER_OP_EQUALS;
																		cqOperand = null; operandAsDVD.setToNull(); // just to be sure
																		break;
			default:
				String errmsg = "Invalid operator detected (not one of the Restriction.ColumnQualifier interface): " + cqOperator;
				logger.logThreadWarning(GDBMessages.ENGINE_OPERATOR_INVALID, "DERBY ERROR: " + errmsg);
				throw new SQLException( errmsg );
		}

		if ( null != cqOperand )
			try { operandAsDVD.setObjectForCast(cqOperand, true, cqOperand.getClass().getName() ); } // cannot use dvd.setValue(operand)
			catch (StandardException e) {
				throw new SQLException("Unable to convert Restriction.ColumnQualifier constant operand to DataValueDescriptor: " + e);
			}
		
		GenericScanQualifier gsq = new GenericScanQualifier();
		gsq.setQualifier( colId-1, operandAsDVD, orderableOperator, negateCR, orderedNulls, unknownRV );
		
		return gsq;
    }
	
	@Override
	public void initScan(String[] projectedCols, Restriction predicates) throws SQLException {
		
		try {
		
		logger.logInfo("initScan() qualifiers: " + (null==predicates?null:predicates.toSQL()));
		Restriction node = predicates;
		
		Qualifier[][] qualifiers = null; //new Qualifier[0][];
//		ArrayList<Qualifier> quals = new ArrayList<Qualifier>();
		
		// Incomplete... - need code to convert full Restriction tree to Qualifier[][]
		// Good Example with data set (0.5),(1.5),(2.5),(3.5),(4.5),(5.5),(6.5):
		// select * from ltblah where x>1 AND (x<2 OR x>3) AND (x<4 OR x>5) AND x<6
		// yields: 1.5, 3.5, 5.5
		if ( null != node && node instanceof Restriction.ColumnQualifier )
			qualifiers = new Qualifier[][] { new Qualifier[] {
					convertToInternalQualifier((Restriction.ColumnQualifier) node, (GaianResultSetMetaData) getMetaData())
			} };

		if ( this instanceof Pushable || this instanceof IQualifyable) {
			VTIEnvironment vtie = new VTIEnvironment() {
				public void setSharedState(String arg0, Serializable arg1) {}
				public Object getSharedState(String arg0) { return null; }
				public boolean isCompileTime() { return false; }
				public int getStatementIsolationLevel() { return 0; }
				public String getOriginalSQL() { return "Unknown"; }
			};
			
			if ( this instanceof Pushable ) {				
				int numCols = 0;
				for ( String s : projectedCols ) { if ( null != s ) numCols++; }
				
				if ( 0 < numCols ) {
				
					int k = 0;
					int[] projectedColIndexes = new int[numCols];
					for ( int i=0; i<projectedCols.length; i++ )
						if ( null != projectedCols[i] ) projectedColIndexes[k++] = i+1;
					
					logger.logInfo("Pushing projection: " + Arrays.asList(projectedCols) + ", ints: " + Util.intArrayAsString(projectedColIndexes));
					((Pushable)this).pushProjection(vtie, projectedColIndexes);
				}
			}
			if ( this instanceof IQualifyable ) ((IQualifyable) this).setQualifiers(vtie, qualifiers);
		}
		
		if ( this instanceof IFastPath ) // && null == nextRow )
			try { ((IFastPath) this).executeAsFastPath(); }
			catch (StandardException e) { throw new SQLException(e); }
			
		} catch ( Exception e ) { logger.logException("BLAH", "Exception in initScan: ", e); }
	}
	
	
	
	
	
	
	
	
	// ResultSet interface
	
	private boolean wasNull = false;
	
	private DataValueDescriptor getCell(int columnIndex) {
		DataValueDescriptor dvd = nextRow[columnIndex-1];
		wasNull = dvd.isNull();
		return dvd;
	}
	
	@Override public boolean absolute(int row) throws SQLException { debugStack(); return false; }
	@Override public void afterLast() throws SQLException { debugStack(); }
	@Override public void beforeFirst() throws SQLException { debugStack(); }
	@Override public void cancelRowUpdates() throws SQLException { debugStack(); }
	@Override public void deleteRow() throws SQLException { debugStack(); }
	@Override public int findColumn(String columnLabel) throws SQLException { debugStack(); return 0; }
	@Override public boolean first() throws SQLException { debugStack(); return false; }
	
	@Override public Array getArray(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public Array getArray(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public InputStream getAsciiStream(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public InputStream getAsciiStream(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		if ( null != nextRow )
			try {
				DataValueDescriptor dvd = getCell(columnIndex);
				return wasNull ? null : new BigDecimal( dvd.getDouble());
			} catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public BigDecimal getBigDecimal(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { debugStack(); return null; }
	@Override public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { debugStack(); return null; }
	
	@Override public InputStream getBinaryStream(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getStream(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public InputStream getBinaryStream(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public Blob getBlob(int columnIndex) throws SQLException {
		if ( null != nextRow )
			try {
				DataValueDescriptor dvd = getCell(columnIndex);
				if ( wasNull ) return null;
				Blob blob = ((EmbedConnection) GaianDBProcedureUtils.getDefaultDerbyConnection()).createBlob();
				blob.setBytes(1, dvd.getBytes());
				return blob;
			} catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public Blob getBlob(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public boolean getBoolean(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getBoolean(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return false;
	}
	@Override public boolean getBoolean(String columnLabel) throws SQLException { debugStack(); return false; }
	
	@Override public byte getByte(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getByte(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return 0;
	}
	@Override public byte getByte(String columnLabel) throws SQLException { debugStack(); return 0; }
	
	@Override public byte[] getBytes(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getBytes(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public byte[] getBytes(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public Reader getCharacterStream(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public Reader getCharacterStream(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public Clob getClob(int columnIndex) throws SQLException { 
		if ( null != nextRow )
			try {
				DataValueDescriptor dvd = getCell(columnIndex);
				if ( wasNull ) return null;
				Clob clob = ((EmbedConnection) GaianDBProcedureUtils.getDefaultDerbyConnection()).createClob();
				clob.setString(1, dvd.getString());
				return clob;
			} catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public Clob getClob(String columnLabel) throws SQLException { debugStack(); return null; }
	
	private static final Calendar calendarReference = new GregorianCalendar();
	@Override public Date getDate(int columnIndex) throws SQLException { return getDate(columnIndex, calendarReference); }
	@Override public Date getDate(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getDate(cal); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public Date getDate(String columnLabel, Calendar cal) throws SQLException { debugStack(); return null; }
	
	@Override public double getDouble(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getDouble(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return 0;
	}
	@Override public double getDouble(String columnLabel) throws SQLException { debugStack(); return 0; }
	
	@Override public float getFloat(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getFloat(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return 0;
	}
	@Override public float getFloat(String columnLabel) throws SQLException { debugStack(); return 0; }

	@Override public int getInt(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getInt(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return 0;
	}
	@Override public int getInt(String columnLabel) throws SQLException { debugStack(); return 0; }

	@Override public long getLong(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getLong(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return 0;
	}
	@Override public long getLong(String columnLabel) throws SQLException { debugStack(); return 0; }
	
	@Override public Reader getNCharacterStream(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public Reader getNCharacterStream(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public NClob getNClob(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public NClob getNClob(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public String getNString(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public String getNString(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public Object getObject(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getObject(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public Object getObject(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { debugStack(); return null; }
	@Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { debugStack(); return null; }
	 public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { debugStack(); return null; }
	 public <T> T getObject(String columnLabel, Class<T> type) throws SQLException { debugStack(); return null; }
	
	 @Override public Ref getRef(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public Ref getRef(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public RowId getRowId(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public RowId getRowId(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public SQLXML getSQLXML(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public SQLXML getSQLXML(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public short getShort(int columnIndex) throws SQLException { debugStack(); return 0; }
	@Override public short getShort(String columnLabel) throws SQLException { debugStack(); return 0; }
	
	@Override public String getString(int columnIndex) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getString(); } catch (StandardException e) { throw new SQLException(e); }
		debugStack();
		return null;
	}
	@Override public String getString(String columnLabel) throws SQLException { debugStack(); return null; }
	
	@Override public Time getTime(int columnIndex) throws SQLException { return getTime(columnIndex, calendarReference); }
	@Override public Time getTime(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getTime(cal); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { debugStack(); return null; }
	
	@Override public Timestamp getTimestamp(int columnIndex) throws SQLException { return getTimestamp(columnIndex, calendarReference); }
	@Override public Timestamp getTimestamp(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		if ( null != nextRow ) try { return getCell(columnIndex).getTimestamp(cal); } catch (StandardException e) { throw new SQLException(e); }
		debugStack(); return null;
	}
	@Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { debugStack(); return null; }
	
	@Override public URL getURL(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public URL getURL(String columnLabel) throws SQLException { debugStack(); return null; }
	@Override public InputStream getUnicodeStream(int columnIndex) throws SQLException { debugStack(); return null; }
	@Override public InputStream getUnicodeStream(String columnLabel) throws SQLException { debugStack(); return null; }

	@Override public int getRow() throws SQLException { debugStack(); return 0; }
	@Override public int getConcurrency() throws SQLException { debugStack(); return 0; }
	@Override public String getCursorName() throws SQLException { debugStack(); return null; }
	@Override public int getHoldability() throws SQLException { debugStack(); return 0; }
	@Override public int getType() throws SQLException { debugStack(); return 0; }
	@Override public Statement getStatement() throws SQLException { debugStack(); return null; }
	@Override public void insertRow() throws SQLException { debugStack(); }
	@Override public boolean isAfterLast() throws SQLException { debugStack(); return false; }
	@Override public boolean isBeforeFirst() throws SQLException { debugStack(); return false; }
	@Override public boolean isFirst() throws SQLException { debugStack(); return false; }
	@Override public boolean isLast() throws SQLException { debugStack(); return false; }
	@Override public boolean last() throws SQLException { debugStack(); return false; }
	@Override public void moveToCurrentRow() throws SQLException { debugStack(); }
	@Override public void moveToInsertRow() throws SQLException { debugStack(); }
	
	private DataValueDescriptor[] nextRow = null;
	
	@Override public boolean next() throws SQLException {
		// This next() method only serves to switch to the IFastPath.nextRow() method.
		// If the sub-class not not implement this, then it should implement ResultSet.next() itself...
		
//		logger.logDetail("Entered VTI60.next() for: " + this + ", is instanceof IFastPath: " + ( this instanceof IFastPath ));
		
		if ( this instanceof IFastPath ) {
			
			if ( null == nextRow ) {
				ResultSetMetaData rsmd = getMetaData();
				nextRow = new DataValueDescriptor[ rsmd.getColumnCount() ];
				for ( int i=0; i<rsmd.getColumnCount(); i++ ) {
					nextRow[i] = RowsFilter.constructDVDMatchingJDBCType( rsmd.getColumnType(i+1) );
//					logger.logDetail("Built nextRow dvd " + (i+1) + ": " + nextRow[i] + ", type: " + rsmd.getColumnType(i+1));
				}
			}
			
			try { return IFastPath.GOT_ROW == ((IFastPath) this).nextRow(nextRow); }
			catch (StandardException e) { logger.logInfo("Unable to fetch nextRow() through IFastPath: " + e); throw new SQLException(e); } // sub-class should log the exception...
		}
		
		// next() should be implemented by sub-class...
		return false;
	}
	
	@Override public boolean previous() throws SQLException { debugStack(); return false; }
	@Override public void refreshRow() throws SQLException { debugStack(); }
	@Override public boolean relative(int rows) throws SQLException { debugStack(); return false; }
	@Override public boolean rowDeleted() throws SQLException { debugStack(); return false; }
	@Override public boolean rowInserted() throws SQLException { debugStack(); return false; }
	@Override public boolean rowUpdated() throws SQLException { debugStack(); return false; }
	@Override public void updateArray(int columnIndex, Array x) throws SQLException { debugStack(); }
	@Override public void updateArray(String columnLabel, Array x) throws SQLException { debugStack(); }
	@Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { debugStack(); }
	@Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { debugStack(); }
	@Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { debugStack(); }
	@Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { debugStack(); }
	@Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { debugStack(); }
	@Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { debugStack(); }
	@Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { debugStack(); }
	@Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { debugStack(); }
	@Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { debugStack(); }
	@Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { debugStack(); }
	@Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { debugStack(); }
	@Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { debugStack(); }
	@Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { debugStack(); }
	@Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { debugStack(); }
	@Override public void updateBlob(int columnIndex, Blob x) throws SQLException { debugStack(); }
	@Override public void updateBlob(String columnLabel, Blob x) throws SQLException { debugStack(); }
	@Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { debugStack(); }
	@Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { debugStack(); }
	@Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { debugStack(); }
	@Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { debugStack(); }
	@Override public void updateBoolean(int columnIndex, boolean x) throws SQLException { debugStack(); }
	@Override public void updateBoolean(String columnLabel, boolean x) throws SQLException { debugStack(); }
	@Override public void updateByte(int columnIndex, byte x) throws SQLException { debugStack(); }
	@Override public void updateByte(String columnLabel, byte x) throws SQLException { debugStack(); }
	@Override public void updateBytes(int columnIndex, byte[] x) throws SQLException { debugStack(); }
	@Override public void updateBytes(String columnLabel, byte[] x) throws SQLException { debugStack(); }
	@Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { debugStack(); }
	@Override public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { debugStack(); }
	@Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { debugStack(); }
	@Override public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException { debugStack(); }
	@Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { debugStack(); }
	@Override public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { debugStack(); }
	@Override public void updateClob(int columnIndex, Clob x) throws SQLException { debugStack(); }
	@Override public void updateClob(String columnLabel, Clob x) throws SQLException { debugStack(); }
	@Override public void updateClob(int columnIndex, Reader reader) throws SQLException { debugStack(); }
	@Override public void updateClob(String columnLabel, Reader reader) throws SQLException { debugStack(); }
	@Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { debugStack(); }
	@Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { debugStack(); }
	@Override public void updateDate(int columnIndex, Date x) throws SQLException { debugStack(); }
	@Override public void updateDate(String columnLabel, Date x) throws SQLException { debugStack(); }
	@Override public void updateDouble(int columnIndex, double x) throws SQLException { debugStack(); }
	@Override public void updateDouble(String columnLabel, double x) throws SQLException { debugStack(); }
	@Override public void updateFloat(int columnIndex, float x) throws SQLException { debugStack(); }
	@Override public void updateFloat(String columnLabel, float x) throws SQLException { debugStack(); }
	@Override public void updateInt(int columnIndex, int x) throws SQLException { debugStack(); }
	@Override public void updateInt(String columnLabel, int x) throws SQLException { debugStack(); }
	@Override public void updateLong(int columnIndex, long x) throws SQLException { debugStack(); }
	@Override public void updateLong(String columnLabel, long x) throws SQLException { debugStack(); }
	@Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { debugStack(); }
	@Override public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { debugStack(); }
	@Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { debugStack(); }
	@Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { debugStack(); }
	@Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException { debugStack(); }
	@Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException { debugStack(); }
	@Override public void updateNClob(int columnIndex, Reader reader) throws SQLException { debugStack(); }
	@Override public void updateNClob(String columnLabel, Reader reader) throws SQLException { debugStack(); }
	@Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { debugStack(); }
	@Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { debugStack(); }
	@Override public void updateNString(int columnIndex, String nString) throws SQLException { debugStack(); }
	@Override public void updateNString(String columnLabel, String nString) throws SQLException { debugStack(); }
	@Override public void updateNull(int columnIndex) throws SQLException { debugStack(); }
	@Override public void updateNull(String columnLabel) throws SQLException { debugStack(); }
	@Override public void updateObject(int columnIndex, Object x) throws SQLException { debugStack(); }
	@Override public void updateObject(String columnLabel, Object x) throws SQLException { debugStack(); }
	@Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { debugStack(); }
	@Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { debugStack(); }
	@Override public void updateRef(int columnIndex, Ref x) throws SQLException { debugStack(); }
	@Override public void updateRef(String columnLabel, Ref x) throws SQLException { debugStack(); }
	@Override public void updateRow() throws SQLException { debugStack(); }
	@Override public void updateRowId(int columnIndex, RowId x) throws SQLException { debugStack(); }
	@Override public void updateRowId(String columnLabel, RowId x) throws SQLException { debugStack(); }
	@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { debugStack(); }
	@Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { debugStack(); }
	@Override public void updateShort(int columnIndex, short x) throws SQLException { debugStack(); }
	@Override public void updateShort(String columnLabel, short x) throws SQLException { debugStack(); }
	@Override public void updateString(int columnIndex, String x) throws SQLException { debugStack(); }
	@Override public void updateString(String columnLabel, String x) throws SQLException { debugStack(); }
	@Override public void updateTime(int columnIndex, Time x) throws SQLException { debugStack(); }
	@Override public void updateTime(String columnLabel, Time x) throws SQLException { debugStack(); }
	@Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { debugStack(); }
	@Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { debugStack(); }
	
	@Override public boolean wasNull() throws SQLException {
		return wasNull;
	}
}
