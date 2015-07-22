/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.sql.Connection;

import java.sql.ResultSet;
import java.sql.SQLException;


import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet20;
//import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.jdbc.EmbedStatement;

/**
 * @author DavidVyvyan
 *
 * This class adds to the standard ResultSet fucntionality in that it allows for fetching a maximum
 * number of rows (as in DB2's "fetch first N rows only" feature)
 * The class may be extended with more custom functionality.
 */
public class ShrinkableResultSet extends EmbedResultSet20 {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
    protected ResultSet rs;
    private Connection c;
    private boolean closeFlag;

    private int rowCount = 0;
    private int rowsToFetch = -1; // initialised to -1, meaning unlimited - i.e. fetch all rows
    
//    public ShrinkableResultSet(String url, String sql) throws SQLException {
//    	this(DriverManager.getConnection(url), sql);
//    	closeFlag = true;
//    }
    
	/**
	 * @param c
	 * @param sql
	 * @throws java.sql.SQLException
	 */
	public ShrinkableResultSet(Connection c, ResultSet rs) throws SQLException {
		super( (EmbedConnection) c, (org.apache.derby.iapi.sql.ResultSet) rs, false, (EmbedStatement) rs.getStatement(), true );
		this.rs = rs;
//		this.c = c;
//		Statement s = c.createStatement();
//		s.execute(sql);
//		rs = s.getResultSet();
	}
	
	public ShrinkableResultSet(Connection c, ResultSet rs, int rowsToFetch) throws SQLException {
    	this(c, rs);
		this.rowsToFetch = rowsToFetch;
	}
	
//	public Statement getStatement() throws SQLException {		
//		return rs.getStatement();
//	}
	
	public boolean next() throws SQLException {
		System.out.println("Entering ShrinkableResultSet next()");
		if ( -1 == rowsToFetch || rowsToFetch > rowCount++ )
			return rs.next();
		return false;
	}

    public void close() throws SQLException {
		System.out.println("Entering ShrinkableResultSet close()");
        rs.close();
        if(closeFlag && c != null) {
            c.close();
            c = null;
        }
    }

//    public boolean wasNull() throws SQLException {
//        return rs.wasNull();
//    }
//
//    public String getString(int i) throws SQLException {
//		System.out.println("Entering ShrinkableResultSet getString()");
//        return rs.getString(i);
//    }
//
//    public boolean getBoolean(int i) throws SQLException {
//        return rs.getBoolean(i);
//    }
//
//    public byte getByte(int i) throws SQLException {
//        return rs.getByte(i);
//    }
//
//    public short getShort(int i) throws SQLException {
//        return rs.getShort(i);
//    }
//
//    public int getInt(int i) throws SQLException {
//        return rs.getInt(i);
//    }
//
//    public long getLong(int i) throws SQLException {
//        return rs.getLong(i);
//    }
//
//    public float getFloat(int i) throws SQLException {
//        return rs.getFloat(i);
//    }
//
//    public double getDouble(int i) throws SQLException {
//        return rs.getDouble(i);
//    }
//
//    public BigDecimal getBigDecimal(int i, int j) throws SQLException {
//        return rs.getBigDecimal(i, j);
//    }
//
//    public byte[] getBytes(int i) throws SQLException {
//        return rs.getBytes(i);
//    }
//
//    public Date getDate(int i) throws SQLException {
//        return rs.getDate(i);
//    }
//
//    public Time getTime(int i) throws SQLException {
//        return rs.getTime(i);
//    }
//
//    public Timestamp getTimestamp(int i) throws SQLException {
//        return rs.getTimestamp(i);
//    }
//
//    public InputStream getAsciiStream(int i) throws SQLException {
//        return rs.getAsciiStream(i);
//    }
//
//    public InputStream getUnicodeStream(int i) throws SQLException {
//        return rs.getUnicodeStream(i);
//    }
//
//    public InputStream getBinaryStream(int i) throws SQLException {
//        return rs.getBinaryStream(i);
//    }
//
//    public String getString(String s) throws SQLException {
//        return rs.getString(s);
//    }
//
//    public boolean getBoolean(String s) throws SQLException {
//    	return rs.getBoolean(s);
//    }
//
//    public byte getByte(String s) throws SQLException {
//        return rs.getByte(s);
//    }
//
//    public short getShort(String s) throws SQLException {
//        return rs.getShort(s);
//    }
//
//    public int getInt(String s) throws SQLException {
//        return rs.getInt(s);
//    }
//
//    public long getLong(String s) throws SQLException {
//        return rs.getLong(s);
//    }
//
//    public float getFloat(String s) throws SQLException {
//        return rs.getFloat(s);
//    }
//
//    public double getDouble(String s) throws SQLException {
//        return rs.getDouble(s);
//    }
//
//    public BigDecimal getBigDecimal(String s, int i) throws SQLException {
//        return rs.getBigDecimal(s, i);
//    }
//
//    public byte[] getBytes(String s) throws SQLException {
//        return rs.getBytes(s);
//    }
//
//    public Date getDate(String s) throws SQLException {
//        return rs.getDate(s);
//    }
//
//    public Time getTime(String s) throws SQLException {
//        return rs.getTime(s);
//    }
//
//    public Timestamp getTimestamp(String s) throws SQLException {
//        return rs.getTimestamp(s);
//    }
//
//    public InputStream getAsciiStream(String s) throws SQLException {
//        return rs.getAsciiStream(s);
//    }
//
//    public InputStream getUnicodeStream(String s) throws SQLException {
//        return rs.getUnicodeStream(s);
//    }
//
//    public InputStream getBinaryStream(String s) throws SQLException {
//        return rs.getBinaryStream(s);
//    }
//
//    public SQLWarning getWarnings() throws SQLException {
//        return rs.getWarnings();
//    }
//
//    public void clearWarnings() throws SQLException {
//        rs.clearWarnings();
//    }
//
//    public String getCursorName() throws SQLException {
//        return rs.getCursorName();
//    }
//
//    public ResultSetMetaData getMetaData() throws SQLException {
//        return rs.getMetaData();
//    }
//
//    public Object getObject(int i) throws SQLException {
//        return rs.getObject(i);
//    }
//
//    public Object getObject(String s) throws SQLException {
//        return rs.getObject(s);
//    }
//
//    public int findColumn(String s) throws SQLException {
//        return rs.findColumn(s);
//    }

//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getCharacterStream(int)
//	 */
//	public Reader getCharacterStream(int columnIndex) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
//	 */
//	public Reader getCharacterStream(String columnName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getBigDecimal(int)
//	 */
//	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
//	 */
//	public BigDecimal getBigDecimal(String columnName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#isBeforeFirst()
//	 */
//	public boolean isBeforeFirst() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#isAfterLast()
//	 */
//	public boolean isAfterLast() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#isFirst()
//	 */
//	public boolean isFirst() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#isLast()
//	 */
//	public boolean isLast() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#beforeFirst()
//	 */
//	public void beforeFirst() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#afterLast()
//	 */
//	public void afterLast() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#first()
//	 */
//	public boolean first() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#last()
//	 */
//	public boolean last() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getRow()
//	 */
//	public int getRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return 0;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#absolute(int)
//	 */
//	public boolean absolute(int row) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#relative(int)
//	 */
//	public boolean relative(int rows) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#previous()
//	 */
//	public boolean previous() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#setFetchDirection(int)
//	 */
//	public void setFetchDirection(int direction) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getFetchDirection()
//	 */
//	public int getFetchDirection() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return 0;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#setFetchSize(int)
//	 */
//	public void setFetchSize(int rows) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getFetchSize()
//	 */
//	public int getFetchSize() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return 0;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getType()
//	 */
//	public int getType() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return 0;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getConcurrency()
//	 */
//	public int getConcurrency() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return 0;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#rowUpdated()
//	 */
//	public boolean rowUpdated() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#rowInserted()
//	 */
//	public boolean rowInserted() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#rowDeleted()
//	 */
//	public boolean rowDeleted() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return false;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateNull(int)
//	 */
//	public void updateNull(int columnIndex) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
//	 */
//	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateByte(int, byte)
//	 */
//	public void updateByte(int columnIndex, byte x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateShort(int, short)
//	 */
//	public void updateShort(int columnIndex, short x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateInt(int, int)
//	 */
//	public void updateInt(int columnIndex, int x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateLong(int, long)
//	 */
//	public void updateLong(int columnIndex, long x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateFloat(int, float)
//	 */
//	public void updateFloat(int columnIndex, float x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateDouble(int, double)
//	 */
//	public void updateDouble(int columnIndex, double x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
//	 */
//	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
//	 */
//	public void updateString(int columnIndex, String x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBytes(int, byte[])
//	 */
//	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
//	 */
//	public void updateDate(int columnIndex, Date x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
//	 */
//	public void updateTime(int columnIndex, Time x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
//	 */
//	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
//	 */
//	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
//	 */
//	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
//	 */
//	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
//	 */
//	public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
//	 */
//	public void updateObject(int columnIndex, Object x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateNull(java.lang.String)
//	 */
//	public void updateNull(String columnName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
//	 */
//	public void updateBoolean(String columnName, boolean x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
//	 */
//	public void updateByte(String columnName, byte x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
//	 */
//	public void updateShort(String columnName, short x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
//	 */
//	public void updateInt(String columnName, int x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
//	 */
//	public void updateLong(String columnName, long x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
//	 */
//	public void updateFloat(String columnName, float x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
//	 */
//	public void updateDouble(String columnName, double x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
//	 */
//	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
//	 */
//	public void updateString(String columnName, String x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
//	 */
//	public void updateBytes(String columnName, byte[] x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
//	 */
//	public void updateDate(String columnName, Date x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
//	 */
//	public void updateTime(String columnName, Time x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
//	 */
//	public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
//	 */
//	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
//	 */
//	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
//	 */
//	public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
//	 */
//	public void updateObject(String columnName, Object x, int scale) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
//	 */
//	public void updateObject(String columnName, Object x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#insertRow()
//	 */
//	public void insertRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateRow()
//	 */
//	public void updateRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#deleteRow()
//	 */
//	public void deleteRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#refreshRow()
//	 */
//	public void refreshRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#cancelRowUpdates()
//	 */
//	public void cancelRowUpdates() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#moveToInsertRow()
//	 */
//	public void moveToInsertRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#moveToCurrentRow()
//	 */
//	public void moveToCurrentRow() throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
//	 */
//	public Object getObject(int i, Map map) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getRef(int)
//	 */
//	public Ref getRef(int i) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getBlob(int)
//	 */
//	public Blob getBlob(int i) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getClob(int)
//	 */
//	public Clob getClob(int i) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getArray(int)
//	 */
//	public Array getArray(int i) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
//	 */
//	public Object getObject(String colName, Map map) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getRef(java.lang.String)
//	 */
//	public Ref getRef(String colName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getBlob(java.lang.String)
//	 */
//	public Blob getBlob(String colName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getClob(java.lang.String)
//	 */
//	public Clob getClob(String colName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getArray(java.lang.String)
//	 */
//	public Array getArray(String colName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
//	 */
//	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
//	 */
//	public Date getDate(String columnName, Calendar cal) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
//	 */
//	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
//	 */
//	public Time getTime(String columnName, Calendar cal) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
//	 */
//	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
//	 */
//	public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getURL(int)
//	 */
//	public URL getURL(int columnIndex) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#getURL(java.lang.String)
//	 */
//	public URL getURL(String columnName) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		return null;
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
//	 */
//	public void updateRef(int columnIndex, Ref x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
//	 */
//	public void updateRef(String columnName, Ref x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
//	 */
//	public void updateBlob(int columnIndex, Blob x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
//	 */
//	public void updateBlob(String columnName, Blob x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
//	 */
//	public void updateClob(int columnIndex, Clob x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
//	 */
//	public void updateClob(String columnName, Clob x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
//	 */
//	public void updateArray(int columnIndex, Array x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
//	 */
//	public void updateArray(String columnName, Array x) throws SQLException {
//		try { throw new Exception("blah haha"); } catch ( Exception e ) { e.printStackTrace(); }
//		
//	}	
}
