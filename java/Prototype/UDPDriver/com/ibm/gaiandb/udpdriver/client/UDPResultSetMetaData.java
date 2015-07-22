/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.client;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.common.protocol.MetaData;

/**
 * Implementation of java.sql.ResultSetMetaData
 * 
 * @author lengelle
 *
 */
public class UDPResultSetMetaData implements ResultSetMetaData
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPResultSetMetaData", 25 );
	
	private int numberOfColumns;
	private String[] columnNames;
	private int[] columnTypes;
	private List<Integer> nullableColumns;
	private int[] columnScale;
	private int[] columnPrecision;
	private int[] columnDisplaySize;
 	
	/**
	 * Creates a new UDPResultSetMetaData object from a UDP driver protocol message MetaData
	 * 
	 * @param metaData
	 */
	public UDPResultSetMetaData( MetaData metaData )
	{
		numberOfColumns = metaData.getNumberOfColumns();
		columnNames = metaData.getColumnNames();
		columnTypes = metaData.getColumnTypes();
		nullableColumns = metaData.getNullableColumns();
		columnScale = metaData.getColumnScale();
		columnPrecision = metaData.getColumnPrecision();
		columnDisplaySize = metaData.getColumnDisplaySize();
	}
	
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getColumnCount()
	 */
	public int getColumnCount() throws SQLException
	{
		return numberOfColumns;
	}
	
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getColumnName(int)
	 */
	public String getColumnName( int column ) throws SQLException
	{
		if ( columnNames!=null && column<=columnNames.length )
		{
			return columnNames[column-1];
		}
		throw new SQLException( "UDPResultSetMetaData : getColumnName() It is not possible to get the column names." );
	}
	
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getColumnType(int)
	 */
	public int getColumnType( int column ) throws SQLException
	{
		if ( columnTypes!=null && column<=columnTypes.length )
		{
			return columnTypes[column-1];
		}
		throw new SQLException( "UDPResultSetMetaData : getColumnType() It is not possible to get the column type." );
	}

	
    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isNullable(int)
     */
    public int isNullable( int column ) throws SQLException 
    {
        if ( nullableColumns!=null )
        {
            if ( nullableColumns.contains( column ) )
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
        throw new SQLException( "UDPResultSetMetaData : isNullable() It is not possible to know if the column is nullable." );
    }
	
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
	 */
	public String getColumnTypeName( int column ) throws SQLException 
	{
		try
		{
			int type = columnTypes[ column-1 ];
			String columnName = this.lookupColumnTypeName( type );

			return columnName;
		}
		catch( Exception e )
		{
			throw new SQLException( "UDPResultSetMetaData : getColumnTypeName( int column ) failed. "+ e );
		}
	}
    
    
    private String lookupColumnTypeName( int type ) throws SQLException
    {
		switch ( type )
		{
			case Types.CHAR:			return "CHAR";
			case Types.VARCHAR:			return "VARCHAR";
			case Types.LONGVARCHAR: 	return "LONGVARCHAR";
			case Types.LONGVARBINARY:	return "LONGVARBINARY";
			case Types.DECIMAL:			return "DECIMAL";
			case Types.NUMERIC:			return "NUMERIC";
			case Types.FLOAT:			return "FLOAT";
			case Types.BINARY:			return "BINARY";
			case Types.VARBINARY:		return "VARBINARY";
			case Types.BIT:				return "BIT";
			case Types.BOOLEAN:			return "BOOLEAN";
			case Types.BLOB:			return "BLOB";
			case Types.CLOB:			return "CLOB";
			case Types.DATE:			return "DATE";
			case Types.TIME:			return "TIME";
			case Types.TIMESTAMP:		return "TIMESTAMP";
			case Types.INTEGER:			return "INTEGER";
			case Types.BIGINT:			return "BIGINT";
			case Types.SMALLINT:		return "SMALLINT";
			case Types.TINYINT:			return "TINYINT";
			case Types.DOUBLE:			return "DOUBLE";
			case Types.REAL:			return "REAL";
			case Types.NULL:			return "NULL";
//			case Types.ARRAY:			
//			case Types.JAVA_OBJECT:		
//			case Types.STRUCT:			
//			case Types.REF:			
//			case Types.ARRAY:			
//			case Types.DATALINK:		
//			case Types.REF:				
//			case Types.DISTINCT:		
//			case Types.OTHER:			
			default: 
				String w = "Unsupported JDBC type: " + type;
				logger.logWarning(GDBMessages.NETDRIVER_COLUMN_LOOKUP_JDBC_TYPE_UNSUPPORTED, w);
				throw new SQLException(w);			
		}
    }
	
    
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
	 */
	public int getColumnDisplaySize( int column ) throws SQLException 
	{
		if ( columnDisplaySize!=null && column<=columnDisplaySize.length )
		{
			return columnDisplaySize[column-1];
		}
		throw new SQLException( "UDPResultSetMetaData : getColumnDisplaySize() It is not possible to get the column display size." );
	}
	
	
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getPrecision(int)
	 */
	public int getPrecision( int column ) throws SQLException 
	{
		if ( columnPrecision!=null && column<=columnPrecision.length )
		{
			return columnPrecision[column-1];
		}
		throw new SQLException( "UDPResultSetMetaData : getPrecision() It is not possible to get the column precision." );
	}


	
	/* (non-Javadoc)
	 * @see java.sql.ResultSetMetaData#getScale(int)
	 */
	public int getScale( int column ) throws SQLException 
	{
		if ( columnScale!=null && column<=columnScale.length )
		{
			return columnScale[column-1];
		}
		throw new SQLException( "UDPResultSetMetaData : getScale() It is not possible to get the column scale." );
	}
	
	public String getCatalogName( int column ) throws SQLException
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getCatalogName( int column ) : Unimplemented method." );
		return null;
	}


	
	public String getColumnClassName( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getColumnClassName( int column ) : Unimplemented method." );
		return null;
	}

	
	public String getSchemaName( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getSchemaName( int column ) : Unimplemented method." );
		return null;
	}


	
	public String getTableName( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getTableName( int column ) : Unimplemented method." );
		return null;
	}


	
	public boolean isAutoIncrement( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isAutoIncrement( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isCaseSensitive( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isCaseSensitive( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isCurrency( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isCurrency( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isDefinitelyWritable( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isDefinitelyWritable( int column ) : Unimplemented method." );
		return false;
	}

	
	
	public boolean isReadOnly( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isReadOnly( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isSearchable( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isSearchable( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isSigned( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isSigned( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isWritable( int column ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isWritable( int column ) : Unimplemented method." );
		return false;
	}


	
	public boolean isWrapperFor( Class<?> iface ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isWrapperFor( Class<?> iface ) : Unimplemented method." );
		return false;
	}


	
	public <T> T unwrap( Class<T> iface ) throws SQLException 
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "unwrap( Class<T> iface ) : Unimplemented method." );
		return null;
	}

	
	public String getColumnLabel( int column ) throws SQLException
	{
		logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getColumnLabel( int column ) : Unimplemented method." );
		return null;
	}
	
}
