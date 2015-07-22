/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.common;

import java.sql.Types;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBit;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLClob;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLDecimal;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongVarbit;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLLongvarchar;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.SQLTinyint;
import org.apache.derby.iapi.types.SQLVarbit;
import org.apache.derby.iapi.types.SQLVarchar;

/**
 * Copy from com.ibm.gaiandb.RowsFilter
 * This class has been copied for modularity reasons, so the UDP driver remains independant
 * from GaianDB.
 * 
 * @author lengelle
 *
 */
public class RowsFilter
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
    
	/**
	 * See com.ibm.gaiandb.RowsFilter
	 * 
	 * @param jdbcType
	 * @return
	 */
    public static DataValueDescriptor constructDVDMatchingJDBCType( int jdbcType ) {
        
        // Build an appropriate DataValueDescriptor for this column
        switch ( jdbcType ) {
        
            // Note BIT is a single binary bit, and the recommended Java mapping for the JDBC BIT type is as a Java boolean:
            // http://java.sun.com/j2se/1.3/docs/guide/jdbc/getstart/mapping.html
        
            // If we were to map BIT to SQLBit(), then we couldn't set its value to an int.
            // However we *can* set the value of a SQLBoolean() to a byte[].
        
            case Types.DECIMAL: case Types.NUMERIC: return new SQLDecimal();
            case Types.CHAR: return new SQLChar();
            case Types.VARCHAR: return new SQLVarchar();
            case Types.LONGVARCHAR: return new SQLLongvarchar();
            case Types.VARBINARY: return new SQLVarbit();
            case Types.LONGVARBINARY: return new SQLLongVarbit();
            case Types.BINARY: return new SQLBit();
            case Types.BOOLEAN: case Types.BIT: return new SQLBoolean();
            case Types.BLOB: return new SQLBlob(); // size must be <= int bytes to be put in memory
            case Types.CLOB: return new SQLClob(); // size must be <= int bytes to be put in memory
            case Types.DATE: return new SQLDate();
            case Types.TIME: return new SQLTime();
            case Types.TIMESTAMP: return new SQLTimestamp();
            case Types.INTEGER: return new SQLInteger();
            case Types.BIGINT: return new SQLLongint();
            case Types.SMALLINT: return new SQLSmallint();
            case Types.TINYINT: return new SQLTinyint(); // prob not used
            case Types.DOUBLE: case Types.FLOAT: return new SQLDouble();
            case Types.REAL: return new SQLReal();
//          case Types.ARRAY: return new SQL (Array) data );
//          case Types.JAVA_OBJECT: case Types.STRUCT: return new SQL data );
//          case Types.REF: return new SQLRef( new RowHeapLocation(s)... );
//          case Types.DATALINK: return new SQL data );
//          case Types.DISTINCT: case Types.NULL: case Types.OTHER: return new SQLN Types.NULL ); // No distinct type supported
            default: ; return null;
        }
    }


}
