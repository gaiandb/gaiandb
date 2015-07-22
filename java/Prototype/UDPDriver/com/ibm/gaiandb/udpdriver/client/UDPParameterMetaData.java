/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.udpdriver.client;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.common.protocol.MetaData;

/**
 * Implementation of java.sql.ParameterMetaData
 * 
 * @author lengelle
 *
 */
public class UDPParameterMetaData implements ParameterMetaData
{
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2010";
	
	private static final Logger logger = new Logger( "UDPParameterMetaData", 25 );
	
    private int numberOfparameters;
    
    public UDPParameterMetaData( MetaData metaData )
    {
        numberOfparameters = metaData.getNumberOfParameters();
    }
    
    /* (non-Javadoc)
     * @see java.sql.ParameterMetaData#getParameterCount()
     */
    public int getParameterCount()
    {
        return numberOfparameters;
    }
    
    
    
    
    public int getParameterType( int paramIndex ) throws SQLException
    {
    	logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getParameterType( int paramIndex ) : Unimplemented method." );
        return -1;
    }
    
    
    public String getParameterClassName( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getParameterClassName( int paramIndex ) : Unimplemented method." );
        return null ;
    }

    
    public int getParameterMode( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getParameterMode( int paramIndex ) : Unimplemented method." );
        return 0 ;
    }

    
    public String getParameterTypeName( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getParameterTypeName( int paramIndex ) : Unimplemented method." );
        return null ;
    }

    
    public int getPrecision( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getPrecision( int paramIndex ) : Unimplemented method." );
        return 0 ;
    }

    
    public int getScale( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "getScale( int paramIndex ) : Unimplemented method." );
        return 0 ;
    }

    
    public int isNullable( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isNullable( int paramIndex ) : Unimplemented method." );
        return 0 ;
    }

    
    public boolean isSigned( int paramIndex ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isSigned( int paramIndex ) : Unimplemented method." );
        return false ;
    }

    
    public boolean isWrapperFor( Class<?> iface ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "isWrapperFor( Class<?> iface ) : Unimplemented method." );
        return false ;
    }

    
    public <T> T unwrap( Class<T> iface ) throws SQLException
    {
        logger.logWarning( GDBMessages.NETDRIVER_UNIMPLEMENTED_METHOD, "unwrap( Class<T> iface ) : Unimplemented method." );
        return null ;
    }

}
