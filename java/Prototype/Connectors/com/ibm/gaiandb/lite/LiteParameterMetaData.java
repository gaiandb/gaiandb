/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.lite;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * @author DavidVyvyan
 */

public class LiteParameterMetaData implements ParameterMetaData {

	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	private int parameterCount = 0;
	
	public void setParameterCount(int parameterCount) {
		this.parameterCount = parameterCount;
	}

	public int getParameterCount() throws SQLException {
		return parameterCount;
	}
	
	public String getParameterClassName(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getParameterMode(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getParameterType(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getParameterTypeName(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getPrecision(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getScale(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int isNullable(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isSigned(int paramIndex) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
}
