/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.utils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class is used to fool the DriverManager - as it refuses to use drivers that were not loaded by the system class loader.
 * 
 * @author drvyvyan
 */
public class DriverWrapper implements Driver {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";

	private Driver driver; public DriverWrapper(Driver driver) { this.driver = driver; }
	@Override public boolean acceptsURL(String url) throws SQLException { return this.driver.acceptsURL(url); }
	@Override public Connection connect(String url, Properties info) throws SQLException { return this.driver.connect(url, info); }
	@Override public int getMajorVersion() { return this.driver.getMajorVersion(); }
	@Override public int getMinorVersion() { return this.driver.getMinorVersion(); }
	@Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException { return this.driver.getPropertyInfo(url, info); }
	@Override public boolean jdbcCompliant() { return this.driver.jdbcCompliant(); }
	@Override public Logger getParentLogger() throws SQLFeatureNotSupportedException { return this.driver.getParentLogger(); }
}
