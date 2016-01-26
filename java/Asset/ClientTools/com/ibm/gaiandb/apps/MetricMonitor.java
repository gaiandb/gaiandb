/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Map;

import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * The metric monitor adds self-monitoring facilities to GaianDB. It creates a
 * logical table called GDB_METRICS (which should not be used by other sources).
 * Anyone can add a monitor by creating an object which implements
 * <code>MetricMonitor.Monitor&lt;T&gt;</code> and adding it to the singleton
 * object retrieved through the <code>getInstance</code> method.
 * 
 * @see Monitor
 * @see #getInstance
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class MetricMonitor implements Runnable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	// Keep this symbol referred to from here - to minimise updates required when eclipse gets upset about mapping this char with UTF8
	public static final char TEMPERATURE_SYMBOL = '\u00b0';
	
	/**
	 * An interface, of which implementing objects can be added to
	 * <code>MetricMonitor</code>. The method <code>getValue</code> must return
	 * a value which will then be inserted into the metrics table.
	 * 
	 * @param <T>
	 *            The type of the value to be returned.
	 * 
	 * @author Samir Talwar - stalwar@uk.ibm.com
	 */
	public static interface Monitor<T> {
		/**
		 * Returns a value which will be converted to a string using its
		 * <code>toString</code> method and inserted into the database.
		 * 
		 * @return A value which, when converted to a string, has a maximum
		 *         length of 255 characters, or null if there is no data to
		 *         return.
		 */
		public T getValue();
	}

	/** The class logger. */
	private static final Logger logger = new Logger("MetricMonitor", 25);

	/**
	 * The interval, in milliseconds, between subsequent inserts of the return
	 * value of each monitor's <code>getValue</code> method.
	 */
	private static final int INTERVAL = 1000;

	/**
	 * The maximum age of the values in seconds. After they pass this threshold,
	 * they are deleted from the table. 
	 */
	private static final int OLD_VALUES_THRESHOLD = 60;

	/** The maximum length of a monitor name. */
	private static final int MAX_NAME_LENGTH = 32;

	/** The maximum length of a value retrieved from a monitor. */
	private static final int MAX_VALUE_LENGTH = 255;

	/**
	 * The name of the physical table. Used primarily for inserts and deletes.
	 */
	public static final String PHYSICAL_TABLE_NAME = "GDB_LOCAL_METRICS";

	/** The name of the logical table. */

	public static final String LOGICAL_TABLE_NAME = "GDB_METRICS";

	/** The name of the logical table when using it with provenance. */
	public static final String LOGICAL_TABLE_NAME_WITH_PROVENANCE =
		LOGICAL_TABLE_NAME + "_P";

	/** When executed, creates the physical table. */
	private static final String CREATE_PHYSICAL_TABLE_SQL =
		"CREATE TABLE " + PHYSICAL_TABLE_NAME + "(" +
		"  name VARCHAR(" + MAX_NAME_LENGTH + ")," +
		"  value VARCHAR(" + MAX_VALUE_LENGTH + ")," +
		"  received_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
		")";
	
	public static String getCreateMetricsTableSQL() {
		return CREATE_PHYSICAL_TABLE_SQL;
	}

	/** When executed, links the logical table name to the physical table. */
	private static final String CREATE_LOGICAL_TABLE_SQL =
		"CALL SETLTFORRDBTABLE(" +
		"  '" + LOGICAL_TABLE_NAME + "'," +
		"  'LOCALDERBY'," +
		"  '" + PHYSICAL_TABLE_NAME + "'" +
		")";

	/** Inserts new data into the table. */
	public static final String INSERT_SQL =
		"INSERT INTO " + PHYSICAL_TABLE_NAME + "(name, value)" +
		" VALUES (?, ?)";

	/** Deletes data older than the threshold from the table. */
	public static final String DELETE_SQL =
		"DELETE FROM " + PHYSICAL_TABLE_NAME +
		" WHERE jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) > " + OLD_VALUES_THRESHOLD;

	/** A set of instances. One is created per database connection. */
	private static final Map<Connection, MetricMonitor> INSTANCES =
		new Hashtable<Connection, MetricMonitor>();

	/** The database connection. */
	private final Connection conn;

	/** The map of monitor names to monitors. */
	private final Map<String, Monitor<?>> monitors = new Hashtable<String, Monitor<?>>();

	/**
	 * Set to true, ending the infinite loop, when <code>stop</code> is called.
	 * 
	 * @see #stop
	 */
	private boolean stopped = false;

	/**
	 * Private constructor that initialises a metric monitor. Only one instance
	 * should exist per connection.
	 * 
	 * @param conn The database connection to use.
	 */
	private MetricMonitor(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Retrieves an instance of <code>MetricMonitor</code> corresponding to the
	 * given connection. If one does not exist, it creates one and starts it.
	 * 
	 * @param conn
	 *            The database connection.
	 * 
	 * @return A new or existing <code>MetricMonitor</code>.
	 */
	public static synchronized MetricMonitor getInstance(Connection conn) {
		MetricMonitor instance = INSTANCES.get(conn);
		if (null == instance) {
			instance = new MetricMonitor(conn);
			INSTANCES.put(conn, instance);
			new Thread(instance,"MetricMonitor").start();
		}
		return instance;
	}

	/**
	 * Creates the physical and logical tables, and starts inserting data using
	 * the list of monitors.
	 */
	public void run() {
		if (!createTable()) {
			stopped = true;
			return;
		}

		PreparedStatement insertStatement;
		PreparedStatement deleteStatement;
		try {
			insertStatement = conn.prepareStatement(INSERT_SQL);
			deleteStatement = conn.prepareStatement(DELETE_SQL);
		}
		catch (SQLException e) {
			logger.logException(GDBMessages.MMON_STATEMENT_PREPARE_ERROR_SQL, "Could not prepare the MetricMonitor insert statement.", e);
			stopped = true;
			return;
		}

		while (!stopped) {
			try {
				deleteStatement.execute();

				insertStatement.clearBatch();
				for (String name : monitors.keySet()) {
					Object value = monitors.get(name).getValue();
					if (null != value) {
						String sValue = truncate(value.toString(), MAX_VALUE_LENGTH);
						insertStatement.clearParameters();
						insertStatement.setString(1, name);
						insertStatement.setString(2, sValue);
						insertStatement.addBatch();
					}
				}
				insertStatement.executeBatch();
			}
			catch (SQLException e) {
				try {
					if (null == conn || conn.isClosed()) {
						logger.logImportant("Connection was closed.");
						break;
					}
					else {
						logger.logException(GDBMessages.MMON_METRICS_INSERT_ERROR_SQL, "Could not insert metrics into the " + LOGICAL_TABLE_NAME + " table.", e);
					}
				}
				catch (SQLException e1) {
					break;
				}
			}

			try {
				Thread.sleep(INTERVAL);
			}
			catch (InterruptedException e) {
				logger.logImportant("The metric monitor thread was interrupted.");
				break;
			}
		}

		if (null != conn) {
			INSTANCES.remove(conn);
		}

		stopped = true;
	}

	/**
	 * Stops the monitor.
	 */
	public void stop() {
		stopped = true;
	}

	/**
	 * Informs the caller whether the monitor is still running.
	 * 
	 * @return True if the monitor is running, or false if it is not.
	 */
	public boolean isRunning() {
		return !stopped;
	}

	/**
	 * Adds monitors that record JVM metrics.
	 */
	public void addJVMMonitors() {
		addMonitor("jvm_used_memory", new MetricMonitor.Monitor<Long>() {
			Runtime runtime = Runtime.getRuntime();
			public Long getValue() {
				return runtime.totalMemory() - runtime.freeMemory();
			}
		});
    }

	/**
	 * Adds a monitor to the list. The name provided will be used when inserting
	 * data into the table. If a monitor with the same name is provided, it will
	 * be overwritten.
	 * 
	 * @param <T>
	 *            The monitor type.
	 * @param name
	 *            The name of the monitor. Maximum length is 32 characters. A
	 *            longer name will be truncated.
	 * @param monitor
	 *            The monitor itself.
	 */
	public <T> void addMonitor(String name, Monitor<T> monitor) {
		monitors.put(truncate(name, MAX_NAME_LENGTH), monitor);
		logger.logInfo("Monitor \"" + name + "\" added.");
	}

	/**
	 * Removes a monitor with the given name from the list.
	 * 
	 * @param name
	 *            The name of the monitor. Maximum length is 32 characters. A
	 *            longer name will be truncated.
	 */
	public void removeMonitor(String name) {
		monitors.remove(truncate(name, MAX_NAME_LENGTH));
		logger.logInfo("Monitor \"" + name + "\" removed.");
	}

	/**
	 * Creates the physical and logical tables.
	 * 
	 * @return True on success; false on failure.
	 */
	private boolean createTable() {
		Statement stmt;
		try {
			stmt = conn.createStatement();
		}
		catch (SQLException e) {
			logger.logException(GDBMessages.MMON_STATEMENT_CREATE_ERROR_SQL, "Could not create the " + PHYSICAL_TABLE_NAME + " table.", e);
			return false;
		}

		try {
			Util.executeCreateIfDerbyTableDoesNotExist( stmt, null, PHYSICAL_TABLE_NAME, CREATE_PHYSICAL_TABLE_SQL );

			String sql = "CALL LISTLTS()";
			ResultSet logicalTables = stmt.executeQuery(sql);
			boolean found = false;
			while (logicalTables.next()) {
				if (logicalTables.getString("LTNAME").equalsIgnoreCase(LOGICAL_TABLE_NAME)) {
					found = true;
					break;
				}
			}

			if (!found) {
				stmt.execute(CREATE_LOGICAL_TABLE_SQL);
			}
		}
		catch (SQLException e) {
			logger.logException(GDBMessages.MMON_STATEMENT_EXECUTE_ERROR_SQL, "Could not create the " + PHYSICAL_TABLE_NAME + " table.", e);
			return false;
		}

		return true;
	}

	/**
	 * Utility function. Truncates the string to the length provided. If the
	 * string is shorter than or of equal length to the maximum length, this
	 * function returns it unchanged.
	 * 
	 * @param s
	 *            The string to be truncated.
	 * @param maxLength
	 *            Its maximum length. Must be greater than or equal to 0.
	 * 
	 * @return A truncated string.
	 */
	private static String truncate(String s, int maxLength) {
		if (maxLength < 0) {
			return s;
		}

		if (s.length() > maxLength) {
			return s.substring(0, maxLength);
		}
		else {
			return s;
		}
	}
}
