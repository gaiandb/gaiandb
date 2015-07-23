/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.gaiandb.apps.MetricMonitor;

import java.awt.Color;

public abstract class MetricValueProcessor {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final Color[] STANDARD_COLOR_PALETTE = new Color[] {
//		new Color(0x00FF00),	// green
//		new Color(0x66CCFF),	// sky blue
//		new Color(0x800080),	// purple
//		new Color(0xDDA0DD),	// plum
//		new Color(0xFFC8CB)		// pink
		new Color(0x0000FF),	// blue
		new Color(0xFFFF00),	// yellow
		new Color(0xFF0000)		// red
	};

	private static final Color[] REVERSED_COLOR_PALETTE = new Color[] {
		new Color(0xFF0000),
		new Color(0xFFFF00),
//		new Color(0x66CCFF),	// sky blue
		new Color(0x0000FF),	// blue
//		new Color(0x00FF00)
	};

	private static final Color[] TEMPERATURE_COLOR_PALETTE = new Color[] {
		new Color(0x66CCFF),
		new Color(0x00FF00),
		new Color(0xFFFF00),
		new Color(0xFF6600),
		new Color(0xFF0000)
	};
	
//	private static final Color[] TEMPERATURE_COLOR_PALETTE = new Color[] {
//		new Color(0xFF0000),
//		new Color(0xFF6600),
//		new Color(0xFFFF00),
//		new Color(0x00FF00),
//		new Color(0x66CCFF)
//	};
	
	private static final String METRIC_ECCENTRICITY = "Eccentricity";
	private static final String METRIC_CONNECTIVITY = "Connectivity";
	private static final String METRIC_THROUGHPUT = "Data Throughput";
	private static final String METRIC_QRY_ACTIVITY = "Query Activity";
	private static final String METRIC_NODE_CPU = "Node CPU";
//	private static final String METRIC_DS_CPU = "DS CPU";
	private static final String METRIC_MEM_JVM = "JVM Used Memory";
	
	private static final String METRIC_CPU = "CPU";
	private static final String METRIC_MEM = "Used Memory";
	private static final String METRIC_DISK_IO = "Disk I/O";
	private static final String METRIC_NET_IO = "Network I/O";
	private static final String METRIC_BATTERY = "Battery Charge";
	private static final String METRIC_TEMPERATURE = "Temperature";
	
//	private static final Set<String> sensorMetricNames = new HashSet<String>( Arrays.asList(
//			new String[] { METRIC_CPU, METRIC_MEM, METRIC_DISK_IO, METRIC_NET_IO, METRIC_BATTERY, METRIC_TEMPERATURE } ) );
//	private static final Set<String> gaianMetricNames = new HashSet<String>( Arrays.asList(
//			new String[] { METRIC_THROUGHPUT, METRIC_MEM_JVM } ) );
	
	static final String[] internalMetrics = new String[] { METRIC_ECCENTRICITY, METRIC_CONNECTIVITY };
	static final Set<String> internalMetricsSet = new HashSet<String>( Arrays.asList( internalMetrics ) );
	
	public static final MonitorInfo[] MONITORS = {
		
		// Absolute measure. Note also in future: should max connections be a hard bound ?
		new MonitorInfo(METRIC_CONNECTIVITY, "CONN", "Links", new String[0],
			false, 0, 10, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_CONNECTIVITY)),
		
		// Relative measure.
		new MonitorInfo(METRIC_ECCENTRICITY, "ECCE", "Steps", new String[0],
			false, 0, 5, REVERSED_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_ECCENTRICITY)),
		
		// Relative.
		new MonitorInfo(METRIC_THROUGHPUT, "THRU", "KB/s", new String[] { METRIC_THROUGHPUT },
			false, 0, 100, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_THROUGHPUT)),
		
		// Relative.
		new MonitorInfo(METRIC_QRY_ACTIVITY, "ACTIV", "ms/s", new String[] { METRIC_QRY_ACTIVITY },
			false, 0, 2000, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_QRY_ACTIVITY)),
		
		// Absolute.
		new MonitorInfo(METRIC_NODE_CPU, "NCPU", "%", new String[] { METRIC_NODE_CPU },
			false, 0, 20, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_NODE_CPU)),
		
//			// Absolute.
//			new MonitorInfo(METRIC_DS_CPU, "DSCPU", "%", new String[] { METRIC_DS_CPU },
//				false, 0, 10, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_DS_CPU)),
		
		// Absolute.
		new MonitorInfo(METRIC_MEM_JVM, "JMEM", "%", new String[] { METRIC_MEM_JVM },
			true, 0, 100, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever(METRIC_MEM_JVM)),
		
		new MonitorInfo(METRIC_CPU, "CPU", "%", new String[] { "CPU Usage" },
			true, 0, 100, STANDARD_COLOR_PALETTE, new MonitorInfo.DefaultValueRetriever("CPU Usage")),
			
		new MonitorInfo(METRIC_MEM, "MEM", "%", new String[] { "Used Memory", "Total Memory" },
			true, 0, 100, STANDARD_COLOR_PALETTE,
			new MonitorInfo.ValueRetriever() {
		    	public Integer get(Map<String, Integer> currentValues) {
		    		Integer usedMemory = currentValues.get("Used Memory");
		    		Integer totalMemory = currentValues.get("Total Memory");
		    		if (null != usedMemory && null != totalMemory) {
		    			return usedMemory * 100 / totalMemory;
		    		}
		    		else {
		    			return null;
		    		}
		    	}
	    	}),
		new MonitorInfo(METRIC_DISK_IO, "DISK", "KB/s", new String[] { "Disk I/O" },
			false, 0, 10000, STANDARD_COLOR_PALETTE,
			new MonitorInfo.DefaultValueRetriever(METRIC_DISK_IO)),
		new MonitorInfo(METRIC_NET_IO, "NET", "KB/s", new String[] { "Network I/O" },
			false, 0, 500, STANDARD_COLOR_PALETTE,
			new MonitorInfo.DefaultValueRetriever(METRIC_NET_IO)),
		new MonitorInfo(METRIC_BATTERY, "BAT", "%", new String[] { "Battery Power" },
			true, 0, 100, REVERSED_COLOR_PALETTE,
			new MonitorInfo.DefaultValueRetriever("Battery Power")),
		new MonitorInfo(METRIC_TEMPERATURE, "TEMP", MetricMonitor.TEMPERATURE_SYMBOL + "C", new String[] { "Temperature" },
			true, 0, 100, TEMPERATURE_COLOR_PALETTE,
			new MonitorInfo.DefaultValueRetriever(METRIC_TEMPERATURE))
	};

//	private static final String SQL_HISTORICAL_METRICS =
//		"    SELECT gdb_node," +
//		"           jSecs(CURRENT_TIMESTAMP) - age received_timestamp," +
//		"           name," +
//		"           CAST(value AS INT) value" +
//		"     FROM new com.ibm.db2j.GaianQuery(" +
//		"		'SELECT name, jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) age, value FROM gdb_local_metrics " +
//		"		UNION SELECT ''Throughput'' name, 0 age, cast(GDB_THROUGHPUT() as char(20)) value from sysibm.sysdummy1 " +
//		"		UNION SELECT ''JVM Used Memory'' name, 0 age, cast(JMEMORYPERCENT() as char(3)) value from sysibm.sysdummy1', " +
//		"		'with_provenance') Q" +
//		"    WHERE name IN ($monitors)" +
//		"      AND age < ?" +
//		" ORDER BY gdb_node, received_timestamp, name";

	private static final String SQL_HISTORICAL_METRICS =
		"    SELECT gdb_node," +
		"           jSecs(CURRENT_TIMESTAMP) - age received_timestamp," +
		"           name," +
		"           CAST(value AS INT) value" +
		"     FROM new com.ibm.db2j.GaianQuery(" +
		"		'SELECT name, jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) age, value FROM gdb_local_metrics " +
		"		UNION SELECT ''Data Throughput'' name, 0 age, cast(GDB_THROUGHPUT()/1000 as char(20)) value from sysibm.sysdummy1 " +
		"		UNION SELECT ''Query Activity'' name, 0 age, cast(GDB_QRY_ACTIVITY() as char(20)) value from sysibm.sysdummy1 " +
		"		UNION SELECT ''Node CPU'' name, 0 age, cast(GDB_NODE_CPU() as char(3)) value from sysibm.sysdummy1 " +
//		"		UNION SELECT ''DS CPU'' name, 0 age, cast(GDB_DS_CPU() as char(3)) value from sysibm.sysdummy1 " +
		"		UNION SELECT ''JVM Used Memory'' name, 0 age, cast(JMEMORYPERCENT() as char(3)) value from sysibm.sysdummy1', " +
		"		'with_provenance') Q" +
		"    WHERE name IN ($monitors)" +
		"      AND age < ?" +
		" ORDER BY gdb_node, received_timestamp, name";
	
	private static final String SQL_LATEST_METRICS =
		"    SELECT gdb_node," +
		"           max( jSecs(CURRENT_TIMESTAMP) - age ) received_timestamp," +
		"           name," +
		"           max( CAST(value AS INT) ) value" +
		"     FROM new com.ibm.db2j.GaianQuery(" +
		"		'SELECT name, jSecs(CURRENT_TIMESTAMP) - jSecs(received_timestamp) age, value FROM gdb_local_metrics " +
		"		UNION SELECT ''Data Throughput'' name, 0 age, cast(GDB_THROUGHPUT()/1000 as char(20)) value from sysibm.sysdummy1 " +
		"		UNION SELECT ''Query Activity'' name, 0 age, cast(GDB_QRY_ACTIVITY() as char(20)) value from sysibm.sysdummy1 " +
		"		UNION SELECT ''Node CPU'' name, 0 age, cast(GDB_NODE_CPU() as char(3)) value from sysibm.sysdummy1 " +
//		"		UNION SELECT ''DS CPU'' name, 0 age, cast(GDB_DS_CPU() as char(3)) value from sysibm.sysdummy1 " +
		"		UNION SELECT ''JVM Used Memory'' name, 0 age, cast(JMEMORYPERCENT() as char(3)) value from sysibm.sysdummy1', " +
		"		'with_provenance') Q" +
		"    WHERE name IN ($monitors)" +
		"      AND age < ?" +
		" GROUP BY gdb_node, name";
	
	private MonitorInfo[] monitors;

	private Connection conn;
	private PreparedStatement statement, statementLatestMetrics;

	public MetricValueProcessor(Connection conn) throws SQLException {
		this.conn = conn;
	}
	
	public void setAllMonitors() throws SQLException {
		setMonitors(MONITORS);
	}

	public MonitorInfo[] getMonitors() {
		return monitors;
	}
	
	private boolean hasExternalMetricMonitor() {
		for ( MonitorInfo mi : monitors )
			if ( !internalMetricsSet.contains(mi.name) ) return true;
		return false;
	}
	
//	private boolean hasSensorMetricMonitor() {
//		for ( MonitorInfo mi : monitors )
//			if ( sensorMetricNames.contains(mi.name) ) return true;
//		return false;
//	}
//	private boolean hasGaianMetricMonitor() {
//		for ( MonitorInfo mi : monitors )
//			if ( !gaianMetricNames.contains(mi.name) ) return true;
//		return false;
//	}

	public void setMonitors(MonitorInfo[] monitors) throws SQLException {
		this.monitors = monitors;
		if (null != this.monitors && hasExternalMetricMonitor()) {			
			statement = conn.prepareStatement(
					SQL_HISTORICAL_METRICS.replace("$monitors", MonitorInfo.getRequiredMetricsAsSql(this.monitors)));
			statementLatestMetrics = conn.prepareStatement(
					SQL_LATEST_METRICS.replace("$monitors", MonitorInfo.getRequiredMetricsAsSql(this.monitors)));

			statement.setQueryTimeout(Dashboard.QUERY_TIMEOUT); // probably (?) has no effect
		}
		else {
			statement = null;
			statementLatestMetrics = null;
		}
	}

	public void clearMonitors() {
		monitors = null;
		statement = null;
		statementLatestMetrics = null;
	}

	public void processLatestMetrics(int maxAge) throws SQLException {
		
		ResultSet rs = null;
		if ( null != statementLatestMetrics ) {
			statementLatestMetrics.setInt(1, maxAge);
			rs = statementLatestMetrics.executeQuery();
		}

		process(rs, false);
	}
	
	public void processHistoricalMetrics(int maxAge) throws SQLException {
		
		ResultSet rs = null;
		if ( null != statement ) {
			statement.setInt(1, maxAge);
			rs = statement.executeQuery();
		}

		process(rs, true);
	}
	
	private void process(ResultSet rs, boolean processByTimestamp) throws SQLException {
		
		if (null != monitors) {
			
			int currentTimestamp = (int) System.currentTimeMillis();
			
			// internal metrics
			if ( null != topologyGraph ) {
				for ( String node : topologyGraph.getAllNodes() ) {
					
					Map<MonitorInfo, Integer> timestampValues = new HashMap<MonitorInfo, Integer>(monitors.length);
					for (MonitorInfo monitor : monitors) {
						if ( monitor.name.equals(METRIC_ECCENTRICITY) ) {
							timestampValues.put(monitor, topologyGraph.getEccentricity(node));
							// apply relative min and max colour bounds
							monitor.minBound = Math.max( 0, topologyGraph.getRadius()-2 );
							monitor.maxBound = topologyGraph.getDiameter() + 2;
						} else if ( monitor.name.equals(METRIC_CONNECTIVITY) )
							timestampValues.put(monitor, topologyGraph.getConnectivity(node));
					}
					add(node, currentTimestamp, timestampValues);
				}
			}
			
			if ( null != rs ) {
				
				// Construct HashMap of monitor values for each node at each distinct timestamp

				String currentNode = null;
				Map<String, Integer> currentValues = null;
				currentTimestamp = 0;
				
				updateBounds();
	
				while (rs.next()) {
					String node = rs.getString("GDB_NODE");
					String monitorType = rs.getString("NAME");
					int timestamp = processByTimestamp ? rs.getInt("RECEIVED_TIMESTAMP") : 0;
					int value = rs.getInt("VALUE");
	
					if (!node.equals(currentNode) || timestamp != currentTimestamp) {
						processTimestamp(currentNode, currentTimestamp, currentValues);
		
						currentNode = node;
						currentTimestamp = timestamp;
						currentValues = new HashMap<String, Integer>();
					}
	
					currentValues.put(monitorType, value);
				}
				processTimestamp(currentNode, currentTimestamp, currentValues);
			}
		}
	}
	
	private Map<MonitorInfo, Integer> maxBounds = new HashMap<MonitorInfo, Integer>();

	private void updateBounds() {
		for (MonitorInfo monitor : monitors) {
			Integer max = maxBounds.get(monitor);
			if ( null != max ) monitor.maxBound = max;
			
		}
		maxBounds.clear();
	}
	
	private void checkBounds( MonitorInfo monitor, Integer value ) {
		if ( null == value ) return;
		Integer v = maxBounds.get(monitor);
		
		if ( null == v ) maxBounds.put(monitor, Math.max(monitor.defaultMaxBound, value));
		else if ( value > v ) maxBounds.put(monitor, value);
	}
	
	private void processTimestamp(String node, int timestamp, Map<String, Integer> values) {
		if (null != values) {
			Map<MonitorInfo, Integer> timestampValues = new HashMap<MonitorInfo, Integer>(monitors.length);
			for (MonitorInfo monitor : monitors) {
				Integer value = monitor.valueRetriever.get(values);
				checkBounds( monitor, value );
				timestampValues.put(monitor, value);
			}
			
			add(node, timestamp, timestampValues);
		}
	}

	protected abstract void add(String node, int timestamp, Map<MonitorInfo, Integer> values);
	
	private TopologyGraph topologyGraph = null;
	public void setTopologyGraph( TopologyGraph topologyGraph ) {
		this.topologyGraph = topologyGraph;
	}
}
