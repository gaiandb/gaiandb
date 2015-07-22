/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensordemo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ibm.gaiandb.apps.MetricMonitor;
import com.ibm.gaiandb.draw.ChartLegend;
import com.ibm.gaiandb.draw.ConnectedSeriesChart;
import com.ibm.gaiandb.draw.DatabaseDiagram;
import com.ibm.gaiandb.draw.NodeGraph;
import com.ibm.gaiandb.draw.TimeChart;

/**
 * Loads the graphs, either from a default set or from a custom properties file.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class GraphLoader {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/** Prefixes to ignore when parsing the custom properties file. */
	private static final String[] IGNORE = { "custom", "refresh" };

	/** The physical metrics table. */
	private static final String PHYSICAL_TABLE = "GDB_LOCAL_METRICS";

	/** The logical metrics table. */
	//private static final String LOGICAL_TABLE = "GDB_METRICS";

	/**
	 * This grabs one value from the logical table.<br />
	 * <strong>Fields:</strong>
	 *     <em>node</em>,
	 *     <em>received</em>,
	 *     <em>value</em>
	 */
	private static final String GET_ONE_VALUE =
		"    SELECT gdb_node node, jSecs(CURRENT_TIMESTAMP) + received received, CAST(value AS INT) value" +
		"     FROM new com.ibm.db2j.GaianQuery(" +
		"			'  SELECT name, jSecs(received_timestamp) - jSecs(CURRENT_TIMESTAMP) received, value" +
		"			     FROM " + PHYSICAL_TABLE + "'," +
		"			'with_provenance') Q" +
		"    WHERE name = ?" +
		"      AND -received < ?" +
		" ORDER BY gdb_node, received";

	/**
	 * This grabs two values from the logical table.<br />
	 * <strong>Fields:</strong>
	 *     <em>node</em>,
	 *     <em>received</em>,
	 *     <em>x</em>,
	 *     <em>y</em>
	 */
	private static final String GET_TWO_VALUES =
		"    SELECT gdb_node node, jSecs(CURRENT_TIMESTAMP) + received received," +
		"           CAST(SUBSTR(value, 2, LOCATE(',', value) - 2) AS INT) x," +
		"           CAST(SUBSTR(value, LOCATE(',', value) + 2, LOCATE(')', value) - LOCATE(',', value) - 2) AS INT) y" +
		"     FROM new com.ibm.db2j.GaianQuery(" +
		"			'  SELECT name, jSecs(received_timestamp) - jSecs(CURRENT_TIMESTAMP) received, value" +
		"			     FROM " + PHYSICAL_TABLE + "'," +
		"			'with_provenance') Q" +
		"    WHERE name = ?" +
		"      AND -received < ?" +
		" ORDER BY gdb_node, received";

	/**
	 * This grabs one value as a percentage of another at the same time on the
	 * same node from the logical table.<br />
	 * <strong>Fields:</strong>
	 *     <em>node</em>,
	 *     <em>received</em>,
	 *     <em>value</em>
	 */
	private static final String GET_ONE_VALUE_AS_PERCENTAGE =
		"   SELECT t1.gdb_node node, jSecs(CURRENT_TIMESTAMP) + t1.received received," +
		"          (CAST(t1.value AS INT) * 100 / CAST(t2.value AS INT)) value" +
		"     FROM new com.ibm.db2j.GaianQuery(" +
		"			'  SELECT name, jSecs(received_timestamp) - jSecs(CURRENT_TIMESTAMP) received, value" +
		"			     FROM " + PHYSICAL_TABLE + "'," +
		"			'with_provenance') t1," +
		"          new com.ibm.db2j.GaianQuery(" +
		"			'  SELECT name, jSecs(received_timestamp) - jSecs(CURRENT_TIMESTAMP) received, value" +
		"			     FROM " + PHYSICAL_TABLE + "'," +
		"			'with_provenance') t2" +
		"    WHERE t1.name = ?" +
		"      AND t2.name = ?" +
		"      AND -t1.received < ?" +
		"      AND t1.gdb_node = t2.gdb_node" +
		"      AND t1.received = t2.received" +
		" ORDER BY t1.gdb_node, t1.received";

	/**
	 * This gets a list of nodes.<br />
	 * <strong>Fields:</strong>
	 *     <em>node</em>,
	 *     <em>node_name</em>,
	 *     <em>updated</em>
	 */
	private static final String GET_NODES =
//		"   SELECT DISTINCT x.*, jSecs(CURRENT_TIMESTAMP) updated" +
//		"     FROM (" +
//		"           SELECT jHash(gdbx_from_node) node, gdbx_from_node node_name" +
//		"             FROM gdb_ltnull_x" + // derby_tables_x" +
//		"            UNION ALL" +
//		"           SELECT jHash(gdbx_to_node) node, gdbx_to_node node_name" +
//		"             FROM gdb_ltnull_x" + // derby_tables_x" +
//		"          ) x" +
//		"    WHERE node_name <> '<SQL QUERY>'" +
//		" ORDER BY node_name";
	
		"	SELECT DISTINCT	jHash(gdbx_to_node) node," + 
		"					gdbx_to_node node_name," +
		"					jSecs(CURRENT_TIMESTAMP) updated" +
		"		FROM gdb_ltnull_x" +
		"	ORDER BY node_name";

	/**
	 * This gets a list of connections between nodes.<br />
	 * <strong>Fields:</strong>
	 *     <em>source</em>, <em>source_name</em>,
	 *     <em>target</em>, <em>target_name</em>,
	 *     <em>updated</em>
	 */
	private static final String GET_NODE_CONNECTIONS =
		"   SELECT jHash(gdbx_to_node) source, gdbx_to_node source_name," +
		"          jHash(gdbx_to_node) target, gdbx_to_node target_name" +
		"     FROM gdb_ltnull_x" + // derby_tables_x" +
		"    WHERE gdbx_depth = 0" + //gdbx_from_node = '<SQL QUERY>'" +
		"    UNION ALL" +
		"   SELECT DISTINCT" +
		"          jHash(gdbx_from_node) source, gdbx_from_node source_name," +
		"          jHash(gdbx_to_node) target, gdbx_to_node target_name" +
		"     FROM gdb_ltnull_x" + // derby_tables_x" +
		"    WHERE gdbx_depth > 0" + //gdbx_from_node <> '<SQL QUERY>'" +
		" ORDER BY source_name, target_name";

	/**
	 * Potential custom diagram types.
	 */
	private enum DiagramType {
		/** A time chart. */
		TIME,
		/** A connected series chart. */
		CONNECTEDSERIES,
		/** A node graph. */
		NODEGRAPH;

		/**
		 * Parses the string argument as a <code>DiagramType</code>.
		 * 
		 * @param name
		 *            A string containing the <code>DiagramType</code>
		 *            representation to be parsed.
		 * @return A <code>DiagramType</code>, or <code>null</code> if there was
		 *         no match.
		 */
		public static DiagramType parse(String name) {
			try {
				return valueOf(name.toUpperCase());
			}
			catch (Exception e) {
				return null;
			}
		}
	}

	/** The connection to the database. Initialised by the constructor. */
	private final Connection conn;

	/** The list of properties. Initialised by the constructor. */
	private final Properties properties;

	/**
	 * Initialises the graph loader.
	 * 
	 * @param conn
	 *            The database connection to query for graph data.
	 * @param customProperties
	 *            Any custom settings or graphs.
	 */
	public GraphLoader(Connection conn, Properties customProperties) {
		this.conn = conn;
		this.properties = customProperties;
	}

	public String get(String name) {
		if (null == name) {
			return "";
		}

		for (Object propertyName : properties.keySet()) {
			String currentName = (String)propertyName;
			if (name.equalsIgnoreCase(currentName)) {
				return properties.getProperty(currentName);
			}
		}

		return "";
	}

	/**
	 * If the <code>custom</code> property is set to <code>on</code>, this loads
	 * the graphs specified in the properties list. Otherwise, it loads a
	 * default set of graphs.
	 * 
	 * @return An array of graphs.
	 */
	public DatabaseDiagram[] load() {
		if (get("custom").equalsIgnoreCase("on")) {
			return loadCustom();
		}
		else {
			return loadDefault();
		}
	}

	/**
	 * <p>Loads a default set of graphs and a legend for use with the sensor
	 * monitor demo.</p>
	 * 
	 * <p><strong>Graphs:</strong></p>
	 * <ul>
	 *     <li>CPU Usage</li>
	 *     <li>Disk I/O</li>
	 *     <li>Battery Power</li>
	 *     <li>Used Memory</li>
	 *     <li>Network I/O</li>
	 *     <li>Temperature</li>
	 *     <li>Acceleration</li>
	 * </ul>
	 * 
	 * @return An array of graphs.
	 */
	public DatabaseDiagram[] loadDefault() {
		TimeChart timeChart;
		ConnectedSeriesChart connectedSeriesChart;
		NodeGraph nodeGraph;

		List<DatabaseDiagram> diagrams = new ArrayList<DatabaseDiagram>();

		try {
			timeChart = new TimeChart(
				conn.prepareStatement(GET_ONE_VALUE), "CPU Usage");
			timeChart.setTitle("CPU Usage (%)");
			timeChart.setMinValue(0);
			timeChart.setMaxValue(100);
			diagrams.add(timeChart);
		}
		catch (Exception e) {
			System.err.println(error("CPU Usage", e));
		}

		try {
			timeChart = new TimeChart(
				conn.prepareStatement(GET_ONE_VALUE), "Disk I/O");
			timeChart.setTitle("Disk I/O (KB/s)");
			diagrams.add(timeChart);
		}
		catch (Exception e) {
			System.err.println(error("Disk I/O", e));
		}

		try {
			timeChart = new TimeChart(
				conn.prepareStatement(GET_ONE_VALUE), "Battery Power");
			timeChart.setTitle("Battery Power (%)");
			timeChart.setMinValue(0);
			timeChart.setMaxValue(100);
			diagrams.add(timeChart);
		}
		catch (Exception e) {
			System.err.println(error("Battery Power", e));
		}

		try {
			timeChart = new TimeChart(
				conn.prepareStatement(GET_ONE_VALUE_AS_PERCENTAGE),
				"Used Memory", "Total Memory");
			timeChart.setTitle("Used Memory (%)");
			timeChart.setMinValue(0);
			timeChart.setMaxValue(100);
			diagrams.add(timeChart);
		}
		catch (Exception e) {
			System.err.println(error("Used Memory", e));
		}

		try {
			timeChart = new TimeChart(
				conn.prepareStatement(GET_ONE_VALUE), "Network I/O");
			timeChart.setTitle("Network I/O (KB/s)");
			diagrams.add(timeChart);
		}
		catch (Exception e) {
			System.err.println(error("Network I/O", e));
		}

		try {
			timeChart = new TimeChart(
				conn.prepareStatement(GET_ONE_VALUE), "Temperature");
			timeChart.setTitle("Temperature (" + MetricMonitor.TEMPERATURE_SYMBOL + "C)");
			timeChart.setMinValue(0);
			timeChart.setMaxValue(100);
			diagrams.add(timeChart);
		}
		catch (Exception e) {
			System.err.println(error("Temperature", e));
		}

		try {
			connectedSeriesChart = new ConnectedSeriesChart(
				conn.prepareStatement(GET_TWO_VALUES), "Acceleration");
			connectedSeriesChart.setTitle("Acceleration");
			diagrams.add(connectedSeriesChart);
		}
		catch (Exception e) {
			System.err.println(error("Acceleration", e));
		}

		try {
			nodeGraph = new NodeGraph(
				conn.prepareStatement(GET_NODE_CONNECTIONS));
			nodeGraph.setTitle("Nodes");
			diagrams.add(nodeGraph);
		}
		catch (Exception e) {
			System.err.println(error("Nodes", e));
		}

		try {
			diagrams.add(createLegend());
		}
		catch (Exception e) {
			System.err.println(error("Legend", e));
		}

		return diagrams.toArray(new DatabaseDiagram[diagrams.size()]);
	}

	/**
	 * Loads custom graphs using the properties provided.
	 * 
	 * @return The custom graphs that were loaded successfully in an array.
	 */
	public DatabaseDiagram[] loadCustom() {
		// Split the properties list up into separate graphs.
		Map<String, Properties> diagramProperties = new LinkedHashMap<String, Properties>();
		for (Object propertyName : properties.keySet()) {
			String name = ((String)propertyName);
			String value = properties.getProperty(name);
			name = name.toLowerCase();

			String[] parts = name.split("_", 2);
			String diagramName = parts[0];

			// We don't want to handle certain properties here.
			// They're used elsewhere.
			if (Arrays.binarySearch(IGNORE, diagramName) >= 0) {
				continue;
			}

			// If we haven't started parsing this graph yet, we create it.
			if (!diagramProperties.containsKey(diagramName)) {
				diagramProperties.put(diagramName, new Properties());
				diagramProperties.get(diagramName).setProperty("enabled", "on");
			}

			// You can use "GraphName = OFF" to turn off the graph.
			if (parts.length == 1) {
				diagramProperties.get(diagramName).setProperty("enabled", value.toLowerCase());
			}
			// "GraphName_PROPERTY = VALUE" is added to the properties list as
			// PROPERTY => VALUE.
			else if (parts.length == 2) {
				String key = parts[1];
				diagramProperties.get(diagramName).setProperty(key, value);
			}
		}

		// Iterate through each graph, process it and add it to the list.
		List<DatabaseDiagram> diagrams = new ArrayList<DatabaseDiagram>(diagramProperties.size());
		String legendSql = null;
		boolean legendEnabled = true;
		for (String diagramName : diagramProperties.keySet()) {
			try {
				Properties currentDiagram = diagramProperties.get(diagramName);

				// The legend only has two properties: ENABLED and SQL.
				// If ENABLED is set and not ON, we don't show it at all.
				// The user can also specify custom legend SQL.
				if (diagramName.equals("legend")) {
					legendEnabled = currentDiagram.getProperty("enabled").equalsIgnoreCase("on");
					legendSql = currentDiagram.getProperty("sql");
					continue;
				}

				// If GraphName_ENABLED is set and not ON, we move on to the next one. 
				if (!currentDiagram.getProperty("enabled", "on").equals("on")) {
					continue;
				}

				// Grab the type. If it's invalid, error out. It will be caught,
				// printed and the next graph will be processed as normal.
				DiagramType type = DiagramType.parse(currentDiagram.getProperty("type"));
				if (null == type) {
					throw new InvalidPropertyException(
						"\"" + type + "\" is an invalid type.\n" +
						"Valid types are \"Time\", \"ConnectedSeries\" and \"NodeGraph\".");
				}

				// All graphs have SQL. Make sure it's there.
				String sql = currentDiagram.getProperty("sql");
				if (null == sql || 0 == sql.length()) {
					throw new InvalidPropertyException(
						"You must provide SQL for your " + diagramName + " graph (\"" + diagramName + "_SQL = ...\").");
				}

				switch (type) {
					case TIME: {
						try {
							// We add an extra section to the WHERE clause here.
							// This makes sure we're not retrieving far too much
							// data by limiting it to the last X seconds.
							TimeChart chart = new TimeChart(conn.prepareStatement(
								addTimeDurationToSQL(sql, currentDiagram.getProperty("duration_field"))));
							// Time charts need to know how to get the previous
							// nodes. We do this by adding a different section
							// to the WHERE clause.
							chart.setPreviousNodesStatement(conn.prepareStatement(
								addTimeDurationToSQL(sql, currentDiagram.getProperty("duration_field"), false)));

							// Chart needs a name. This creates one, in the form
							// "{name} ({unit})".
							chart.setTitle(constructChartTitle(
								currentDiagram.getProperty("name", "Custom Time Chart"),
								currentDiagram.getProperty("unit", "")));

							String duration = currentDiagram.getProperty("duration");
							if (duration != null) {
								chart.setMaxDuration(Integer.parseInt(duration));
							}

							String min = currentDiagram.getProperty("min");
							if (min != null) {
								chart.setMinValue(Integer.parseInt(min));
							}

							String max = currentDiagram.getProperty("max");
							if (max != null) {
								chart.setMaxValue(Integer.parseInt(max));
							}

							String extrapolation = currentDiagram.getProperty("extrapolation");
							if (extrapolation != null) {
								chart.setExtrapolation(extrapolation);
							}

							diagrams.add(chart);
						}
						catch (Exception e) {
							throw new InvalidPropertyException(
								error(currentDiagram.getProperty("name", diagramName), e));
						}
	
						break;
					}

					// See the comments in the previous case - they apply here too.
					case CONNECTEDSERIES: {
						try {
							sql = addTimeDurationToSQL(sql, currentDiagram.getProperty("duration_field"));
							ConnectedSeriesChart chart = new ConnectedSeriesChart(
								conn.prepareStatement(sql));

							chart.setTitle(constructChartTitle(
								currentDiagram.getProperty("name", "Custom Connected Series Chart"),
								currentDiagram.getProperty("unit", "")));
	
							String duration = currentDiagram.getProperty("duration");
							if (duration != null) {
								chart.setMaxDuration(Integer.parseInt(duration));
							}
	
							diagrams.add(chart);
						}
						catch (Exception e) {
							throw new InvalidPropertyException(
								error(currentDiagram.getProperty("name", diagramName), e));
						}
	
						break;
					}

					case NODEGRAPH: {
						try {
							// Node graphs are apparently a lot simpler.
							NodeGraph graph = new NodeGraph(
								conn.prepareStatement(sql));

							graph.setTitle(currentDiagram.getProperty("name", "Custom Node Graph"));

							diagrams.add(graph);
						}
						catch (Exception e) {
							throw new InvalidPropertyException(
								error(currentDiagram.getProperty("name", diagramName), e));
						}
	
						break;
					}
				}
			}
			catch (InvalidPropertyException e) {
				System.err.println(e.getMessage());
			}
		}

		// This section creates the legend depending on the parameters parsed earlier.
		if (legendEnabled) {
			try {
				diagrams.add(null == legendSql ? createLegend() : createLegend(legendSql));
			}
			catch (Exception e) {
				System.err.println(error("Legend", e));
			}
		}

		return diagrams.toArray(new DatabaseDiagram[diagrams.size()]);
	}

	/**
	 * Creates a graph legend using the node names.
	 * 
	 * @return A legend.
	 * @throws SQLException
	 *             if the SQL used could not be prepared correctly.
	 */
	public ChartLegend createLegend() throws SQLException {
		return createLegend(GET_NODES);
	}

	/**
	 * Creates a graph legend using the SQL provided.
	 * 
	 * @param sql
	 *            The SQL to use to retrieve the legend data.
	 * 
	 * @return A legend.
	 * @throws SQLException
	 *             if the SQL provided could not be prepared correctly.
	 */
	public ChartLegend createLegend(String sql) throws SQLException {
		ChartLegend legend = new ChartLegend(
			conn.prepareStatement(sql));
		legend.setTitle("Legend");
		return legend;
	}

	/**
	 * Constructs a chart title from a name and the chart units.
	 * 
	 * @param name
	 *            The name of the chart.
	 * @param units
	 *            The units the chart uses.
	 * @return The chart title.
	 */
	private String constructChartTitle(String name, String units) {
		if (units.length() > 0) {
			return name + " (" + units + ")";
		}
		else {
			return name;
		}
	}

	/**
	 * Adds a minimum time cutoff to the result set by modifying the SQL
	 * provided. Useful for custom charts, as users will not have to provide SQL
	 * they don't understand.
	 * 
	 * @param sql
	 *            The SQL to be modified.
	 * @param durationField
	 *            The field that contains the time of the message in the result
	 *            set, represented as Unix time.
	 * @return A new SQL string with an extra WHERE clause if the modification
	 *         was possible, or a copy of the same SQL string otherwise.
	 */
	private String addTimeDurationToSQL(String sql, String durationField) {
		return addTimeDurationToSQL(sql, durationField, true);
	}

	/**
	 * <p>Adds a minimum or maximum time cutoff to the result set by modifying
	 * the SQL provided. Useful for custom charts, as users will not have to
	 * provide SQL they don't understand.</p>
	 * 
	 * <p>This method adds an unbound parameter to the SQL, which will have to
	 * be accounted for when executing the query.</p>
	 * 
	 * @param sql
	 *            The SQL to be modified.
	 * @param durationField
	 *            The field that contains the time of the message in the result
	 *            set, represented as Unix time.
	 * @param minimum
	 *            True if we're setting a minimum time cutoff, or false if we're
	 *            setting a maximum.
	 * @return A new SQL string with an extra WHERE clause if the modification
	 *         was possible, or a copy of the same SQL string otherwise.
	 */
	private String addTimeDurationToSQL(String sql, String durationField, boolean minimum) {
		if (null != durationField) {
			// SQL is case-insensitive, so searching through it has to be too.
			String searchSql = sql.toLowerCase();

			// Find the beginning of the WHERE clause.
			int whereStarts = searchSql.indexOf(" where ");

			// Find the end of the WHERE clause.
			int whereEnds = -1;
			int groupByStarts = searchSql.indexOf(" group by ");
			int orderByStarts = searchSql.indexOf(" order by ");
			if (groupByStarts >= 0 && orderByStarts >= 0) {
				whereEnds = Math.min(groupByStarts, orderByStarts);
			}
			else if (groupByStarts >= 0) {
				whereEnds = groupByStarts;
			}
			else if (orderByStarts >= 0) {
				whereEnds = orderByStarts;
			}

			// If we can't find it or something went wrong in finding it, assume
			// it continues to the end of the statement.
			if (whereEnds <= whereStarts) {
				whereEnds = searchSql.length();
			}

			// Generate the SQL to be inserted into the WHERE clause.
			// If minimum:
			//   jSecs(CURRENT_TIMESTAMP) - {durationField} < ?
			// Else:
			//   jSecs(CURRENT_TIMESTAMP) - {durationField} >= ?
			String timeWhereClause =
				"jSecs(CURRENT_TIMESTAMP) - " + durationField +
				(minimum ? " < ?" : " >= ?");

			StringBuffer buf = new StringBuffer(sql);
			// If there is a WHERE clause, bracket it so it's not affected
			// adversely, and add the additional SQL onto the end.
			if (whereStarts >= 0) {
				buf.insert(whereStarts + 7, "(");
				buf.insert(whereEnds + 1, ") AND " + timeWhereClause);
				return buf.toString();
			}
			// If there isn't, create one.
			else if (whereEnds >= 0) {
				buf.insert(whereEnds, " WHERE " + timeWhereClause);
				return buf.toString();
			}
			// Something failed. Return a copy of the original SQL.
			else {
				return buf.toString();
			}
		}
		// Something failed. Return a copy of the original SQL.
		else {
			return new String(sql);
		}
	}

	/**
	 * Returns an error message created for a specific graph.
	 * 
	 * @param title
	 *            The title of the graph.
	 * @param e
	 *            The exception to retrieve the error message from.
	 * @return An error message.
	 */
	private String error(String title, Exception e) {
	    return error(title, e.getMessage());
    }

	/**
	 * Returns an error message created for a specific graph.
	 * 
	 * @param title
	 *            The title of the graph.
	 * @param message
	 *            The error message.
	 * @return An error message.
	 */
	private String error(String title, String message) {
		return "Could not create the \"" + title + "\" graph.\n" + message;
	}
}
