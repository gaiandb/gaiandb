/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import prefuse.action.ActionList;

import prefuse.action.assignment.ShapeAction;

import prefuse.action.assignment.ColorAction;
import prefuse.data.Table;
import prefuse.data.expression.Predicate;
import prefuse.data.expression.parser.ExpressionParser;
import prefuse.util.collections.IntIterator;

public abstract class Graph extends DatabaseDiagram {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final long serialVersionUID = 8651247478054867510L;

	private class Edge {
		public int source = -1;
		public int target = -1;

		public Edge(int source, int target) {
			this.source = source;
			this.target = target;
		}

		public boolean equals(Object o) {
			if (o instanceof Edge) {
				Edge other = (Edge)o;
				return
					super.equals(o) ||
					(this.source == other.source &&
					 this.target == other.target) ||
					(this.source == other.target &&
					 this.target == other.source);
			}
			else {
				return super.equals(o);
			}
		}

		public int hashCode() {
			if (source < target) {
				return (source << 16) + (target & 0xFFFF);
			}
			else {
				return (target << 16) + (source & 0xFFFF);
			}
		}
	}

	public static final String NODE_GROUP = GROUP + ".nodes";
	public static final String EDGE_GROUP = GROUP + ".edges";

	public static final String NODE_ID_COLUMN = "NODE";
	public static final String SOURCE_ID_COLUMN = "SOURCE";
	public static final String TARGET_ID_COLUMN = "TARGET";
	public static final String TIMESTAMP_COLUMN = "UPDATED";

	public static final String NODE_PREFIX = "NODE";
	public static final String SOURCE_PREFIX = "SOURCE";
	public static final String TARGET_PREFIX = "TARGET";

	protected prefuse.data.Graph data;
	protected Table nodes;
	protected Table edges;

	protected PreparedStatement statement;
	protected int firstStatementVar = 1;

	protected Set<String> extraNodeColumns;
	protected Set<String> extraEdgeColumns;

	protected Map<Integer, Integer> nodeIndex = new Hashtable<Integer, Integer>();
	protected Map<Edge, Integer> edgeIndex = new Hashtable<Edge, Integer>();

	protected ActionList draw = new ActionList();

	protected ShapeAction nodeShapeAction;
	public ShapeAction getNodeShapeAction() {
		return nodeShapeAction;
	}
	public void setNodeShapeAction(ShapeAction nodeShapeAction) {
		draw.remove(this.nodeShapeAction);
		this.nodeShapeAction = nodeShapeAction;
		draw.add(this.nodeShapeAction);
	}

	protected ColorAction nodeColorAction;
	public ColorAction getNodeColorAction() {
		return nodeColorAction;
	}
	public void setNodeColorAction(ColorAction nodeColorAction) {
		draw.remove(this.nodeColorAction);
		this.nodeColorAction = nodeColorAction;
		draw.add(this.nodeColorAction);
	}

	protected ColorAction edgeColorAction;
	public ColorAction getEdgeColorAction() {
		return edgeColorAction;
	}
	public void setEdgeColorAction(ColorAction edgeColorAction) {
		draw.remove(this.edgeColorAction);
		this.edgeColorAction = edgeColorAction;
		draw.add(this.edgeColorAction);
	}

	public Graph(PreparedStatement statement, Object... params)
	            throws SQLException {
		super();

		this.statement = statement;
		firstStatementVar = TableUtil.bindToPreparedStatement(statement, params);
	}

	public void populateGraph(Object... params) throws SQLException {
		TableUtil.bindToPreparedStatement(statement, params, firstStatementVar);

		nodes = new Table();
		nodes.addColumn(NODE_ID_COLUMN, int.class);
		nodes.addColumn(TIMESTAMP_COLUMN, long.class);
		edges = new Table();
		edges.addColumn(SOURCE_ID_COLUMN, int.class);
		edges.addColumn(TARGET_ID_COLUMN, int.class);
		edges.addColumn(TIMESTAMP_COLUMN, long.class);

		ResultSet resultSet = statement.executeQuery();

		extraNodeColumns = new HashSet<String>();
		extraEdgeColumns = new HashSet<String>();
		addExtraColumns(resultSet);

		beforeUpdate();
		addResultSet(resultSet);
		afterUpdate();

		data = new prefuse.data.Graph(
			nodes, edges, false,
			NODE_ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN);

		prepareVisualization();
	}

	public void updateGraph(Object... params) throws SQLException {
		if (null == data) {
			populateGraph(params);
		}
		else {
			TableUtil.bindToPreparedStatement(statement, params, firstStatementVar);
			ResultSet resultSet = statement.executeQuery();

			beforeUpdate();
			addResultSet(resultSet);
			afterUpdate();

			removeOldNodes();

			updateVisualization();
		}
	}

	// These are abstract, but not required.
	protected void beforeUpdate() throws SQLException { }
	protected void afterUpdate() throws SQLException { }

	public void prepareVisualization() {
		if (null == data || 0 == data.getNodeCount()) {
			preparation = PreparationStages.UNPREPARED;
			return;
		}

		super.prepareVisualization();
	}

	public void updateVisualization() {
		if (null == data || 0 == data.getNodeCount()) {
			return;
		}

		super.updateVisualization();
	}

	private void addExtraColumns(ResultSet resultSet) throws SQLException {
		ResultSetMetaData metadata = resultSet.getMetaData();
		int cols = metadata.getColumnCount();
		String[] prefixes = { SOURCE_PREFIX, TARGET_PREFIX };

		for (int i = 1; i <= cols; i++) {
			String edgeColumnName = metadata.getColumnName(i);
			Class<?> type = handler.getDataType(edgeColumnName, metadata.getColumnType(i));

			if (null == edges.getColumn(edgeColumnName)) {
				edges.addColumn(edgeColumnName, type);
				extraEdgeColumns.add(edgeColumnName);
			}

			for (String prefix : prefixes) {
				if (edgeColumnName.startsWith(prefix + "_")) {
					String suffix = edgeColumnName.substring(prefix.length() + 1);
					String nodeColumnName = NODE_PREFIX + "_" + suffix;
					if (null == nodes.getColumn(nodeColumnName)) {
						nodes.addColumn(nodeColumnName, type);
						extraNodeColumns.add(suffix);
					}

					break;
				}
			}
		}
	}

	private int addResultSet(ResultSet resultSet) throws SQLException {
		int count = 0;

		while (resultSet.next()) {
			int sourceId = resultSet.getInt(SOURCE_ID_COLUMN);
			int targetId = resultSet.getInt(TARGET_ID_COLUMN);
			long updated = System.currentTimeMillis() / 1000;

			synchronized (nodes) {
				int sourceRow;
				Integer row = nodeIndex.get(sourceId);

				if (null == row) {
					sourceRow = nodes.addRow();
					count++;
					nodeIndex.put(sourceId, sourceRow);
					nodes.setInt(sourceRow, NODE_ID_COLUMN, sourceId);
				}
				else {
					sourceRow = row;
				}

				for (String suffix : extraNodeColumns) {
					nodes.set(sourceRow, NODE_PREFIX + "_" + suffix,
						resultSet.getObject(SOURCE_PREFIX + "_" + suffix));
				}
				nodes.setLong(sourceRow, TIMESTAMP_COLUMN, updated);

				if (sourceId != targetId) {
					int targetRow;
					row = nodeIndex.get(targetId);

					if (null == row) {
						targetRow = nodes.addRow();
						count++;
						nodeIndex.put(targetId, targetRow);
						nodes.setInt(targetRow, NODE_ID_COLUMN, targetId);
					}
					else {
						targetRow = row;
					}

					for (String suffix : extraNodeColumns) {
						nodes.set(targetRow, NODE_PREFIX + "_" + suffix,
							resultSet.getObject(TARGET_PREFIX + "_" + suffix));
					}
					nodes.setLong(targetRow, TIMESTAMP_COLUMN, updated);
				}
			}

			if (sourceId != targetId) {
				synchronized (edges) {
					int edgeRow;
					Edge edge = new Edge(sourceId, targetId);
					Integer row = edgeIndex.get(edge);

					if (null == row) {
						edgeRow = edges.addRow();
						count++;
						edgeIndex.put(edge, edgeRow);
						edges.setInt(edgeRow, SOURCE_ID_COLUMN, sourceId);
						edges.setInt(edgeRow, TARGET_ID_COLUMN, targetId);
						for (String column : extraEdgeColumns) {
							edges.set(edgeRow, column, resultSet.getObject(column));
						}
					}
					else {
						edgeRow = row;
					}
					edges.setLong(edgeRow, TIMESTAMP_COLUMN, updated);
				}
			}
		}

		return count;
	}

	public int removeOldNodes() {
		int now = (int)(System.currentTimeMillis() / 1000);
		int timeThreshold = nodes.getRowCount() / 100 + 2;
		Predicate old = ExpressionParser.predicate(
			now + " - [" + TIMESTAMP_COLUMN + "] > " + timeThreshold);

		int count = 0;

		IntIterator oldEdges = edges.rows(old);
		IntIterator oldNodes = nodes.rows(old);

		while (oldEdges.hasNext()) {
			int row = -1;
			try {
				row = oldEdges.nextInt();
				edgeIndex.remove(new Edge(
					edges.getInt(row, SOURCE_ID_COLUMN),
					edges.getInt(row, TARGET_ID_COLUMN)));
				edges.removeRow(row);
				count++;
			}
			catch (Exception e) {
				System.err.println("Could not remove edge " + row + ".");
			}
		}

		while (oldNodes.hasNext()) {
			int row = -1;
			try {
				row = oldNodes.nextInt();
				nodeIndex.remove(nodes.getInt(row, NODE_ID_COLUMN));
				nodes.removeRow(row);
				count++;
			}
			catch (Exception e) {
				System.err.println("Could not remove node " + row + ".");
			}
		}

		return count;
	}
}
