/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import java.awt.Insets;
import java.awt.Shape;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import javax.swing.BorderFactory;

import prefuse.Constants;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.ColorAction;
import prefuse.action.layout.AxisLabelLayout;
import prefuse.action.layout.AxisLayout;
import prefuse.data.expression.parser.ExpressionParser;
import prefuse.data.query.RangeQueryBinding;
import prefuse.render.AbstractShapeRenderer;
import prefuse.render.AxisRenderer;
import prefuse.render.Renderer;
import prefuse.render.RendererFactory;
import prefuse.util.collections.IntIterator;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.VisiblePredicate;

public class TimeChart extends Chart {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	public enum ExtrapolationStyle {
		FIRST,
		AVERAGE,
		LAST;

		public static ExtrapolationStyle parse(String name) {
			try {
				return valueOf(name.toUpperCase());
			}
			catch (IllegalArgumentException e) {
				return null;
			}
		}
	}

	private class Row implements Cloneable {
		public String node;
		public int value;
		public long received;

		public Row(String node, int value, long received) {
			this.node = node;
			this.value = value;
			this.received = received;
		}

		public Row(int row) {
			this.node = data.getString(row, NODE_NAME);
			this.value = data.getInt(row, Y_AXIS);
			this.received = data.getLong(row, X_AXIS);
		}

		public Row clone() {
			return new Row(node, value, received);
		}

		public int addToTable() {
			int rowId = data.addRow();
			data.setLong(rowId, X_AXIS, received);
			data.setInt(rowId, Y_AXIS, value);
			data.setString(rowId, NODE_NAME, node);
			return rowId;
		}
	}

	private static final long serialVersionUID = -8625439444277732390L;

	public static final String X_AXIS = "RECEIVED";
	public static final String Y_AXIS = "VALUE";
	public static final String NODE_NAME = "NODE";

	private static final int axisWidth = 20;
	private static final int axisHeight = 10;

	private AxisLayout xAxis;
	private AxisLayout yAxis;
	private Rectangle2D chartBounds = new Rectangle2D.Double();
	private Rectangle2D xAxisBounds = new Rectangle2D.Double();
	private Rectangle2D yAxisBounds = new Rectangle2D.Double();
	private RangeQueryBinding xAxisRange;
	private RangeQueryBinding yAxisRange;

	private int minValue = 0;
	public int getMinValue() { return minValue; }
	public void setMinValue(int minValue) { this.minValue = minValue; }
	private int maxValue = 0;
	public int getMaxValue() { return maxValue; }
	public void setMaxValue(int maxValue) { this.maxValue = maxValue; }

	private PreparedStatement previousNodesStatement = null;
	public PreparedStatement getPreviousNodesStatement() {
    	return previousNodesStatement;
    }
	public void setPreviousNodesStatement(PreparedStatement previousNodesStatement) {
    	this.previousNodesStatement = previousNodesStatement;
    }

	private ExtrapolationStyle extrapolation = ExtrapolationStyle.AVERAGE;
	public ExtrapolationStyle getExtrapolation() { return extrapolation; }
	public void setExtrapolation(ExtrapolationStyle extrapolation) {
		this.extrapolation = extrapolation;
	}
	public void setExtrapolation(String extrapolation) {
		ExtrapolationStyle eS = ExtrapolationStyle.parse(extrapolation);
		if (null != eS) {
			this.extrapolation = eS;
		}
	}

	private HashMap<String, HashSet<VisualItem>> chartPoints =
		new HashMap<String, HashSet<VisualItem>>();

	public TimeChart(PreparedStatement statement, Object... params) throws SQLException {
		super(statement, params);
		keys.add(X_AXIS);
		keys.add(NODE_NAME);
		setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
	}

	protected void afterUpdate(Object[] params) throws SQLException {
		if (ExtrapolationStyle.AVERAGE == extrapolation) {
			return;
		}

		if (null == previousNodesStatement) {
			return;
		}

		TableUtil.bindToPreparedStatement(previousNodesStatement, params);
		TableUtil.addResultSetToTable(previousNodesStatement, data, handler, keys);

		if (0 == data.getRowCount()) {
			return;
		}

		Hashtable<String, Row> nodeLastRow = new Hashtable<String, Row>();
		List<Row> newRows = new ArrayList<Row>();

		IntIterator i = data.rowsSortedBy(X_AXIS, true);
		Row row = new Row(i.nextInt());
		nodeLastRow.put(row.node, row);
		while (i.hasNext()) {
			Row nextRow = new Row(i.nextInt());

			addMissingRows(newRows, row, nextRow);
			row = nextRow;
			nodeLastRow.put(row.node, row);
		}

		if (ExtrapolationStyle.FIRST == extrapolation) {
			for (String node : nodeLastRow.keySet()) {
				Row secondToLast = nodeLastRow.get(node);
				Row last = secondToLast.clone();
				last.received = System.currentTimeMillis() / 1000;
				addMissingRows(newRows, secondToLast, last);
			}
		}

		for (Row newRow : newRows) {
			newRow.addToTable();
		}
	}

	private void addMissingRows(List<Row> rows, Row start, Row end) {
		long now = System.currentTimeMillis() / 1000;
		long startTime = Math.max(start.received, now - 60) + 1;
		long endTime = Math.min(end.received, now) - 1;

		String node;
		int value;
		if (ExtrapolationStyle.FIRST == extrapolation) {
			node = start.node;
			value = start.value;
		}
		else if (ExtrapolationStyle.LAST == extrapolation) {
			node = end.node;
			value = end.value;
		}
		else {
			return;
		}

		for (long i = startTime; i <= endTime; i++) {
			rows.add(new Row(node, value, i));
		}
	}

	protected void prepareVis() {
		vt = m_vis.addTable(GROUP, data);

		m_vis.setRendererFactory(new RendererFactory() {
			private Renderer yAxisRenderer = new AxisRenderer(Constants.LEFT, Constants.TOP);
			private Renderer lineRenderer = new AbstractShapeRenderer() {
				// Return a line from the last point to this point.
				protected Shape getRawShape(VisualItem item) {
					// Get the list of points on the same node.
					String node = item.getString(NODE_NAME);
					HashSet<VisualItem> nodeItems;
					synchronized (chartPoints) {
						nodeItems = chartPoints.get(node);
						if (null == nodeItems) {
							nodeItems = new HashSet<VisualItem>();
							chartPoints.put(node, nodeItems);
						}
					}

					// Find the latest point prior to this one.
					VisualItem lastItem = null;
					for (VisualItem currentItem : nodeItems) {
						if ((null == lastItem ||
						 lastItem.getLong(X_AXIS) < currentItem.getLong(X_AXIS)) &&
						 currentItem.getLong(X_AXIS) < item.getLong(X_AXIS)) {
							lastItem = currentItem;
						}
					}

					nodeItems.add(item);

					if (null != lastItem) {
						return new Line2D.Double(
							lastItem.getX(), lastItem.getY(),
							item.getX(), item.getY()
						);
					}
					else {
						return new Line2D.Double(
							item.getX(), item.getY(),
							item.getX(), item.getY()
						);
					}
				}
			};

			public Renderer getRenderer(VisualItem item) {
				return
					item.isInGroup("yAxis") ? yAxisRenderer :
					lineRenderer;
			}
		});

		// Define the axis regions.
		xAxis = new AxisLayout(
			GROUP, X_AXIS, Constants.X_AXIS, VisiblePredicate.TRUE);
		yAxis = new AxisLayout(
			GROUP, Y_AXIS, Constants.Y_AXIS, VisiblePredicate.TRUE);
		xAxis.setLayoutBounds(chartBounds);
		yAxis.setLayoutBounds(chartBounds);

		xAxisRange = new RangeQueryBinding(data, X_AXIS);
		xAxis.setRangeModel(xAxisRange.getModel());

		AxisLabelLayout yAxisLabel = new AxisLabelLayout(
			"yAxis", yAxis, yAxisBounds);

		ActionList axes = new ActionList();
		axes.add(xAxis);
		axes.add(yAxis);
		axes.add(yAxisLabel);
		m_vis.putAction("axes", axes);

		if (null == colorAction) {
			colorAction = new ColorAction(GROUP, VisualItem.STROKECOLOR);
		}

		ActionList draw = new ActionList();
		draw.add(colorAction);
		draw.add(new RepaintAction());
		m_vis.putAction("draw", draw);

		// Reset the object boundaries whenever the window is resized.
		addComponentListener(new ComponentAdapter() {
            public void componentResized(ComponentEvent e) {
            	updateVisualization();
            }
        });
		updateVis();
	}

	protected void updateVis() {
		long latestTime = System.currentTimeMillis() / 1000;

		if (data.getRowCount() > 0) {
			// Update the axes to show the latest {maxDuration} seconds.
			xAxisRange.getNumberModel().setValueRange(
				latestTime - maxDuration, latestTime,
				latestTime - maxDuration, latestTime);
		}

		// Remove old data from the table.
		synchronized (data) {
			String expression = latestTime + " - [" + X_AXIS + "] > " + maxDuration;
			data.remove(ExpressionParser.predicate(expression));
		}

		// Remove old data from the list of nodes.
		synchronized (chartPoints) {
			for (String node : chartPoints.keySet()) {
				HashSet<VisualItem> nodeItems = chartPoints.get(node);
				Iterator<VisualItem> i = nodeItems.iterator();
				while (i.hasNext()) {
					VisualItem item = i.next();
					try {
						item.getInt(X_AXIS);
					}
					catch (IllegalStateException e) {
						i.remove();
					}
				}
			}
		}

		if (data.getRowCount() > 0) {
			yAxisRange = new RangeQueryBinding(data, Y_AXIS);
			yAxis.setRangeModel(yAxisRange.getModel());

			// Set up the y axis min and max values.
			if (minValue < maxValue) {
				yAxisRange.getNumberModel().setValueRange(
					minValue, maxValue, minValue, maxValue);
			}
			else {
				yAxisRange.getNumberModel().setMinValue(minValue);
			}
		}

		// Update the axis and chart boundaries.
		Insets insets = getInsets();
		int width = getWidth();
		int height = getHeight();
		int insetsWidth = insets.left + insets.right;
		int insetsHeight = insets.top + insets.bottom;

		chartBounds.setRect(
			insets.left + axisWidth,
			insets.top,
			width - insetsWidth - axisWidth,
			height - insetsHeight - axisHeight);
		xAxisBounds.setRect(
			insets.left + axisWidth,
			height - insets.bottom - axisHeight,
			width - insetsWidth - axisWidth,
			axisHeight);
		yAxisBounds.setRect(
			insets.left,
			insets.top,
			width - insetsWidth,
			height - insetsHeight - axisHeight);

		m_vis.run("axes");
		m_vis.run("draw");
	}
}
