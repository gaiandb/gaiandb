/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.awt.BasicStroke;
import java.awt.Insets;
import java.awt.Shape;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import javax.swing.BorderFactory;

import prefuse.Constants;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.StrokeAction;
import prefuse.action.layout.AxisLabelLayout;
import prefuse.action.layout.AxisLayout;
import prefuse.data.expression.parser.ExpressionParser;
import prefuse.data.query.RangeQueryBinding;
import prefuse.render.AbstractShapeRenderer;
import prefuse.render.AxisRenderer;
import prefuse.render.Renderer;
import prefuse.render.RendererFactory;
import prefuse.util.ColorLib;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.VisiblePredicate;

public class ConnectedSeriesChart extends Chart {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final long serialVersionUID = 3256785306768329206L;

	private static final int accelerometerRange = 50;
	protected int maxDuration = 10;

	private static final String xAxisName = "X";
	private static final String yAxisName = "Y";
	private static final String zAxisName = "NODE";
	private static final String wAxisName = "RECEIVED";

	private static final int axisWidth = 20;
	private static final int axisHeight = 10;
	private static final int lineWidth = 5;

	private Rectangle2D barChartBounds = new Rectangle2D.Double();
	private Rectangle2D xAxisBounds = new Rectangle2D.Double();
	private Rectangle2D yAxisBounds = new Rectangle2D.Double();

	private HashMap<String, HashSet<VisualItem>> graphPoints =
		new HashMap<String, HashSet<VisualItem>>();

	public ConnectedSeriesChart(PreparedStatement statement, Object... params)
	                           throws SQLException {
		super(statement, params);
		keys.add(wAxisName);
		keys.add(zAxisName);
		setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
	}

	protected void prepareVis() {
		vt = m_vis.addTable(GROUP, data);

		m_vis.setRendererFactory(new RendererFactory() {
			private int counter;

			private Renderer yAxisRenderer = new AxisRenderer(
				Constants.LEFT, Constants.CENTER);
			private Renderer xAxisRenderer = new AxisRenderer(
				Constants.CENTER, Constants.FAR_BOTTOM);
			private Renderer seriesRenderer = new AbstractShapeRenderer() {
				protected Shape getRawShape(VisualItem item) {
					// Get the list of points on the same node.
					String node = item.getString(zAxisName);
					HashSet<VisualItem> nodeItems;
					synchronized (graphPoints) {
						nodeItems = graphPoints.get(node);
						if (null == nodeItems) {
							nodeItems = new HashSet<VisualItem>();
							graphPoints.put(node, nodeItems);
						}
					}

					// Find the latest point prior to this one.
					VisualItem lastItem = null;
					for (VisualItem currentItem : nodeItems) {
						if ((null == lastItem ||
						 lastItem.getLong(wAxisName) < currentItem.getLong(wAxisName)) &&
						 currentItem.getLong(wAxisName) < item.getLong(wAxisName)) {
							lastItem = currentItem;
						}
					}

					nodeItems.add(item);

					Line2D line;
					if (null != lastItem) {
						line = new Line2D.Double(
							lastItem.getX(), lastItem.getY(),
							item.getX(), item.getY()
						);
					}
					else {
						line = new Line2D.Double(
							item.getX(), item.getY(),
							item.getX(), item.getY()
						);
						counter = vt.getRowCount();
					}

					item.setStrokeColor(ColorLib.setAlpha(
						item.getStrokeColor(),
						255 * counter-- / vt.getRowCount()));

					return line;
				}
			};

			public Renderer getRenderer(VisualItem item) {
				return item.isInGroup(yAxisName) ? yAxisRenderer :
				       item.isInGroup(xAxisName) ? xAxisRenderer :
				       seriesRenderer;
			}
		});

		AxisLayout xAxis = new AxisLayout(
			GROUP, xAxisName, Constants.X_AXIS, VisiblePredicate.TRUE);
		AxisLayout yAxis = new AxisLayout(
			GROUP, yAxisName, Constants.Y_AXIS, VisiblePredicate.TRUE);
		RangeQueryBinding xAxisRange = new RangeQueryBinding(data, xAxisName);
		xAxis.setRangeModel(xAxisRange.getModel());
		xAxisRange.getNumberModel().setValueRange
			(-accelerometerRange, accelerometerRange,
			 -accelerometerRange, accelerometerRange);
		RangeQueryBinding yAxisRange = new RangeQueryBinding(data, yAxisName);
		yAxis.setRangeModel(yAxisRange.getModel());
		yAxisRange.getNumberModel().setValueRange
			(-accelerometerRange, accelerometerRange,
			 -accelerometerRange, accelerometerRange);

		xAxis.setLayoutBounds(barChartBounds);
		yAxis.setLayoutBounds(barChartBounds);

		AxisLabelLayout xAxisLabel = new AxisLabelLayout(
			xAxisName, xAxis, xAxisBounds, 40);
		AxisLabelLayout yAxisLabel = new AxisLabelLayout(
			yAxisName, yAxis, yAxisBounds);

		if (null == colorAction) {
			colorAction = new ColorAction(GROUP, VisualItem.STROKECOLOR);
		}

		ActionList axes = new ActionList();
		axes.add(xAxis);
		axes.add(yAxis);
		axes.add(xAxisLabel);
		axes.add(yAxisLabel);
		m_vis.putAction("axes", axes);

		ActionList draw = new ActionList();
		draw.add(new StrokeAction(GROUP, new BasicStroke(
			lineWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_BEVEL)));
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

	// Sets the boundaries of the objects (chart and axes)
	// relative to the current window. 
	protected void updateVis() {
		// Remove old data from the table.
		long latestTime = System.currentTimeMillis() / 1000;
		String expression = latestTime + " - [" + wAxisName + "] > " + maxDuration;
		data.remove(ExpressionParser.predicate(expression));

		// Remove old data from the list of nodes.
		synchronized (graphPoints) {
			for (String node : graphPoints.keySet()) {
				HashSet<VisualItem> nodeItems = graphPoints.get(node);
				Iterator<VisualItem> i = nodeItems.iterator();
				while (i.hasNext()) {
					VisualItem item = i.next();
					try {
						item.getInt(xAxisName);
					}
					catch (IllegalStateException e) {
						i.remove();
					}
				}
			}
		}

		// Update the axis and chart boundaries.
		Insets insets = getInsets();
		int width = getWidth();
		int height = getHeight();
		int insetsWidth = insets.left + insets.right;
		int insetsHeight = insets.top + insets.bottom;

		barChartBounds.setRect(
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
};
