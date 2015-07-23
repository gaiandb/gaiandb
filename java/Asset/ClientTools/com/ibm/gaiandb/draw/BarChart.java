/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.awt.Insets;
import java.awt.Shape;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.geom.Rectangle2D;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.swing.BorderFactory;

import prefuse.Constants;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.DataShapeAction;
import prefuse.action.assignment.ShapeAction;
import prefuse.action.layout.AxisLabelLayout;
import prefuse.action.layout.AxisLayout;
import prefuse.data.query.RangeQueryBinding;
import prefuse.render.AxisRenderer;
import prefuse.render.Renderer;
import prefuse.render.RendererFactory;
import prefuse.render.ShapeRenderer;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.VisiblePredicate;

public class BarChart extends Chart {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 5832341171219181163L;
	
	private static final String xAxisName = "NODE";
	private static final String yAxisName = "VALUE";

	private static final int axisWidth = 20;
	private static final int axisHeight = 10;
	private static final int barGap = 50;

	private int totalBarWidth;
	private float barCount;

	private Rectangle2D chartBounds = new Rectangle2D.Double();
	private Rectangle2D xAxisBounds = new Rectangle2D.Double();
	private Rectangle2D yAxisBounds = new Rectangle2D.Double();

	private int minValue = 0;
	public int getMinValue() { return minValue; }
	public void setMinValue(int minValue) { this.minValue = minValue; }

	private int maxValue = 100;
	public int getMaxValue() { return maxValue; }
	public void setMaxValue(int maxValue) { this.maxValue = maxValue; }

	public BarChart(PreparedStatement statement, Object... params) throws SQLException {
		super(statement, params);
		setBorder(BorderFactory.createEmptyBorder(10, 10, 30, 10));
	}

	public void afterUpdate() {
		barCount = data.getRowCount();
	}

	protected void prepareVis() {
		vt = m_vis.addTable(GROUP, data);

		m_vis.setRendererFactory(new RendererFactory() {
			private Renderer yAxisRenderer = new AxisRenderer(Constants.LEFT, Constants.TOP);
			private Renderer xAxisRenderer = new AxisRenderer(Constants.CENTER, Constants.FAR_BOTTOM);
			private Renderer barRenderer = new ShapeRenderer() {
				protected Shape getRawShape(VisualItem item) {
					double x = item.getX();
					double y = item.getY();
					if (Double.isNaN(x) || Double.isInfinite(x))
						x = getInsets().left + axisWidth + totalBarWidth / 2;
					if (Double.isNaN(y) || Double.isInfinite(y))
						y = 0;

					double width = totalBarWidth / (barCount + 1) - barGap;
					double height = getHeight() - getInsets().bottom - axisHeight - y;
					x -= width / 2;

					return rectangle(x, y, width, height);
				}
			};

			public Renderer getRenderer(VisualItem item) {
				return item.isInGroup("yAxis") ? yAxisRenderer :
				       item.isInGroup("xAxis") ? xAxisRenderer :
				       barRenderer;
			}
		});

		// Define the axis regions.
		AxisLayout xAxis = new AxisLayout(GROUP, xAxisName, Constants.X_AXIS, VisiblePredicate.TRUE);
		AxisLayout yAxis = new AxisLayout(GROUP, yAxisName, Constants.Y_AXIS, VisiblePredicate.TRUE);
		RangeQueryBinding yAxisRange = new RangeQueryBinding(data, yAxisName);
		yAxis.setRangeModel(yAxisRange.getModel());
		yAxisRange.getNumberModel().setValueRange(minValue, maxValue, minValue, maxValue);

		xAxis.setLayoutBounds(chartBounds);
		yAxis.setLayoutBounds(chartBounds);

		AxisLabelLayout xAxisLabel = new AxisLabelLayout("xAxis", xAxis, xAxisBounds);
		AxisLabelLayout yAxisLabel = new AxisLabelLayout("yAxis", yAxis, yAxisBounds);

		ActionList axes = new ActionList();
		axes.add(xAxis);
		axes.add(yAxis);
		axes.add(xAxisLabel);
		axes.add(yAxisLabel);
		m_vis.putAction("axes", axes);

		// Define the shape and color of the data points.
		int[] shapePalette = { Constants.SHAPE_RECTANGLE };
		ShapeAction shapeAction = new DataShapeAction(GROUP, yAxisName, shapePalette);

		if (null == colorAction) {
			colorAction = new GradientColorAction(
				GROUP, yAxisName, Constants.NUMERICAL, VisualItem.FILLCOLOR);
		}

		if (colorAction instanceof GradientColorAction) {
			((GradientColorAction)colorAction).setMinValue(minValue);
			((GradientColorAction)colorAction).setMaxValue(maxValue);
		}

		ActionList draw = new ActionList();
		draw.add(shapeAction);
		draw.add(colorAction);
		draw.add(new RepaintAction());
		m_vis.putAction("draw", draw);

		// Reset the object boundaries whenever the window is resized.
		addComponentListener(new ComponentAdapter() {
            public void componentResized(ComponentEvent e) {
            	updateVis();
            }
        });
		updateVis();
	}

	// Sets the boundaries of the objects (chart and axes)
	// relative to the current window. 
	protected void updateVis() {
		Insets insets = getInsets();
		int width = getWidth();
		int height = getHeight();
		int insetsWidth = insets.left + insets.right;
		int insetsHeight = insets.top + insets.bottom;

		totalBarWidth = width - insetsWidth - axisWidth;
		chartBounds.setRect(
			insets.left + axisWidth + totalBarWidth * (1 / (barCount + 1)),
			insets.top,
			barCount > 1 ? totalBarWidth * ((barCount - 1) / (barCount + 1)) : 1,
			height - insetsHeight - axisHeight);
		xAxisBounds.setRect(
			insets.left + axisWidth + totalBarWidth * (1 / (barCount + 1)),
			height - insets.bottom - axisHeight,
			barCount > 1 ? totalBarWidth * ((barCount - 1) / (barCount + 1)) : 1,
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
