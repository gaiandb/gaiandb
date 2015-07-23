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
import java.util.ArrayList;

import javax.swing.BorderFactory;

import prefuse.Constants;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.ShapeAction;
import prefuse.action.layout.AxisLabelLayout;
import prefuse.action.layout.AxisLayout;
import prefuse.data.expression.parser.ExpressionParser;
import prefuse.render.AxisRenderer;
import prefuse.render.Renderer;
import prefuse.render.RendererFactory;
import prefuse.render.ShapeRenderer;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.VisiblePredicate;

public class ChartLegend extends Chart {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final long serialVersionUID = 6130753939483349262L;

	private static final String nameColumn = "NODE_NAME";
	private static final String timeColumn = "UPDATED";

	private static final int colorBoxWidth = 15;
	private static final int lineHeight = 15;
	private Rectangle2D colorBoxBounds = new Rectangle2D.Double();
	private Rectangle2D axisBounds = new Rectangle2D.Double();

	private ArrayList<VisualItem> flipped = new ArrayList<VisualItem>();

	public ChartLegend(PreparedStatement statement, Object... params) throws SQLException {
		super(statement, params);
		keys.add(nameColumn);
		setBorder(BorderFactory.createEmptyBorder(20, 80, 20, 80));
	}

	protected void prepareVis() {
		vt = m_vis.addTable(GROUP, data);

		m_vis.setRendererFactory(new RendererFactory() {
			private Renderer shapeRenderer = new ShapeRenderer() {
				protected Shape getRawShape(VisualItem item) {
					item.setX(getInsets().left + item.getSize() / 2);

					Insets insets = getInsets();
					if (Double.isNaN(item.getY())) {
						item.setY(insets.top + (getHeight() - insets.top - insets.bottom - lineHeight) / 2);
					}
					else if (data.getRowCount() > 1 && !flipped.contains(item)) {
						item.setY(getHeight() - item.getY() + insets.top - insets.bottom);
						flipped.add(item);
					}

					return super.getRawShape(item);
				}
			};
			private Renderer axisRenderer = new AxisRenderer(Constants.LEFT, Constants.CENTER);

			public Renderer getRenderer(VisualItem item) {
				return item.isInGroup("axis") ?
					axisRenderer :
					shapeRenderer;
			}
		});

		AxisLayout axis = new AxisLayout(GROUP, nameColumn, Constants.Y_AXIS, VisiblePredicate.TRUE);
		axis.setLayoutBounds(colorBoxBounds);
		AxisLabelLayout axisLabel = new AxisLabelLayout("axis", axis, axisBounds, 1);
		axisLabel.setAscending(false);

		if (null == colorAction) {
			colorAction = new ColorAction(GROUP, VisualItem.STROKECOLOR);
		}

		ActionList axes = new ActionList();
		axes.add(axis);
		axes.add(axisLabel);
		m_vis.putAction("axes", axes);

		ActionList draw = new ActionList();
		draw.add(new ShapeAction(GROUP));
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
		// Remove old data from the table.
		long latestTime = System.currentTimeMillis() / 1000;
		String expression = latestTime + " - [" + timeColumn + "] > " + maxDuration;
		data.remove(ExpressionParser.predicate(expression));

		flipped.clear();

		Insets insets = getInsets();
		int width = getWidth() - insets.left - insets.right;
		int height = getHeight() - insets.top - insets.bottom;
		int legendHeight = data.getRowCount() * lineHeight;

		colorBoxBounds.setRect(
			insets.left,
			insets.top + (height - legendHeight) / 2,
			colorBoxWidth,
			legendHeight);
		axisBounds.setRect(
			insets.left + colorBoxWidth,
			insets.top + (height - legendHeight) / 2,
			width - colorBoxWidth,
			legendHeight);

		m_vis.run("axes");
		m_vis.run("draw");
	}
}
