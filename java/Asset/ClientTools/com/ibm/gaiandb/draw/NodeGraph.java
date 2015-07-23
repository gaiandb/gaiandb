/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.awt.Graphics2D;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.MouseEvent;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import prefuse.Constants;
import prefuse.Visualization;
import prefuse.action.Action;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.ShapeAction;
import prefuse.action.layout.graph.ForceDirectedLayout;
import prefuse.activity.Activity;
import prefuse.controls.Control;
import prefuse.controls.ControlAdapter;
import prefuse.controls.DragControl;
import prefuse.controls.PanControl;
import prefuse.controls.WheelZoomControl;
import prefuse.controls.ZoomControl;
import prefuse.controls.ZoomToFitControl;
import prefuse.render.DefaultRendererFactory;
import prefuse.render.EdgeRenderer;
import prefuse.render.ShapeRenderer;
import prefuse.util.force.DragForce;
import prefuse.util.force.ForceSimulator;
import prefuse.util.force.NBodyForce;
import prefuse.util.force.SpringForce;
import prefuse.visual.VisualItem;

public class NodeGraph extends Graph {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final long serialVersionUID = 8753567782561481785L;

	public static final String NAME_SUFFIX = "_NAME";
	public static final String NAME_COLUMN = NODE_ID_COLUMN + NAME_SUFFIX;
	public static final String EDGE_SOURCE_COLUMN = SOURCE_ID_COLUMN + NAME_SUFFIX;
	public static final String EDGE_TARGET_COLUMN = TARGET_ID_COLUMN + NAME_SUFFIX;

	public static final int EDGE_THICKNESS = 1;
	public static final int EDGE_COLOR = 0xFFCCCCCC;
	public static final int SELECTED_EDGE_COLOR = 0xFF000000;

	private boolean updating = false;

	private String currentHoverItemName = null;

	public int getNodeCount() {
		return null != data ? data.getNodeCount() : 0;
	}

	public String getCurrentItemName() {
		return currentHoverItemName;
	}

	private ActionList position = new ActionList(Activity.INFINITY); 
	private SpringForce springForce = new SpringForce(
		1e-5f, SpringForce.DEFAULT_SPRING_LENGTH);
	private static final int SPRING_LENGTH_MULTIPLIER = 100;

	public NodeGraph(PreparedStatement statement, Object... params) throws SQLException {
		super(statement, params);
	}

	public void beforeUpdate() {
		updating = true;
		if (null != position) {
			position.cancel();
		}
	}

	public void afterUpdate() {
		if (null != position) {
			m_vis.run("position");
		}
		updating = false;
    }

	private class ActionSilencer extends Action {
		Action action;

		public ActionSilencer(Action action) {
			super();
			this.action = action;
		}

		public void run(double frac) {
			try {
				action.run(frac);
			}
			catch (Exception e) {
				// Ignore it. Silent, remember?
			}
		}

		public void setVisualization(Visualization vis) {
			super.setVisualization(vis);
			action.setVisualization(vis);
		}
	}

	protected synchronized void prepareVis() {
		m_vis.addGraph(GROUP, data);

//		LabelRenderer lr = new LabelRenderer(NAME_COLUMN);
//		lr.setRoundedCorner(10, 10);
		
		DefaultRendererFactory renderer = new DefaultRendererFactory(
			new ShapeRenderer(),
//			lr,
			new EdgeRenderer() {
				public void render(Graphics2D g, VisualItem item) {
					if (updating) {
						return;
					}

					if (null != currentHoverItemName &&
					    (currentHoverItemName.equals(item.getString(EDGE_SOURCE_COLUMN)) ||
					     currentHoverItemName.equals(item.getString(EDGE_TARGET_COLUMN)))) {
						item.setSize(EDGE_THICKNESS * 2);
						item.setStrokeColor(SELECTED_EDGE_COLOR);
					}
					else {
						item.setSize(EDGE_THICKNESS);
						item.setStrokeColor(EDGE_COLOR);
					}

					super.render(g, item);
				}
			}
		);
		m_vis.setRendererFactory(renderer);

		// Set up the drawing actions.
		if (null == nodeShapeAction) {
			nodeShapeAction = new ShapeAction(NODE_GROUP, Constants.SHAPE_ELLIPSE);
			draw.add(nodeShapeAction);
		}
		if (null == nodeColorAction) {
			nodeColorAction = new ColorAction(NODE_GROUP, VisualItem.FILLCOLOR);
			draw.add(nodeColorAction);
		}
		if (null == edgeColorAction) {
			edgeColorAction = new ColorAction(EDGE_GROUP, VisualItem.STROKECOLOR, EDGE_COLOR);
			draw.add(edgeColorAction);
		}

		draw.add(new RepaintAction());
		m_vis.putAction("draw", draw);

		// Set up the positioning actions.
		ForceSimulator forces = new ForceSimulator();
		forces.addForce(new NBodyForce());
		forces.addForce(springForce);
		forces.addForce(new DragForce());

		position.add(new ActionSilencer(new ForceDirectedLayout(GROUP, forces, false)));
		position.add(new ActionSilencer(new RepaintAction()));
		m_vis.putAction("position", position);

		// Set up movement and zooming for windows and nodes.
		addControlListener(new DragControl());
		addControlListener(new PanControl());
		addControlListener(new ZoomControl());
		addControlListener(new WheelZoomControl());
		addControlListener(new ZoomToFitControl(Control.MIDDLE_MOUSE_BUTTON));
		addControlListener(new ControlAdapter() {
			public void itemEntered(VisualItem item, MouseEvent event) {
				if (item.isInGroup(NODE_GROUP)) {
					try {
						currentHoverItemName = item.getString(NAME_COLUMN);
					}
					catch (IndexOutOfBoundsException e) {
						// Ignore it. It means we're in the middle of updating.
					}
				}
			}

			public void itemExited(VisualItem item, MouseEvent event) {
				currentHoverItemName = null;
			}
		});

		// Centre the graph.
		pan(getWidth() / 2, getHeight() / 2);

		// Reset the object boundaries whenever the window is resized.
		addComponentListener(new ComponentAdapter() {
			public void componentResized(ComponentEvent e) {
				updateVisualization();
			}
		});

		m_vis.run("position");

		updateVis();
	}

	protected synchronized void updateVis() {
		springForce.setMaxValue(SpringForce.SPRING_LENGTH,
			(float)(Math.log10(data.getNodeCount()) * SPRING_LENGTH_MULTIPLIER));
		m_vis.run("draw");
	}
}
