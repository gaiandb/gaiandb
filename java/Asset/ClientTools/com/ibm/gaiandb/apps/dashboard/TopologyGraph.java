/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.geom.Rectangle2D;

import com.ibm.gaiandb.tools.NetworkLinksAnalyser;

import prefuse.Constants;
import prefuse.Display;
import prefuse.Visualization;
import prefuse.action.Action;
import prefuse.action.ActionList;
import prefuse.action.RepaintAction;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.ShapeAction;
import prefuse.action.assignment.SizeAction;
import prefuse.action.layout.graph.ForceDirectedLayout;
import prefuse.activity.Activity;
import prefuse.controls.Control;
import prefuse.controls.ControlAdapter;
import prefuse.controls.DragControl;
import prefuse.controls.FocusControl;
import prefuse.controls.PanControl;
import prefuse.controls.WheelZoomControl;
import prefuse.controls.ZoomControl;
import prefuse.controls.ZoomToFitControl;
import prefuse.data.Graph;
import prefuse.data.Schema;
import prefuse.data.Table;
import prefuse.data.tuple.TupleSet;
import prefuse.render.DefaultRendererFactory;
import prefuse.render.EdgeRenderer;
import prefuse.render.LabelRenderer;
import prefuse.render.RendererFactory;
import prefuse.render.ShapeRenderer;
import prefuse.util.GraphicsLib;
import prefuse.util.display.DisplayLib;
import prefuse.util.force.DragForce;
import prefuse.util.force.ForceSimulator;
import prefuse.util.force.NBodyForce;
import prefuse.util.force.SpringForce;
import prefuse.visual.EdgeItem;
import prefuse.visual.VisualItem;

public class TopologyGraph extends Display {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 4659721334875164242L;

	private static final String GROUP = "data";
	private static final String NODE_GROUP = GROUP + ".nodes";
	private static final String EDGE_GROUP = GROUP + ".edges";

	private static final String NODE_ID_FIELD = "node";
	private static final String SOURCE_ID_FIELD = "source";
	private static final String TARGET_ID_FIELD = "target";
	private static final String NODE_NAME_FIELD = "name";

	private static final Color TEXT_COLOR = Color.WHITE;
	private static final Color DEFAULT_NODE_COLOR = Color.BLACK;
	private static final Color EDGE_COLOR = Color.LIGHT_GRAY;
	private static final Color SELECTED_EDGE_COLOR = Color.BLACK;
	private static final int EDGE_THICKNESS = 1;

	private static final int SPRING_LENGTH_MULTIPLIER = 100;	

	private NetworkLinksAnalyser nla = new NetworkLinksAnalyser();
	
	// Visualisation structures for paths leading to the source data of a logical table
	private boolean isQueryPathSet = false;
	private ArrayList<ArrayList<String>> queryPaths = new ArrayList<ArrayList<String>>();
	private int animationTick =0;
	
	public static class Node implements Comparable<Node> {
		private static final Map<String, Node> INSTANCES = new HashMap<String, Node>();

		private final String name;
		private Color color = null;

		private Node(String name) {
			this.name = name;
		}

		public static synchronized Node getInstance(String name) {
			Node instance = INSTANCES.get(name);
			if (null == instance) {
				instance = new Node(name);
				INSTANCES.put(name, instance);
			}

			return instance;
		}
		
		public static Set<String> getAllNodeNames() {
			return INSTANCES.keySet();
		}

		public static void removeInstance(String name) {
			INSTANCES.remove(name);
		}

		public String getName() {
			return name;
		}

		public Color getColor() {
			return color;
		}

		public void setColor(Color color) {
			this.color = color;
		}

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (!(o instanceof Node)) {
				return false;
			}

			return name.equals(((Node)o).name);
		}

		public int hashCode() {
			return name.hashCode();
		}

		public int compareTo(Node o) {
			return name.compareToIgnoreCase(o.name);
		}

		public String toString() {
			return name;
		}
	}

	public static class Edge implements Comparable<Edge> {
		public final Node source;
		public final Node target;
	
		public Edge(String sourceName, String targetName) {
			Node source = Node.getInstance(sourceName);
			Node target = Node.getInstance(targetName);
			int comparison = source.compareTo(target);

			if (comparison <= 0) {
				this.source = source;
				this.target = target;
			}
			else {
				this.source = target;
				this.target = source;
			}
		}

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (!(o instanceof Edge)) {
				return false;
			}

			Edge other = (Edge)o;
			return (
				source.equals(other.source) &&
				target.equals(other.target)
			);
		}

		public int hashCode() {
			return source.hashCode() ^ target.hashCode();
		}

		public int compareTo(Edge o) {
			int ret;
			ret = source.compareTo(o.source);
			if (0 != ret) {
				return ret;
			}

			ret = target.compareTo(o.target);
			if (0 != ret) {
				return ret;
			}

			return 0;
		}

		public String toString() {
			return source.name + " <-> " + target.name;
		}
	}

	public Graph data;
	public Table nodeTable;
	public Table edgeTable;

	private Map<Node, Integer> nodes = new TreeMap<Node, Integer>();
	private Map<Edge, Integer> edges = new TreeMap<Edge, Integer>();

	private Map<Node, Color> newNodeColors = new TreeMap<Node, Color>();
	private Set<Edge> newEdges = new TreeSet<Edge>();

	private MonitorInfo monitor = null;
	
//	private JPopupMenu popup = new JPopupMenu("POPUP!");

	private String localNode = null;
	private String currentHoverItemName = null;
	private String lastClickedNode = null;

	private RendererFactory rendererWithoutLabels;
	private RendererFactory rendererWithLabels;
	
	private boolean isBackgroundSelected = false;
		
	public synchronized void recenter() {

		if ( null == m_vis || isTranformInProgress() || null != currentHoverItemName || isBackgroundSelected ) return;
		try {
			long m_duration = 500;
			int m_margin = 50;
			
			Rectangle2D bounds = m_vis.getBounds( Visualization.ALL_ITEMS );
	        
//	        System.out.println(
//					"Display size: width: " + this.getWidth() + ", height: " + this.getHeight()
//					"\nVis bounds: center: " + bounds.getCenterX() + ", " + bounds.getCenterY() +
//	        		", bounds X: " + bounds.getMinX() + ", " + bounds.getMaxX() +
//	        		", bounds Y: " + bounds.getMinY() + ", " + bounds.getMaxY()
//	        );
	        
	        if ( bounds.getMinX() == bounds.getMaxX() || bounds.getMinY() == bounds.getMaxY())
	        	return;
	        
	        GraphicsLib.expand(bounds, m_margin + (int)(1/getScale()));
	        DisplayLib.fitViewToBounds(this, bounds, m_duration);
		} catch( Exception e ) { System.out.println("Exception in recenter(): " + e); }
	}

	private SpringForce springForce = new SpringForce(
		1e-5f, SpringForce.DEFAULT_SPRING_LENGTH);

	public MonitorInfo getMonitor() {
		return monitor;
	}

	public void setMonitor(MonitorInfo monitor) {
		this.monitor = monitor;
	}

	public int getNodeCount() {
		return null != data ? data.getNodeCount() : 0;
	}

	public String getLocalNode() {
		return localNode;
	}

	public void setLocalNode(String localNode) {
		this.localNode = localNode;
	}

	public String getCurrentItemName() {
		return currentHoverItemName;
	}
	
	public String getAndUnsetLastClickedNode() {
		String lcn = lastClickedNode;
		lastClickedNode = null;
		return lcn;
	}

	public void setNodeRenderer(boolean withLabels) {
		if (withLabels) {
			m_vis.setRendererFactory(rendererWithLabels);
		}
		else {
			m_vis.setRendererFactory(rendererWithoutLabels);
		}
	}
	
	private static final Object lock = new Object();
	private static TopologyGraph graph = null;
	public static TopologyGraph getSingleton() {
		if ( null == graph ) synchronized (lock) { if ( null == graph ) graph = new TopologyGraph(); }
		return graph;
	}

	private TopologyGraph() {
		super(new Visualization());

		String[] nodeColumns = { NODE_ID_FIELD, NODE_NAME_FIELD };
		Class<?>[] nodeColumnTypes = { int.class, String.class };
		nodeTable = new Schema(nodeColumns, nodeColumnTypes).instantiate();
		data = new Graph(nodeTable, false, NODE_ID_FIELD, SOURCE_ID_FIELD, TARGET_ID_FIELD);
		edgeTable = data.getEdgeTable();

		m_vis.addGraph(GROUP, data);

		ShapeRenderer nodeRendererWithoutLabels = new ShapeRenderer();
		LabelRenderer nodeRendererWithLabels = new LabelRenderer(NODE_NAME_FIELD);
		nodeRendererWithLabels.setRoundedCorner(10, 10);
		EdgeRenderer edgeRenderer = new EdgeRenderer() {
			
			public void render(Graphics2D g, VisualItem item) {
				
//				System.out.println("Call stack: " + Util.getStackTraceDigest() + "\nTimeNow: " + System.currentTimeMillis());
				
				EdgeItem edgeItem = (EdgeItem) item;
				
				String src = edgeItem.getSourceItem().getString(NODE_NAME_FIELD);
				String tgt = edgeItem.getTargetItem().getString(NODE_NAME_FIELD);
				
//				String edgeString1 = src + "  " + tgt, edgeString2 =  tgt + "  " + src;
//				
//				if ( !edgesRenderedAfterUpdate.contains(edgeString1) && !edgesRenderedAfterUpdate.contains(edgeString2) ) {
//					edgesRenderedAfterUpdate.add(edgeString1);
//					System.out.println("Rendering edge: " + edgeString1);
//				}
				
				if ( null != currentHoverItemName && (currentHoverItemName.equals( src ) || currentHoverItemName.equals( tgt )) ) {
					item.setSize(EDGE_THICKNESS * 2);
					item.setStrokeColor(SELECTED_EDGE_COLOR.getRGB());
				
				} else if ( isQueryPathSet && edgeHighlighted( src, tgt ) ) {
					item.setSize(EDGE_THICKNESS * 2);
					item.setStrokeColor(Color.RED.getRGB());
				
				} else if ( isQueryPathSet && edgeOnPath( src, tgt ) ) {
					item.setSize(EDGE_THICKNESS * 2);
					item.setStrokeColor( Color.BLUE.getRGB() );
				
				} else {
					item.setSize(EDGE_THICKNESS);
					item.setStrokeColor(EDGE_COLOR.getRGB());
				}

				super.render(g, item);
			}
			
			private boolean edgeHighlighted(String src, String tgt) {
				
				if ( 0 > animationTick ) return false; // disabled for now if = -1
				
				synchronized ( queryPaths ) {
					
					for ( ArrayList<String> path : queryPaths ) {
						
						if ( path != null  && ! path.isEmpty() ) {					
							int pathSize = path.size();
							String tickSrc = path.get(animationTick % pathSize);
							String tickTgt = path.get((animationTick + 1) % pathSize);
							if ( src.equals(tickSrc) && tgt.equals(tickTgt) || tgt.equals(tickSrc) && src.equals(tickTgt))
								return true;
						}
					}	
				}
				
				return false;
			}

			private boolean edgeOnPath(String n1, String n2) {
				
				synchronized ( queryPaths ) {
					
					Iterator<ArrayList<String>> iter = queryPaths.iterator();
					
					while ( iter.hasNext() ) {
						ArrayList<String> path = (ArrayList<String>) iter.next();
						
						// Check whether the egde(n1, n2) is one of the segments of this path (note that the path cannot loop)
						for ( int i=0; i < path.size()-1 ; i++ ) {
							String src = path.get(i), tgt = path.get(i+1);
							if ( src.equals(n1) || src.equals(n2) )
								if ( tgt.equals(n1) || tgt.equals(n2) ) return true; else break;
						}
					}
				}
				
				return false;
			}
		};

		rendererWithoutLabels = new DefaultRendererFactory( nodeRendererWithoutLabels, edgeRenderer );
		rendererWithLabels = new DefaultRendererFactory( nodeRendererWithLabels, edgeRenderer );
		m_vis.setRendererFactory( rendererWithoutLabels );

		// Set up the drawing actions.
		ActionList draw = new ActionList();
		draw.add(new ShapeAction(NODE_GROUP, Constants.SHAPE_ELLIPSE) {
			public int getShape(VisualItem item) {
				if (null != localNode && localNode.equals(item.getString(NODE_NAME_FIELD))) {
					return Constants.SHAPE_STAR;
				}
				else {
					return super.getShape(item);
				}
			}
		});
		draw.add(new SizeAction(NODE_GROUP) {
			public double getSize(VisualItem item) {
				if (null != localNode && localNode.equals(item.getString(NODE_NAME_FIELD))) {
					return super.getSize(item) * 2;
				}
				else {
					return super.getSize(item);
				}
			}
		});
//		draw.add(new ColorAction(NODE_GROUP, VisualItem.TEXTCOLOR, TEXT_COLOR.getRGB()));
		draw.add(new ColorAction(EDGE_GROUP, VisualItem.STROKECOLOR, EDGE_COLOR.getRGB()));
		draw.add(new RepaintAction());
		m_vis.putAction("draw", draw);
		
		Action selectNode = new ColorAction( NODE_GROUP, VisualItem.STROKECOLOR, DEFAULT_NODE_COLOR.getRGB()) {
			public int getColor(VisualItem item) {
				if ( !item.isInGroup(Visualization.FOCUS_ITEMS) ) return item.getFillColor();
				return Color.CYAN.getRGB();
			}
		};		
		m_vis.putAction("selectNode", selectNode);
		
		ActionList nodeColor = new ActionList();
		nodeColor.add( new ColorAction(NODE_GROUP, VisualItem.FILLCOLOR, DEFAULT_NODE_COLOR.getRGB()) {
			public int getColor(VisualItem item) {				
				if (null == monitor) return super.getColor(item);

				String nodeName = item.getString(NODE_NAME_FIELD);
				Color color = Node.getInstance(nodeName).getColor();
				if (null == color) return super.getColor(item);

				return color.getRGB();
			}
		});
		nodeColor.add( new ColorAction(NODE_GROUP, VisualItem.TEXTCOLOR, TEXT_COLOR.getRGB()) {
			public int getColor(VisualItem item) {
				String nodeName = item.getString(NODE_NAME_FIELD);
				Color color = Node.getInstance(nodeName).getColor();
				if (null == color || !color.equals(Color.YELLOW) ) return Color.WHITE.getRGB();
				return Color.BLACK.getRGB();
			}
		});
		nodeColor.add( selectNode );
		m_vis.putAction("nodeColor", nodeColor);

		// Set up the positioning actions.
		ForceSimulator forces = new ForceSimulator();
		forces.addForce(new NBodyForce());
		forces.addForce(springForce);
		forces.addForce(new DragForce());

		ActionList position = new ActionList(Activity.INFINITY);
		position.add(new ForceDirectedLayout(GROUP, forces, false));
		position.add(new RepaintAction());
		m_vis.putAction("position", position);
		
//		for ( String s : new String[] { "Kill", "Execute Last Query" } ) {
//			JMenuItem jm = new JMenuItem(s);
////			jm.addActionListener(this);
//			popup.add(jm);
//		}
		
		// Set up movement and zooming for windows and nodes.
		addControlListener(new DragControl()); // { public void mouseClicked(MouseEvent e) { graph.requestFocus(); super.mouseClicked(e); } } );
		addControlListener(new PanControl());
		addControlListener(new ZoomControl());
		addControlListener(new WheelZoomControl());
		addControlListener(new FocusControl(1, "selectNode") {
		    public void mouseClicked(MouseEvent e) {
//		    	System.out.println("button clicked: " + e.getButton() + ", LEFT=" + Control.LEFT_MOUSE_BUTTON + 
//		    			", fo: " + graph.isFocusOwner() + ", fr: " + graph.isRequestFocusEnabled() + ", froot: " + graph.isFocusCycleRoot());
		    	if ( !graph.isFocusOwner() ) graph.requestFocus();
		    	else if ( MouseEvent.BUTTON1 == e.getButton() && !e.isControlDown() ) {
	                TupleSet ts = m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
	                ts.clear(); curFocus = null; m_vis.run(activity);
		    	}
		    	super.mouseClicked(e);
		    }
		    public void itemClicked(VisualItem item, MouseEvent e) {
		    	super.itemClicked(item, e);
		    	if ( !e.isControlDown() ) {
	                TupleSet ts = m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
	                ts.clear(); curFocus = null; m_vis.run(activity);
		    	}		    		
		    }
		});
		addControlListener(new ZoomToFitControl(Control.MIDDLE_MOUSE_BUTTON));
		addControlListener(new ControlAdapter() {
			public void itemEntered(VisualItem item, MouseEvent event) {
				if (item.isInGroup(NODE_GROUP)) {
					currentHoverItemName = item.getString(NODE_NAME_FIELD);
				}
			}

			public void itemExited(VisualItem item, MouseEvent event) {
				currentHoverItemName = null;
			}
			
			public void mousePressed( MouseEvent event) {
				isBackgroundSelected = true;
			}
			
			public void mouseReleased( MouseEvent event) {
				isBackgroundSelected = false;
			}
			
			public void itemClicked(VisualItem item, MouseEvent event) {
				if (event.isShiftDown() && item.isInGroup(NODE_GROUP)) { //2 == event.getClickCount() ) {
					// DRV - 22/10/2011 - commented out node clicking action. Security credentials are not to be entered this way anymore.
					// this should be customer/policy-plugin specific now
//					lastClickedNode = item.getString(NODE_NAME_FIELD);
					return;
				}
				
//				if ( MouseEvent.BUTTON3 == event.getButton() ) {
////			    	System.out.println("comp: " + event.getComponent() + ", X: " + event.getX() + ", Y: " + event.getY() );
////			    	System.out.println("comp: " + event.getComponent() + ", X: " + event.getXOnScreen() + ", Y: " + event.getYOnScreen());
//					popup.show(event.getComponent(), event.getX(), event.getY());
//				}
			}
			
            public void keyTyped(KeyEvent event) {
				if (event.isControlDown() && event.getKeyChar() == 1) { // == Ctrl-A => toggle select/unselect all nodes
					TupleSet ts = m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
					int tupleCount = ts.getTupleCount();
					ts.clear();
					if ( nodeTable.getRowCount() != tupleCount ) {
			            @SuppressWarnings("unchecked")  // prefuse code is not generic
						Iterator<VisualItem> it = m_vis.getGroup(NODE_GROUP).tuples();
						while (it.hasNext()) ts.addTuple(it.next());
					}
					m_vis.run("selectNode");
				}
			}
		});
	}
	
	public Set<String> getSelectedNodes() {
		TupleSet ts = (TupleSet) m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
		Set<String> nodes = new HashSet<String>();
        @SuppressWarnings("unchecked")  // prefuse code is not generic
		Iterator<VisualItem> it = ts.tuples();
		try {
			while ( it.hasNext() )
				nodes.add(it.next().getString(NODE_NAME_FIELD));
		} catch (Exception e) {
			return null;
		}
		return nodes;
	}

	public synchronized void update() {
		synchronized (newEdges) {
						
			m_vis.cancel("position");

			processNewEdges();

			if (0 == data.getNodeCount()) {
				return;
			}

			springForce.setMaxValue(SpringForce.SPRING_LENGTH,
				(float)(Math.log10(data.getNodeCount()) * SPRING_LENGTH_MULTIPLIER));
			m_vis.run("position");

			m_vis.run("draw");
			m_vis.run("nodeColor");
		}
	}

	public synchronized void updateValues() {
		processNewNodeColors();
	}

	public void setNodeColor(String nodeName, Color color) {
		if (null != color) {
			synchronized (newNodeColors) {
				newNodeColors.put(Node.getInstance(nodeName), color);
			}
		}
	}

	public void setEdge(String sourceName, String targetName) {
//		System.out.println("Setting edge: " + sourceName + "->" + targetName);
		synchronized (newEdges) {
			if ( null != sourceName && null != targetName )
				newEdges.add(new Edge(sourceName, targetName));
		}
	}

	private void processNewNodeColors() {
		for (Node node : nodes.keySet()) {
			node.setColor(null);
		}

		for (Entry<Node, Color> nodeValue : newNodeColors.entrySet()) {
			nodeValue.getKey().setColor(nodeValue.getValue());
		}

		newNodeColors.clear();

		m_vis.run("nodeColor");
	}

	private void processNewEdges() {
		
		boolean isGraphChanged = false;
		
		Set<Node> newNodes = new HashSet<Node>();
		for (Edge edge : newEdges) {
			newNodes.add(edge.source);
			newNodes.add(edge.target);
		}

		Iterator<Edge> edgeIterator = edges.keySet().iterator();
		while (edgeIterator.hasNext()) {
			Edge edge = edgeIterator.next();
			if (!newEdges.contains(edge)) {				
				Integer row = edges.get(edge);
				if (null != row) {
					edgeTable.removeRow(row);
				}
				edgeIterator.remove();
			}
		}

		Iterator<Node> nodeIterator = nodes.keySet().iterator();
		while (nodeIterator.hasNext()) {
			Node node = nodeIterator.next();
			if (!newNodes.contains(node)) {
				isGraphChanged = true;
				
				nodeTable.removeRow(nodes.get(node));
				nodeIterator.remove();
				Node.removeInstance(node.name);
			}
		}

		for (Edge edge : newEdges) {
			if (!edges.containsKey(edge)) {
				isGraphChanged = true;
				
				Node source = Node.getInstance(edge.source.name);
				Integer sourceId = nodes.get(edge.source);
				if (null == sourceId) {
					sourceId = nodeTable.addRow();
					nodeTable.setInt(sourceId, NODE_ID_FIELD, sourceId);
					nodeTable.setString(sourceId, NODE_NAME_FIELD, edge.source.name);
					nodes.put(source, sourceId);
				}

				Node target = Node.getInstance(edge.target.name);
				Integer targetId = nodes.get(edge.target);
				if (null == targetId) {
					targetId = nodeTable.addRow();
					nodeTable.setInt(targetId, NODE_ID_FIELD, targetId);
					nodeTable.setString(targetId, NODE_NAME_FIELD, edge.target.name);
					nodes.put(target, targetId);
				}

				if (sourceId != targetId) {
					Integer edgeId = edgeTable.addRow();
					edgeTable.setInt(edgeId, SOURCE_ID_FIELD, sourceId);
					edgeTable.setInt(edgeId, TARGET_ID_FIELD, targetId);
					edges.put(edge, edgeId);
				}
				else {
					edges.put(edge, null);
				}
			}
		}

		newEdges.clear();
		
		if ( isGraphChanged ) {
			nla.computeStats(edges.keySet());
			hasChanged = true;
		}
	}
	
	private boolean hasChanged = false;
	boolean hasChanged() {
		boolean hasChanged = this.hasChanged;
		this.hasChanged = false;
		return hasChanged;
	}
	
	Set<String> getAllNodes() {
//		Set<String> nodeNames = new HashSet<String>();
//		for ( Node node : nodes.keySet() ) nodeNames.add(node.getName());
//		return nodeNames;
		return Node.getAllNodeNames();
	}

	int getConnectivity( String node ) { return nla.getNumConnections(node); }
	int getEccentricity( String node ) { return nla.getEccentricity(node); }
	Set<String> getFurthestNodes( String node ) { return nla.getFurthestNodes(node); }
	ArrayList<Integer> getStepCardinalities( String node ) { return nla.getStepCardinalities(node); }
	
	int getDiameter() {	return nla.getDiameter(); }
	int getRadius() { return nla.getRadius(); }
	String getNodesPerEccentricity() { return nla.getNodesPerEccentricity(); }
	String getNodesPerConnectivity() { return nla.getNodesPerConnectivity(); }
	public void highLightLinks(ArrayList<ArrayList<String>> qPaths, int animationTick) {
		synchronized ( queryPaths ) { queryPaths.clear(); isQueryPathSet = !qPaths.isEmpty(); if ( isQueryPathSet ) queryPaths.addAll( qPaths ); }
//		System.out.println("The updated Query Paths : " + queryPaths);
		this.animationTick = animationTick;
//		edgesRenderedAfterUpdate.clear();
	}
//	private Set<String> edgesRenderedAfterUpdate = new HashSet<String>();
}
