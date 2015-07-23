/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

public class TopologyTab extends UpdatingTab implements ActionListener {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = -9067938005215536453L;

	private static final Logger LOGGER = Logger.getLogger(TopologyTab.class.getName());

	private static final int GRAPH_INTERVAL = 10000;
//	private static final int VALUE_INTERVAL = 1000;
	private static final int LABEL_INTERVAL = 100;

	private static final int MAX_AGE = 5;

	private String tableToQuery = null;

	private JComboBox comboLogicalTables = new JComboBox(new String[] {"Select target..."});
	
	private static final String GRAPH_SQL =
		"    SELECT gdbx_from_node source, gdbx_to_node target" +
		"    FROM new com.ibm.db2j.GaianTable('gdb_ltnull', 'explainds') T WHERE gdbx_depth > 0";
//		"  ORDER BY source, target";
	
/*
	Alternative query - this shows the paths to all nodes, not just those returning data. 
	
	SELECT DISTINCT
	gdbx_from_node source,
			 gdbx_to_node target, gdbx_precedence, gdbx_depth
			FROM new com.ibm.db2j.GaianTable('lt0', 'explain') T
	WHERE gdbx_depth > 0 order by gdbx_precedence, gdbx_depth
*/
	
//	TODO : SQL statement to get Path
	ArrayList<ArrayList<String>> queryPaths = new ArrayList<ArrayList<String>>(); 

	private final ActionListener MONITOR_SWITCH = new ActionListener() {
		public void actionPerformed(ActionEvent event) {
			String command = event.getActionCommand();
			synchronized (this) {
				if (null == command || command.equals(LABEL_NONE)) {
					valueProcessor.clearMonitors();
					graph.setMonitor(null);
					for ( int i=0; i<NUM_RANGE_COLORS; i++ ) {
						((JLabel)colorRangePanel.getComponent(i)).setText("");
					}
				}
				else {
					try {
						MonitorInfo selectedMonitor = MetricValueProcessor.MONITORS[Integer.parseInt(command)];
						valueProcessor.setMonitors(new MonitorInfo[] {selectedMonitor});
						graph.setMonitor(selectedMonitor);
												
						Color[] monitorColorRange = selectedMonitor.getColorRange(NUM_RANGE_COLORS);
						for ( int i=0; i<NUM_RANGE_COLORS; i++ ) {
							colorRangePanel.getComponent(i).setBackground(monitorColorRange[i]);
							((JLabel)colorRangePanel.getComponent(i)).setText(EMPTY_COLOR_FIELD);
						}
						
						((JLabel)colorRangePanel.getComponent(0)).setText(""+selectedMonitor.minBound);
						((JLabel)colorRangePanel.getComponent(NUM_RANGE_COLORS-1)).setText(""+selectedMonitor.maxBound);
						((JLabel)colorRangePanel.getComponent(NUM_RANGE_COLORS/2)).setText(selectedMonitor.unit);
						((JLabel)colorRangePanel.getComponent(NUM_RANGE_COLORS/2)).setForeground(Color.BLACK);
					}
					catch (Exception e) {
						valueProcessor.clearMonitors();
						graph.setMonitor(null);
					}
				}
				updateGraph();
			}
		}
	};
	
	private JPanel monitorPanel = null;
	private JLabel nodeInfo = null;
	private TopologyGraph graph = null;
//	private JPanel graphPanel = null;
	private Updater graphUpdater = null, colourUpdater;

	private PreparedStatement graphStatement;

	private MetricValueProcessor valueProcessor;
	
	private JPanel colorRangePanel = null;
	private static final int NUM_RANGE_COLORS=10;
	private static final String EMPTY_COLOR_FIELD = " ";
	
	private static final String LABEL_NONE = "None";
	
	private String clickedNode = null;
	
	private String localNodeID = null;

	private ConfigurationDialog configurationDialog = null;
	
	private static final String USR_LABEL = "Username";
	private static final String PWD_LABEL = "Password";
		
	private static final String[] textFieldLabels = new String[] { USR_LABEL, PWD_LABEL };
	
	public TopologyTab(Dashboard container) {
		super(container, new BorderLayout(Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE));
	}

	protected void create() {
		Border topBorder = BorderFactory.createEmptyBorder(Dashboard.BORDER_SIZE, 0, 0, 0);
		Border topBorder2 = BorderFactory.createEmptyBorder(Dashboard.BORDER_SIZE * 2, 0, 0, 0);
		
		monitorPanel = new JPanel(new GridBagLayout());

		GridBagConstraints c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = GridBagConstraints.RELATIVE;
		c.anchor = GridBagConstraints.LINE_START;

		ButtonGroup monitorGroup = new ButtonGroup();
//		JRadioButton none = new JRadioButton(LABEL_NONE);
//		none.setActionCommand(null);
//		none.addActionListener(MONITOR_SWITCH);
//		none.setSelected(true);
//		monitorGroup.add(none);
//		monitorPanel.add(none, c);
		for (int i = 0; i < MetricValueProcessor.MONITORS.length; i++) {
			JRadioButton monitorSelector = new JRadioButton(MetricValueProcessor.MONITORS[i].name);
			monitorSelector.addActionListener(MONITOR_SWITCH);
			monitorSelector.setActionCommand(Integer.toString(i));
			monitorGroup.add(monitorSelector);
			monitorPanel.add(monitorSelector, c);
		}
		
		c.gridx = GridBagConstraints.RELATIVE;
		c.weightx = 1;
		c.fill = GridBagConstraints.HORIZONTAL;
		
		colorRangePanel = new JPanel(new GridBagLayout());
		colorRangePanel.setBorder(topBorder);
		for ( int i=0; i<NUM_RANGE_COLORS; i++ ) {
			JLabel jl = new JLabel(EMPTY_COLOR_FIELD, SwingConstants.CENTER);
			Font f = jl.getFont();
			jl.setFont( f.deriveFont(f.getStyle()^Font.BOLD) );
			jl.setOpaque(true);
			jl.setForeground(Color.WHITE);
			colorRangePanel.add(jl,c);
		}
		
		c.gridx = 0;
		monitorPanel.add(colorRangePanel,c);
		
		c.weightx = 0;
		c.fill = GridBagConstraints.NONE;

		JLabel intervalLabel = new JLabel("Update Interval (s):");
		intervalLabel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, Dashboard.BORDER_SIZE / 2));
		JSpinner intervalSpinner = new JSpinner(new SpinnerNumberModel(GRAPH_INTERVAL / 1000, 1, 60, 1));
		intervalSpinner.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				Integer interval = (Integer)((JSpinner)e.getSource()).getValue();
				if (null != interval) {
					if (null != graphUpdater) graphUpdater.setInterval(interval * 1000);
					if (null != colourUpdater) colourUpdater.setInterval(interval * 1000);
				}
			}
		});

		JPanel intervalPanel = new JPanel(new GridBagLayout());
		intervalPanel.setBorder(topBorder);
		intervalPanel.add(intervalLabel);
		intervalPanel.add(intervalSpinner);
		monitorPanel.add(intervalPanel, c);
		
		JButton refresh = new JButton("Update Now");
//		refresh.setBorder(topBorder);
		refresh.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
//				try { updateLogicalTablesComboBox(); } catch ( SQLException e ) {}
				updateGraph();
				updateValues();
			}
		});
//		c.fill = GridBagConstraints.HORIZONTAL;
//		c.anchor = GridBagConstraints.CENTER;
		monitorPanel.add(refresh, c);

		JCheckBox showNames = new JCheckBox("Show Node IDs", true);
		showNames.setBorder(topBorder);
		showNames.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent e) {
				if (null != graph) {
					graph.setNodeRenderer(e.getStateChange() == ItemEvent.SELECTED);
				}
			}
		});
		monitorPanel.add(showNames, c);
		
		comboLogicalTables.addActionListener( new java.awt.event.ActionListener() {
		    
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboTableActionPerformed(evt);
			}

			private void comboTableActionPerformed(ActionEvent evt) {
				
				comboLogicalTables.transferFocus(); // allows us to update the list more readily if necessary

				JComboBox cb = (JComboBox)evt.getSource();
				if ( null == cb ) return;
				tableToQuery = (String)cb.getSelectedItem();

//				System.out.println("comboTableActionPerformed event selection: " + tableToQuery);				
				if ( 0 == cb.getSelectedIndex() || null != tableToQuery && 0 == tableToQuery.trim().length() )
					tableToQuery = null;
				
				updateGraph();
			}
		});
		
		comboLogicalTables.addFocusListener( new java.awt.event.FocusListener() {

			@Override
			public void focusGained(FocusEvent e) {
//				System.out.println("Updating from focusGained");
				try { updateLogicalTablesComboBox(); }
				catch (SQLException ex) {}
			}

			@Override public void focusLost(FocusEvent e) {}
		});
		
		// this doesn't trigger when the item is selected some other way (e.g. through track pad or keyboard..)
//		comboLogicalTables.addMouseListener( new java.awt.event.MouseListener() {
//
//			@Override
//			public void mouseClicked(MouseEvent e) {
//				System.out.println("Updating from MouseClicked");
//				try { updateLogicalTablesComboBox(); }
//				catch (SQLException ex) {} 
//			}
//
//			@Override public void mouseEntered(MouseEvent e) {}
//			@Override public void mouseExited(MouseEvent e) {}
//			@Override public void mousePressed(MouseEvent e) {}
//			@Override public void mouseReleased(MouseEvent e) {}
//		});
		
		comboLogicalTables.setMaximumRowCount(20);
		
		JPanel pathTablePanel = new JPanel(new GridBagLayout());
		pathTablePanel.setBorder(topBorder2);
		pathTablePanel.add( new JLabel("Show Logical Table Paths"), c); // for Path Visualisation") , c);
		pathTablePanel.add(comboLogicalTables, c);
		monitorPanel.add(pathTablePanel, c);

		c.fill = GridBagConstraints.VERTICAL;
		c.weighty = 1;
		monitorPanel.add(new JPanel(), c);

		nodeInfo = new JLabel("", SwingConstants.TRAILING);

		graph = TopologyGraph.getSingleton();
		graph.setNodeRenderer(true); // set default node renderer to show node names rather than symbols
		graph.setBorder(BorderFactory.createEtchedBorder());
		graph.panAbs(graph.getWidth() / 2, graph.getHeight() / 2);
		
		localNodeID = container.getLocalNodeID();
		graph.setLocalNode( localNodeID );

		try {
			graphStatement = conn.prepareStatement( GRAPH_SQL );
			graphStatement.setQueryTimeout(Dashboard.QUERY_TIMEOUT);
			valueProcessor = new MetricValueProcessor(conn) {
				protected void add(String node, int timestamp, Map<MonitorInfo, Integer> values) {
					for (Integer value : values.values()) {
						// if graph has not been destroyed
						if ( null != graph ) graph.setNodeColor(node, valueProcessor.getMonitors()[0].getColor(value));
					}
				}
			};
			valueProcessor.clearMonitors();
			valueProcessor.setTopologyGraph(graph);
		}
		catch (SQLException e) {
//			e.printStackTrace();
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not retrieve the graph topology information.");
			}
			return;
		}

		addUpdater(graphUpdater = new Updater("TopologyTab.updateGraph", GRAPH_INTERVAL) {
			protected boolean update() {
				return updateGraph();
			}
		});

		addUpdater(colourUpdater = new Updater("TopologyTab.updateValues", GRAPH_INTERVAL) {
			protected boolean update() {
				return updateValues();
			}
		});

		addUpdater(new Updater("TopologyTab.updateInfoLabel", LABEL_INTERVAL) {
			protected boolean update() {
				return updateInfoLabel();
			}
		});
		
		addUpdater(new Updater("TopologyTab.enterNodeDetails", LABEL_INTERVAL) {
			protected boolean update() {
				return enterNodeDetails();
			}
		});

		hideMessage();
		add(nodeInfo, BorderLayout.PAGE_START);
		add(monitorPanel, BorderLayout.LINE_START);		
		add(graph, BorderLayout.CENTER);
		
		((JRadioButton)monitorPanel.getComponent(0)).doClick();
		

//		System.out.println("Updating from create()");
		try { updateLogicalTablesComboBox(); }
		catch (SQLException e) {
//			e.printStackTrace();
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not retrieve the list of logical tables for this node.");
			}
			return;
		}
		
//		graphPanel = new JPanel(new GridBagLayout());
//		c.gridx = 0; c.gridy = 0; c.weightx = 1; c.weighty = 1;
//		c.fill = GridBagConstraints.BOTH;	
//		graphPanel.add(graph, c);
//
//		JPanel tgtPanel = new JPanel(new GridBagLayout());
//		c.weightx = 0; c.weighty = 0;
//		c.fill = GridBagConstraints.NONE;
//		tgtPanel.add(new JLabel("Targetted Nodes: "), c);
//		
//		JButton clrButton = new JButton("Clear");
//		c.gridx = 2; c.gridy = 0;
//		tgtPanel.add(clrButton, c);
//		
//		JTextArea qryInput = new JTextArea();
//		c.gridx = 1; c.gridy = 0; c.weightx = 1;
//		c.fill = GridBagConstraints.HORIZONTAL;
//		tgtPanel.add(createScroller(qryInput, -1, getFont().getSize()*4), c);
//		
//		tgtPanel.setBorder(topBorder);
//		
//		c.gridx = 0; c.gridy = 1;
//		graphPanel.add(tgtPanel, c);
//		
//		add(graphPanel, BorderLayout.CENTER);
 	}

	private Set<String> nodeLogicalTables = new HashSet<String>();
	private PreparedStatement getLTsStatement = null;
		
	private synchronized void updateLogicalTablesComboBox() throws SQLException {
		
		if ( null == getLTsStatement || getLTsStatement.isClosed() ) getLTsStatement =
			conn.prepareStatement("select LTNAME from new com.ibm.db2j.GaianConfig('LTDEFS') GC ORDER BY LTNAME");
		
		ResultSet rs = getLTsStatement.executeQuery();
//		ResultSet rs = conn.createStatement().executeQuery("select LTNAME from new com.ibm.db2j.GaianConfig('LTDEFS') GC");
		
		if ( rs.next() ) {
			Set<String> newItems = new HashSet<String>();
			
			int count = 0;
			do {
				count++;
				String lt = rs.getString(1);
				newItems.add(lt);
				if ( ! nodeLogicalTables.remove(lt) )
					comboLogicalTables.insertItemAt( lt, count ); // insert instead of adding to maintain the order
				
			} while ( rs.next() );
			
			for ( String oldItem : nodeLogicalTables )
				comboLogicalTables.removeItem(oldItem);
			
			nodeLogicalTables.clear();
			nodeLogicalTables = newItems;
			
//			System.out.println("Items count: " + count);
		}
		rs.close();
	}
	
	AtomicBoolean isGraphUpdating = new AtomicBoolean( false );
	
	protected boolean updateGraph() {

		if ( null == graph ) return false; // no-op if graph was destroyed
		
		if ( isGraphUpdating.compareAndSet( false, true ) ) {
			
			try {
				
				graph.setEdge(localNodeID, localNodeID);
				graphStatement = null;
				
//				System.out.println("Updating Graph: tableToQuery: " + tableToQuery );
								
				boolean isTableToQuerySetForPathVisualisation = null != tableToQuery;
				
				if ( isTableToQuerySetForPathVisualisation )
					try {
						String pathSQL =
							"SELECT gdbx_from_node source, gdbx_to_node target, gdbx_count, gdbx_precedence " +
							"FROM new com.ibm.db2j.GaianTable('" + tableToQuery + "', 'explainds') T " +
							"WHERE gdbx_depth > 0 ORDER BY gdbx_depth";
						
//						System.out.println("Using path graph SQL: " + pathSQL);
						graphStatement = conn.prepareStatement( pathSQL );
					}
					catch (Exception e) {
//						System.out.println("Caught exception: " + e);
						isTableToQuerySetForPathVisualisation = false;
						comboLogicalTables.setSelectedIndex(0);
					}
				
				if ( null == graphStatement )
					graphStatement = conn.prepareStatement( GRAPH_SQL );
				
				ResultSet resultSet = graphStatement.executeQuery();
				queryPaths.clear();
				
				while ( resultSet.next() ) {
					
					String src = resultSet.getString("SOURCE");
					String tgt = resultSet.getString("TARGET");
					
					if ( isTableToQuerySetForPathVisualisation && resultSet.getLong("GDBX_COUNT") > 0
							&& "F".equals( resultSet.getString("GDBX_PRECEDENCE") ) ) {
						
						boolean isNewPath = true;
						
//						System.out.println(src + " -> " + tgt + " " + resultSet.getLong("GDBX_COUNT"));
						
						if ( queryPaths.isEmpty() )
							queryPaths.add(new ArrayList<String>(Arrays.asList(src,tgt)));
						else {
							// extend any existing paths ending with 'src' by appending target 'tgt'
							for( ArrayList<String> path : queryPaths ) {
								if( path != null && ! path.isEmpty() ) {
									if( src.equals( path.get(path.size()-1) ) ) {
										path.add(tgt);
										isNewPath = false;
										break;
									}
								}
							}
							// if no paths were extended (above), create a new path based on an existing sub-path
							if ( isNewPath ) {
								ArrayList<String> newPath = null;
								for ( ArrayList<String> path : queryPaths ) {
									for ( int i=0; i < path.size()-1 ; i++ ) {
										if ( src.equals( path.get(i) ) ) {
											newPath = new ArrayList<String>( path.subList(0,i+1) );
											newPath.add(tgt);
											break;
										}
									}
									if ( null != newPath ) {
										queryPaths.add( newPath );
										break;
									}
								}
							}
						}
					}
					graph.setEdge(src,tgt);
				}
				
//				System.out.println("PATHS: " + Arrays.asList( queryPaths ) );
				
				graph.highLightLinks(queryPaths,-1); // animation tick = -1, i.e. disabled for now
				
				graph.update();
				updateValues();
				graph.recenter();
				
			} catch (SQLException e) {
				
				if (container.checkConnection()) {
					LOGGER.severe(unravelMessages(e));
					destroy();
					showError("Could not retrieve the graph topology information.");
				}
				return false;
			
			} finally {
				isGraphUpdating.set(false);	
			}
		}

		return true;
	}

	protected boolean updateValues() {
		synchronized (this) {
			try {
				valueProcessor.processLatestMetrics(MAX_AGE);
			}
			catch (SQLException e) {
				if (container.checkConnection()) {
					LOGGER.severe(unravelMessages(e));
					destroy();
					showError("Could not process latest metrics on graph topology.");
				}
				return false;
			}
		}
		
		if ( null != graph )
			graph.updateValues();

		MonitorInfo[] monitors = valueProcessor.getMonitors();
		if ( null != monitors )
			((JLabel)colorRangePanel.getComponent(NUM_RANGE_COLORS-1)).setText(""+monitors[0].maxBound);

		return true;
	}
		
	protected boolean enterNodeDetails() {
		if (null == graph) {
			return false;
		}
		
//		int nodeCount = graph.getNodeCount();
		String lastClickedNode = graph.getAndUnsetLastClickedNode();
		
		if (null != lastClickedNode) {
//			System.out.println("configurationDialog isVisible? " + (null==configurationDialog?false:configurationDialog.isVisible()));
			// show nodeDetails input fields box
			if ( null == configurationDialog || !configurationDialog.isVisible() ) {
				clickedNode = lastClickedNode;
				configurationDialog = new ConfigurationDialog( this, textFieldLabels, clickedNode );
//				configurationDialog.addWindowListener(this); // in case we want to know when the config panel was deactivated...
				configurationDialog.setVisible(true);
			} else {
				configurationDialog.requestFocus();
			}
		}

		return true;
	}
	
	private String previousHoveredOverNode = null;
	protected boolean updateInfoLabel() {
		if (null == graph) {
			return false;
		}
		
		String hoveredOverNode = graph.getCurrentItemName();
		boolean hasGraphChanged = graph.hasChanged();
		if ( hoveredOverNode != null && hoveredOverNode.equals(previousHoveredOverNode) && !hasGraphChanged && 0 < nodeInfo.getText().length())
			return true;
		previousHoveredOverNode = hoveredOverNode;
		
		if (null == hoveredOverNode) {			
			String txt =
				"Node Count: " + graph.getNodeCount() + "  |  " +
				"Radius: " + graph.getRadius() + "  |  " +
				"Diameter: " + graph.getDiameter() + "  |  " +
				"Nodes per Eccentricity: " + graph.getNodesPerEccentricity() + "  |  " +
				"Nodes per Connectivity: " + graph.getNodesPerConnectivity();
			
			if ( hasGraphChanged ) System.out.println( txt );
			
			nodeInfo.setText( txt );
			
		} else {			
			ArrayList<String> fn = new ArrayList<String>( graph.getFurthestNodes(hoveredOverNode) );
			int fnsize = fn.size();
			while( fn.size() > 2 ) fn.remove(0);
			
			nodeInfo.setText(
//				"Node Count: " + nodeCount + "  |  " +
				"Current Node: " + hoveredOverNode + "  |  " +
				"Eccentricity: " + graph.getEccentricity(hoveredOverNode) + "  |  " +
				"Nodes reached per step: " + graph.getStepCardinalities(hoveredOverNode) + "  |  " +
				"Furthest Nodes: " + fn + ( fnsize > 2 ? "  (" + (fnsize-2) + " more...)" : "" )
			);
		}

		return true;
	}

	protected void destroy() {
		if (null != monitorPanel) {
			remove(monitorPanel);
			monitorPanel = null;
		}

		if (null != nodeInfo) {
			remove(nodeInfo);
			nodeInfo = null;
		}

		if (null != graph) {
			remove(graph);
			graph = null;
		}
		
//		if (null != graphPanel) {
//			remove(graphPanel);
//			graphPanel = null;
//		}

		graphUpdater = null;
	}

	public void actionPerformed(ActionEvent e) {
		
		Properties configValues = configurationDialog.getConfigValuesIfReady();
		
		if ( null != configValues ) {
			
			try {
//				System.out.println("clickedNode " + clickedNode);
				
				String username = configValues.getProperty(USR_LABEL);
				String password = configValues.getProperty(PWD_LABEL);
				
				if ( 0==username.length() || 0==password.length() )
					throw new Exception("Each field must be set");
				
				container.securityAgent.setRemoteAccessCredentials(clickedNode, username, password);
				
				configurationDialog.setVisible(false);
				
			} catch (Exception e1) {
				String msg = e1.toString();
//				e1.printStackTrace();
				int exNameStartIndex = msg.lastIndexOf('.', msg.indexOf(':')) + 1;
				configurationDialog.requestNewConfigValues(msg.substring(exNameStartIndex));
			}
		}
	}
	
	public void updatePathVisualisation(int tickCount) {
//		If network created 
		if(graph != null) {
		
		    
			graph.highLightLinks(queryPaths,tickCount);
			
//		Find Path
	
		
//		Pass in values of which to highlight
								
		
//		Send path as Array of String to the Topology Graph
//		Write procedure that checks if Links on part of the Array
		
		
		
//		pass data to Topology Tab
		
//		System.out.println(graph.getDisplayX());
//		graph.dataPath();
		}
	}
}
