/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.Border;

public class StatsTab extends UpdatingTab {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 3463679662252114164L;

	private static final Logger LOGGER = Logger.getLogger(StatsTab.class.getName());

	private static final int INTERVAL = 1000;

	private static final int CHART_HEIGHT = 80;
	private static final int CHART_INSET_SIZE = 5;
	private static final int CHART_DURATION = 60;

	private static final int NODE_SELECTION_PANEL_WIDTH = 120;
	private static final Color UNSELECTED_NODE_COLOR = Color.BLACK;

	Map<String, StatsChart> charts;

	private MetricValueProcessor valueProcessor;
	private Set<String> liveNodes;
	private Map<String, Color> selectedNodes;
	private ColorAllocator<String> nodeColorAllocator;

	private JPanel nodeSelectionPanel, nodeSelectionContainer;
	private JPanel chartPanel;
	private JScrollPane chartPanelScroller;

	private final ItemListener nodeCheckBoxSelected = new ItemListener() {
		public void itemStateChanged(ItemEvent e) {
			JCheckBox checkBox = (JCheckBox)e.getSource();
			String node = checkBox.getText();
			boolean checked = e.getStateChange() == ItemEvent.SELECTED;
			if (checked) {
				Color color = nodeColorAllocator.get(node);
				selectedNodes.put(node, color);
				checkBox.setForeground(color);
			}
			else {
				selectedNodes.remove(node);
				checkBox.setForeground(UNSELECTED_NODE_COLOR);
				nodeColorAllocator.deallocate(node);
			}
		}
	};

	public StatsTab(Dashboard container) {
		super(container);
	}

	protected void create() {
		Border rightBorder = BorderFactory.createEmptyBorder(
			0, 0, 0, Dashboard.BORDER_SIZE);

		nodeSelectionContainer = new JPanel();
		nodeSelectionContainer.setLayout(new BoxLayout(nodeSelectionContainer, BoxLayout.PAGE_AXIS));

		JLabel nodeSelectionLabel = new JLabel("Nodes:");

		nodeSelectionPanel = new JPanel();
		nodeSelectionPanel.setLayout(new BoxLayout(nodeSelectionPanel, BoxLayout.PAGE_AXIS));
		nodeSelectionPanel.setBackground(Color.WHITE);

		nodeSelectionContainer.add(nodeSelectionLabel);
		nodeSelectionContainer.add(createScroller(nodeSelectionPanel, NODE_SELECTION_PANEL_WIDTH, -1));

		chartPanel = new JPanel();
		chartPanel.setLayout(new BoxLayout(chartPanel, BoxLayout.PAGE_AXIS));
		chartPanel.setBorder(rightBorder);

		charts = new LinkedHashMap<String, StatsChart>();

		selectedNodes = new HashMap<String, Color>();
		nodeColorAllocator = new ColorAllocator<String>();

		for (MonitorInfo monitor : MetricValueProcessor.MONITORS) {
			if ( monitor.isInternalMetric() ) continue;
			StatsChart chart = new StatsChart(CHART_DURATION, selectedNodes);
			chart.setInsets(CHART_INSET_SIZE);
			chart.setTitle(monitor.name + " (" + monitor.unit + ")");
			if (monitor.hardBounds) {
				chart.setValueRange(monitor.minBound, monitor.maxBound);
			}
			charts.put(monitor.name, chart);
		}

		boolean first = true;
		for (StatsChart chart : charts.values()) {
			chart.setBackground(Color.WHITE);
			chart.setBorder(BorderFactory.createEtchedBorder());
			chart.setPreferredSize(new Dimension(-1, CHART_HEIGHT));

			JLabel label = new JLabel(chart.getTitle() + ":");
			if (first) {
				first = false;
			}
			else {
				label.setBorder(BorderFactory.createEmptyBorder(
					Dashboard.BORDER_SIZE, 0, 0, 0));
			}

			chartPanel.add(label);
			chartPanel.add(chart);
		}

		chartPanelScroller = createScroller(chartPanel, BorderFactory.createEmptyBorder(), -1, -1);

		try {
			valueProcessor = new MetricValueProcessor(conn) {
				public void processHistoricalMetrics(int maxAge) throws SQLException {
					liveNodes = new TreeSet<String>();
					super.processHistoricalMetrics(maxAge);
				}

				protected void add(String node, int timestamp, Map<MonitorInfo, Integer> values) {
					liveNodes.add(node);
			    	for (Entry<MonitorInfo, Integer> current : values.entrySet()) {
			    		MonitorInfo monitor = current.getKey();
						if ( monitor.isInternalMetric() ) continue;
			    		Integer value = current.getValue();
			    		if (null != value) {
			    			charts.get(monitor.name).addStat(node, timestamp, value);
			    		}
			    	}
				}
			};
			valueProcessor.setAllMonitors();
//			valueProcessor.setTopologyGraph(TopologyGraph.getSingleton());
		}
		catch (SQLException e) {
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not query the database.");
			}
			return;
		}

		addUpdater(new Updater("StatsTab.updateCharts", INTERVAL) {
			protected boolean update() {
				return updateCharts();
			}
		});

		hideMessage();
		add(nodeSelectionContainer, BorderLayout.LINE_START);
		add(chartPanelScroller);
	}

	protected boolean updateCharts() {
		try {
			synchronized (nodeSelectionPanel) {
				valueProcessor.processHistoricalMetrics(CHART_DURATION);

				Component[] components = nodeSelectionPanel.getComponents();
				for (Component component : components) {
					String node = ((JCheckBox)component).getText();
					if (!liveNodes.contains(node)) {
						selectedNodes.remove(node);
						nodeColorAllocator.deallocate(node);
						nodeSelectionPanel.remove(component);
					}
				}

				int count = nodeSelectionPanel.getComponentCount();
				for (String node : liveNodes) {
					int position = 0;
					int comparison = -1;
					while (position < count && (comparison = node.compareTo(((JCheckBox)nodeSelectionPanel.getComponent(position)).getText())) > 0) {
						position++;
					}

					if (comparison != 0) {
						JCheckBox checkBox = new JCheckBox(node);
						checkBox.setContentAreaFilled(false);
						checkBox.addItemListener(nodeCheckBoxSelected);
						nodeSelectionPanel.add(checkBox, position);
						count++;

						if (node.equals(container.getLocalNodeID())) {
							Color color = nodeColorAllocator.get(node);
							selectedNodes.put(node, color);
							checkBox.setForeground(color);
							checkBox.setSelected(true);
						}
						else {
							checkBox.setForeground(UNSELECTED_NODE_COLOR);
							checkBox.setSelected(false);
						}
					}
				}
			}

			validate();
			repaint();
		}
		catch (SQLException e) {
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not query the database.");
			}

			return false;
		}

		return true;
	}

	protected void destroy() {
		if (null != nodeSelectionPanel) {
			synchronized (nodeSelectionPanel) {
				remove(nodeSelectionContainer);
				nodeSelectionContainer = null;
				nodeSelectionPanel = null;
				liveNodes = null;
			}
		}

		if (null != chartPanel) {
			remove(chartPanelScroller);
			chartPanelScroller = null;
			chartPanel = null;
			charts = null;
		}
	}
}
