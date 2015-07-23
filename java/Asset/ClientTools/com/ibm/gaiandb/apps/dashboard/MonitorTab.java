/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.logging.Logger;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

public class MonitorTab extends UpdatingTab {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = -4540409245533811759L;

	private static final Logger LOGGER = Logger.getLogger(MonitorTab.class.getName());

	private static final int INTERVAL = 1000;

	private static final int MAX_AGE = 5;

	private static final String[] COLUMN_NAMES = 
		new String[MetricValueProcessor.MONITORS.length-MetricValueProcessor.internalMetrics.length];

	private MetricValueProcessor valueProcessor;
	private List<Node> nodes;
//	private SortedSet<Node> nodes;
//	private Set<String> liveNodes;
	
	private String sortColumn = null;
	private boolean reverseSortOrder = false;

	private JPanel scrollerPanel;
	private JPanel monitorPanel;

	public MonitorTab(Dashboard container) {

		super(container, new GridBagLayout());
//		super(container);

		int i = 0;
		for (MonitorInfo monitor : MetricValueProcessor.MONITORS) {
			if ( monitor.isInternalMetric() ) continue;
			COLUMN_NAMES[i] = monitor.shortName; // + ")";
			i++;
		}
	}
	
	private class Node implements Comparable<Node> {
		public final String name;
		public Integer sortValue = null;
		public Node( String name ) { this.name = name; }
		public boolean equals(Object other) { return other instanceof Node && name.equals(((Node) other).name); }
		public int compareTo(Node other) {
			return null == sortValue ? other.name.compareTo(name) : sortValue.compareTo(other.sortValue);
		}
		public String toString() { return name; }
	}

	protected void create() {
		nodes = new ArrayList<Node>(); //.indexOf(object);

		try {
			valueProcessor = new MetricValueProcessor(conn) {
				boolean isNewProcessedDataSet = true;

				public void processLatestMetrics(int maxAge) throws SQLException {
					isNewProcessedDataSet = true;
					super.processLatestMetrics(maxAge);
				}

				protected void add(String nodeName, int timestamp, Map<MonitorInfo, Integer> values) {
					
					if ( isNewProcessedDataSet ) {
						
//						System.out.println("Removing nodes " + nodes + " not in list: " + liveNodes);
						
						Component[] components = monitorPanel.getComponents();
						ListIterator<Node> iterator = nodes.listIterator();
						int idx = 0;
						while (iterator.hasNext()) {
							iterator.next(); idx++;
							int index = idx * (COLUMN_NAMES.length + 1);
							for (int j = 0; j < COLUMN_NAMES.length + 1; j++)
								monitorPanel.remove(components[index + j]);
							
							iterator.remove();
						}
						nodes.clear();
					}
					
					ListIterator<Node> iterator = nodes.listIterator();
					int i = nodes.size();
					Node newNode = new Node(nodeName);
					if ( null != sortColumn && !sortColumn.equals("NODE") )
						for (MonitorInfo monitor : MetricValueProcessor.MONITORS) {
							if ( sortColumn.startsWith( monitor.shortName ) ) { //.equalsIgnoreCase(sortColumn) ) {
								Integer v = values.get(monitor);
								newNode.sortValue = null == v ? 0 : v;
								break;
							}
						}
					
					while (iterator.hasNext()) {
						int colComparison = iterator.next().compareTo(newNode);
						if ( reverseSortOrder ? colComparison > 0 : colComparison < 0 ) {
							iterator.previous();
							i = iterator.nextIndex();
							break;
						}
					}
					iterator.add(newNode);
					
//					System.out.println("Adding values from index i = " + i);

					i = (i + 1) * (COLUMN_NAMES.length + 1);

					monitorPanel.add(new JLabel(nodeName), i);
					for (MonitorInfo monitor : MetricValueProcessor.MONITORS) {
						if ( monitor.isInternalMetric() ) continue;
						i++;
						Integer value = values.get(monitor);
						if (null != value) {
							monitorPanel.add(new JLabel(value.toString()+ " " + monitor.unit, SwingConstants.RIGHT), i); // + " " + monitor.unit
						}
						else {
							monitorPanel.add(new JLabel("X", SwingConstants.RIGHT), i);
						}
					}
					
					isNewProcessedDataSet = false;
				}
			};
			valueProcessor.setAllMonitors();
		}
		catch (SQLException e) {
			LOGGER.severe(unravelMessages(e));
			destroy();
			showError("Could not query the database.");
			return;
		}

		monitorPanel = new JPanel(
			new GridLayout(-1, COLUMN_NAMES.length + 1, Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE));

		// Add the headers.

		JLabel jl = new JLabel("NODE");
		addSortableHeaderLabel(jl);
		for (String columnName : COLUMN_NAMES) {
			jl = new JLabel(columnName, SwingConstants.RIGHT);
			addSortableHeaderLabel(jl);
		}

		addUpdater(new Updater("MonitorTab.updateMonitors", INTERVAL) {
			protected boolean update() {
				return updateMonitors();
			}
		});

		hideMessage();
		
		GridBagConstraints componentConstraints = new GridBagConstraints();
		componentConstraints.anchor = GridBagConstraints.NORTHWEST;
		componentConstraints.fill = GridBagConstraints.BOTH;
		componentConstraints.weightx = 1;
		componentConstraints.weighty = 1;
//		componentConstraints.insets = new Insets(0, Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE);
		
//		monitorPanel.add(new JPanel(new GridBagLayout()), componentConstraints);
		
		scrollerPanel = new JPanel(new GridBagLayout());
		scrollerPanel.add(createScroller(monitorPanel, -1, -1), componentConstraints);

		componentConstraints.weighty = 0.25;
		add(scrollerPanel, componentConstraints);
		
		componentConstraints.gridx = 0;
		componentConstraints.gridy = 1;
		componentConstraints.weighty = 1;		
		add(new JPanel(new GridBagLayout()), componentConstraints);
	}
	
	private void addSortableHeaderLabel( JLabel jl ) {
		jl.addMouseListener(new MouseListener() {
			public void mouseClicked(MouseEvent e) {
				String clickedColumn = ((JLabel) e.getComponent()).getText();
				reverseSortOrder = clickedColumn.equals(sortColumn) ? !reverseSortOrder : false;
				sortColumn = clickedColumn;
			}
			public void mouseEntered(MouseEvent e) {} public void mouseExited(MouseEvent e) {}
			public void mousePressed(MouseEvent e) {} public void mouseReleased(MouseEvent e) {}				
		});
		monitorPanel.add(jl);
	}

	protected boolean updateMonitors() {
		synchronized (monitorPanel) {
						
			// Get the new values.
			try {
				valueProcessor.processLatestMetrics(MAX_AGE);
			}
			catch (SQLException e) {
				if (container.checkConnection()) {
					destroy();
					LOGGER.severe(unravelMessages(e));
					showError("Could not update the GaianDB monitors.");
					return false;
				}
			}
		}

		validate();
		repaint();

		return true;
	}

	protected void destroy() {
		if (null != monitorPanel) {
			synchronized (monitorPanel) {
				remove(scrollerPanel);
				scrollerPanel = null;
				monitorPanel = null;
			}
		}
	}
}
