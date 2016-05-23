/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.net.InetAddress;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
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
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSpinner;
import javax.swing.JTextField;
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

	private JComboBox<String> comboLogicalTables = new JComboBox<String>(new String[] {"Select target..."});
	
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

	private final ActionListener monitorSwitchActionListener = new ActionListener() {
		public void actionPerformed(ActionEvent event) {
			
			JComboBox<String> cb = (JComboBox<String>) event.getSource();
			if ( null == cb ) return;
			
//			String command = event.getActionCommand();
			String command = "" + cb.getSelectedIndex();
			
//			System.out.println("Action Performed on Monitor Selection Dropdown for Colour Coding Nodes in Topology Graph. Command = " + command);
			
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
	
	
	private final ActionListener discoveryModeButtonsActionListener = new ActionListener() {
		public void actionPerformed(ActionEvent event) {
			
			if ( null == subnetsChks ) return;
			
//			String command = event.getActionCommand();
//			System.out.println("Action Performed on Discovery options - Radio Button = " + command);
			
			synchronized (this) {
				if ( radioButtonDiscoveryOff.isSelected() ) {
					setGaianConfigProperty( DISCOVERY_IP, "" );
					multicastGroupPanel.setVisible(false);
					discoveryGatewaysTextField.setEnabled(false);
					for ( JCheckBox chk : subnetsChks ) chk.setEnabled(false);
					return;
				}

				boolean isMulticast = radioButtonDiscoveryMulticast.isSelected();
				String discoveryIP;
				
				if ( isMulticast ) {
					if ( 1 > multicastGroupTextField.getText().length() ) multicastGroupTextField.setText(DEFAULT_MULTICAST_GROUP);
					discoveryIP = multicastGroupTextField.getText();
					if ( discoveryIP.equals( DEFAULT_MULTICAST_GROUP ) ) discoveryIP = null; // equivalent to the default 
				}
				else {
//					System.out.println("broadcastTargets: " + broadcastTargets + ", allBroadcastOptions: " + allBroadcastOptions); 
					int numBroadcasts = broadcastTargets.size();
					
					if ( allBroadcastOptions.size() == numBroadcasts ) discoveryIP = BROADCAST_ALL;
					else if ( 1 > numBroadcasts ) discoveryIP = "UNICAST";
					else {
						discoveryIP = broadcastTargets.toString();
						discoveryIP = discoveryIP.substring(1, discoveryIP.length()-1).trim(); // remove wrapping square brackets
					}
				}
				
//				System.out.println("setGaianConfigProperty("+DISCOVERY_IP+","+discoveryIP+")");
				setGaianConfigProperty( DISCOVERY_IP, discoveryIP );

				multicastGroupPanel.setVisible( isMulticast );
				
				discoveryGatewaysTextField.setEnabled(true);
				
//				// Get some info to log to System.out
//				int i = 0;
//				String[] netInfoNeeded = new String[netInfos.size()];
//				for ( String[] ni : netInfos ) netInfoNeeded[i++] = ni[ isMulticast ? IDX_NETIP : IDX_NETBCAST ];
//				System.out.println("Updating checkboxes for new selection: " + (isMulticast ? multicastTargets : broadcastTargets)
//						+ " - using netInfos: " + Arrays.asList( netInfoNeeded ) );
				
				// Update text in checkboxes and also their selections - based on discovery mode and IPs selections
				// Note multicastTargets/broadcastTargets are the loaded user selections; netInfos holds the info associated with all the checkboxes
				int i = 0;
				Set<String> selectedNetworks = isMulticast ? multicastTargets : broadcastTargets;
				JCheckBox chkPrevious = null;
				for ( JCheckBox chk : subnetsChks ) { // Select new relevant checkboxes
					String[] netInfo = netInfos.get(i++);
					String netInfoIP = netInfo[ isMulticast ? IDX_NETIP : IDX_NETBCAST ];
					boolean isAvailable = null != netInfoIP;
					
					chk.setText( netInfo[ IDX_NETNAME ] + ": " + ( isAvailable ? netInfoIP : "unavailable" ) );
					
					boolean isVisible = null == chkPrevious || false == chk.getText().equals(chkPrevious.getText());
					chk.setVisible( isVisible );
					if ( false == isVisible ) continue;
					chkPrevious = chk;
					
					chk.setEnabled( isAvailable );
					chk.setSelected( isAvailable ? selectedNetworks.contains( netInfoIP ) : false );
				}
			}
		}
	};
	
	private synchronized void setGaianConfigProperty( final String dprop, final String dval ) {
		
		try {
			if ( null == setGaianConfigPropertyStatement || setGaianConfigPropertyStatement.isClosed() )
				setGaianConfigPropertyStatement = conn.prepareStatement( SET_GAIAN_CONFIG_PROPERTY );

			setGaianConfigPropertyStatement.setString(1, dprop);
			setGaianConfigPropertyStatement.setString(2, dval);
			setGaianConfigPropertyStatement.execute();
			
		} catch ( Exception e ) {
			System.out.println("Unable to setGaianConfigProperty(), Exception: " + e);
			e.printStackTrace();
		}
	}
	
	private static final String SET_GAIAN_CONFIG_PROPERTY = "call setconfigproperty(?, ?)";
	private static final String LIST_NETINFO = "call listnet('')";

	private static final String ALL = "ALL", BROADCAST_ALL = "BROADCAST_ALL";
	private static final String OffButtonLabel = "Off", MulticastButtonLabel = "Multicast", BroadcastButtonLabel = "Broadcast";

	private static final String DEFAULT_MULTICAST_GROUP = com.ibm.gaiandb.GaianNodeSeeker.DEFAULT_MULTICAST_GROUP_IP; // compile-time link only
	
	private static final String DISCOVERY_IP = "DISCOVERY_IP";
	private static final String MULTICAST_INTERFACES = "MULTICAST_INTERFACES";
	private static final String DISCOVERY_GATEWAYS = "DISCOVERY_GATEWAYS";
	private static final String ACCESS_CLUSTERS = "ACCESS_CLUSTERS";
	
	private static final String GET_DISCOVERY_PROPS = ""
			+ "select discovery_prop, discovery_val from ("
		    // Note the '=' char prefix in "=DISCOVERY_IP" or "=MULTICAST_INTERFACES" makes the function resolve a default value if the property is not set
			+ "	select '"+DISCOVERY_IP+"' DISCOVERY_PROP, getconfigproperty('="+DISCOVERY_IP+"') DISCOVERY_VAL from sysibm.sysdummy1 union all"
			+ "	select '"+MULTICAST_INTERFACES+"' DISCOVERY_PROP, getconfigproperty('="+MULTICAST_INTERFACES+"') DISCOVERY_VAL from sysibm.sysdummy1 union all"
			+ "	select '"+DISCOVERY_GATEWAYS+"' DISCOVERY_PROP, getconfigproperty('"+DISCOVERY_GATEWAYS+"') DISCOVERY_VAL from sysibm.sysdummy1 union all"
			+ "	select '"+ACCESS_CLUSTERS+"' DISCOVERY_PROP, getconfigproperty('"+ACCESS_CLUSTERS+"') DISCOVERY_VAL from sysibm.sysdummy1"
			+ ") sq"
	;
	
//	select discovery_prop, case when discovery_val is null then 'null' else discovery_val end discovery_val from (
//			select 'DISCOVERY_IP' DISCOVERY_PROP, getconfigproperty('=DISCOVERY_IP') DISCOVERY_VAL from sysibm.sysdummy1 union all
//			select 'MULTICAST_INTERFACES' DISCOVERY_PROP, getconfigproperty('=MULTICAST_INTERFACES') DISCOVERY_VAL from sysibm.sysdummy1 union all
//			select 'DISCOVERY_GATEWAYS' DISCOVERY_PROP, getconfigproperty('DISCOVERY_GATEWAYS') DISCOVERY_VAL from sysibm.sysdummy1 union all
//			select 'ACCESS_CLUSTERS' DISCOVERY_PROP, getconfigproperty('ACCESS_CLUSTERS') DISCOVERY_VAL from sysibm.sysdummy1
//		) sq
	
	private JRadioButton radioButtonDiscoveryOff = null, radioButtonDiscoveryBroadcast = null, radioButtonDiscoveryMulticast = null;
	
	private JPanel subnetsPanel = null;
	private JCheckBox[] subnetsChks = null;

	private JPanel multicastGroupPanel = null;
	private JTextField multicastGroupTextField = null;
	private JTextField discoveryGatewaysTextField = null;
	private JTextField clusterIDsTextField = null;
	
	private final int IDX_NETNAME = 0, IDX_NETIP = 1, IDX_NETBCAST = 2;
	
	private List<String[]> netInfos = null;
	private Set<String> netInfoSet = null;
	private Set<String> broadcastTargets = null, multicastTargets = null;
	private Set<String> allBroadcastOptions = new HashSet<String>();
	
	private PreparedStatement listNetInfoStatement = null, getDiscoveryConfigStatement = null, setGaianConfigPropertyStatement = null;
	
	/**
	 * Load any changes to discovery properties, based also on any changes in network interfaces info. 
	 * Note: If the GaianDB config is simultaneously updated by some other process, then user choices in the UI may be overwritten
	 *  
	 * @return true if check/load completed without errors - false otherwise.
	 */
	private synchronized boolean loadDiscoveryConfig() {
		
		try {
			// Check current list of ips and their associated broadcast ips + load any new ones.
			
			if ( null == listNetInfoStatement || listNetInfoStatement.isClosed() )
				listNetInfoStatement = conn.prepareStatement( LIST_NETINFO );
			
			Set<String> netInfoSetNew = new HashSet<String>();
			List<String[]> netInfosNew = new ArrayList<String[]>();

			// New set of selected target networks (for multicast or broadcast) 
			Set<String> broadcastTargetsNew = new HashSet<String>(), multicastTargetsNew = new HashSet<String>();
			
			ResultSet rs = listNetInfoStatement.executeQuery();
			
			while ( rs.next() ) {
				String ifaceName = rs.getString("INTERFACE").trim(), ifaceDesc = rs.getString("DESCRIPTION").trim(), 
						ipv4 = rs.getString("IPV4").trim(), ipv4Broadcast = rs.getString("BROADCAST");
				
				multicastTargetsNew.add(ipv4);
				if ( null != ipv4Broadcast ) { broadcastTargetsNew.add( ipv4Broadcast = ipv4Broadcast.trim() ); }
				
//				System.out.println("listNet entry: ifaceName: " + ifaceName + ", ipv4: " + ipv4 + ", broadcast: " + ipv4Broadcast);
				
				netInfoSetNew.add( ifaceName + " " + ipv4 + " " + ipv4Broadcast );
				netInfosNew.add( new String[] { ifaceName, ipv4, ipv4Broadcast } ); // IDX_NETNAME = 0, IDX_NETIP = 1, IDX_NETBCAST = 2
			}
			rs.close();
			
			allBroadcastOptions.clear(); allBroadcastOptions.addAll( broadcastTargetsNew );
			
//			System.out.println("netInfoSet: " + netInfoSet + " -> " + netInfoSetNew);
			
			boolean netInfoSetChanged = false == netInfoSetNew.equals( netInfoSet );
			
			if ( netInfoSetChanged ) {
				if (null!=netInfoSet) netInfoSet.clear(); netInfoSet = netInfoSetNew;
				if (null!=netInfos) netInfos.clear(); netInfos = netInfosNew;
			}
			
			// Check discovery config properties + load any changes
			
			if ( null == getDiscoveryConfigStatement || getDiscoveryConfigStatement.isClosed() )
				getDiscoveryConfigStatement = conn.prepareStatement( GET_DISCOVERY_PROPS );

			boolean isMulticastPropVal = false, isOff = false;
			
			rs = getDiscoveryConfigStatement.executeQuery();
			
			while ( rs.next() ) {
				
				String dprop = rs.getString(1).trim();
				String dval = rs.getString(2);
				
//				System.out.println("Discovery property entry dprop: " + dprop + ", dval: " + dval + ", null==dval ? " + (null==dval));
				
				if ( DISCOVERY_IP.equalsIgnoreCase(dprop) ) {
					
					if ( 1 > dval.length() ) isOff = true; // switch discovery off
					else if ( 0 > dval.indexOf(',') && -1 < dval.indexOf('.') &&
							InetAddress.getByName(dval).isMulticastAddress() ) isMulticastPropVal = true;
					
					if ( netInfoSetChanged || null == subnetsChks ) {
						int netInfoIdxForSubnetDescriptors = isMulticastPropVal ? IDX_NETIP : IDX_NETBCAST; // index of ipv4 value or broadcast value
						
//						System.out.println("Setting checkbox values with info from each netInfo[" + netInfoIdxForSubnetDescriptors + "]");
						
						// Set relevant info in subnets scroller box - either the ipv4 for Multicast or the ipv4Broadcast for Broadcast
						int i = 0; subnetsChks = new JCheckBox[netInfos.size()];
						for ( String[] netInfo : netInfos ) {
							String netInfoIP = netInfo[ netInfoIdxForSubnetDescriptors ];
							subnetsChks[i++] = new JCheckBox( netInfo[IDX_NETNAME] + ": " + ( null == netInfoIP ? "unavailable" : netInfoIP ) );
						}
						
						subnetsPanel.removeAll();
						JCheckBox chkPrevious = null;
						for ( JCheckBox chk : subnetsChks ) {
							chk.setVisible( null == chkPrevious || false == chk.getText().equals(chkPrevious.getText()) );
							
							subnetsPanel.add( chkPrevious = chk, cGoDown );

							chk.addActionListener( new ActionListener() {
								public void actionPerformed(ActionEvent e) {
									
//									System.out.println("CheckBox state changed for: " + ((JCheckBox) e.getSource()).getText());

									// radio button selected must be either multicast or broadcast - NOT OFF, because check boxes would be disabled.
									final boolean isMulticast = radioButtonDiscoveryMulticast.isSelected();
									
									int i = 0; List<String> targetIPs = new ArrayList<String>();
									for ( JCheckBox chk : subnetsChks ) {
										if ( chk.isVisible() && chk.isEnabled() && chk.isSelected() )
											targetIPs.add( netInfos.get(i)[ isMulticast ? IDX_NETIP : IDX_NETBCAST ] );
										i++;
									}
									
									if ( false == isMulticast && targetIPs.isEmpty() ) {
										setGaianConfigProperty( DISCOVERY_IP, "UNICAST" ); // Only discovery gateways can discovery
										return;
									}
									
//									System.out.println("targetIPs: " + targetIPs + ", allBroadcastOptions: " + allBroadcastOptions);
									
									final boolean isAllSelected = targetIPs.size() == ( isMulticast ? netInfos : allBroadcastOptions ).size();
									
									if ( isAllSelected )
										setGaianConfigProperty( isMulticast ? MULTICAST_INTERFACES : DISCOVERY_IP, isMulticast ? ALL : BROADCAST_ALL );
									else {
										String targetsCSV = targetIPs.toString();
										targetsCSV = targetsCSV.substring(1, targetsCSV.length()-1).trim(); // remove wrapping square brackets
										setGaianConfigProperty( isMulticast ? MULTICAST_INTERFACES : DISCOVERY_IP, targetsCSV );
									}
								}
							});
						}
						
						// Empty panel to keep the checkboxes at the top
						subnetsPanel.add(new JPanel(), cExpandDown);
					}
					
					if ( isOff ) continue;
					if ( isMulticastPropVal ) {
						// Overwrite GroupIP text field - but only if the value is different and the focus is not on it.
						if ( false == multicastGroupTextField.getText().equals(dval) && false == multicastGroupTextField.isFocusOwner() )
							multicastGroupTextField.setText( dval );
						
					} else if ( false == BROADCAST_ALL.equals(dval) )
						broadcastTargetsNew.retainAll( Arrays.asList(splitByCommas(dval)) ); // remove elements that aren't listed
				}
				else if ( MULTICAST_INTERFACES.equalsIgnoreCase(dprop) && false == ALL.equals(dval) )
						multicastTargetsNew.retainAll( Arrays.asList(splitByCommas(dval)) ); // remove elements that aren't listed
				
				else if ( DISCOVERY_GATEWAYS.equalsIgnoreCase(dprop) ) {
					if ( false == discoveryGatewaysTextField.getText().equals(dval) && false == discoveryGatewaysTextField.isFocusOwner() )
						discoveryGatewaysTextField.setText( null == dval ? "" : dval );
				}
				else if ( ACCESS_CLUSTERS.equalsIgnoreCase(dprop) )
					if ( false == clusterIDsTextField.getText().equals(dval) && false == clusterIDsTextField.isFocusOwner() )
						clusterIDsTextField.setText( null == dval ? "" : dval );
			}
			rs.close();
			
			boolean isCheckBoxSelectionsChanged = false;

			// Only update the target (i.e. selected) multicast IPs if the selection changed
			if ( false == multicastTargetsNew.equals( multicastTargets ) ) {
				isCheckBoxSelectionsChanged = true;
				if (null!=multicastTargets) multicastTargets.clear();
				multicastTargets = multicastTargetsNew;
			}
			
			// Only update the target (i.e. selected) broadcast IPs if the selection changed
			if ( false == broadcastTargetsNew.equals( broadcastTargets ) ) {
				isCheckBoxSelectionsChanged = true;
				if (null!=broadcastTargets) broadcastTargets.clear();
				broadcastTargets = broadcastTargetsNew;
			}

			// Update netInfo list and selections as appropriate - i.e. if discovery mode changed or if target nets changed
			
			final JRadioButton radioButtonToBeSelected = isOff ? radioButtonDiscoveryOff : 
				isMulticastPropVal ? radioButtonDiscoveryMulticast : radioButtonDiscoveryBroadcast;
			
			if ( false == radioButtonToBeSelected.isSelected() || isCheckBoxSelectionsChanged ) {
				radioButtonToBeSelected.doClick(); // Refresh target networks selections
				monitorPanel.validate(); // Validate layout - this clears issue whereby a QueryTab re-connect makes the checkboxes disappear
			}
			
		} catch ( Exception e ) {
			System.out.println("Unable to loadDiscoveryConfig(), Exception: " + e);
			e.printStackTrace();
			
			return false;
		}
		
		return true;
	}
	
	private static String[] splitByCommas( String list ) {
    	return splitByTrimmedDelimiter( list, ',' );
    }
    
	private static String[] splitByTrimmedDelimiter( String list, char delimiter ) {
		if ( null == list || 0 == list.length() ) return new String[0];
		return list.trim().split("[\\s]*" + delimiter + "[\\s]*");
	}
	
	
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
	
	private GridBagConstraints cDefault = null, cGoDown = null, cExpandDown = null; 

	protected void create() {
		Border topBorderPadding = BorderFactory.createEmptyBorder(Dashboard.BORDER_SIZE, 0, 0, 0);
		Border bottomBorderPadding = BorderFactory.createEmptyBorder(0, 0, Dashboard.BORDER_SIZE, 0);
		Border topAndBottomBorderPadding = BorderFactory.createEmptyBorder(Dashboard.BORDER_SIZE, 0, Dashboard.BORDER_SIZE, 0);
		
		cDefault = new GridBagConstraints();
		cDefault.weightx = 1;
		cDefault.fill = GridBagConstraints.BOTH;
		
		cGoDown = (GridBagConstraints) cDefault.clone();
		cGoDown.gridx = 0;
		
		cExpandDown = (GridBagConstraints) cGoDown.clone();
		cExpandDown.weighty = 1;

		monitorPanel = new JPanel(new GridBagLayout());
		
		//////////////////////
		// Show Nodes checkbox
		//////////////////////

		JCheckBox showNames = new JCheckBox("Show Node IDs", true);
		
		showNames.setBorder(bottomBorderPadding);
		showNames.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent e) {
				if (null != graph) {
					graph.setNodeRenderer(e.getStateChange() == ItemEvent.SELECTED);
				}
			}
		});
		monitorPanel.add(showNames, cDefault);
		
		///////////////////////////////////////////////////////
		// Coloring criteria drop-down list and color range key
		///////////////////////////////////////////////////////
		
		monitorPanel.add(new JLabel("Node Coloring Scheme"), cGoDown);

		JComboBox<String> metricsDropDown = new JComboBox<String>();
		for (int i = 0; i < MetricValueProcessor.MONITORS.length; i++) {

			metricsDropDown.addItem( MetricValueProcessor.MONITORS[i].name );
			metricsDropDown.addActionListener(monitorSwitchActionListener);
		}

		monitorPanel.add(metricsDropDown, cGoDown);
		
//		ButtonGroup monitorGroup = new ButtonGroup();
////		JRadioButton none = new JRadioButton(LABEL_NONE);
////		none.setActionCommand(null);
////		none.addActionListener(MONITOR_SWITCH);
////		none.setSelected(true);
////		monitorGroup.add(none);
////		monitorPanel.add(none, cGoDown);
//		for (int i = 0; i < MetricValueProcessor.MONITORS.length; i++) {
//			JRadioButton monitorSelector = new JRadioButton(MetricValueProcessor.MONITORS[i].name);
//			monitorSelector.addActionListener(monitorSwitchActionListener);
//			monitorSelector.setActionCommand(Integer.toString(i));
//			monitorGroup.add(monitorSelector);
//			monitorPanel.add(monitorSelector, cGoDown);
//		}
		
		colorRangePanel = new JPanel(new GridBagLayout());
		colorRangePanel.setBorder(topBorderPadding);
		for ( int i=0; i<NUM_RANGE_COLORS; i++ ) {
			JLabel jl = new JLabel(EMPTY_COLOR_FIELD, SwingConstants.CENTER);
			Font f = jl.getFont();
			jl.setFont( f.deriveFont(f.getStyle()^Font.BOLD) );
			jl.setOpaque(true);
			jl.setForeground(Color.WHITE);
			colorRangePanel.add(jl, cDefault);
		}
		
		monitorPanel.add(colorRangePanel, cGoDown);
		
		////////////////////////////////
		// Logical tables drop down list
		////////////////////////////////
		
		comboLogicalTables.addActionListener( new java.awt.event.ActionListener() {
		    
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboTableActionPerformed(evt);
			}

			private void comboTableActionPerformed(ActionEvent evt) {
				
				comboLogicalTables.transferFocus(); // allows us to update the list more readily if necessary

				JComboBox<String> cb = (JComboBox<String>)evt.getSource();
				if ( null == cb ) return;
				tableToQuery = (String)cb.getSelectedItem();

//				System.out.println("comboTableActionPerformed event selection " + tableToQuery);				
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
		pathTablePanel.setBorder(topAndBottomBorderPadding);
		pathTablePanel.add( new JLabel("Show Logical Table Paths"), cGoDown); // for Path Visualisation") , cGoDown);
		pathTablePanel.add(comboLogicalTables, cGoDown);
		
		monitorPanel.add(pathTablePanel, cGoDown); 	// = Label and drop-down for choosing a logical table for path visualisation
		
		monitorPanel.add(new JSeparator(SwingConstants.HORIZONTAL), cGoDown);
		
		//////////////
		// Cluster IDs
		//////////////
		
		monitorPanel.add(new JLabel("Cluster IDs (memberships)"), cGoDown);
		clusterIDsTextField = new JTextField();
		monitorPanel.add(clusterIDsTextField, cGoDown);
		monitorPanel.add(new JSeparator(SwingConstants.HORIZONTAL), cGoDown);
		
		clusterIDsTextField.addActionListener( new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
		    	setGaianConfigProperty( ACCESS_CLUSTERS, clusterIDsTextField.getText() );      
		    }
		});
		
		//////////////////
		// Discovery Panel
		//////////////////
		
		monitorPanel.add(new JLabel("Peer Discovery"), cGoDown);
		
		ButtonGroup discoveryIPOptionsGroup = new ButtonGroup();
		radioButtonDiscoveryOff = new JRadioButton( OffButtonLabel, false );
		radioButtonDiscoveryBroadcast = new JRadioButton( BroadcastButtonLabel, false );
		radioButtonDiscoveryMulticast = new JRadioButton( MulticastButtonLabel, false );
		for ( JRadioButton radioButton : new JRadioButton[] { radioButtonDiscoveryOff, radioButtonDiscoveryBroadcast, radioButtonDiscoveryMulticast } ) {
//			radioButton.addActionListener(discoveryOptionActionListener);
			monitorPanel.add( radioButton, cGoDown ); // = Protocol buttons: Off / Broadcast / Multicast
			radioButton.addActionListener(discoveryModeButtonsActionListener);
			radioButton.setActionCommand( radioButton.getText() );
			discoveryIPOptionsGroup.add( radioButton );
		}

		// Multicast group label and text field - should hide away or be grayed out when broadcast is selected
		multicastGroupPanel = new JPanel(new GridBagLayout());
		multicastGroupPanel.add( new JLabel("Group IP") );
		multicastGroupTextField = new JTextField();
		multicastGroupTextField.setDocument( Field.getValidatedDocument("[0-9\\.]*") ); //"[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+") ); //"[0-9\\.]*") );
		multicastGroupPanel.add( multicastGroupTextField, cDefault );
		
		monitorPanel.add( multicastGroupPanel, cGoDown ); // = Multicast Group IP label + input text field
		
		multicastGroupTextField.addActionListener( new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
				String discoveryIP = multicastGroupTextField.getText().trim();
				if ( 1 > discoveryIP.length() || discoveryIP.equals( DEFAULT_MULTICAST_GROUP ) ) discoveryIP = null; // equivalent to the default 
		    	setGaianConfigProperty( DISCOVERY_IP, discoveryIP );      
		    }
		});

		JLabel targetNetworksLabel = new JLabel("Target Networks");
		targetNetworksLabel.setBorder(topBorderPadding);
		monitorPanel.add( targetNetworksLabel, cGoDown);
		
		int baseSize = getFont().getSize();
		JScrollPane subnetsScrollPane = null;
		
		// Position the JPanel that will hold the JCheckBox values for the netInfo IPs
		monitorPanel.add( subnetsScrollPane = createScroller( subnetsPanel = new JPanel(new GridBagLayout()), -1, baseSize * 8 ), cGoDown);
		subnetsScrollPane.setMinimumSize(new Dimension(-1, baseSize * 8)); // limit compression on this Pane when the window shrinks
		
		JLabel label = new JLabel("Gateway IPs");
		label.setBorder(topBorderPadding);
		monitorPanel.add(label, cGoDown);
		discoveryGatewaysTextField = new JTextField();
		discoveryGatewaysTextField.setDocument( Field.getValidatedDocument("[0-9\\.,]*") ); //"[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+") ); //"[0-9\\.]*") );
		monitorPanel.add(discoveryGatewaysTextField, cGoDown);
		
		discoveryGatewaysTextField.addActionListener( new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
		    	setGaianConfigProperty( DISCOVERY_GATEWAYS, discoveryGatewaysTextField.getText() );      
		    }
		});
		
//		discoveryGatewaysPanel.addFocusListener(new FocusListener() {
//			public void focusGained(FocusEvent e) {}
//			public void focusLost(FocusEvent e) {
//				JTextField tBox = (JTextField)e.getSource();
//				if ( 0 == tBox.getText().length() ) tBox.setText("1");
//			}
//		});
		
//		JButton reDiscover = new JButton("Re-Discover Peers");
////		refresh.setBorder(topBorder);
//		reDiscover.addActionListener(new ActionListener() {
//			public void actionPerformed(ActionEvent event) {
////				try { updateLogicalTablesComboBox(); } catch ( SQLException e ) {}
//				updateGraph();
//				updateValues();
//			}
//		});
//		
//		monitorPanel.add(reDiscover, cGoDown);
		
		
		//////////////////
		// Update interval
		//////////////////
		
		monitorPanel.add(new JSeparator(SwingConstants.HORIZONTAL), cGoDown);

		JLabel intervalLabel = new JLabel("Update Interval (s)");
//		intervalLabel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, Dashboard.BORDER_SIZE / 2));
		JSpinner intervalSpinner = new JSpinner(new SpinnerNumberModel(GRAPH_INTERVAL / 1000, 1, 60, 1));
		intervalLabel.setBorder(BorderFactory.createEmptyBorder(0, Dashboard.BORDER_SIZE / 2, 0, 0));
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
//		intervalPanel.setBorder(topBorder1);
		intervalPanel.add(intervalLabel);
		intervalPanel.add(intervalSpinner);
		
		JButton refresh = new JButton("Update Now");
//		refresh.setBorder(topBorder);
		refresh.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
//				try { updateLogicalTablesComboBox(); } catch ( SQLException e ) {}
				updateGraph();
				updateValues();
				loadDiscoveryConfig();
			}
		});
//		refresh.setBorder(topBorderPadding);
		intervalPanel.add(refresh, cGoDown);
		
		monitorPanel.add(intervalPanel, cGoDown);

		//////////////////////////////////////////////////////
		// Empty panel to keep the monitor controls at the top
		//////////////////////////////////////////////////////
		monitorPanel.add(new JPanel(), cExpandDown);
		
		////////////////

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
		
		addUpdater(new Updater("TopologyTab.updateDiscoveryConfig", GRAPH_INTERVAL) {
			protected boolean update() {
				return loadDiscoveryConfig();
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
		
		metricsDropDown.setSelectedIndex(0); // activate the top selection "Connectivity" (default color coding scheme)
		
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
//		cGoDown.gridx = 0; cGoDown.gridy = 0; cGoDown.weightx = 1; cGoDown.weighty = 1;
//		cGoDown.fill = GridBagConstraints.BOTH;	
//		graphPanel.add(graph, cGoDown);
//
//		JPanel tgtPanel = new JPanel(new GridBagLayout());
//		cGoDown.weightx = 0; cGoDown.weighty = 0;
//		cGoDown.fill = GridBagConstraints.NONE;
//		tgtPanel.add(new JLabel("Targetted Nodes: "), cGoDown);
//		
//		JButton clrButton = new JButton("Clear");
//		cGoDown.gridx = 2; cGoDown.gridy = 0;
//		tgtPanel.add(clrButton, cGoDown);
//		
//		JTextArea qryInput = new JTextArea();
//		cGoDown.gridx = 1; cGoDown.gridy = 0; cGoDown.weightx = 1;
//		cGoDown.fill = GridBagConstraints.HORIZONTAL;
//		tgtPanel.add(createScroller(qryInput, -1, getFont().getSize()*4), cGoDown);
//		
//		tgtPanel.setBorder(topBorder);
//		
//		cGoDown.gridx = 0; cGoDown.gridy = 1;
//		graphPanel.add(tgtPanel, cGoDown);
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
		subnetsChks = null;
		if ( null != netInfos ) netInfos.clear(); netInfos = null;
		if ( null != netInfoSet ) netInfoSet.clear(); netInfoSet = null;
		if ( null != allBroadcastOptions ) allBroadcastOptions.clear(); // don't set this one to null
		if ( null != broadcastTargets ) broadcastTargets.clear(); broadcastTargets = null;
		if ( null != multicastTargets ) multicastTargets.clear(); multicastTargets = null;
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
