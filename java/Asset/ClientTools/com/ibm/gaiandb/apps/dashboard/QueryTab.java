/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.awt.Color;
import java.awt.Component;
import java.awt.Desktop;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import javax.swing.AbstractCellEditor;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.apps.SecurityClientAgent;
import com.ibm.gaiandb.diags.GDBMessages;

public class QueryTab extends Tab {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 3983956371547599148L;
	
	private static final int INPUT_TAB_SIZE = 4;

	private static final int COLUMN_PADDING = 10;

	static final int WARNINGS_COLUMN = 4;
	static final String WARNINGS_SQL = "call.*listwarnings\\(\\)";
	
	private Connection conn;
	private Statement stmt;

	final Vector<String> sqlHistory = new Vector<String>();
	private int sqlHistoryIndex = 0;

	private final JTextArea input;
	private final JTextField reIterateBox;
	private final JButton back;
	private final JButton forward;
	private final JButton submit;
	private final JButton lookupError;
	private final JButton reconnectButton;
	private final JTable results;
	private final JLabel queryInfo;
	private final JList errors;
	
	private final JComboBox apiCallsDropDown; //, historyDropDown;

	private static final String SHORTCUTS_LABEL = "Shortcuts and History...";
	private static final String EXPAND_AS_DIRECTED_SUBQ = 
		"<EXPAND CURRENT QUERY AS DIRECTED SUB-QUERY ON SELECTED NODES IN NETWORK TOPOLOGY>";
	private static final String EXPAND_AS_ADDQUERY_CALL = 
		"<EXPAND CURRENT QUERY AS CALL TO ADDQUERY STORED PROCEDURE>";

	private static final String[] APIS_LIST = new String[] {
			"select * from LT0                      -- Query sample logical table LT0",
			"select * from LT0_P                  -- Query sample logical table LT0 with provenance",
			"select * from LT0_0                  -- Query LT0 at depth 0, i.e. federating local data sources only",
			"call listnodes()                           -- List GaianDB nodes in network",	
			"call listrdbc()                             -- List JDBC Connections", //. Specify '*' for all nodes or null for local config only",
			"call listlts()                                 -- List Logical Tables", //. Specify '*' for all nodes or null for local config only",
			"call listltmatches()                     -- List logical table definition matches",
			"call listds()                                 -- List Data Sources", //. Specify '*' for all nodes or null for local config only",
			"call listwarnings()                       -- List latest warnings", //. Specify '*' for all nodes or null for local config only",
			"call listconfig()                           -- List GaianDB Config Properties", //. Specify '*' for all nodes or null for local config only",
			"call listspfs()                               -- List Stored Procedures and Functions",
//			"call listapi()                                 -- List GaianDB API",
			"call listnet('')                               -- Show net interface info for ips having the given prefix",
			"call listflood()                             -- Show propagation route of an empty query",
			"call listqueries(-1)                       -- Show all existing queries on nodes out to a given depth (use -1 to query all nodes)", //. Specify -1 for all nodes
			"call listexplain('<sql>')               -- Show route and cumulative returned row counts for a given query",
			"call logtail(null, 100)                   -- Show last 100 lines gaiandb.log. The 1st arg can be a nodelist or '*' for all nodes",
			"call setloglevel('NONE')               -- Disable Server Logging",
			"call setloglevel('LESS')                 -- Performance Logging",
			"call setloglevel('MORE')               -- Verbose Logging",
			"call addgateway('<ipaddress>')                                             -- Register a discover gateway IP, i.e. use it as a relay node for discovery",
			"call removegateway('<ipaddress>')                                       -- De-register a discovery gateway IP",
			"call cancelquery('<queryID>')                                               -- Cancel execution of a query (use listqueries() to resolve a queryID)",
			"call gconnect('<cid>', '<ipaddress>')                                     -- Create a 1-way connection (naming its ID) to a GaianDB running at the given IP",
			"call gdisconnect('<cid>')                                                         -- Disconnect the 1-way connection designated by the connection ID",
			"call setrdbc('<cid>', '<driver>', '<url>', '<usr>', '<pwd>')   -- Register a new connection id (cid) with the given JDBC connection details",
			"call removerdbc('<cid>')                                                         -- Remove the given connection ID, clearing all associated JDBC connections",
			"call setltforrdbtable('<ltname>', '<cid>', '<tableExpr>')       -- Set logical table based on connection id + physical table expression (may include joins + where clause...)",
			"call setltforfile('<ltname>', '<filepath>')                                 -- Set logical table based on a text file",
			"call setltforexcel('<ltname>', '<spreadsheet_parameters>')  -- Set logical table based on an excel spreadsheet",
			"call setltfornode('<ltname>', '<nodeid>')                                -- Set logical table based on its definition on another node",
			"call removelt('<ltname>')                                                         -- Remove the given logical table and all dependant data sources",
			"call setminconnections(<numconnections>)                            -- Set Number of Sought After Connections",
//			"call setdiscoveryhosts('<host1,...>')                                      -- Set List of Hostnames considered for node discovery", -- now deprecated
			"call setdiscoveryip('<ipaddress>')                                           -- Set IP broadcast address or multicast group for discovery",
			"call setconfigproperty('<key>', '<value>')                             -- Set a specifc registry property (admin only)",
//			"select gkill() ok from sysibm.sysdummy1     -- Kill the local node",
			"VALUES CURRENT_USER                       -- Display current user",
	};
	
	private final DefaultTableModel resultsModel;
	private final DefaultListModel errorsModel;
	
	public QueryTab(final Dashboard container) {
		super(container, new GridBagLayout());

		GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.anchor = GridBagConstraints.LINE_START;
		labelConstraints.fill = GridBagConstraints.HORIZONTAL;
		labelConstraints.gridwidth = 3;
		labelConstraints.gridx = 0;
		labelConstraints.insets = new Insets(
			Dashboard.BORDER_SIZE,
			Dashboard.BORDER_SIZE,
			0,
			Dashboard.BORDER_SIZE);
		labelConstraints.weightx = 1;

		GridBagConstraints componentConstraints = new GridBagConstraints();
		componentConstraints.anchor = GridBagConstraints.LINE_START;
		componentConstraints.fill = GridBagConstraints.BOTH;
		componentConstraints.gridwidth = 3;
		componentConstraints.gridx = 0;
		componentConstraints.insets = new Insets(
			0,
			Dashboard.BORDER_SIZE,
			Dashboard.BORDER_SIZE,
			Dashboard.BORDER_SIZE);
		componentConstraints.weightx = 1;

		input = new JTextArea();
		input.setFont(new Font("Monospaced", Font.PLAIN, input.getFont().getSize()));
		input.setTabSize(INPUT_TAB_SIZE);
		input.addKeyListener(new KeyListener() {
			public void keyTyped(KeyEvent e) {
				if (e.isControlDown() && e.getKeyChar() == '\n') {
					submitQuery(input.getText());
				}
			}

			public void keyReleased(KeyEvent e) {}
			public void keyPressed(KeyEvent e) {}
		});

		// Look for icons - if they dont exist use text
		if ( 0 < BACK_ICON.getIconHeight() ) {
			back = new JButton(BACK_ICON);
			back.setDisabledIcon(BACK_DISABLED_ICON);
			forward = new JButton(FORWARD_ICON);
			forward.setDisabledIcon(FORWARD_DISABLED_ICON);
		} else {
			back = new JButton("<=");
			forward = new JButton("=>");		
		}
		
		back.setEnabled(false);
		back.setMargin(new Insets(1, 1, 1, 1));
		back.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				back();
			}
		});

		forward.setEnabled(false);
		forward.setMargin(new Insets(1, 1, 1, 1));
		forward.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				forward();
			}
		});

		submit = new JButton("Run");
		submit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				if ( null != executingSQL ) { // Cancelling/Stopping query
					lastCancelTime = System.currentTimeMillis();
					submit.setText("Run");
					queryInfo.setText("Cancelled at " + new SimpleDateFormat("HH:mm:ss").format(new Date()) + ". ");
					executingSQL = null;
					if ( null != stmt ) try { stmt.close();	stmt = null; } catch ( Exception ex ) { setErrorsWithCode(GDBMessages.CLIENT_STMT_CLOSE_ERROR, ex); }
					return;
				}
				
				submitQuery(input.getText());
			}
		});

		resultsModel = new DefaultTableModel();
		results = new JTable(resultsModel);
		results.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		results.setColumnSelectionAllowed(true);
//		results.setSelectionMode(ListSelectionModel.SINGLE_SELECTION); // single cell selection
//		results.getTableHeader().setReorderingAllowed(true); // column 'drag accross' re-ordering
//		results.getTableHeader().addMouseListener(  ) ); // attempt to allow sorting on columns
		
		queryInfo = new JLabel("");

		errorsModel = new DefaultListModel();
		errors = new JList(errorsModel);
		
		lookupError = new JButton("Lookup");
		lookupError.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String code = ((String) errorsModel.get(0)).split(" ")[0];
				openErrorDoc(code);
			}
		});
		
		reconnectButton = new JButton("Reconnect");
		reconnectButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				container.reconnect();
			}
		});
		
		reconnectButton.setEnabled(false);
		
		setErrors();
		
		Vector<String> v = new Vector<String>( Arrays.asList( SHORTCUTS_LABEL, "" ) );
		v.addAll( Arrays.asList(APIS_LIST) );
		v.add( "" );
		v.add(EXPAND_AS_DIRECTED_SUBQ);
		v.add(EXPAND_AS_ADDQUERY_CALL);
		v.add("_____________________________________________________________________________________");
		v.add("");
		
		apiCallsDropDown = new JComboBox( v );
		apiCallsDropDown.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JComboBox cb = (JComboBox)e.getSource();
			    String sql = ((String)cb.getSelectedItem()).trim();
			    if ( 0<sql.length() && !sql.equals(SHORTCUTS_LABEL) && !sql.startsWith("_") ) {
			    				    	
			    	if ( cb.getSelectedIndex() < APIS_LIST.length+2 ) {
			    		int idx = sql.indexOf("--");
			    		if ( -1 < idx ) sql = sql.substring(0, idx);
			    		sql = sql.trim();
			    		
			    	} else if ( sql.equals(EXPAND_AS_DIRECTED_SUBQ) ) {			    					    		
			    		sql = "\t" + input.getText().replaceAll("'", "''").replaceAll("\n", "\n\t");
			    		sql = "SELECT * FROM NEW com.ibm.db2j.GaianQuery('\n" + sql + 
			    			"\n', 'with_provenance') GQ";
			    		Set<String> selectedNodes = TopologyGraph.getSingleton().getSelectedNodes();
			    		if ( null != selectedNodes && !selectedNodes.isEmpty() ) {
				    		StringBuffer whereNodes = new StringBuffer(" WHERE GDB_NODE<'!'\n");
				    		int i=0;
				    		for ( String n : selectedNodes ) {
				    			if ( 0 == ++i%3 ) whereNodes.append('\n');
				    			whereNodes.append("OR GDB_NODE='" + n + "' ");
				    		}
				    		sql += whereNodes;
			    		}
			    	} else if ( sql.equals(EXPAND_AS_ADDQUERY_CALL) ) {
			    		sql = input.getText().replace("'", "''");
			    		sql = "call addquery('<id>', '<description>', '<issuer>', '" + sql + "', '<fields>')";
			    	}
			    	input.setText(sql);
			    	input.requestFocus();
			    }
		    	cb.setSelectedIndex(0);
			}
		});
		apiCallsDropDown.setMaximumRowCount(v.size()+10);
		
//		historyDropDown = new JComboBox();
//		historyDropDown.addActionListener(new ActionListener() {
//			public void actionPerformed(ActionEvent e) {
//				JComboBox cb = (JComboBox)e.getSource();
//			    String sql = (String)cb.getSelectedItem();
//			    if ( !sql.equals(CLICK_FOR_API_AND_HISTORY) && 0<sql.length()) {
//		    	input.setText(sql);
//		    	cb.setSelectedIndex(0);
//		    	
//			}
//		});

		int baseSize = getFont().getSize();
		GridBagConstraints c;

		JPanel p1 = new JPanel(new GridBagLayout()), p2 = new JPanel(new GridBagLayout()), p3 = new JPanel(new GridBagLayout());
		
		labelConstraints.gridy = 0;
		p1.add(new JLabel("SQL Query:"), labelConstraints);
		
		c = (GridBagConstraints)componentConstraints.clone();
		c.gridwidth = 3;
		c.gridheight = 4;
		c.gridx = 0;
		c.gridy = 1;
		c.weighty = 0.15;
		p1.add(createScroller(input, -1, baseSize * 6), c);
		
		c = (GridBagConstraints)componentConstraints.clone();
		c.gridwidth = 1;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1;
		c.gridx = 0;
		c.gridy = 5;
		p1.add(apiCallsDropDown, c);
		
//		c = (GridBagConstraints)componentConstraints.clone();
//		c.gridwidth = 2;
//		c.fill = GridBagConstraints.NONE;
//		c.weightx = 1;
//		c.gridx = 2;
//		c.gridy = 3;
//		p1.add(historyDropDown, c); //new JLabel("BLAH BLAH BLAH BLAH BLAH BLAH BLAH BLAH BLAH:"), c);
		
		c = (GridBagConstraints)componentConstraints.clone();
		c.anchor = GridBagConstraints.PAGE_START;
		c.fill = GridBagConstraints.NONE;
		c.insets = new Insets(c.insets.top, 0, c.insets.bottom, c.insets.right);
		c.gridwidth = 1;
		c.gridheight = 1;
		c.gridx = 3;
		c.gridy = 1;
		c.weightx = 0;
		c.weighty = 0;
		p1.add(back, c);
		
		c.gridx = 4;
		c.anchor = GridBagConstraints.EAST;
		p1.add(forward, c);
		
		c.weighty = 1;
		c.anchor = GridBagConstraints.PAGE_END;
		c.fill = GridBagConstraints.BOTH;
		c.gridwidth = 2;
		c.gridx = 3;
		c.gridy = 2;
		p1.add(submit, c);
		
//		c = (GridBagConstraints)componentConstraints.clone();
		GridBagConstraints c2 = new GridBagConstraints();
		c2.gridx = GridBagConstraints.RELATIVE;
		c2.gridy = 0;
		c2.anchor = GridBagConstraints.NORTH;

		c2.fill = GridBagConstraints.HORIZONTAL; //c2.weightx = 1;
		
		JPanel reIteratePanel = new JPanel(new GridBagLayout());
		reIterateBox = new JTextField();
		reIterateBox.setDocument(Field.getValidatedDocument("[1-9][0-9]*"));
		reIterateBox.setText("1");
		reIterateBox.setBorder(BorderFactory.createEtchedBorder());
		reIteratePanel.add(new JLabel("x "), c2);

		c2.weightx = 1;
		reIteratePanel.add(reIterateBox, c2);
		
		reIterateBox.addFocusListener(new FocusListener() {
			public void focusGained(FocusEvent e) {}
			public void focusLost(FocusEvent e) {
				JTextField tBox = (JTextField)e.getSource();
				if ( 0 == tBox.getText().length() ) tBox.setText("1");
			}
		});
		
		c.fill = GridBagConstraints.HORIZONTAL; c.weighty = 0;
		c.gridx = 3; c.gridy = 3; c.gridwidth = 2;
		c.anchor = GridBagConstraints.NORTH;
		p1.add(reIteratePanel, c);
		
		
		labelConstraints.gridy = 3;
		p2.add(new JLabel("Results:"), labelConstraints);

		c = (GridBagConstraints)componentConstraints.clone();
		c.gridy = 4;
		c.insets = new Insets(0, Dashboard.BORDER_SIZE, 0, Dashboard.BORDER_SIZE);
		c.weighty = 0.75;
		p2.add(createScroller(results, -1, baseSize * 10), c);
		
//		resultsModel.setColumnCount(0);
//		resultsModel.setRowCount(0);
		

		labelConstraints.gridy = 5;
		Insets holding = labelConstraints.insets;
		labelConstraints.insets = new Insets(0, Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE);
		p2.add(queryInfo, labelConstraints);
		labelConstraints.insets = holding;
	

	
		// Final Panel
		labelConstraints.gridy = 0;
		p3.add(new JLabel("Errors/Warnings:"), labelConstraints);

		c = (GridBagConstraints)componentConstraints.clone();
		c.fill = GridBagConstraints.BOTH;
		c.gridwidth = 3;
		c.gridheight = 4;
		c.gridy = 1;
		c.gridx = 0;
		c.weightx = 0.9;
		p3.add(createScroller(errors, -1, baseSize * 6), c);
		

		c.weightx = 0;
		c.anchor = GridBagConstraints.EAST;
		c.gridwidth = 1;
		c.gridheight = 1;
		c.gridy = 3;
		c.gridx = 3;
		p3.add(lookupError, c);
		
		c.gridy = 4;
		p3.add(reconnectButton, c);		

		// Add them to the tab
		c = (GridBagConstraints)componentConstraints.clone();
		c.weighty = 0.8;
		c.anchor = GridBagConstraints.PAGE_START;
		c.fill = GridBagConstraints.BOTH;
		c.gridheight = 2;
		c.gridwidth = 1;
		c.weightx = 0.2;
		this.add(new JSplitPane(JSplitPane.VERTICAL_SPLIT, p1, p2), c);

		c.anchor = GridBagConstraints.PAGE_END;
		c.weighty = 0.2;
		this.add(p3, c);
		
		initialiseResultsTable();
		
//		new Thread( new Runnable() {
//			public void run() { 
//				while ( true ) { try { Thread.sleep(1000); } catch (InterruptedException e) {} System.gc(); } }
//		}, "DashGC" ).start();
	}
	
	private void initialiseResultsTable() {
		
		// Need to initialise the results table before running queries in a separate thread as the initialisation
		// involves synchronized awt code under the covers (e.g. java/awt/Component.setFont(Component.java:1646)) 
		// which sometimes causes a deadlock when accessed concurrently by separate threads.
		
		// Initialise the cell renderer - includes colours and fonts
		resultsModel.setColumnIdentifiers(new String[] {""});
		resultsModel.addRow(new String[] {""});	
		results.getCellRenderer(0, 0);
		
		// Clear table again immediately
		resultsModel.setColumnCount(0);
		resultsModel.setRowCount(0);
	}
	
	private void addToSQLHistory( String sql ) {
		
		String last = sqlHistory.isEmpty() ? null : sqlHistory.lastElement();
		
		// If this sql is different to the last issued query (incl if there wasnt one), add the query to the history.
		if ( !sql.equals(last) ) {
			sqlHistory.add(sql);

			boolean isInDropDownHistory = false;
			int dropDownItemsCount = apiCallsDropDown.getItemCount();
			for ( int i=0; i<dropDownItemsCount; i++ )
				if ( ((String)apiCallsDropDown.getItemAt(i)).startsWith(sql) ) {
					isInDropDownHistory = true; break;
				}
			
			if ( !isInDropDownHistory ) apiCallsDropDown.addItem(sql);
		}

		sqlHistoryIndex = sqlHistory.size()-1;
		
		if ( 0 < sqlHistoryIndex )
			back.setEnabled(true);
		
		forward.setEnabled(false);
	}
	
	private String executingSQL = null;
	private long lastCancelTime = 0, lastStartTime = 0;
	private long previousCellCount = 0;
	private boolean memDecreasedSubstantially = false;

	protected synchronized void submitQuery(String sql) {
		
		if (null==sql || 0 == sql.trim().length() && null != executingSQL ) return;
		
		if (null == conn) {
			JOptionPane.showMessageDialog(
				this,
				"You must be connected to a GaianDB node in order to execute queries.",
				"Not Connected",
				JOptionPane.ERROR_MESSAGE);
			return;
		}
		
		addToSQLHistory(sql);
				
		executingSQL = sql;
		new Thread( new Runnable() {
			public void run() {
				
				resultsModel.setColumnCount(0);
				resultsModel.setRowCount(0);
				
				long startTime = lastStartTime = System.currentTimeMillis();
				
				try {
					if ( null == stmt ) stmt = conn.createStatement();
//					stmt.setQueryTimeout(Dashboard.QUERY_TIMEOUT); // causes issues if the SQL is an INSERT
					ResultSet resultSet = null;
					long execTime = 0;
					
					// Clear previous security credentials hint
					int credIndex = executingSQL.indexOf("\n-- " + SecurityClientAgent.GDB_CREDENTIALS);
					if ( -1 != credIndex ) executingSQL = executingSQL.substring(0, credIndex);
					
					// Insert new credentials value as hint if one or more were specified
					if ( container.securityAgent.isSecurityCredentialsSpecified() ) {

						container.securityAgent.refreshPublicKeysFromServers(stmt);
						executingSQL += "\n-- " + SecurityClientAgent.GDB_CREDENTIALS + "=" +
							container.securityAgent.getEncryptedCredentialsValueInBase64(executingSQL) + "\n";
						input.setText( executingSQL );
					}
					
					String repeatTxt = reIterateBox.getText();
					if ( null == repeatTxt || 1 > repeatTxt.length() )
						reIterateBox.setText(repeatTxt = "1");
					
					int repeatCount = Integer.parseInt( repeatTxt );
					
					setErrors("");
					lookupError.setEnabled(false);
					queryInfo.setText("Executing Query, please wait...");
					submit.setText("Stop");

					long aggregateRowCount = 0;
					long totalTime = -System.currentTimeMillis();
					
					int nextLogTimeDurationUnits = 0; // 10ths of a second
					int countdown = repeatCount;
					
					while ( 0 < countdown-- ) {
						
						int rowsFetched = 0;
						
						int queryIndex = repeatCount - countdown;
						boolean showProgress = nextLogTimeDurationUnits < (System.currentTimeMillis() + totalTime)/100;
						if ( showProgress ) {
							nextLogTimeDurationUnits++;
							queryInfo.setText("Repetition " + queryIndex + ", please wait...");
						}
					
						execTime -= System.currentTimeMillis();
						stmt.execute(executingSQL);
						execTime += System.currentTimeMillis();
						
						if ( startTime < lastCancelTime ) return;
						
						resultSet = stmt.getResultSet();
						
						if (null != resultSet) {
							
							if ( 0 < countdown ) {
								while (resultSet.next()) {
									if ( startTime < lastCancelTime ) return;
									showProgress = nextLogTimeDurationUnits < (System.currentTimeMillis() + totalTime)/200;
									if ( showProgress ) { //0 == rowsFetched % 1000 )
										nextLogTimeDurationUnits++;
										queryInfo.setText(
												( 1 == repeatCount ? "" : "Repetition " + queryIndex + ". " ) + 
												"Fetching rows... " + (0==rowsFetched?"":rowsFetched));
									}
									rowsFetched++;
								}
							}
							else {
								ResultSetMetaData metadata = resultSet.getMetaData();
		
								int cols = metadata.getColumnCount();
								String[] columnNames = new String[cols];
								for (int i = 0; i < cols; i++) {
									columnNames[i] = metadata.getColumnName(i + 1);
								}
								resultsModel.setColumnIdentifiers(columnNames);
								
								// Now add the cell "Editors" to the table
								// We're not actually editing, but the cell editor
								// is fired when the user double clicks. We will
								// capture that event do useful stuff
								for (int i = 0; i < cols; i++) {
									results.getColumnModel().getColumn(i)
											.setCellEditor(new QueryTabResultsEditorEvent(executingSQL));
								}
								
								// Adjust/Reset the cell renderer
								if (Pattern.compile(WARNINGS_SQL).matcher(executingSQL.toLowerCase()).find()) {
									results.setDefaultRenderer(Object.class, new ListWarningsTableCellRenderer());
								} else {
									results.setDefaultRenderer(Object.class, new ResetTableCellRenderer());
								}
								

								while (resultSet.next()) {
									Object[] data = new Object[cols];
									for (int i = 0; i < cols; i++)
										data[i] = resultSet.getObject(i + 1); // getString(i + 1);
									
									// Check now if the query was cancelled as data may be corrupted at this point
									if ( startTime < lastCancelTime ) {
										resultsModel.setColumnCount(0);
										resultsModel.setRowCount(0);
										return;
									}
									
									resultsModel.addRow(data);
									showProgress = nextLogTimeDurationUnits < (System.currentTimeMillis() + totalTime)/200;
									if ( showProgress ) { //0 == rowsFetched % 1000 )
										nextLogTimeDurationUnits++;
										queryInfo.setText(
												( 1 == repeatCount ? "" : "Repetition " + queryIndex + ". " ) + 
												"Fetching rows... " + (0==rowsFetched?"":rowsFetched));
									}
									rowsFetched++;
								}
							}	
							resultSet.close();
							
						}
						aggregateRowCount += rowsFetched;
					}
					
					
					totalTime += System.currentTimeMillis();
					
					// Release/Clear statement and result-set asap
					if ( null != stmt ) { stmt.close(); stmt = null; }

					int rowCount = results.getRowCount();
					int columnCount = results.getColumnCount();
					TableColumnModel columnModel = results.getColumnModel();
					for (int y = 0; y < columnCount; y++) {
						TableColumn column = columnModel.getColumn(y);
						int maxWidth = column.getPreferredWidth();
						for (int x = 0; x < rowCount; x++) {
							maxWidth = Math.max(
								maxWidth,
								results.getCellRenderer(x, y)
									.getTableCellRendererComponent(results, results.getValueAt(x, y), false, false, x, y)
									.getPreferredSize().width);
							
//							System.out.println(executingSQL);
//							// Set the text blue, if we know it is a URL
//							if ((executingSQL.equals(WARNINGS_SQL)) && (y == WARNINGS_COLUMN)) {
//								results.getCellRenderer(x, WARNINGS_COLUMN)
//									.getTableCellRendererComponent(results, results.getValueAt(x, WARNINGS_COLUMN), false, false, x, WARNINGS_COLUMN).setForeground(Color.BLUE);
//							} else {
//								results.getCellRenderer(x, y)
//								.getTableCellRendererComponent(results, results.getValueAt(x, y), false, false, x, y).setForeground(Color.BLACK);
//							}
						}
						column.setPreferredWidth(maxWidth + COLUMN_PADDING);

					}
					
					
//					input.setText("");
//					currentSql = "";

					queryInfo.setText(
						"Completed at " + new SimpleDateFormat("HH:mm:ss").format(new Date()) + ". " +
						( aggregateRowCount == rowCount ?
							"Fetched " + rowCount + " rows." :
							"Aggregate Fetch: " + aggregateRowCount + " rows. Last Fetch: " + rowCount + " rows."
						) +
						" Total Time: " + totalTime + "ms" +
						" (Execution Time: " + execTime + "ms)" +
						" "+(aggregateRowCount*1000/(0==totalTime ? 1:totalTime))+" rows/s"						
					);

					setErrors();
					
				} catch (Exception e) {
					
					queryInfo.setText("Error/Warning at " + new SimpleDateFormat("HH:mm:ss").format(new Date()) + ". ");
					
					// Search for root cause in case of IEX 				
					Throwable cause = e;
					String msg = "";
					while (true) {
						cause = cause.getCause();
						if (cause != null) {
							msg = cause.getMessage();
						} else {
							break;
						}
					}
					
					String extractedCode = "";
					if (msg.matches("^.*" + GaianTable.IEX_PREFIX.replaceAll("\\*","\\\\*") + ".*$")) {
						extractedCode = msg.toString().split("'")[1].split(":")[0];
					}
					
					try {
						if (null != GDBMessages.class.getDeclaredField(extractedCode) ) {
							setErrorsWithCode(extractedCode, e);
						} else {
							throw new Exception();
						}
					} catch (Exception e1) {
						setErrorsWithCode(GDBMessages.CLIENT_STMT_EXEC_RETURNED_ERROR, e);
					}
					
				} finally {
					
					// Ensure statement and result-set are definitely released/cleared
					if ( null != stmt ) { try { stmt.close(); } catch (SQLException e) {} stmt = null; }
					
					if ( startTime > lastCancelTime ) {
						executingSQL = null;
						submit.setText("Run");
					}

					// Try to free up some memory if a large number of cells has been cleared.
					long mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted();					
					long cellCount = (long) resultsModel.getRowCount() * resultsModel.getColumnCount();
					
					if ( previousCellCount/10 > cellCount/9 || (previousCellCount/9 > cellCount/10 && memDecreasedSubstantially) ) {
						int i=0;
						while ( ++i < 100 && lastStartTime == startTime ) // only let one completing thread run this loop
							try { System.gc(); Thread.sleep(100); } catch (InterruptedException e) {}

//						System.out.println("Called GC " + i + " times");
					}
					
					if ( lastStartTime == startTime ) {
						previousCellCount = cellCount;
						memDecreasedSubstantially = mem - ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted() > mem/100;
					}
				}
			}
		}, "QueryExecutor").start();
	}

	private void back() {
				
		sqlHistory.set(sqlHistoryIndex--, input.getText());
		input.setText( sqlHistory.get(sqlHistoryIndex) );
		
		if ( 0 == sqlHistoryIndex ) back.setEnabled(false);
		forward.setEnabled(true);
	}

	private void forward() {
		
		sqlHistory.set(sqlHistoryIndex++, input.getText());
		input.setText( sqlHistory.get(sqlHistoryIndex) );
		
		if ( sqlHistory.size()-1 == sqlHistoryIndex ) forward.setEnabled(false);
		back.setEnabled(true);
	}

//	private void setErrors(Exception e) {
//		setErrorsWithCode(null, e);
//	}
	
	private void setErrorsWithCode(String code, Exception e) {
		List<String> errors = new LinkedList<String>();
		StringBuffer extrace = new StringBuffer();

		for ( StackTraceElement ste : e.getStackTrace() ) 
			extrace.insert(0, ">" + ste.getMethodName() + ":" + ste.getLineNumber());

		Throwable cause = e;
		while (true) {
			cause = cause.getCause();

			if (null != cause) {
				if (!errors.contains(cause.getMessage())) {
					errors.add(cause.getMessage());
				}
			}
			else {
				break;
			}
		}
		
		errors.add("Diag:"+extrace.toString());

		setErrorsWithCode(code, errors.toArray(new String[errors.size()]));
	}
	
	private void setErrors(String... errors) {
		setErrorsWithCode(null, errors);
	}
	
	private void setErrorsWithCode(String code, String... errors) {
		errorsModel.clear();
		if (errors.length == 0) {
			if (null == conn) {
				errorsModel.addElement("You must be connected to a GaianDB node in order to execute queries.");
			}
			else {
				errorsModel.addElement("There are no errors or warnings to report.");
			}
		}
		else {
			for (String error : errors) {
				if (null != code && errors[0].equals(error)) {
					errorsModel.addElement(code + " : " + error);
				} else {
					errorsModel.addElement(error);
				}
			}
		}
		
		// Do we have a unique error code
		try {
			if ((null != code) && (null != GDBMessages.class.getDeclaredField(code)) && (Logger.findDocumentation(code).startsWith("file"))) {
				lookupError.setEnabled(true); 
			} else {
				lookupError.setEnabled(false);
			}
		} catch (Exception e) {
			lookupError.setEnabled(false);
		}
	}

	public void connected(Connection newConn) {
		conn = newConn;
		reconnectButton.setEnabled(true);
		setErrors();
	}

	public void disconnected() {
		if ( null != conn ) try { conn.close(); } catch ( SQLException e ) {}
		conn = null;
		setErrors();
	}
	
    public void activated() { }

    public void deactivated() { }

	public static void openErrorDoc(String errorCode) {
		String code = null != errorCode ? errorCode : Logger.UNKNOWN_WARNING;
		
		String doc = Logger.findDocumentation(code);

		// Special chars
		doc = doc.replaceAll(" ", "%20");
		
		Desktop dt = Desktop.getDesktop();
		try {
			dt.browse(new URI(doc));
		} catch (Exception e) {
//			setErrorsWithCode(GDBMessages.CLIENT_DOC_LOOKUP_ERROR, "");
		}
	}
	
}

// This is called when a user double clicks on a table cell
class QueryTabResultsEditorEvent extends AbstractCellEditor implements TableCellEditor {
	private static final long serialVersionUID = 2943781958170741556L;

//	int rowIndex = -1;
//	int colIndex = -1;
//	String cellText = null;

	String lastCall = null;

	public QueryTabResultsEditorEvent(String sqlStatment) {
		lastCall = sqlStatment;
	}

	// When editing begins...
	public Component getTableCellEditorComponent(JTable table, Object value,
			boolean isSelected, int row, int column) {

//		rowIndex = row;
//		colIndex = column;
		final String cellText = null==value ? "" : value.toString();
		
		// Do what we do based on the last call
		if (lastCall.toLowerCase().matches(QueryTab.WARNINGS_SQL)
				&& (column == QueryTab.WARNINGS_COLUMN)) {
			// Extract error code
			String code = cellText.split(":")[0];
			QueryTab.openErrorDoc(code);
		}

		return null;
	}

	// When editing finishes...
	public Object getCellEditorValue() {
		// Not needed
		return null;
	}
}

class ListWarningsTableCellRenderer extends DefaultTableCellRenderer {
    private static final long serialVersionUID = 1L;

    public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {
		Component c = super.getTableCellRendererComponent(table, value,
				isSelected, hasFocus, row, column);

		// Only for specific column
		if (column == QueryTab.WARNINGS_COLUMN) {
			c.setForeground(Color.BLUE);
		} else {
			c.setForeground(Color.BLACK);
		}
		return c;
	}
}

class ResetTableCellRenderer extends DefaultTableCellRenderer {
    private static final long serialVersionUID = 1L;

	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {
		Component c = super.getTableCellRendererComponent(table, value,
				isSelected, hasFocus, row, column);

		// For all cells
		c.setForeground(Color.BLACK);
		return c;
	}
}
