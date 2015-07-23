/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.swing.AbstractCellEditor;
import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

public class LtAndDsTab extends UpdatingTab implements ActionListener {

	private static final long serialVersionUID = 7386019679008602855L;

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final Logger LOGGER = Logger.getLogger(LtAndDsTab.class
			.getName());

	private static final int GRAPH_INTERVAL = 10000;

	private static final String GRAPH_SQL = "SELECT GDB_NODE, LTNAME, LTDEF from new com.ibm.db2j.GaianQuery('select * from new com.ibm.db2j.GaianConfig(''LTDEFS'') GC', 'with_provenance') GQ order by LTNAME";
	private static String PROPDEF_SQL = "select PROPID, PROPDEF from new com.ibm.db2j.GaianQuery("
			+ "'select * from new com.ibm.db2j.GaianConfig(''FULLCONFIG'') GC', 'with_provenance') GQ";

	static final String[] COLUMN_NAMES = { "Name", "Definition", "Constants" };
	static final int LT_NAME_COLUMN = 0;
	static final int LT_DEF_COLUMN = 1;
	static final int LT_CONSTANT_COLUMN = 2;

	private static final int COLUMN_PADDING = 10;

	private JPanel filterPanel = null;
	private JPanel tablePanel = null;
	private JSplitPane graphTableSplit = null;
	private JTable table = null;

	private JLabel filterLabel = new JLabel("Filter: ");
	JTextField filterText = new JTextField(20);

	Map<String, String> logicalTables = new TreeMap<String, String>();
	Map<String, String> listConfig = new HashMap<String, String>();

	private LtAndDsGraph graph = null;

	private String localNodeID = null;

	private PreparedStatement graphStatement;
	private PreparedStatement configStatement;

	Updater graphUpdater;
	private long lastUpdate = 0;
	String savedFilter = null;

	private DefaultTableModel tableModel = null;

	public LtAndDsTab(Dashboard container) {
		super(container, new BorderLayout(Dashboard.BORDER_SIZE,
				Dashboard.BORDER_SIZE));
	}

	@Override
	protected void create() {
		/*
		 * FilterGraph Panel
		 */

		filterPanel = new JPanel(new GridBagLayout());

		filterText.setEditable(true);
		filterText.addKeyListener(new KeyListener() {
			public void keyPressed(KeyEvent arg0) {
				// DO NOTHING!
			}

			public void keyReleased(KeyEvent arg0) {

				String input;
				if (KeyEvent.getKeyText(arg0.getKeyCode()).equals("Escape")) {
					input = null;
					filterText.setText("");
				} else {
					input = filterText.getText();
				}
				// For refreshes
				savedFilter = input;

				// System.out.println("key released" + arg0.toString());

				// Do this last
				filterDisplay(input);
			}

			public void keyTyped(KeyEvent arg0) {
				// DO NOTHING!
			}

		});

		GridBagConstraints c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = 0;
		filterPanel.add(filterLabel, c);

		c.gridx = 1;
		filterPanel.add(filterText, c);

		/*
		 * Graph Panel
		 */
		graph = LtAndDsGraph.getSingleton(this);
		graph.setNodeRenderer(true);
		graph.setBorder(BorderFactory.createEtchedBorder());
		graph.panAbs(graph.getWidth() / 2, graph.getHeight() / 2);

		localNodeID = container.getLocalNodeID();
		graph.setLocalNode(localNodeID);

		try {
			graphStatement = conn.prepareStatement(GRAPH_SQL);
			graphStatement.setQueryTimeout(Dashboard.QUERY_TIMEOUT);

			configStatement = conn.prepareStatement(PROPDEF_SQL
					+ " WHERE GDB_NODE = '" + localNodeID + "'");
			configStatement.setQueryTimeout(Dashboard.QUERY_TIMEOUT);

		} catch (SQLException e) {
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not retrieve the logical table information.");
			}
			return;
		}

		graphUpdater = new Updater("LtAndDsTab.updateTab", GRAPH_INTERVAL) {
			protected boolean update() {
				if (System.currentTimeMillis() > lastUpdate
						+ (long) GRAPH_INTERVAL) {
					// System.out.println("Updating...");
					return updateData();
				} else {
					return true;
				}
			}

			public void wake() {
				super.wake();
				this.update();
			}

		};
		addUpdater(graphUpdater);

		/*
		 * Table Panel
		 */
		tablePanel = new JPanel(new GridBagLayout());
		// tablePanel.setBackground(Color.BLUE); // For Testing

		graphTableSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, graph,
				tablePanel);
		graphTableSplit.setResizeWeight(0.75);

		tableModel = new DefaultTableModel();
		table = new JTable(tableModel);

		c.anchor = GridBagConstraints.LINE_START;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		tablePanel.add(new JScrollPane(table), c);
		tableModel.setColumnIdentifiers(COLUMN_NAMES);
		
		hideMessage();
		this.add(filterPanel, BorderLayout.PAGE_START);
		this.add(graphTableSplit, BorderLayout.CENTER);
		updateData();

	}

	protected synchronized Boolean updateData() {
		// Get the logical Tables
		try {
			ResultSet resultSet = graphStatement.executeQuery();

			logicalTables.clear();

			if (!resultSet.isClosed()) {
				while (resultSet.next()) {
					if (resultSet.getString("GDB_NODE").equals(localNodeID)) {
						String lt = resultSet.getString("LTNAME").toUpperCase();
						String ltDef = resultSet.getString("LTDEF");

						logicalTables.put(lt, ltDef);
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not display the logical table and data source information.");
			}
			return false;
		}

		// Get the config information
		try {
			ResultSet resultSet = configStatement.executeQuery();

			if (!resultSet.isClosed()) {
				while (resultSet.next()) {
					String key = resultSet.getString("PROPID").toUpperCase();
					String value = resultSet.getString("PROPDEF");
					// System.out.println("key: " + key + " value: " + value);
					listConfig.put(key, value);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not display the logical table and data source information.");
			}
			return false;
		}

		return filterDisplay(savedFilter);
	}

	protected boolean filterDisplay() {
		return filterDisplay(null);
	}

	protected synchronized boolean filterDisplay(String filter) {
		lastUpdate = System.currentTimeMillis();

		// savedFilter may have been changed by LtAndDsGraph
		if (!filterText.getText().equals(savedFilter)) {
			filterText.setText(savedFilter);
		}

		// Work out the regex
		String regex = "^";
		if (filter != null) {
			regex += filter.toLowerCase();
			if (filter.contains("*")) {
				regex = regex.replaceAll("\\*", ".*");
			} else {
				regex += ".*";
			}
		} else {
			regex += ".*";
		}
		regex += "$";
		// System.out.println("Compiled regex: " +
		// Pattern.compile(regex).toString());

		if (null == graph)
			return false; // no-op if graph was destroyed

		try {
			// Show the node at the middle
			graph.setEdge(localNodeID, localNodeID);
			// Clear the table
			tableModel.setRowCount(0);

			for (Entry<String, String> logicalTable : logicalTables.entrySet()) {

				String ltName = logicalTable.getKey();
				// String ltDef = logicalTable.getValue();

				if (!regex.equals("^")) {
					if (Pattern.compile(regex).matcher(ltName.toLowerCase())
							.find()) {

						// Draw on the graph
						graph.setEdge(ltName, localNodeID);

						// Now do the table
						Object[] ltRow = new Object[3];
						ltRow[0] = ltName;
						ltRow[1] = listConfig
								.get(ltName.toUpperCase() + "_DEF");
						ltRow[2] = listConfig.get(ltName.toUpperCase()
								+ "_CONSTANTS");

						tableModel.addRow(ltRow);

					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			if (container.checkConnection()) {
				LOGGER.severe(unravelMessages(e));
				destroy();
				showError("Could not display the logical table and data source information.");
			}
			return false;
		}

		// Get the widths right
		int columnCount = table.getColumnCount();
		TableColumnModel columnModel = table.getColumnModel();
		for (int y = 0; y < columnCount; y++) {
			TableColumn column = columnModel.getColumn(y);
			int maxWidth = column.getPreferredWidth();
			for (int x = 0; x < table.getRowCount(); x++) {
				maxWidth = Math.max(
						maxWidth,
						table.getCellRenderer(x, y)
								.getTableCellRendererComponent(table,
										table.getValueAt(x, y), false, false,
										x, y).getPreferredSize().width);
			}
			column.setPreferredWidth(maxWidth + COLUMN_PADDING);
		}	
		
		// Now add the cell Editors to the table
		for (int i = 0; i < columnCount; i++) {
			table.getColumnModel().getColumn(i)
					.setCellEditor(new LtAndDsTableCellEditor(this));
		}
		tablePanel.updateUI();

		for (KeyListener listener : table.getKeyListeners()) {
			System.out.println("KeyListener: " + listener.toString());
		}
		
		graph.update();
		graph.recenter();
		return true;
	}

	@Override
	protected void destroy() {
		if (null != filterPanel) {
			remove(filterPanel);
			filterPanel = null;
		}

		if (null != graph) {
			remove(graph);
			graph = null;
		}

		if (null != tablePanel) {
			remove(tablePanel);
			tablePanel = null;
		}

		if (null != graphTableSplit) {
			remove(graphTableSplit);
			graphTableSplit = null;
		}

		graphUpdater = null;
	}

	public void actionPerformed(ActionEvent arg0) {
		// TODO Auto-generated method stub

	}

}

class LtAndDsTableCellEditor extends AbstractCellEditor implements
		TableCellEditor {
	private static final long serialVersionUID = 3426752811053766328L;

	// This is the component that will handle the editing of the cell value
	JComponent component = new JTextField();

	LtAndDsTab context = null;

	int rowIndex = -1;
	int colIndex = -1;

	String originalText = null;
	String ltName = null;
	String ltDef = null;
	String ltConstants = null;

	public LtAndDsTableCellEditor(LtAndDsTab callerContext) {
		context = callerContext;
	}

	// When editing begins...
	public Component getTableCellEditorComponent(JTable table, Object value,
			boolean isSelected, int row, int column) {

//		System.out.println("Started editing");
		context.filterText.setEnabled(false);
		context.graphUpdater.suspend();

		component.setBorder(BorderFactory.createEmptyBorder());

		// Escape on the table cancels editing, which is not handled by
		// default. We handle it here instead and enable the filter text
		if (component.getKeyListeners().length == 0) {
			component.addKeyListener(new KeyListener() {
				public void keyPressed(KeyEvent arg0) {
					if (KeyEvent.getKeyText(arg0.getKeyCode()).equals("Escape")) {
//						System.out.println("Escaped!");
						context.filterText.setEnabled(true);
						context.graphUpdater.wake();
					}
				}

				public void keyReleased(KeyEvent arg0) {
					// DO NOTHING!
				}

				public void keyTyped(KeyEvent arg0) {
					// DO NOTHING!
				}
			});
		}
		
		rowIndex = row;
		colIndex = column;
		originalText = (String) value;

		ltName = (String) table.getValueAt(row, LtAndDsTab.LT_NAME_COLUMN);
		ltDef = (String) table.getValueAt(row, LtAndDsTab.LT_DEF_COLUMN);
		ltConstants = (String) table.getValueAt(row,
				LtAndDsTab.LT_CONSTANT_COLUMN);
		if (ltConstants == null) {
			ltConstants = "";
		}

		// Configure the component with the specified value
		((JTextField) component).setText((String) value);

		// Return the configured component
		return component;
	}

	// When editing finishes...
	public Object getCellEditorValue() {
		// System.out.println(((JTextField) component).getText());
		Boolean goToEnd = false;

		String newText = ((JTextField) component).getText();

		// Did we make any changes?
		if (newText.equals(originalText)) {
			goToEnd = true;
		}

		// Check we set the row properly, if not return
		// the original text
		if ((goToEnd == false) && (rowIndex == -1 || colIndex == -1)) {
			newText = originalText;
			goToEnd = true;
		}

		// Did we edit the table name or the definition?
		if (goToEnd == false) {
			if (colIndex == LtAndDsTab.LT_NAME_COLUMN) {
				// To change the name, we will remove the old LT and create a
				// new one
				try {
					Statement remove = context.conn.createStatement();
					remove.setQueryTimeout(Dashboard.QUERY_TIMEOUT);

					String sql = "call removelt('" + ltName + "')";
					remove.execute(sql);
					remove.close();
				} catch (SQLException e) {
					e.printStackTrace();
					goToEnd = true;
				}

				ltName = newText;

			} else if (colIndex == LtAndDsTab.LT_DEF_COLUMN) {
				ltDef = newText;
			} else if (colIndex == LtAndDsTab.LT_CONSTANT_COLUMN) {
				ltConstants = newText;
			}
		}

		if (goToEnd == false) {
			try {
				Statement editDef = context.conn.createStatement();
				editDef.setQueryTimeout(Dashboard.QUERY_TIMEOUT);
				String sql = "call setlt('" + ltName.toUpperCase() + "','"
						+ ltDef + "','" + ltConstants + "')";
				editDef.execute(sql);
				editDef.close();
			} catch (SQLException e) {
				System.out.println("WARNING! Caught SQLException");
				e.printStackTrace();
			}
		}

		// goToEnd
//		System.out.println("Stopped editing");
		context.filterText.setEnabled(true);

		// Update now, so that graph is up to date
		context.graphUpdater.wake();

		return newText;
	}
}
