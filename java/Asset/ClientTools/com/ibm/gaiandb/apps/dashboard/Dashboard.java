/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import javax.security.auth.login.LoginException;
import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JTabbedPane;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.apps.DBConnector;
import com.ibm.gaiandb.apps.SecurityClientAgent;

/**
 * <p>The GaianDB Dashboard is an application that lets the user monitor and query
 * the GaianDB network using a connection to a specific node. It consists of a
 * set of tabs that provide specific functions to the user.</p>
 * 
 * <p><strong>Tabs:</strong></p>
 * <ul>
 *   <li><em>Connection</em> - connects to and disconnects from the node
 *   specified</li>
 *   <li><em>Network Topology</em> - shows the network topology as a graph, with
 *   the ability to monitor one of several metrics across the network</li>
 *   <li><em>Node Statistics</em> - shows graphs of various metrics over time
 *   for a specific node</li>
 *   <li><em>SQL Queries</em> - provides an interface for the user to query the
 *   database using SQL</li>
 *   <li><em>Logival Tables and Data Sources</em> - show the federated view of logical tables and their data sources</li>
 * </ul>
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class Dashboard extends JFrame {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 5198136049229737430L;

	/**
	 * The application logger. Writes to <code>System.err</code> and
	 * {@link #LOG_FILE}.
	 */
	private static final Logger LOGGER = Logger.getLogger(Dashboard.class.getName());
	/** The name of the dashboard log file. */
	private static final String LOG_FILE = "dashboard.log";
	/** The name of the prefuse log file. */
	private static final String PREFUSE_LOG_FILE = "prefuse.log";

	/** The starting size of the application window. */
	static final Dimension DEFAULT_SIZE = new Dimension(900, 700); //700, 520); // 640, 480

	/** The size of the gap between window components. */
	static final int BORDER_SIZE = 10;
	/** A border that can be used by components to provide default spacing. */
	static final Border DEFAULT_BORDER = BorderFactory.createEmptyBorder(
		BORDER_SIZE, BORDER_SIZE, BORDER_SIZE, BORDER_SIZE);

	/** The time, in seconds, before a query times out. */
	public static final int QUERY_TIMEOUT = 5;

	/** Retrieves the ID of the local node, as defined by GaianDB. */
	private static final String LOCAL_NODE_SQL =
		"SELECT gdbx_to_node local_node FROM new com.ibm.db2j.GaianTable('gdb_ltnull', 'explain') T WHERE gdbx_depth = 0"; // from_node = '<SQL QUERY>'";

	/** Connects to the database the user specifies. */
	private DBConnector connector = new DBConnector();
	/** The database connection. */
	private Connection conn = null;

	/** The set of window tabs. */
	private Tab[] tabs;


	/**
	 * A thread pool that handles talking to tabs, so we're not constantly
	 * spawning and dropping threads.
	 */
	private ExecutorService tabNotifier;

	/** The name of the local node. */
	private String localNodeID = null;
	
	final SecurityClientAgent securityAgent = new SecurityClientAgent();

	/**
	 * Sets up the various loggers - the application, prefuse and the
	 * DBConnector - and creates the window.
	 * 
	 * @param args The application arguments. Unused.
	 */
	public static void main(String[] args) {
				
		// Adds file output to the logger.
		try {
			Logger.getLogger(Dashboard.class.getPackage().getName()).addHandler(
				new StreamHandler(new FileOutputStream(LOG_FILE), new SimpleFormatter()));
		}
		catch (Exception e) {
			LOGGER.warning("Could not create the dashboard log file. Log messages will appear here only.");
		}

		// Stops prefuse from annoying the user and stores it all in a file instead.
		Logger prefuseLogger = Logger.getLogger("prefuse");
		prefuseLogger.setLevel(Level.WARNING);
		for (Handler handler : prefuseLogger.getHandlers()) {
			prefuseLogger.removeHandler(handler);
		}

		try {
			prefuseLogger.addHandler(new StreamHandler(
				new FileOutputStream(PREFUSE_LOG_FILE), new SimpleFormatter()));
		}
		catch (Exception e) {
			LOGGER.warning("Could not create the prefuse log file. Prefuse errors will be ignored.");
		}

		// Stops the DBConnector from hassling us.
		Logger.getLogger(DBConnector.class.getName()).setLevel(Level.OFF);

		// Let's go.
		Dashboard dashboard = new Dashboard();
		dashboard.setVisible(true);
	}

	/**
	 * Constructs the dashboard window.
	 */
	public Dashboard() {
		super("GaianDB Dashboard");

		// Destroy the window when the Close button is hit.
		// Any rampant threads will not be killed - they must end themselves
		// or the application will not exit.
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				// Disconnects all tabs before closing. Any thread cleanup
				// should be done in the tab's disconnect method.
				disconnect();
				tabNotifier.shutdown();
			}
		});
		setPreferredSize(DEFAULT_SIZE);
		setLocationByPlatform(true);

		try {
			// The JRE v5 doesn't support Windows' Aero Glass, but tries to
			// anyway. This is bad, so we're trying to avoid it.
			String javaVersionS = System.getProperty("java.version");
			float javaVersion = Float.parseFloat(javaVersionS.substring(0, javaVersionS.indexOf('.', 3)));

			if (Util.isWindowsOS) { //System.getProperty("os.name").startsWith("Windows")) {
				float osVersion = Float.parseFloat(System.getProperty("os.version").split(" ")[0]);
				if (javaVersion < 1.6 && osVersion >= 6) {
					throw new Exception("Does not support Aero.");
				}
			}

			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		}
		catch (Exception e) {
			// It doesn't matter if this doesn't work.
			LOGGER.info("Cannot use the system's native look and feel.");
		}

		((JComponent)getContentPane()).setBorder(DEFAULT_BORDER);

		// Set up the tabs.
		// Each one has its own class for code management purposes.
		
		boolean isTopologyGraphAvailable = false;
		try { getClass().getClassLoader().loadClass(TopologyGraph.class.getName()); isTopologyGraphAvailable = true;}
		catch ( Throwable e ) { System.out.println("Could not load TopologyGraph (TopologyTab will not appear): " + e); }
		tabs = isTopologyGraphAvailable ?
				new Tab[] {
					new ConnectionTab(this),
					new TopologyTab(this),
//					new LtAndDsTab(this),
					new MonitorTab(this),
					new StatsTab(this),
					new QueryTab(this)
				} :
				new Tab[] {
					new ConnectionTab(this),
//					new TopologyTab(this),
//					new LtAndDsTab(this),
					new MonitorTab(this),
					new StatsTab(this),
					new QueryTab(this)
				};

		String[] tabNames = isTopologyGraphAvailable ?
				new String[] {
					"Connection",
					"Network Topology",
//					"Logical Tables",
					"Current Metrics",
					"Historical Metrics",
					"SQL Queries"
				} :
				new String[] {
					"Connection",
//					"Network Topology",
//					"Logical Tables",
					"Current Metrics",
					"Historical Metrics",
					"SQL Queries"
				};

		JTabbedPane tabbedPane = new JTabbedPane();
		for (int i = 0; i < tabs.length; i++) {
			tabbedPane.addTab(tabNames[i], tabs[i]);
		}
		add(tabbedPane);

		// We need to notify the tabs when they're being activated/deactivated.
		tabbedPane.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				final int selected = ((JTabbedPane)e.getSource()).getSelectedIndex();
				if (selected >= 0 && selected < tabs.length) {
					tabNotifier.execute(new Runnable() {
						public void run() {
							tabs[selected].activated();
						}
					});
				}

				for (final Tab tab : tabs) {
					if (tab != tabs[selected]) {
						tabNotifier.execute(new Runnable() {
							public void run() {
								tab.deactivated();
							}
						});
					}
				}
			}
		});

		pack();
		((ConnectionTab)tabs[0]).submit.requestFocusInWindow();

		// Set up the tab notification thread pool.
		tabNotifier = Executors.newFixedThreadPool(tabs.length);
	}
	
	private String lastURL = null;
	private Properties lastInfo = null;

	// This method must never be called before the first connect (the reconnect button in QueryTab is greyed out before then)
	void reconnect() {
		if ( null == lastURL || null == lastInfo ) return; // should never happen
		((ConnectionTab) tabs[0]).connect( lastURL, lastInfo ); // Success with this will be confirmed in each tab as part of this method call
	}

	/**
	 * Connects the application to the database specified.
	 * 
	 * @param url
	 *            The database URL.
	 * @param user
	 *            Your username.
	 * @param password
	 *            Your password.
	 * 
	 * @return True on success; false on failure.
	 * 
	 * @throws ClassNotFoundException
	 *             if the JDBC drivers cannot be found.
	 * @throws LoginException 
	 */
	boolean connect(String url, Properties info) throws ClassNotFoundException, LoginException {
		
		if (isConnected()) disconnect();

		conn = connector.connect(url, info, false);
		
		if ( false == isConnected() ) return false;
		
		lastURL = url;
		lastInfo = info;
		
		try {
			Statement statement = conn.createStatement();
			statement.setQueryTimeout(Dashboard.QUERY_TIMEOUT);
			ResultSet resultSet = statement.executeQuery(LOCAL_NODE_SQL);
			if (resultSet.next()) {
				localNodeID = resultSet.getString("LOCAL_NODE");
			}
		}
		catch (SQLException e) {
			LOGGER.warning(e.getMessage());
			if ( false == checkConnection() ) return false;
		}
		
		// Create the metrics table by instantiating an unused metric monitor.
//		MetricMonitor.getInstance(conn).stop();

		for (final Tab tab : tabs) {
			tabNotifier.execute(new Runnable() {
				public void run() {
					tab.connected(conn);
				}
			});
		}
		
		return true;
	}

	/**
	 * Disconnects from the current database.
	 */
	void disconnect() {
		if (null != conn) {
			try {
		        conn.close();
	        }
	        catch (SQLException e) {}
			conn = null;

			for (final Tab tab : tabs) {
				tabNotifier.execute(new Runnable() {
					public void run() {
						tab.disconnected();
					}
				});
			}

			localNodeID = null;
		}
	}

	/**
	 * Tests whether the database connection exists and is open.
	 * 
	 * @return True if the connection is active; false otherwise.
	 */
	public boolean isConnected() {
		try {
	        return conn != null && !conn.isClosed();
        }
        catch (SQLException e) {
        	conn = null;
        	return false;
        }
	}

	/**
	 * <p>Checks the database connection. To be used when a database exception
	 * has been caught and the connection may not be alive.</p>
	 * 
	 * <p>If the connection is no longer active, this will properly disconnect
	 * the application.</p>
	 */
	boolean checkConnection() {
		if (null != conn) {
			try {
				if (conn.isClosed()) {
					LOGGER.info("The connection has been terminated.");
					disconnect();
					((ConnectionTab)tabs[0]).showMessage("The connection has been terminated.");
					return false;
				}
			}
			catch (SQLException e) {
				LOGGER.severe(e.getMessage());
				disconnect();
				((ConnectionTab)tabs[0]).showMessage("The connection has been terminated.");
				return false;
			}
		}

		return true;
	}

	String getLocalNodeID() {
		return localNodeID;
	}

	// Used for path animation in topology tab.. not enabled yet..
	public void updatePathVisualisation(int tickCount) {
		((TopologyTab)tabs[1]).updatePathVisualisation(tickCount);
	}
}
