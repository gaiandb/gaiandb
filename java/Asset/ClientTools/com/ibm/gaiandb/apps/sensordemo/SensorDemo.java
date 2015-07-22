/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensordemo;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.GridLayout;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;

import javax.security.auth.login.LoginException;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import prefuse.action.assignment.ColorAction;
import prefuse.visual.VisualItem;

import com.ibm.gaiandb.draw.Chart;
import com.ibm.gaiandb.draw.ChartLegend;
import com.ibm.gaiandb.draw.DatabaseDiagram;
import com.ibm.gaiandb.draw.Graph;
import com.ibm.gaiandb.draw.NodeGraph;
import com.ibm.gaiandb.draw.ReservedColorAction;
import com.ibm.gaiandb.apps.DBConnector;

/**
 * Draw sensor monitor data coming from any number of PCs.
 * Data includes CPU usage, memory usage, temperature, acceleration,
 * disk and network I/O and battery levels.
 * 
 * This class will create a window which draws chart representations
 * of the data, refreshing every three seconds.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public final class SensorDemo extends DBConnector {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/** The configuration file. Used for custom graphs. */
	private static final String configFile = "demoPCM.conf";

	/** The directories to check for the config file. */
	private static final String[] configDirectories = { ".", "demoPCM" };

	/** The colors to use in each diagram. */
	private static final int[] colorPalette = {
		0xFF3333FF, // blue
		0xFFFF0000, // red
		0xFF00FF00, // green
		0xFFFF6600, // orange
		0xFFCC00CC, // purple
		0xFFFFFF00, // yellow
		0xFF9999FF, // cyan
		0xFFFFAACC  // pink
	};

	/** The maximum number of tries per diagram before we give up. */
	private static final int maxTries = 3;

	/** The window width. */
	private static final int windowWidth = 640;

	/** The window height. */
	private static final int windowHeight = 480;

	/** The height of each graph title. */
	private static final int titleHeight = 20;

	/** The width of each diagram. */
	private int diagramWidth;

	/** The height of each diagram. */
	private int diagramHeight;

	/** The diagrams. */
	private DatabaseDiagram[] diagrams;

	/** The original titles of the diagrams, used when we wish to alter them. */
	private Map<DatabaseDiagram, String> titles = new Hashtable<DatabaseDiagram, String>();

	/** The diagram title labels. */
	private Map<DatabaseDiagram, JLabel> titleLabels = new Hashtable<DatabaseDiagram, JLabel>();

	/** Tries left before we give up on the diagram. */
	private Map<DatabaseDiagram, Integer> triesLeft = new Hashtable<DatabaseDiagram, Integer>();

	/** The interval between refreshes, in milliseconds. */
	private int refreshRateMs = 3000;

	/**
	 * Creates a new <code>SensorDemo</code> and begins drawing graphs.
	 * 
	 * @param args
	 *            The program arguments.
	 * @throws InterruptedException
	 *             if the thread is interrupted while sleeping.
	 */
	public static void main(String[] args) throws InterruptedException {
		SensorDemo app;
		try {
			app = new SensorDemo(args);
		}
		catch (Exception e) {
			terminate(e.getMessage());
			return;
		}

		app.load();
		app.createWindow();
		app.updateDiagrams();
	}

	/**
	 * Initializes the demo by connecting to the database using the arguments
	 * provided.
	 * 
	 * @param args
	 *            An array containing, in order, the JDBC URL, username, and
	 *            password.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 * @throws InterruptedException
	 *             if an interrupt occurs while sleeping between connection
	 *             attempts.
	 * @throws LoginException 
	 */
	public SensorDemo(String[] args) throws ClassNotFoundException, InterruptedException, LoginException {
		super(args);
		connect();
	}

	/**
	 * Reads in the configuration file if there is one, then creates the graphs
	 * using the <code>GraphLoader</code>. Handles any custom settings.
	 */
	public void load() {
		// Stop annoying prefuse log messages from showing up.
		Logger.getLogger("prefuse").setLevel(Level.WARNING);

		// Load the configuration file from wherever it might be.
		Properties customProperties = new Properties();
		for (String directory : configDirectories) {
			try {
				customProperties.load(new FileInputStream(directory + "/" + configFile));
				break;
			}
			catch (Exception e) {
				// Doesn't exist. Let's try the next one.
			}
		}

		// Load the graphs. The GraphLoader does the heavy lifting here.
		GraphLoader loader = new GraphLoader(conn, customProperties);
		diagrams = loader.load();
		if (diagrams.length == 0) {
			System.err.println("No usable graphs. Terminating.");
			System.exit(1);
		}

		// Initialise the diagrams.
		for (DatabaseDiagram diagram : diagrams) {
			titles.put(diagram, diagram.getTitle());
			triesLeft.put(diagram, maxTries);

			if (diagram instanceof ChartLegend) {
				((Chart)diagram).setColorAction(new ReservedColorAction<String>("names",
					DatabaseDiagram.GROUP, "NODE_NAME", VisualItem.FILLCOLOR, colorPalette));
			}
			else if (diagram instanceof Chart) {
				((Chart)diagram).setColorAction(new ReservedColorAction<String>("names",
					DatabaseDiagram.GROUP, "NODE", VisualItem.STROKECOLOR, colorPalette));
			}
			else if (diagram instanceof Graph) {
				((Graph)diagram).setNodeColorAction(new ReservedColorAction<String>("names",
					Graph.NODE_GROUP, "NODE_NAME", VisualItem.FILLCOLOR, colorPalette));
				((Graph)diagram).setEdgeColorAction(new ColorAction(
					Graph.EDGE_GROUP, VisualItem.STROKECOLOR, 0xFFCCCCCC));
			}
		}

		// Process any custom settings.
		String refreshRate = loader.get("refresh_rate");
		if (refreshRate.length() > 0) {
			try {
				refreshRateMs = Integer.parseInt(refreshRate);
			}
			catch (NumberFormatException e) {
				System.err.println("Could not process a refresh rate of " + refreshRate + ".");
				// Go with the default.
			}
		}
	}

	/**
	 * Creates a window and adds each diagram to it, then initialises the
	 * diagrams.
	 */
	public void createWindow() {
		// System look and feel would be nice, but it's not necessary.
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		}
		catch (Exception e) {
			System.err.println("Cannot use the system's native look and feel.");
		}

		// Calculate the grid size once.
		final int rows, columns;
		// Make it a square to begin with.
		columns = (int)Math.ceil(Math.sqrt(diagrams.length));
		// If we can drop a row, do it.
		if ((columns - 1) * columns >= diagrams.length) {
			rows = columns - 1;
		}
		else {
			rows = columns;
		}

		// Set the size of each diagram.
		diagramWidth = windowWidth / rows;
		diagramHeight = windowHeight / columns - titleHeight;

		// Create the window.
		final JFrame frame = new JFrame("GaianDB Demo :: System Statistics");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setPreferredSize(new Dimension(windowWidth, windowHeight));
		frame.setLocationByPlatform(true);

		// The default Java font looks a little poor. Let's use something native.
		String[] fonts = GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames();
		Font labelFont;
		if (Arrays.binarySearch(fonts, "Segoe UI") >= 0) {
			labelFont = new Font("Segoe UI", Font.PLAIN, 12);
		}
		else if (Arrays.binarySearch(fonts, "Calibri") >= 0) {
			labelFont = new Font("Calibri", Font.PLAIN, 12);
		}
		else {
			labelFont = new Font("SansSerif", Font.PLAIN, 12);
		}
		frame.setFont(labelFont);

		// JFrames can't have backgrounds, so we use a panel instead.
		JPanel panel = new JPanel(new GridLayout(rows, columns, 10, 10));
		panel.setBackground(Color.white);
		panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
		frame.add(panel);

		// Add the diagrams to the panel.
		for (DatabaseDiagram diagram : diagrams) {
			panel.add(createPanel(diagram));
		}

		// Tell the diagrams to resize with the window.
		frame.addComponentListener(new ComponentAdapter() {
			public void componentResized(ComponentEvent e) {
				resizeDiagrams(frame, rows, columns);
			}
		});

		for (final DatabaseDiagram diagram : diagrams) {
			try {
				if (diagram instanceof Chart) {
					((Chart)diagram).populateTable(((Chart)diagram).getMaxDuration());
				}
				else if (diagram instanceof Graph) {
					((Graph)diagram).populateGraph();
					if (diagram instanceof NodeGraph) {
						new Thread() {
							public void run() {
								while (true) {
									String currentItem = ((NodeGraph)diagram).getCurrentItemName();
					    			titleLabels.get(diagram).setText(
					    				titles.get(diagram) +
					    				" (" + ((NodeGraph)diagram).getNodeCount() + ")" +
					    				(null != currentItem
					    					? "     Current Node: " + currentItem
					    					: ""));
	
					    			try {
		                                Thread.sleep(100);
	                                }
	                                catch (InterruptedException e) {
	                                	return;
	                                }
								}
							}
						}.start();
					}
				}
			}
			catch (SQLException e) {
				System.err.println("Populating \"" + diagram.getTitle() + "\" failed due to a " + e.getClass().getName() + ": " + e.getMessage());
				triesLeft.put(diagram, 0);
			}
		}

		// Boom. Let's go.
		frame.pack();
		frame.validate();
		frame.setVisible(true);
	}

	/**
	 * Resizes the diagrams to fit perfectly within the window.
	 * 
	 * @param frame
	 *            The window.
	 * @param rows
	 *            The number of rows.
	 * @param columns
	 *            The number of columns.
	 */
	public void resizeDiagrams(JFrame frame, int rows, int columns) {
		diagramWidth = frame.getWidth() / rows;
		diagramHeight = frame.getHeight() / columns - titleHeight;

    	for (DatabaseDiagram diagram : diagrams) {
			diagram.setPreferredSize(new Dimension(diagramWidth, diagramHeight));
    	}
	}

	/**
	 * Repeatedly loops through the diagrams and updates them, pausing between
	 * each one.
	 * 
	 * @throws InterruptedException if the thread is interrupted while sleeping.
	 */
	public void updateDiagrams() throws InterruptedException {
		System.out.println("Executing SQL queries every " + refreshRateMs + "ms.\n");

		// Start passing the prepared statements to the frame.
		// The frame will execute it and update accordingly.
		Thread.sleep(refreshRateMs);
		for (int i = 0; ; i = (i + 1) % diagrams.length) {
			final DatabaseDiagram diagram = diagrams[i];
			if (triesLeft.get(diagram) > 0) {
				// UI stuff goes in UI thread. It's easier that way.
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						try {
							if (diagram instanceof Chart) {
								((Chart)diagram).updateTable(refreshRateMs / 1000 + 1);
							}
							else if (diagram instanceof Graph) {
								((Graph)diagram).updateGraph();
							}

							triesLeft.put(diagram, maxTries);
						}
						catch (SQLException e) {
							System.err.println("Updating \"" + diagram.getTitle() + "\" failed due to a " + e.getClass().getName() + ": " + e.getMessage());
							triesLeft.put(diagram, triesLeft.get(diagram) - 1);
						}
					}
				});
			}

			Thread.sleep(refreshRateMs / diagrams.length);
		}
	}

	/**
	 * Creates a panel for the diagram provided and its title to reside in.
	 * 
	 * @param diagram
	 *            The diagram to place within the panel.
	 * @param labelFont
	 *            The font to use for the title.
	 * 
	 * @return A new panel.
	 */
	private JPanel createPanel(DatabaseDiagram diagram) {
		// Create the panel.
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
		panel.setBackground(Color.white);
		panel.setPreferredSize(new Dimension(diagramWidth, diagramHeight + titleHeight));

		// Create the title.
		JLabel titleLabel = new JLabel(diagram.getTitle());
		titleLabel.setSize(diagramWidth, titleHeight);
		titleLabel.setHorizontalTextPosition(SwingConstants.CENTER);
		titleLabels.put(diagram, titleLabel);

		// Resize the diagram.
		diagram.setPreferredSize(new Dimension(diagramWidth, diagramHeight));

		panel.add(titleLabel);
		panel.add(diagram);

		panel.validate();
		return panel;
	}
}
