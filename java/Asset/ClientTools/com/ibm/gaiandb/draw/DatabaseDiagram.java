/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import javax.swing.SwingUtilities;

import prefuse.Display;
import prefuse.Visualization;
import prefuse.data.io.sql.DefaultSQLDataHandler;
import prefuse.data.io.sql.SQLDataHandler;

/**
 * An abstract class representing a prefuse diagram that retrieves its data from
 * a prepared SQL statement.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public abstract class DatabaseDiagram extends Display {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final long serialVersionUID = -139709993864391362L;

	/**
	 * The name of the prefuse visualization group holding our data.
	 */
	public static final String GROUP = "data";

	/**
	 * The diagram title.
	 */
	protected String title = "";

	/**
	 * Retrieves the diagram title.
	 * 
	 * @return The diagram title (empty by default).
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * Sets the diagram title.
	 * 
	 * @param title
	 *            The new diagram title.
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * The stages of preparation.
	 */
	public enum PreparationStages {
		/** The diagram is completely unprepared. */
		UNPREPARED,
		/** The diagram is in the process of being prepared. */
		PREPARING,
		/** The diagram has been prepared. */
		PREPARED
	}

	/**
	 * The visualization's current preparation stage.
	 */
	protected PreparationStages preparation = PreparationStages.UNPREPARED;

	/**
	 * Retrieves the visualization's current preparation stage.
	 * 
	 * @return The current preparation stage.
	 */
	public PreparationStages getPreparationStage() {
		return preparation;
	}

	/**
	 * The prefuse SQL handler. Used for converting SQL data types into Java
	 * data types, and adding a <code>ResultSet</code> row to a prefuse
	 * <code>Table</code>.
	 */
	protected SQLDataHandler handler = new DefaultSQLDataHandler();

	/**
	 * Creates a new database diagram.
	 */
	public DatabaseDiagram() {
		// We initialise with an empty visualisation because this way, if the
		// diagram fails, AWT will render a blank box rather than throwing a
		// NullPointerException.
		super(new Visualization());
	}

	/**
	 * Prepares the visualization by creating objects and prefuse actions.
	 */
	public void prepareVisualization() {
		synchronized (preparation) {
			preparation = PreparationStages.PREPARING;
	
			setVisualization(new Visualization());
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					try {
						prepareVis();
						preparation = PreparationStages.PREPARED;
					}
					catch (IllegalArgumentException e) {
						preparation = PreparationStages.UNPREPARED;
					}
				}
			});
		}
	}

	/**
	 * Does the real visualization preparation work.
	 */
	abstract protected void prepareVis();

	/**
	 * Updates the visualization - rejiggers, redraws and repaints.
	 */
	public void updateVisualization() {
		synchronized (preparation) {
			switch (preparation) {
				case UNPREPARED:
					prepareVisualization();
					break;
				case PREPARED:
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							try {
								updateVis();
							}
							catch (IllegalArgumentException e) {}
						}
					});
					break;
			}
		}
	}

	/**
	 * Does the real visualization update work.
	 */
	abstract protected void updateVis();
}
