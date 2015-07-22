/* Licensed Materials - Property of IBM
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import prefuse.action.assignment.ColorAction;
import prefuse.data.Table;
import prefuse.visual.VisualTable;

/**
 * Represents a chart, which in reality is just a visual frontend for a prefuse
 * {@link prefuse.data.Table Table} structure. Retrieves data from a prepared
 * SQL statement and renders it according to the subclass.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public abstract class Chart extends DatabaseDiagram {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 6717483758565644169L;

	/** Stores the data to be rendered in the chart. */
	protected Table data;

	/**
	 * A version of {@link #data} that provides access to graphical and visual
	 * properties.
	 */
	protected VisualTable vt;

	/**
	 * The prepared statement that is executed on every update. It may have
	 * unbound parameters - these are set on each update via arguments to
	 * {@link #populateTable} and {@link #updateTable}.
	 */
	protected final PreparedStatement statement;

	/**
	 * The first parameter of {@link #statement} to be set when updating the
	 * table through {@link #populateTable} or {@link #updateTable}. Any
	 * parameter before this will be set once in the constructor, and then
	 * left alone.
	 */
	protected final int firstStatementVar;

	/**
	 * A set of field names that can be used to compare two rows in {@link data}
	 * for row equality. Used to decide whether to add a row or update an
	 * existing one. The keys are set by the concrete class.
	 */
	protected final Set<String> keys = new HashSet<String>();

	/**
	 * The maximum duration the data in the chart should be left alive.
	 * Implementation (or lack thereof) is left to the concrete subclass.
	 */
	protected int maxDuration = 60;

	/**
	 * Retrieves the maximum time the data in the chart should be left alive.
	 * 
	 * @return The maximum duration of the data in the chart, in seconds.
	 */
	public int getMaxDuration() { return maxDuration; }

	/**
	 * Sets a new maximum for the lifetime of data in the chart.
	 * 
	 * @param maxDuration
	 *            The maximum duration of the data in the chart, in seconds.
	 */
	public void setMaxDuration(int maxDuration) {
		if (maxDuration > 0) {
			this.maxDuration = maxDuration;
		}
	}

	/**
	 * Used by prefuse to determine the chart colors.
	 */
	protected ColorAction colorAction;

	/**
	 * Retrieves the diagram's color action. Color actions are used by prefuse
	 * to determine the colors used in the chart.
	 * 
	 * @return A prefuse color action.
	 */
	public ColorAction getColorAction() {
		return colorAction;
	}

	/**
	 * Sets the new color action. Color actions are used by prefuse to determine
	 * the colors used in the chart.
	 * 
	 * @param colorAction
	 *            The new color action.
	 */
	public void setColorAction(ColorAction colorAction) {
		this.colorAction = colorAction;
	}

	/**
	 * Creates a new chart, using the provided <code>PreparedStatement</code> as
	 * the method of data retrieval. The parameters are added to the prepared
	 * statement as permanent bound objects. Any unbound parameters are bound on
	 * each update via {@link #populateTable} or {@link #updateTable} for the
	 * lifecycle of the update.
	 * 
	 * @param statement
	 *            The prepared statement to use when retrieving data for
	 *            updates.
	 * @param params
	 *            The parameters to be permanently bound to the prepared
	 *            statement.
	 * @throws SQLException
	 *             if binding parameters fails.
	 */
	public Chart(PreparedStatement statement, Object... params) throws SQLException {
		super();
		this.statement = statement;
		firstStatementVar = TableUtil.bindToPreparedStatement(statement, params);
	}

	/**
	 * This method binds the parameters provided to the prepared statement,
	 * executes it and creates a new data structure from the result. It then
	 * creates a new visualization and prepares it for rendering.
	 * 
	 * @param params
	 *            The parameters to be bound to the prepared statement. These
	 *            will not overwrite those bound in the constructor, but will be
	 *            appended to the end of them.
	 */
	public synchronized void populateTable(Object... params) throws SQLException {
		TableUtil.bindToPreparedStatement(statement, params, firstStatementVar);
		ResultSet resultSet = statement.executeQuery();

		beforeUpdate();
		data = TableUtil.addResultSetToTable(resultSet, null, handler, keys);
		afterUpdate();

		prepareVisualization();
	}

	/**
	 * This method binds the parameters provided to the prepared statement,
	 * executes it and adds the result to its internal data structure. It then
	 * updates and redraws the visualization.
	 * 
	 * @param params
	 *            The parameters to be bound to the prepared statement. These
	 *            will not overwrite those bound in the constructor, but will be
	 *            appended to the end of them.
	 */
	public synchronized void updateTable(Object... params) throws SQLException {
		if (null == data) {
			populateTable(params);
			return;
		}

		synchronized (data) {
			TableUtil.bindToPreparedStatement(statement, params, firstStatementVar);
			ResultSet resultSet = statement.executeQuery();

			beforeUpdate();
			TableUtil.addResultSetToTable(resultSet, data, handler, keys);
			afterUpdate();
		}

		updateVisualization();
	}

	/**
	 * An overridable method run before each update.
	 * 
	 * @throws SQLException
	 * 
	 * @see #afterUpdate
	 */
	protected void beforeUpdate() throws SQLException { }

	/**
	 * An overridable method run after each update.
	 * 
	 * @throws SQLException
	 * 
	 * @see #beforeUpdate
	 */
	protected void afterUpdate() throws SQLException { }

	public void prepareVisualization() {
		if (null == data || 0 == data.getRowCount()) {
			preparation = PreparationStages.UNPREPARED;
			return;
		}

		super.prepareVisualization();
	}

	public void updateVisualization() {
		if (null == data || 0 == data.getRowCount()) {
			return;
		}

		super.updateVisualization();
	}
}
