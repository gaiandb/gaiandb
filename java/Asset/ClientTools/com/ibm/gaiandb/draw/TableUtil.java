/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;

import prefuse.data.Schema;
import prefuse.data.Table;
import prefuse.data.io.sql.SQLDataHandler;
import prefuse.util.collections.IntIterator;

/**
 * A bunch of utility functions for dealing with prefuse
 * {@link prefuse.data.Table Table}s.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class TableUtil {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	/** Utility class is static - doesn't need to be instantiated. */
	private TableUtil() { }

	/**
	 * Prints a basic rendition of the table provided to {@link System.out} for
	 * debugging purposes.
	 * 
	 * @param table
	 *            The table to be printed.
	 */
	public static void print(Table table) {
		int cols = table.getColumnCount();

		for (int col = 1; col <= cols; col++) {
			System.out.format(" %-17s |", table.getColumnName(col));
		}
		System.out.println();

		for (int col = 1; col <= cols; col++) {
			System.out.print("--------------------");
		}
		System.out.println();

		IntIterator i = table.rows();
		while (i.hasNext()) {
			int row = i.nextInt();
			for (int col = 1; col <= cols; col++) {
				System.out.format(" %-17s |", table.get(row, col));
			}
			System.out.println();
		}
		System.out.println();
	}

	/**
	 * Binds the parameters provided to the prepared statement.
	 * 
	 * @param statement
	 *            The statement to be bound to.
	 * @param params
	 *            The parameters to be bound.
	 * 
	 * @return The number of items bound + 1, to be used when binding further
	 *         parameters.
	 * 
	 * @throws SQLException
	 *             if {@link PreparedStatement#setObject} fails.
	 */
	public static int bindToPreparedStatement(PreparedStatement statement,
	                                          Object[] params)
	                                         throws SQLException {
		return bindToPreparedStatement(statement, params, 1);
	}

	/**
	 * Binds the parameters provided to the prepared statement, leaving those
	 * before <code>start</code> to remain bound as before. Iterates from
	 * <code>start</code> to <code>start + params.length</code>.
	 * 
	 * @param statement
	 *            The statement to be bound to.
	 * @param params
	 *            The parameters to be bound.
	 * @param start
	 *            The statement parameter index to start from.
	 * 
	 * @return The number of items bound + 1, to be used when binding further
	 *         parameters.
	 * 
	 * @throws SQLException
	 *             if {@link PreparedStatement#setObject} fails.
	 */
	public static int bindToPreparedStatement(PreparedStatement statement,
	                                          Object[] params,
	                                          int start)
	                                         throws SQLException {
		int paramCount = statement.getParameterMetaData().getParameterCount();
		for (Object param : params) {
			if (start > paramCount) {
				break;
			}

			statement.setObject(start++, param);
		}
		return start;
	}

	public static Table addResultSetToTable(PreparedStatement statement,
	                                        Table table,
	                                        SQLDataHandler handler,
	                                        Collection<String> keys)
	                                       throws SQLException {
		return addResultSetToTable(statement.executeQuery(), table, handler, keys);
	}

	/**
	 * <p> Adds a database result set to a prefuse table using a
	 * {@linkplain prefuse.data.io.sql.SQLDatahandler prefuse SQL data handler}.
	 * Uses the list of keys provided to determine whether to add a new row to
	 * the table for a particular row in the result set or whether to update an
	 * old row. </p>
	 * 
	 * <p> If <code>null</code> is passed as the table, a new table is created
	 * and returned.</p>
	 * 
	 * @param resultSet
	 *            The result set to add.
	 * @param table
	 *            The table to add to.
	 * @param handler
	 *            The prefuse SQL data handler to use to add the data.
	 * @param keys
	 *            A list of keys used to determine whether two rows are
	 *            equivalent.
	 * 
	 * @return A new table if none is provided, or the same table passed in if
	 *         one is.
	 * 
	 * @throws SQLException
	 *             if there is an error in processing the result set.
	 */
	public static Table addResultSetToTable(ResultSet resultSet,
	                                        Table table,
	                                        SQLDataHandler handler,
	                                        Collection<String> keys)
	                                       throws SQLException {
		ResultSetMetaData metadata = resultSet.getMetaData();
		if (null == table) {
			table = createSchema(metadata, handler).instantiate();
		}

		synchronized (table) {
			int cols = metadata.getColumnCount();
			while (resultSet.next()) {
				try {
					int row = getRow(resultSet, table, keys);
					for (int i = 1; i <= cols; i++) {
						handler.process(table, row, resultSet, i);
					}
				}
				catch (IllegalArgumentException e) {
					// Ignore it. The row's probably been deleted.
				}
			}
		}

		return table;
	}

	/**
	 * Creates a {@linkplain prefuse.data.Schema prefuse schema} equivalent to
	 * the result set.
	 * 
	 * @param metadata
	 *            The metadata of the result set to create the schema from.
	 * @param handler
	 *            The prefuse SQL data handler which converts SQL data types to
	 *            Java data types.
	 * 
	 * @return A new prefuse schema.
	 * 
	 * @throws SQLException
	 *             if the metadata cannot be processed.
	 */
	public static Schema createSchema(ResultSetMetaData metadata,
	                                  SQLDataHandler handler)
	                                 throws SQLException {
		int columnCount = metadata.getColumnCount();
		Schema schema = new Schema(columnCount);
		for (int i = 1; i <= columnCount; i++) {
			String name = metadata.getColumnName(i);
			Class<?> type = handler.getDataType(name, metadata.getColumnType(i));
			schema.addColumn(name, type);
		}

		return schema;
	}

	/**
	 * Using the keys provided for comparison, returns either an existing
	 * prefuse table row number if the result set row matches one, or a new
	 * prefuse table row number if it does not.
	 * 
	 * @param resultSetRow
	 *            The row of the result set to be compared.
	 * @param table
	 *            The table to be searched.
	 * @param keys
	 *            The names of the fields to be compared.
	 * 
	 * @return A row number corresponding to a row in <code>table</code>.
	 * 
	 * @throws SQLException
	 *             if the result set cannot retrieve an object.
	 */
	private static int getRow(ResultSet resultSetRow,
	                          Table table,
	                          Collection<String> keys)
	                         throws SQLException {
		if (null == keys || 0 == keys.size()) {
			return table.addRow();
		}

		IntIterator rows = table.rows();
		while (rows.hasNext()) {
			int row = rows.nextInt();
			if (rowsAreEqual(table, row, resultSetRow, keys)) {
				return row;
			}
		}

		return table.addRow();
	}

	/**
	 * Compares two rows (one in a prefuse table, and one in a database result
	 * set) using the keys provided.
	 * 
	 * @param table
	 *            The table containing the row.
	 * @param row
	 *            The table row number to be compared.
	 * @param resultSetRow
	 *            The row of the result set to be compared.
	 * @param keys
	 *            The names of the fields to be compared.
	 * 
	 * @return True if the rows are equal according to the keys provided; false
	 *         otherwise.
	 * 
	 * @throws SQLException
	 *             if the result set cannot retrieve an object.
	 */
	private static boolean rowsAreEqual(Table table,
	                                    int row,
	                                    ResultSet resultSetRow,
	                                    Collection<String> keys)
	                                   throws SQLException {
		boolean match = true;
		for (String key : keys) {
			if (!table.get(row, key).equals(resultSetRow.getObject(key))) {
				match = false;
			}
		}

		return match;
	}
}
