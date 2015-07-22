/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml.schema;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * The class that implements the column objects of the managed runtime, which
 * are accessed through the {@link IRow} interface. Columns are typically retrieved
 * by name using the {@link com.ibm.gaiandb.plugins.wpml.schema.IRow#getColumnByName(String)}
 * method.
 * 
 * @author pzerfos@us.ibm.com
 *
 */
public class Column implements IColumn {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/**
	 * The name of the column as it appears in the result set meta data
	 */
	private String columnName = null;
	
	/**
	 * The index of the column if the column is queried
	 */
	private int columnNumber = 0;
	
	/**
	 * The data of the cell returned as a Derby {@link DataValueDescriptor} object
	 */
	private DataValueDescriptor columnData = null;

	/**
	 * Constructor
	 * 
	 * @param colName the string with the same of the column as retrieved from the result set meta data
	 * @param colNo the index of the column in the logical table
	 * @param colData a {@link #DataValueDescriptor} object with the data of the cell of that column
	 */
	public Column(String colName, int colNo, DataValueDescriptor colData) {
		columnName = colName;
		columnNumber = colNo;
		columnData = colData;
	}
	
	public Column(String colName, int colNo) {
		this(colName, colNo, null);
	}
	
	public Column(String colName) {
		this(colName, 0, null);
	}
	
	public String getName() {
		return (columnName == null ? "" : columnName);
	}

	public DataValueDescriptor getCellData() {
		return columnData;
	}
	
	public int getColumnNumber() {
		return columnNumber;
	}
	
	public void setColumnCellData(DataValueDescriptor cellDesc) {
		columnData = cellDesc;
	}
	
}
