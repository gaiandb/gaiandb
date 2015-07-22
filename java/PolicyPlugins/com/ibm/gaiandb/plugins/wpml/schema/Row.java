/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml.schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;


//import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * The anchor class for the Row, that will be passed for evaluation to the ObjectPEP.
 * Provides functions for referencing columns of a row by name, as well as setting 
 * the cell data upon each call of {@link #setRowData(DataValueDescriptor[])} by
 * {@link com.ibm.gaiandb.plugins.wpml.PolicyEnabledFilter#filterRow(DataValueDescriptor[])}
 * 
 * @author pzerfos@us.ibm.com
 *
 */
public class Row implements IRow {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/**
	 * Map between the column name and {@link Column} objects
	 */
	private HashMap<String, Column> columnsMap = null;
	
	/**
	 * Map between column number that is queried and the column name
	 */
	private HashMap<Integer, String> columnsNumberMap = null;
	
	/**
	 * an array with a map of the columns that are queried
	 */
	private int[] queriedColumns;
	
	private int rowIndex;
	
	public void setRowIndex( int idx ) {
		rowIndex = idx;
	}
	

	/**
	 * Construct the {@link Row} object for policy evaluation using only
	 * columns that have been queried. It instantiates and populates the
	 * maps for finding the queries columns of the row

	 * @param logicalTableRSMD a {@link ResultSetMetaData} object with meta data information on the columns
	 * @param queriedColumns an array with columns that are actually queried
	 */
	public Row(ResultSetMetaData logicalTableRSMD, int[] queriedColumns) {
		
		if (logicalTableRSMD == null) {
			System.err.println("ERROR: PFG: cannot obtain meta data information on columns"); 
			return;
		}
		
		// Create the mapping between column names and column objects
		columnsMap = new HashMap<String, Column>();
		columnsNumberMap = new HashMap<Integer, String>();
		
		// Store in the Row only the columns that are queried
		for (int i = 0; i < queriedColumns.length; i++) {
			int columnNumber = queriedColumns[i];
			String colName;
			try {
				colName = logicalTableRSMD.getColumnName(columnNumber);
				Column col = new Column(colName, columnNumber);
				columnsMap.put(colName, col);
				columnsNumberMap.put(columnNumber, colName);
			} catch (SQLException e) {
				System.err.println("ERROR: PFG: could not retrieve column name for column " + columnNumber + " : " + 
						e.getMessage());
				System.exit(1);
			}
		}
		
		this.queriedColumns = queriedColumns;
	}

	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.schema.IRow#getColumn(java.lang.String)
	 */
	public IColumn getColumnByName(String name) {
		IColumn c = null;
		
//		System.out.println("getColumnByName " + name);
		
		/*
		 * If no column name has been specified, return an empty IColumn object
		 */
		if (name == null)
			return new Column("", -1);
		
		if (columnsMap.containsKey(name))
			c =  columnsMap.get(name);
		if (c == null) {
			System.out.println("getColumnByName() null");
			return new Column("", -1);
		}
		
		return c;
	}

	/* (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.schema.IRow#getColumns()
	 */
	public ArrayList<IColumn> getColumns() {
		ArrayList<IColumn> arrayListCols = new ArrayList<IColumn>(columnsMap.values());
		return arrayListCols;
	}
	
	/**
	 * Set the DataValueDescriptors <i>only</i> for those columns in the record template
	 * that have been queried and are thus populated with data from the result set
	 * 
	 * @param record the array of DataValueDescriptors, one for each column of the row
	 */
	public void setRowData(DataValueDescriptor[] record) {
		
		/*
		 * Return if there was an error with the data in the record
		 */
		if (record == null)
			return;
		
		for (int i = 0; i < queriedColumns.length; i++) {
			int columnNumber = queriedColumns[i];
			String colName = columnsNumberMap.get(columnNumber);
			columnsMap.get(colName).setColumnCellData(record[columnNumber - 1]);
		}
	}
	
	
	
	/*
	 * (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.schema.IRow#hasColumn(java.lang.String)
	 */
	public boolean hasColumn(String columnName) {
		
//		System.out.println("HasColumn " + columnName + " : " + columnsMap.get(columnName) + " :: " + columnsMap.containsKey(columnName));
		
		return columnsMap.containsKey(columnName);
//		
//		IColumn column = null;
//		
//		column = columnsMap.get(columnName);
//		if (column == null)
//			return false;
//		else
//			return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.watson.pml.pfg.schema.IRow#getTypeColumn()
	 */
	public IColumn getTypeColumn() {
		return getColumnByName("TYPE");
	}
	
	public int getIndex() {
		return rowIndex;
	}
	
//	private String user = null;
//	public void setUser( String uid ) {
//		user = uid;
//	}
//
//	public boolean isIdentifiedAs(String uid) {
//		return null == user ? false : user.equals(uid);
//	}
	
}
