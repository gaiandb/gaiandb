/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml.schema;

/**
 * Interface for the anchor class for the Row, which will be used in policy evaluation.
 * <P>
 * This interface is used in SPL policies for retrieving the columns of a logical table
 * based on their name, to be used for policy decisions and actions (for obligation policies).
 * To make use of the methods of this anchor class in SPL policies, the following import needs
 * to be included in the policy:
 * <P>
 * <code>Import  Class com.ibm.watson.pml.pfg.schema.IRow:row;</code>
 * <P>
 * Then, columns included in the <code>row</code> object can be accessed from the SPL policy
 * through the methods of this interface.
 * 
 * @author pzerfos@us.ibm.com
 *
 */
public interface IRow {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/**
	 * Search for a column in the logical table that was be specified in the query and return the
	 * representation of that column ({@link IColumn}) for further accessing the cell's contents.
	 * 
	 * @param name a string for the name of the column as specified in the logical table
	 * @return an {@link IColumn} object with the column of that name, or an empty object if not found
	 */
	public IColumn getColumnByName(String name);
	
	/**
	 * Check for existence of a column by the specified name in the {@link com.ibm.gaiandb.plugins.wpml.schema#IRow} object.
	 * The column must have been included in the query in order to exist in the logical row.
	 * 
	 * @param columnName a string with the name of the column to be checked for existence
	 * 
	 * @return <code>true</code> if the column exists in the row, <code>false</code> otherwise
	 */
	public boolean hasColumn(String columnName);
	
	/**
	 * Return the index of this row in the sequence of filtered rows
	 * 
	 * @return
	 */
	public int getIndex();
	
}
