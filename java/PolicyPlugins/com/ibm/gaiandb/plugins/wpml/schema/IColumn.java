/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.plugins.wpml.schema;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Interface for the anchor class for the column, which will be used in policy evaluation.
 * <P>
 * This interface is used in SPL policies for retrieving the name and the cell data for
 * a particular column. The data of the cell of a column is represented using a
 * {@link org.apache.derby.iapi.types.DataValueDescriptor} object, and set/get operations
 * can be carried through the DataValueDescriptor's interface.
 * To make use of the methods of this anchor class in SPL policies, the following import needs
 * to be included in the policy:
 * <P>
 * <code>Import  Class com.ibm.watson.pml.pfg.schema.IColumn:column;</code>
 * <P>
 * 
 * @author pzerfos@us.ibm.com
 *
 */
public interface IColumn {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/**
	 * Retrieve the name of an {@link IColumn} object. It can be used to retrieve
	 * the names of the columns that are stored in a {@link #IRow} object
	 * 
	 * @return a string with the name of a column or empty string (not <code>null</code)
	 * if such column was not found
	 */
	public String getName();
	
	/**
	 * Retrieve the cell data from the column (for the particular {@link IRow} of
	 * that column). The data is represented using an {@link DataValueDescriptor}
	 * JDBC object, through which the cell value can be accessed using the getXXX()
	 * methods, or set using the setValue(XXX) family of methods.
	 * 
	 * @return a {@link org.apache.derby.iapi.types.DataValueDescriptor} object with the data
	 * of the cell of that particular column
	 */
	public DataValueDescriptor getCellData();
}
