/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author DavidVyvyan
 */
public interface GaianChildVTI { //extends IFastPath {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	// Used for passing global query arguments from upper layers - e.g. GaianTable or GaianResult
    public void setArgs( String[] args ) throws Exception;
    
    public void setExtractConditions( Qualifier[][] qualifiers, int[] projectedColumns, int[] physicalColumnsMapping ) throws Exception;
    
    public boolean fetchNextRow( DataValueDescriptor[] row ) throws Exception;
    
//    // Lightweight equivalent returning just the low level types, not wrapped inside DVDs
//    public boolean fetchNextRow( Object[] row ) throws Exception;
    
    public int getRowCount() throws Exception;
    
    public boolean isBeforeFirst() throws SQLException;
    
    public boolean isScrollable();
    
    // Compatibility methods from PreparedStatement and ResultSet
    public ResultSetMetaData getMetaData() throws SQLException;
    public void close() throws SQLException;
    
    /**
     * Reinitialises the VTI and tells us if it is ready for a new execution.
     * Note that when the VTI is invoked directly by Derby, Derby will just re-execute the VTI directly without calling this method.
     * 
     * @return true if the VTI can be re-executed
     * @throws Exception if attempt to reinitialise fails, in which case the VTI cannot be re-executed.
     */
    public boolean reinitialise() throws Exception;
}
