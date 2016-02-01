/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.vti;

import com.ibm.db2j.AbstractVTI;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.Util;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;

/**
 * The purpose of this sample class is to help understand the structure of a VTI and when each method is called in the query life cycle.
 * Each method logs a message to gaiandb.log when it is invoked by Gaian, and each message is prefixed with the timestamp and class name.
 * The messages will appear in gaiandb.log as long as you set LOGLEVEL appropriately, e.g. one of the following:
 * call setloglevel('MORE')
 * call setloglevel('ALL')
 * 
 * The VTI just returns 1 record with 1 column value holding a random greeting message
 * 
 * @author DavidVyvyan
 */

public class SampleHelloWorldVTI extends AbstractVTI {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";

	// This is Gaian's Logger class - Use an instance of this to merge the VTI's logs with Gaian's.
	// The 2nd argument is cosmetic and determines the indentation for its messages in the log file.
	// Indentation is usually made larger for deeper levels of Java stack nesting.
	private static final Logger logger = new Logger( "SampleHelloWorldVTI", 30 );
	
	private long rowCount = 0;
	private Qualifier[][] qualifiers = null; // Conjunctive-normal-form structure representing "where-clause" conditions on columns - See Derby docs
	private int[] projectedColumns = null; // Array of column indexes (1-based) selected in the query from this VTI - See Derby doc
	
	private String message;
	private static final String[] words1 = new String[] { "Hello", "Greetings", "Welcome", "Hi", "Hey" };
	private static final String[] words2 = new String[] { "World", "Earth", "People", "Moon", "Sky" };
	
	private String vtiFirstArgumentFromGaianConfigFile = null;
	
	/**
	 * The args passed in here are the values set against the '_ARGS' property (in gaiandb_config.properties) of the wrapping data source
	 */	
	public SampleHelloWorldVTI(String vtiArgs) throws Exception {
		super(vtiArgs, "SamplePluralizedVTI");
		logger.logThreadInfo("Exiting SampleHelloWorldVTI() constructor.");
		// Use AbstractVTI.getPrefix() to get the first argument -
		vtiFirstArgumentFromGaianConfigFile = getPrefix();
		
		// Note: The 'prefix' argument is also used to resolve static VTI properties relative to the Class name in gaiandb_config.properties
		// e.g: SampleHelloWorldVTI.prefixArg.staticPropertyURL=<URL>
		// The <URL> property above would be retrieved using:
		// String url = AbtsractVTI.getProperty('staticPropertyURL')
	}
	
	/**
	 * Method getMetaData() gives the VTI's table schema, i.e. number of columns, their types, names, sizes etc.
	 * It may sometimes be appropriate to deduce this from the first row of data..
	 * This method is always called by the querying engine (Gaian or Derby) *before* query execution.
	 * If this method is not implemented, Gaian will try to resolve the schema based off a 'schema' property for this VTI in
	 * gaiandb_config.properties, or failing that it will resolve it based on the schema of the logical table.
	 */
	@Override public GaianResultSetMetaData getMetaData() throws SQLException {
		logger.logThreadInfo("Entered getMetaData()");
//		return super.getMetaData();
		// Alternate method for enforcing a schema in the VTI - Use GaianResultSetMetaData: 
		try { return new GaianResultSetMetaData("MSG VARCHAR(20)"); }
		catch (Exception e) { throw new SQLException("Unable to build basic table structure for SampleHelloWorldVTI", e); }
	}
	
	/**
	 * The args[] passed here contain all sorts of query context fields - Take a look in gaiandb.log to see them all
	 */
	@Override public void setArgs(String[] args) throws Exception {
		logger.logThreadInfo("Entered setArgs(), args: " + Arrays.asList(args) );
	};
	
	/**
	 * This is called by Gaian to specify the list of columns that are queried
	 */
	@Override public boolean pushProjection(VTIEnvironment arg0, int[] projectedColumns) throws SQLException {
		logger.logThreadInfo("Entered pushProjection(), projectedColumns: " + Util.intArrayAsString(projectedColumns));
		this.projectedColumns = projectedColumns;
		return true;
	}
	
	/**
	 * IQualifyable interface - used to process predicates against our columns -
	 */
	@Override public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qualifiers) throws SQLException {
		logger.logThreadInfo("Entered setQualifiers(), qualifiers: " + RowsFilter.reconstructSQLWhereClause(qualifiers));
		this.qualifiers = qualifiers;
	}
	
	/**
	 * This method is called after the ones above and computes the result.
	 * Must always return true - this tells Derby to use the IFastPath interface method nextRow() to extract records, rather
	 * than expecting it to call the VTI's PreparedStatement.executeQuery() method to execute as a regular ResultSet VTI.
	 */
	@Override public boolean executeAsFastPath() throws StandardException, SQLException {
		logger.logThreadInfo("Entered executeAsFastPath()");
		
		if ( null != projectedColumns && 1 != projectedColumns[0] ) {
			logger.logInfo("projectedColumns do not include the MESSAGE column - aborting query");
			return true;
		}
		
		int i1 = (int) (System.nanoTime()/1000 % words1.length);
		try { Thread.sleep(10); } catch (Exception e) {}
		int i2 = (int) (System.nanoTime()/1000 % words2.length);
		String middleText = null == vtiFirstArgumentFromGaianConfigFile ? " " : ' ' + vtiFirstArgumentFromGaianConfigFile + ' ';
		
		message = words1[i1] + middleText + words2[i2] + '!';
		
		return true;
	}
	
	/**
	 * Gaian extracts rows by calling this method repeatedly
	 */
	@Override public int nextRow(DataValueDescriptor[] row) throws StandardException, SQLException {
		logger.logDetail("Entered nextRow(), row.length: " + row.length );
		
		if ( 0 < rowCount++ ) return IFastPath.SCAN_COMPLETED;
		
		row[0] = new SQLChar( message );
		
		if ( true == RowsFilter.testQualifiers( row, qualifiers ) ) return IFastPath.GOT_ROW;
		else return IFastPath.SCAN_COMPLETED;
	}
	
	/**
	 * Clears the query objects and returns true
	 */
	@Override public boolean reinitialise() {
		logger.logThreadInfo("Entered reinitialise()");
		rowCount = 0;
		return true;
	}
	
	@Override public int getRowCount() throws Exception { return (int) rowCount; }

	/**
	 * Use this method to empty local heap resources for this instance and set them to null if possible
	 * This method is called when this VTI instance is discarded (e.g. if it stops being referenced as a data source wrapper)
	 */
	@Override public void close() throws SQLException { super.close(); }
	
	@Override public boolean isBeforeFirst() throws SQLException { return 0 == rowCount; }
	
	/**
	 * VTICosting methods - used for JOIN optimisations
	 */
	@Override public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException { return false; }
}
