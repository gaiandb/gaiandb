/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.lite;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;

import com.ibm.db2j.GaianQuery;
import com.ibm.db2j.GaianTable;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;

/**
 * A statement used to map a "table" into a "result".
 * GaianTable, GaianQuery and indeed other VTI implementations have an ordered list of columns they expose just like
 * any physical database table would. However, this LiteGaianStatement is instantiated as a JDBC statement which is executed
 * then used to expose a "result" whose columns need to represent the result columns specified in the SQL query.
 * Therefore, we have to map the column results from the tableMetaData structure into the resultMetaData structure at extraction time.
 * 
 * Furthermore, there may also be result columns that are expressed in the SQl as compound expressions which may or not include
 * any number of columns from the logical table, as well as functions and arithmetic.
 * 
 * @author DavidVyvyan
 *
 */

public class LiteGaianStatement extends GaianTable implements TableResultExtractor {

	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	private static final Logger logger = new Logger( "LiteGaianStatement", 30 );
	
	private DataValueDescriptor[] rowTemplateForExposedColumns = null; // table row sent down to the GaianVTI at fetch time before being reduced to a result row
	private GaianResultSetMetaData tableMetaData = null; // meta data for all columns in the table
	private GaianResultSetMetaData resultMetaData = null; // meta data for all cols of the query result
	private int[] resultColumns = null; // subset of table columns - expected as result of query by the calling code
	
	@Override
	public GaianResultSetMetaData getTableMetaData() throws SQLException { return tableMetaData; }
	
	public void setResultColumns(int[] resultColumns) { this.resultColumns = resultColumns; }
	
	public LiteGaianStatement() throws Exception {}
	
	public LiteGaianStatement(String logicalTable, String tableArguments) throws Exception {
		this(logicalTable, tableArguments, null, null);
	}
	
	public LiteGaianStatement(String logicalTable, String tableArguments, String tableDefinition, String forwardingNode) throws Exception {
		super(logicalTable, tableArguments, tableDefinition, forwardingNode);
		initialise( super.getMetaData() );
	}

	private void initialise( GaianResultSetMetaData tableMetaData ) throws SQLException {
		this.tableMetaData = tableMetaData;
		DataValueDescriptor[] temp = tableMetaData.getRowTemplate();
		rowTemplateForExposedColumns = new DataValueDescriptor[temp.length];
		for ( int i=0; i<rowTemplateForExposedColumns.length; i++ )
			rowTemplateForExposedColumns[i] = temp[i].getNewNull();

		logger.logThreadInfo("Resolved vtiRow from tableMetaData: " + tableMetaData + ", template: " + Arrays.asList(temp));
	}
	
	@Override
	public GaianResultSetMetaData getMetaData() throws SQLException {

		if ( null == resultMetaData ) {			
			if ( null == resultColumns )
				resultMetaData = tableMetaData; // all cols selected in query
			else {
				StringBuffer sb = new StringBuffer();
				if ( 0 < resultColumns.length )
					sb.append( tableMetaData.getColumnDescription(resultColumns[0]) );
				for ( int i=1; i<resultColumns.length; i++ )
					sb.append( ", " + tableMetaData.getColumnDescription(resultColumns[i]) );
				
				try {
					resultMetaData = new GaianResultSetMetaData(sb.toString());
				} catch (Exception e) {	throw new SQLException(e); }

				logger.logThreadInfo("Resolved resultMetaData: " + resultMetaData);
			}
		}
		
		return resultMetaData;
	}
	
	@Override
    public int nextRow( DataValueDescriptor[] resultRow ) throws StandardException, SQLException {
		return null == resultColumns ? super.nextRow(resultRow) : getResultRow( super.nextRow(rowTemplateForExposedColumns), resultRow );
    }
	
	private int getResultRow( int rc, DataValueDescriptor[] resultRow ) throws StandardException {
		
		if ( IFastPath.SCAN_COMPLETED != rc )
			for ( int i=0; i<resultColumns.length; i++ )
				resultRow[i].setValue( rowTemplateForExposedColumns[ resultColumns[i]-1 ] );
		
		return rc;
	}

	public class LiteGaianQueryStatement extends GaianQuery implements TableResultExtractor {

		public LiteGaianQueryStatement(String sqlQuery, String tableArguments, String queryArguments, String tableDef, String forwardingNode)
				throws Exception {
			super(sqlQuery, tableArguments, queryArguments, tableDef, forwardingNode);
			initialise( super.getMetaData() );
		}

		@Override
		public GaianResultSetMetaData getMetaData() throws SQLException {
			return LiteGaianStatement.this.getMetaData();
		}

		@Override
	    public int nextRow( DataValueDescriptor[] resultRow ) throws StandardException, SQLException {
			return null == resultColumns ? super.nextRow(resultRow) : getResultRow( super.nextRow(rowTemplateForExposedColumns), resultRow );
	    }
		
		@Override
		public GaianResultSetMetaData getTableMetaData() throws SQLException {
			return LiteGaianStatement.this.getTableMetaData();
		}

		public void setResultColumns(int[] resultColumns) {
			LiteGaianStatement.this.resultColumns = resultColumns;
		}
	}
}
