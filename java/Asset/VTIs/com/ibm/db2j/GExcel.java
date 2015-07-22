/*
 * (C) Copyright IBM Corp. 2010
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * Derby VTI for excel spreadsheets.
 *
 * Are not yet implemented :
 * 
 * - physical/logical column mapping
 * - scrollability 
 *
 * @author lengelle
 */
public class GExcel extends VTI60 implements VTICosting, IFastPath, GaianChildVTI
{
	
	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "GExcel", 30 );
	
	private static final int CELL_WITH_NO_TYPE = -232; // Why did I choose -232 ? Honestly, I don't know..
	private static final int DATE_TYPE = 233; // Excel Poi does not have a specific type for dates
	
	private static final char ARG_SEPARATOR = ',';
	
	private static String DEFAULT_COLUMN_LABEL = "COLUMN";
	
	private Workbook workbook;
	private FormulaEvaluator evaluator;
	private Sheet sheet;
	private InputStream inputStream;
	
	private int numberOfColumns;
	private List<Integer> columnIndexes;
	private int[] columnTypes;
	private List<String> columnNames;
	
	private Qualifier[][] qualifiers;
	private Row currentRow;
	
	private ResultSetMetaData rsmd;
	
//	private CellReference firstCell;
//	private CellReference lastCell;
	
	private int firstColumnIndex;
	private int lastColumnIndex;
	private int firstRowIndex;
	private int lastRowIndex;
	
	private boolean stopScanOnFirstEmptyRow = false;
	
	private boolean firstRowIsMetaData;
	
	/**
	 * Receives in parameter the argument from gaiandb configuration file.
	 * 
	 * @param args
	 * @throws SQLException
	 */
	public GExcel( String args ) throws SQLException
	{
		super();
		//System.out.println( "*** constructeur 1 parameter : "+args );
	
		String[] splitArgs = Util.splitByTrimmedDelimiter( args, ARG_SEPARATOR );
		
		try {
			if ( splitArgs.length == 2 ) {		
				initialize( splitArgs[0], splitArgs[1], null, null, true );
			}
			else if ( splitArgs.length == 3 ) {
				initialize( splitArgs[0], splitArgs[1], null, null, Boolean.parseBoolean( splitArgs[2] ) );
			}
			else if ( splitArgs.length == 4 ) {
				initialize( splitArgs[0], splitArgs[1], splitArgs[2], splitArgs[3], true );
			}
			else if ( splitArgs.length == 5 ) {
				initialize( splitArgs[0], splitArgs[1], splitArgs[2], splitArgs[3], Boolean.parseBoolean( splitArgs[4] ) );
			}
			else {
				throw new SQLException( "This number of parameter is not supported." );		
			}
			
		} catch ( Exception e ) {
			logger.logInfo("Unable to initialise GExcel: " + e);
			throw new SQLException( e );
		}
	}

	
	// ------------------ VTICosting methods -----------------------------------------------
	public double getEstimatedCostPerInstantiation( VTIEnvironment arg0 ) throws SQLException
	{
		//System.out.println( "*** getEstimatedCostPerInstantiation" );
		return 0;
	}
	public double getEstimatedRowCount( VTIEnvironment arg0 ) throws SQLException
	{
		//System.out.println( "*** getEstimatedRowCount: " + (lastRowIndex - firstRowIndex) );
		return lastRowIndex - firstRowIndex;
	}
	public boolean supportsMultipleInstantiations( VTIEnvironment arg0 ) throws SQLException
	{
		//System.out.println( "*** supportsMultipleInstantiations" );
		return false;
	}
	
	// ------------------ IFastPath methods -----------------------------------------------
	public void currentRow( ResultSet arg0, DataValueDescriptor[] arg1 ) throws StandardException, SQLException
	{
		//System.out.println( "*** currentRow" );
	}
	
	// Called if GExcel is invoked directly in SQL
	public boolean executeAsFastPath() throws StandardException, SQLException
	{
		//System.out.println( "*** executeAsFastPath" );
		
		reinitialise();
		
//    	checkSheetTypeConsistency( columnIndexes );
				
		return true;

	}
	public int nextRow( DataValueDescriptor[] arg0 ) throws StandardException, SQLException
	{
		//System.out.println( "*** nextRow" );
		
		int result = createNextRow( sheet, arg0);

		if ( result==GOT_ROW && qualifiers!=null )
		{
			boolean areQualifiersMet = RowsFilter.testQualifiers( arg0, qualifiers );
			while ( result==GOT_ROW && !areQualifiersMet )
			{
				result = createNextRow( sheet, arg0);
				areQualifiersMet = RowsFilter.testQualifiers( arg0, qualifiers );
			}
		}
		
		return result;

	}
	public void rowsDone() throws StandardException, SQLException
	{
		//System.out.println( "*** rowsDone" );
	}
	
	// ------------------ GaianChildVTI methods -----------------------------------------------
	public boolean fetchNextRow( DataValueDescriptor[] row ) throws Exception {
		return IFastPath.GOT_ROW == nextRow(row);
	}
	public int getRowCount() throws Exception
	{
		//System.out.println( "*** getRowCount" );
		return 0;
	}
	public boolean isScrollable()
	{
		//System.out.println( "*** isScrollable" );
		return false;
	}
	public void setArgs( String[] args ) throws Exception
	{
		//System.out.println( "*** setArgs : "+args[0] );
	}
	public void setExtractConditions( Qualifier[][] qualifiers, int[] projectedColumns, int[] physicalColumnsMapping ) throws Exception
	{
		//System.out.println( "*** setExtractConditions" );
		this.qualifiers = qualifiers;
//		System.out.println("Cols involved: " + Util.intArrayAsString(projectedColumns));
	}
	
	
	// ------------------ From extend methods -----------------------------------------------
    public ResultSetMetaData getMetaData() throws SQLException
    {
    	//System.out.println( "*** getMetaData" );
    	reinitialise();
		rsmd = createStringTypeMetaData();
    	
		logger.logInfo("GExcel columnIndexes: " + columnIndexes );    	
    	return rsmd;
    }
    
    // Use the following method from the GaianDB GExcel API to do extra column type inferring
    public ResultSetMetaData getMetaDataByInferringTypes() throws SQLException
    {
    	//System.out.println( "*** getMetaData" );
    	reinitialise();
    	rsmd = checkSheetTypeConsistency( columnIndexes ) ? createMetaData() : createStringTypeMetaData();
    	
    	return rsmd;
    }
    
	@Override
	public boolean reinitialise() {
    	findColumns( sheet );
		columnTypes = new int[ columnIndexes.size() ];
		
		for ( int i=0; i<columnTypes.length; ++i )
			columnTypes[i] = CELL_WITH_NO_TYPE;
		
		currentRow = null;
		
		// also need to take into account + reinitialise based on whether we have new values for:
		//	1) sheet name, 2) first cell index, 3) last cell index and 4) flag for interpretFirstLineAsMetaData
		
		return false; // isPoolable? i.e. ready for re-use? not until comment above is addressed... test poolability with: Test_setDsExcel
	}
    
    public void close() throws SQLException {
    	try {
    		logger.logInfo( "*** Closing GExcel Spreadsheet data source wrapper" );

    		inputStream.close();
    		inputStream=null;
    		
    		if ( columnIndexes!=null )
    		{
    			columnIndexes.clear();
    			columnIndexes = null;
    		}
    		
    		if ( columnNames!=null )
    		{
    			columnNames.clear();
    			columnNames = null;
    		}

    		qualifiers = null;
    		columnTypes = null;
    		currentRow = null;
    		rsmd = null;
    		evaluator = null;
    		sheet = null;
    		workbook = null;
    	}
    	catch( Exception e ) {
    		logger.logWarning( GDBMessages.DSWRAPPER_GEXCEL_CLOSE_ERROR, "*** Failed to close GExcel Spreadsheet data source wrapper: " + e );
    	}
    }
    
	public boolean isBeforeFirst() {
		return null == currentRow;
	}
    
    public ResultSet executeQuery(java.lang.String sql)
    {
    	//System.out.println( "*** executeQuery(sql)" );
		return null;
    }
    
    public ResultSet executeQuery()
    {
    	//System.out.println( "*** executeQuery()" );
		return null;
    }
    
    public ResultSet getResultSet()
    {
    	//System.out.println( "*** getResultSet" );
		return null;
    }
    
    
    // ------------------ Spreadsheet methods -----------------------------------------------
    
    /**
     * Initialize the attributes :
     * 
     * - inputStream
     * - workbook
     * - evaluator
     * - sheet
     * - firstRowIsMetaData
     * 
     * - firstColumnIndex
     * - firstRowIndex
     * - lastColumnIndex
     * - lastRowIndex
     * 
     * @param fileName
     * @param spreadsheetName
     * @param firstCellRange
     * @param lastCellRange
     * @param interpretFirstLineAsMetaData
     * @throws SQLException
     */
    public void initialize( String fileName, String spreadsheetName, String firstCellRange, String lastCellRange, boolean interpretFirstLineAsMetaData ) throws SQLException
    {
		try
		{
			inputStream = new FileInputStream( fileName );
			workbook = WorkbookFactory.create( inputStream );
			evaluator = workbook.getCreationHelper().createFormulaEvaluator();
			sheet = findSpreadsheet( workbook, spreadsheetName );
			firstRowIsMetaData = interpretFirstLineAsMetaData;
			
			if ( firstCellRange!=null && lastCellRange!=null )
			{				
				CellReference firstCell = new CellReference( firstCellRange );
				
				// Deduce last row number if it was not specified
				if ( lastCellRange.matches("[a-zA-Z]+") ) {
					lastCellRange += (sheet.getLastRowNum() + 1); //Note: getLastRowNum is 0-based
					stopScanOnFirstEmptyRow = true;
					logger.logInfo("Deduced last row in Excel table: " + lastCellRange + " - but scans will end on first empty row");
				}
				
				CellReference lastCell = new CellReference( lastCellRange );
				
				firstColumnIndex = firstCell.getCol();
				firstRowIndex = firstCell.getRow(); // + (firstRowIsMetaData?1:0);
				lastColumnIndex = lastCell.getCol();
				lastRowIndex = lastCell.getRow();
			}
			else
			{
				Row firstRow = locateFirstRow( sheet );
				
				if ( firstRow==null )
				{
					throw new SQLException( "Empty spreadsheet !" );
				}
				
				firstRowIndex = firstRow.getRowNum(); // + (firstRowIsMetaData?1:0);
				lastRowIndex = sheet.getLastRowNum();
				firstColumnIndex = firstRow.getFirstCellNum();   //Note: getFirstCellNum is 0-based
				lastColumnIndex = firstRow.getLastCellNum() - 1; //Note: getLastCellNum is 1-based
			}
			
			//System.out.println("sheet: " + sheet.getSheetName() + ", firstcolindex: " + firstColumnIndex + ", lastcolindex: " + lastColumnIndex + ", firstrowindex: " + firstRowIndex + ", lastrowindex: " + lastRowIndex);
		}
		catch( Exception e )
		{
			throw new SQLException( e.getMessage() );
		}
    }
    
    /**
     * Put the next row in the dvd row given in parameter.
     * Return SCAN_COMPLETED if there is no more row in the spreadsheet, or GOT_ROW if a row was successfully put in the dvd row.
     * 
     * Uses the attribute currentRow to save the previous row fetched.
     * 
     * @param sheet
     * @param dvdr
     * @param numberOfLogicalColumnsInvolved
     * @param columnIndexes
     * @return SCAN_COMPLETED or GOT_ROW
     * @throws SQLException
     */
    private int createNextRow( Sheet sheet, DataValueDescriptor[] dvdr)
    {
    	boolean gotData = false;
    	
    	/*
    	 * Find the next row to return.
    	 * 
    	 * currentRow should currently point to the last row returned.
    	 * If that's null, then start from first row.
    	 * Else, search for the next non-empty row (until we hit the end of the prescribed range).
    	 */
		if ( currentRow == null )
			currentRow = sheet.getRow( firstRowIndex + ( firstRowIsMetaData ? 1 : 0 ) );
		else
		{
    		int nextRowIndex = currentRow.getRowNum()+1;
    		currentRow = null;
    		
    		if(stopScanOnFirstEmptyRow) {
    			currentRow = sheet.getRow( nextRowIndex );
    		}
    		else {
	    		while ( currentRow==null && nextRowIndex<=lastRowIndex )
	    		{    			
	    			currentRow = sheet.getRow( nextRowIndex );
	    			nextRowIndex++;
	    		}
    		}
		}
		
		/*
		 * If we've run out of spreadsheet (currentRow == null) or gone out of the prescribed range,
		 * then scan complete - return that.
		 */
		if ( currentRow==null || currentRow.getRowNum() > lastRowIndex ) {
    		return SCAN_COMPLETED;
		}
		
		/*
		 * Get the offset of the first column in the spreadsheet.
		 * Note: this is used when iterating below, so that we can correctly relate 
		 * the actual column in the spreadsheet to the correct 'column' in the 
		 * DataValueDescriptor [] representing the row.
		 */ 
		int columnOffset = firstColumnIndex;
		
		//Figure out how many columns there are
		int numberOfColumns = lastColumnIndex - firstColumnIndex + 1;
		
		for(int i = 0; i < numberOfColumns; i++) {
		/*
		 * Note: i is used to refer to the index of the DataValueDescriptor which represents
		 * the actual spreadsheet column (at i + columnOffset) in the DataValueDescriptor[] 
		 * representing this row. 
		 */
			
			Cell cell = currentRow.getCell(i + columnOffset);
			
			if(cell == null) {
				dvdr[i].setToNull();
			}
			else {
				try {
    				int cellValueType = cell.getCellType();
    				
    				if (cellValueType == Cell.CELL_TYPE_FORMULA) cellValueType = cell.getCachedFormulaResultType(); 
    				
	        		switch( cellValueType ) {
	        		
	    	    		case Cell.CELL_TYPE_STRING:
	    	    			dvdr[i].setValue( cell.getStringCellValue() );
	    	    			break;
	    	    			
	    	    		case Cell.CELL_TYPE_NUMERIC:
	    	    			if ( DateUtil.isCellDateFormatted( cell ) )
	        	    			dvdr[i].setValue( new java.sql.Date( cell.getDateCellValue().getTime() ) );
	        	    		else {
	        	    			cell.setCellType(Cell.CELL_TYPE_STRING);
	        	    			dvdr[i].setValue( cell.getStringCellValue() );
	        	    		}
	    	    			break;
	    	    			
	    	    		case Cell.CELL_TYPE_BOOLEAN:
	    	    			dvdr[i].setValue( cell.getBooleanCellValue() );
	    	    			break;
	    	    			
	    	    		default:
	    	    			dvdr[i].setToNull();
	    	    			break;
	        		}
	        		
	        		//If a cell has data that is not null - then flag that we actually have data to return
	        		if ( !dvdr[i].isNull() )
	        			gotData = true;
	        		
    			} catch ( Exception e ) {
    				dvdr[i].setToNull();
    				logger.logWarning( GDBMessages.DSWRAPPER_GEXCEL_MAP_LT_ERROR, "Excel cell [spreadsheet "+sheet.getSheetName()+"; row "+cell.getRow().getRowNum()+"; column "+cell.getColumnIndex()+"; value "+cell+"] could not be mapped into the logical table because of the column logical type: " + e);
    			}
			}
		}
		
    	if ( !gotData && stopScanOnFirstEmptyRow ) {
    		logger.logInfo("Ending GExcel table scan on first empty row (as no row limit was specified in the ending cell config constraint)");
    		return SCAN_COMPLETED;
    	}
    	
    	return GOT_ROW;
    }
    
    
    /**
     * Creates and returns the ResultSetMetaData object using the fields :
     * - columnNames
     * - columnTypes
     * 
     * The method maps excel types into SQL types.
     * 
     * @return the ResultSetMetaData object
     * @throws SQLException
     */
    private ResultSetMetaData createMetaData() throws SQLException
    {
    	try
    	{
    		StringBuffer metaData = new StringBuffer();
    		
    		assert( columnNames.size()==columnTypes.length );
    		    		
    		for ( int i=0; i<columnTypes.length; ++i )
    		{
    			if(i>0) metaData.append( ", ");
    			
    			metaData.append( columnNames.get( i ) );
    			metaData.append( ' ' );
    			metaData.append( getStringSqlTypeFromSpreadsheetType( columnTypes[i] ) );
    		}
    		
        	return new GaianResultSetMetaData( metaData.toString() );
    	}
    	catch( Exception e )
    	{
    		throw new SQLException( "Problem occurs while creating the Meta-data: " + e, e );
    	}
    }
    
    
    /**
     * Creates and returns the ResultSetMetaData object using the fields :
     * - columnNames
     * 
     * All the SQL types are defined as VARCHAR with this method.
     * 
     * @return the ResultSetMetaData object
     * @throws SQLException
     */
    private ResultSetMetaData createStringTypeMetaData() throws SQLException
    {
    	try
    	{
    		StringBuffer metaData = new StringBuffer();
    		
    		metaData.append( columnNames.get( 0 ) );
    		metaData.append( ' ' );
    		metaData.append( getStringSqlTypeFromSpreadsheetType( Cell.CELL_TYPE_STRING ) );
    		
    		for ( int i=1; i<columnNames.size(); ++i )
    		{
    			metaData.append( ", " );
        		metaData.append( columnNames.get( i ) );
        		metaData.append( ' ' );
        		metaData.append( getStringSqlTypeFromSpreadsheetType( Cell.CELL_TYPE_STRING ) );
    		}
    		
        	return new GaianResultSetMetaData( metaData.toString() );
    	}
    	catch( Exception e )
    	{
    		throw new SQLException( "Problem occured while creating the Meta-data: " + e );
    	}
    }
    
    
    /**
     * Returns a string containing the SQL type definition of the excel type given in parameter.
     * 
     * @param excelType
     * @return the SQL type definition
     * @throws SQLException
     */
    private String getStringSqlTypeFromSpreadsheetType( int excelType ) throws SQLException
    {
		switch( excelType )
		{
    		case Cell.CELL_TYPE_STRING:  return "VARCHAR(50)";
    		case Cell.CELL_TYPE_NUMERIC: return "INT";
    		case Cell.CELL_TYPE_BOOLEAN: return "BOOLEAN";
    		case Cell.CELL_TYPE_FORMULA: return "VARCHAR(50)";
    		case DATE_TYPE: return "DATE";
    		case CELL_WITH_NO_TYPE:	return "VARCHAR(50)";
    		default:
    			throw new SQLException( "Unknow type detected !" );
		}
    }
    
    
    /**
     * Finds the spreadsheet defined by the name given in parameter and return it if found.
     * Else return null.
     * 
     * @param workbook
     * @param spreadsheetName
     * @return the spreadsheet
     * @throws SQLException
     */
	private Sheet findSpreadsheet( Workbook workbook, String spreadsheetName ) throws SQLException
	{
		boolean sheetFound = false;
		Sheet sheetTmp = null;
		for ( int i=0; i<workbook.getNumberOfSheets() && !sheetFound; ++i )
		{
			sheetTmp = workbook.getSheetAt( i );
			
			if ( sheetTmp.getSheetName().equals( spreadsheetName ) )
				sheetFound = true;
		}
		
		if ( sheetFound==false && sheet==null )
		{
			throw new SQLException( "The file does not contain a spreadsheet named : "+spreadsheetName );
		}
		
		return sheetTmp;
	}
    
	
	/**
	 * Locates and return the first row of the the spreadsheet.
	 * 
	 * @param sheet
	 * @return the first row
	 */
	private Row locateFirstRow( Sheet sheet )
	{
	    for ( Row row : sheet )
	    {
	    	for ( Cell cell : row )
	    	{
	    		switch( cell.getCellType() )
	    		{
		    		case Cell.CELL_TYPE_STRING:
		    		case Cell.CELL_TYPE_NUMERIC:
		    		case Cell.CELL_TYPE_BOOLEAN:
		    		case Cell.CELL_TYPE_FORMULA:
		    			
		    			return row;
		    			
		    		default:
		    			break;
	    		}
	    	}
	    }
	    
	    return null;
	    
	    
	}
	
	
	/**
	 * looks for the column definition and initializes the following attributes :
	 * 
	 * - numberOfColumns
	 * - columnIndexes
	 * - columnNames
	 *
	 * If a column which contains no values is ignored.
	 * 
	 * If firstRowIsMetaData is true, the column names will be extract from the first row of the spreadsheet.
	 * Else, they will be automatically generated : COLUMN1, COLUMN2...
	 * 
	 * @param sheet
	 */
	private void findColumns( Sheet sheet )
	{
		numberOfColumns = 0;
		
		columnIndexes = new ArrayList<Integer>();
		columnNames = new ArrayList<String>(); 
		
		Row firstRow = sheet.getRow( firstRowIndex );
		
		int columnLabelIndex = 1;

			
		if ( firstRowIsMetaData )
		{
			//For each column
	    	for ( int i=firstColumnIndex; i<=lastColumnIndex; ++i )
	    	{
	    		//Get the first cell in the column
	    		Cell cell = firstRow.getCell( i, Row.CREATE_NULL_AS_BLANK );
	    		
	    		columnIndexes.add( cell.getColumnIndex() );
	    		
	    		int cellType = cell.getCellType();
	    		if ( Cell.CELL_TYPE_FORMULA == cellType ) {
	    			cellType = cell.getCachedFormulaResultType();
//	    			System.out.println("cell type is now getCachedFormulaResultType() = " + cellType );
	    		}
		
				//Build the column names depending on it's type
	    		switch( cell.getCellType() )
	    		{
	    			case Cell.CELL_TYPE_STRING:
//	    			case Cell.CELL_TYPE_FORMULA: // DO NOT USE: getCellFormula() !!!
	    				
//		    			System.out.println("cell type string" );
	    				
		    			// Note: Javadoc on method getStringCellValue() states:
		    			// "get the value of the cell as a string - for numeric cells we throw an exception. For blank cells we return an empty string. 
		    			// For formulaCells that are not string Formulas, we throw an exception"
	
	    				++numberOfColumns;
	    				columnNames.add( cell.getStringCellValue().replaceAll("[\\ ]", "_") ); // Note we should not have to do this in future... once defect is fixed
	    				break;
	    			
		    		case Cell.CELL_TYPE_NUMERIC:

//		    			System.out.println("cell type numeric " + 
//		    					( DateUtil.isCellDateFormatted( cell ) ? "date: " + cell.getDateCellValue().toString() : "num: " + cell.getNumericCellValue() ) );
		    			
		    			++numberOfColumns;
		    			columnNames.add( DateUtil.isCellDateFormatted( cell ) ? cell.getDateCellValue().toString() : "" + cell.getNumericCellValue() );
		    			break;
		    			
		    		case Cell.CELL_TYPE_BOOLEAN:
		    			
//		    			System.out.println("cell type boolean" );
		    			
		    			++numberOfColumns;
		    			columnNames.add( "" + cell.getBooleanCellValue() );
		    			break;

		    		default:

//		    			System.out.println("cell type default" );
		    			
	    				++numberOfColumns;
	    				columnNames.add( DEFAULT_COLUMN_LABEL+""+columnLabelIndex );
		    			break;
	    		}
	    		
	    		columnLabelIndex++;
	    	}
		}
		else
		{
			//For each column
	    	for ( int i=firstColumnIndex; i<=lastColumnIndex; ++i )
	    	{
	    		//Get the first cell in the column
	    		Cell cell = firstRow.getCell( i, Row.CREATE_NULL_AS_BLANK );
	    		
	    		columnIndexes.add( cell.getColumnIndex() );
	    		columnNames.add( DEFAULT_COLUMN_LABEL+""+columnLabelIndex++ );
	    	}
		}
	}
	
	/**
	 * This method checks if the spreadsheet is well typed.
	 * This means that all values in each column have the same excel type.
	 * 
	 * Returns true if the spreadsheet is well typed, else returns false.
	 *
	 * In addition, this method tries to deduce the excel types from each columns and initializes the attribute : columnTypes.
	 * If a column is empty, then its type is CELL_WITH_NO_TYPE.
	 * The attribute columnTypes must only be used if the spreadsheet is well typed. In the other cases, columnTypes is not significant.
	 * 
	 * @param columnIndexes
	 * @return returns true if the spreadsheet is well typed, else returns false
	 */
	private boolean checkSheetTypeConsistency( List<Integer> columnIndexes )
	{
		boolean isConsistent = true;
				
		int firstRow = firstRowIndex;
		if ( firstRowIsMetaData )
		{
			++firstRow;
		}
	
		Row currentRow;
		Cell cell;
		int index;
		for ( int i=firstRow; i<=lastRowIndex; ++i )
		{
			currentRow = sheet.getRow( i );
			if ( currentRow!=null )
			{
				index = 0;
				for ( int j=firstColumnIndex; j<=lastColumnIndex; ++j )
				{
					cell = currentRow.getCell( j, Row.CREATE_NULL_AS_BLANK );
					if ( cell != null )
					{
//						logger.logInfo("Checking non-null cell: " + cell);
						int cellType = -1;
						try { cellType = evaluator.evaluateInCell(cell).getCellType(); }
						catch( Exception e ) {
							logger.logWarning(GDBMessages.DSWRAPPER_GEXCEL_CELL_TYPE_EVALUATION_FAILURE,
									"Unable to evaluate type for cell at row " + i + " col " + j + ": " + cell);
							isConsistent = false;
							continue;
						}
						
		        		switch( cellType )
		        		{
		    	    		case Cell.CELL_TYPE_STRING:
		    	    			
		    	    			isConsistent = checkSheetConsistencySubMethod( cell, index, isConsistent );
		    	    			++index;
		    	    			break;
		    	    			
		    	    		case Cell.CELL_TYPE_NUMERIC:

		    	    			isConsistent = checkSheetConsistencySubMethod( cell, index, isConsistent );
		    	    			++index;
		    	    			break;
		    	    			
		    	    		case Cell.CELL_TYPE_BOOLEAN:
		    	    			
		    	    			isConsistent = checkSheetConsistencySubMethod( cell, index, isConsistent );
		    	    			++index;
		    	    			break;
		    	    			
		    	    		default:
		    	    			
		    	    			if ( index<columnIndexes.size() && columnIndexes.get( index )==cell.getColumnIndex() )
		    	    			{
		    	    				// The cell is null for this column
		    	    				++index;
		    	    			}
		    	    			break;
		        		}
					}
				}
			}
		}
		
		return isConsistent;
	}
	
	private boolean checkSheetConsistencySubMethod( Cell cell, int index, boolean isConsistent )
	{
		if ( index<columnTypes.length )
		{
			boolean isADate = ( cell.getCellType()==Cell.CELL_TYPE_NUMERIC && DateUtil.isCellDateFormatted( cell ) );
			
			if ( columnTypes[index]==CELL_WITH_NO_TYPE )
			{
				if ( isADate )
				{
					// Specific case where the cell is a date
					columnTypes[index]=DATE_TYPE;
				}
				else
				{
					columnTypes[index]=cell.getCellType();
				}
			}
			else if ( isADate && columnTypes[index]!=DATE_TYPE )
			{
				// Specific case where the cell is a date
				return false;
			}
			else if ( !isADate && ( columnTypes[index]!=cell.getCellType() ) )
			{
				return false;
			}
			
		}
		return isConsistent;
	}
}
