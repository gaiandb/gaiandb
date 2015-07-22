/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.db2j;

import java.io.File;
import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;
import org.apache.derby.vti.VTIMetaDataTemplate;

import com.ibm.db2j.tools.ImportExportSQLException;
import com.ibm.gaiandb.GaianChildVTI;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.GaianResultSetMetaData;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.RowsFilter;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.lite.LiteParameterMetaData;

/**
 * @author DavidVyvyan
 */
public class FileImport extends AbstractVTI implements GaianChildVTI { // Note does not support IFastPath yet

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "FileImport", 30 );

    private final String filePath;
    
	// Count of all columns in the File
	private int columnCount = 0;

    private String[] currentRowOfStringsFromFile = null;
    private DataValueDescriptor[] dvdRowTemplateForFile = null;

    private String[] blankRowOfStrings = {null};
    
    private int[] projectedColumns = null;
    private Qualifier[][] qualifiers = null;
    
//    private static String getFile( String s ) throws IOException {
//    	InputStream is = null;
//    	String ucs = s.toUpperCase();
//    	if ( ucs.endsWith(".ZIP") ) is = new ZipInputStream( new FileInputStream(s) );
//    	if ( ucs.endsWith(".GZIP") ) is = new GZIPInputStream( new FileInputStream(s) );
//    	
//    	if ( null != is ) {
//    		System.out.println("unizipping...");
//    	    s = s.substring(0, s.lastIndexOf('.'));
//    	    OutputStream os = new FileOutputStream(s);
//
//    	    byte[] bytes = new byte[1024];
//    	    int numBytes;
//    	    while (0 < (numBytes = is.read(bytes)))
//    	    	os.write(bytes, 0, numBytes);
//
//    	    is.close(); os.close();
//    	}
//    	return s;
//    }
	
	private static final Map<String, String> controlFiles = new Hashtable<String, String>();
    private static final String EXTN_PROPERTIES = ".properties";
    private static final String FILE_IMPORT_DEFAULT_PROPERTIES = "FileImportDefaults" + EXTN_PROPERTIES;
    private static final String FILE_IMPORT_CONTROLS_DIR = "FileImportControls" + File.separatorChar;
    
    /**
     * The "control file" is used by the Derby FileImport class, and contains properties that describe the structure of the File to be processed. 
     * The resolution of the location of a control file is best described with an example: 
     * Assuming a workspace path for the GaianDB node at '/node-workspace', the control file for a data file '/a/b/file.dat' will be searched for and 
     * resolved in this order of precedence:
     * 
     * 		1) /a/b/file.dat.properties
     * 		2) /a/b/FileImportDefaults.properties
     * 		3) /node-workspace/FileImportControls/a/b/file.dat.properties
     * 		4) /node-workspace/FileImportControls/a/b/FileImportDefaults.properties
     * 		5) /node-workspace/FileImportControls/b/file.dat.properties
     * 		6) /node-workspace/FileImportControls/b/FileImportDefaults.properties
     * 		7) /node-workspace/FileImportControls/file.dat.properties
     * 		8) /node-workspace/FileImportControls/FileImportDefaults.properties
     * 
     * If no control file is resolved using this process, the default CSV format is assumed.
     * 
     * @param s The path to the data file.
     * @return The path to the control file for the given data file.
     */
    public static String getControlFile( String datafilePath ) {
    	String controlfilePath = null, gdbCtrlDir = null;
    	
//    	System.out.println("ctrl file exists: " + s + ".properties: " + new File(s+".properties").exists()); 
//    	System.out.println("Default line separator: " + System.getProperty("line.separator"));
//    	System.out.println("Default codeset: " + (new InputStreamReader(System.in)).getEncoding());
//    	String ucs = s.toUpperCase();
//    	if ( ucs.endsWith(".ZIP") || ucs.endsWith(".GZIP") ) s = s.substring(0, s.lastIndexOf('.'));
    	
    	// NOTE : By convention, all variable names here ending in 'Dir' designate directory names INCLUDING a folder separator ('/' or '\') at the end.
    	
    	File datafile = new File(datafilePath);
    	String datafileDir = datafile.getParent() + File.separatorChar;
    	String datafileName = datafile.getName();
    	
    	for ( String candidate : new String[] { datafilePath+EXTN_PROPERTIES, datafileDir + FILE_IMPORT_DEFAULT_PROPERTIES } )
    		if ( new File(candidate).exists() ) { controlfilePath = candidate; break; }
    	
    	if ( null == controlfilePath ) {
			try { gdbCtrlDir = GaianNode.getWorkspaceDir(); }
			catch (Exception e) { logger.logInfo("getControlFile("+datafilePath+") unable to resolve install path: " + e); }
			
			logger.logDetail("Workspace path: " + gdbCtrlDir);
			if ( null != gdbCtrlDir ) {
				
				gdbCtrlDir += File.separatorChar + FILE_IMPORT_CONTROLS_DIR;
				
				int idx;
				String relativeDir = datafileDir;
				if ( Util.isWindowsOS && -1 != (idx = relativeDir.indexOf(':')) ) relativeDir = relativeDir.substring(idx+1);
				// Don't use File.separatorChar on Windows... '/' is valid everywhere.
				while ( 0 < relativeDir.length() && Util.isSeparatorChar( relativeDir.charAt(0) ) ) relativeDir = relativeDir.substring(1);

				logger.logInfo("Searching under config folder " + gdbCtrlDir + ", all sub-locations of relativeDir: " + relativeDir);
				for ( idx = 0; null == controlfilePath && -1 < idx; relativeDir = relativeDir.substring(idx+1) ) {
					logger.logDetail("Testing relativeDir: " + relativeDir);
					for ( String candidate : new String[] {
							gdbCtrlDir + relativeDir + datafileName+EXTN_PROPERTIES,
							gdbCtrlDir + relativeDir + FILE_IMPORT_DEFAULT_PROPERTIES } ) {
						logger.logDetail("Resolving path for: " + datafileName + ", candidate: " + candidate);
			    		if ( new File(candidate).exists() ) { controlfilePath = candidate; break; }
					}
					idx = Util.indexOfFileSeparator( relativeDir ); // Don't use File.separatorChar on Windows... '/' is valid everywhere.
				}
			}
    	}

    	logger.logInfo("Resolved controlfilePath: " + controlfilePath);
    	if ( null != controlfilePath ) controlFiles.put(datafilePath, controlfilePath);
    	else logger.logWarning(GDBMessages.CONFIG_LT_SET_CONTROL_FILE_NOT_FOUND, "No control file found for: "
				+ datafilePath + " (defaulting to csv format) - i.e. Couldn't resolve '"
				+ datafileName + ".properties' or 'FileImportDefaults.properties' at data file location or at workspace locations under: "
				+ gdbCtrlDir );
    	
    	return controlfilePath;
    }
    
	public GaianResultSetMetaData getMetaData() {
		try { return new GaianResultSetMetaData( fileImportMetaData, null ); }
		catch (Exception e) { e.printStackTrace(); }
		return null;
	}
    
	private final ResultSetMetaData fileImportMetaData;
	
	private class FileImportMedataData extends VTIMetaDataTemplate {

		private static final int DEFAULT_COLUMN_WIDTH = 255;
		
		private final ResultSetMetaData md;
		private FileImportMedataData( ResultSetMetaData rsmd ) { md = rsmd; }
		
	    public int getColumnCount() throws SQLException { return md.getColumnCount(); }
	    public String getColumnName(int i) throws SQLException { return md.getColumnName(i); }
	    public int getColumnType(int i) throws SQLException { return md.getColumnType(i); }
	    public int isNullable(int i) throws SQLException { return md.isNullable(i); }
	    public String getColumnTypeName(int i) { return "VARCHAR"; }

	    public int getColumnDisplaySize(int i) throws SQLException {
	        int w = md.getColumnDisplaySize(i);
	        // The following condition is true if the file is not ASCII_FIXED (by default it is ASCII_DELIMITED).
	        if ( 0x7fffffff == w ) return DEFAULT_COLUMN_WIDTH;
	        return w;
	    }

		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return false;
		}

		public <T> T unwrap(Class<T> iface) throws SQLException {
			return null;
		}
	}
	
    private final FileImportDerby fileImport;
    
    private class FileImportDerby extends com.ibm.db2j.tools.FileImport {
    	public FileImportDerby(String filePath, String controlFilePath) throws Exception { super(filePath, controlFilePath); }
    	public String[] getFetchedRow() { return nextRow; }
    }
    
	/**
	 * This is the entry point for the VTI.
	 * 
	 * @param s The file path
	 * @throws Exception
	 */
	public FileImport(String s) throws Exception {
		s = GaianDBConfig.resolvePathTags(s);
		fileImport = new FileImportDerby( s, getControlFile(s) );
//		super(/*getFile(s)*/ s, getControlFile(s));
		
		fileImportMetaData = new FileImportMedataData( fileImport.getMetaData() );
		
//		System.out.println("CFR: cdef " + getControlFileReader().getColumnDefinition() + ", format " + getControlFileReader().getFormat());
		
		filePath = s;
		columnCount = getMetaData().getColumnCount();
		
		projectedColumns = new int[ columnCount ];
		for ( int i=0; i<columnCount; i++ ) projectedColumns[i] = i+1; // 1-based
		
    	dvdRowTemplateForFile = new DataValueDescriptor[ columnCount ];
		for ( int i=0; i<columnCount; i++ ) dvdRowTemplateForFile[i] = new SQLChar();
	}
    
	@Override public boolean pushProjection(VTIEnvironment arg0, int[] arg1) throws SQLException {
		logger.logThreadDetail("Entered FileImport.pushProjection(), projection: " + Util.intArrayAsString(arg1));
		if ( null != arg1) projectedColumns = arg1;
		return true;
	}
	
	@Override public void setQualifiers(VTIEnvironment vtie, Qualifier[][] qual) throws SQLException {
		logger.logThreadDetail("Entered FileImport.setQualifiers(), qualifiers: " + RowsFilter.reconstructSQLWhereClause(qual));
		qualifiers = qual;
	}

	@Override public boolean executeAsFastPath() throws StandardException, SQLException {
		reinitialise(); // necessary if this FileImport is not a data source of Gaian... (Gaian would explicitly invoke it when recycling the source)
		return true; // nothing else to execute
	}
	
    /**
     * Overrides IFastPath.nextRow() - Derby API method used when this VTI is referenced directly in SQL, or invoked by AbstractVTI.
     * NOTE: Column indexes in the row[] are relative to the physical source, i.e. the File's columns (i.e NOT a logical table set of columns)
     */
    @Override public int nextRow( final DataValueDescriptor[] vtiRow ) throws SQLException {
    	
    	// 'vtiRow' contains DataValueDescriptor col type classes for the queried columns.
    	// The nextRow String types from the file will get converted into the row types using these Derby DataValueDescriptor type classes.
    	
    	//repeat until we get a non blank line that matches the qualifier conditions
    	do {
	    	// System.out.println("ImportAbstract.next()...");

     			try { if ( false == fileImport.next() ) return IFastPath.SCAN_COMPLETED; }
//     			try { if ( false == fileImport.next() ) { logger.logInfo("NO MORE ROWS " + this); return IFastPath.SCAN_COMPLETED; } logger.logInfo("GOT ROW " + this);}
    			catch ( ImportExportSQLException e ) {
    				System.out.println("Check File: " + filePath);
    				logger.logWarning(GDBMessages.DSWRAPPER_FILE_IMPORT_NEXT_ERROR_SQL, "Error in next() while importing " + filePath +  e);
    				return IFastPath.SCAN_COMPLETED;
    			}
    			catch ( Exception e ) {
    				logger.logException(GDBMessages.DSWRAPPER_FILE_IMPORT_POSSIBLE_STRUCTURE_ERROR,
    						"FileImport exception in next(). Possible structure issue with file "
    						+ filePath + ", e.g. missing final record delimiter at EOF: " + e,e );
    				return IFastPath.SCAN_COMPLETED;
    			}

    			currentRowOfStringsFromFile = fileImport.getFetchedRow();

	    	if  (!Arrays.equals(currentRowOfStringsFromFile,blankRowOfStrings)) {
	    		for ( int i=0; i<projectedColumns.length; i++ ) {

	    			int pColID = projectedColumns[i]-1;
	    			try {
	    				//	    			if ( null == currentRowOfStringsFromFile[pcolID] ) {
	    				//	    				logger.logWarning("File was modified during row fetch - truncated stream - ending fetch for this source");
	    				//	    				return false;
	    				//	    			}
	    				vtiRow[pColID].setValue( currentRowOfStringsFromFile[pColID] ); // This does type conversion from String to the expected type for this column in vtiRow[]

	    			} catch ( ArrayIndexOutOfBoundsException e ) {
	    				logger.logException( GDBMessages.DSWRAPPER_LOGICAL_COLUMN_REF_ERROR, "Error referencing Physical column " + (pColID+1)
	    						+  " which does not exist in File " + filePath + ". Null Field will be returned for this node", e);
	    				vtiRow[pColID].setToNull();

	    			} catch (Exception e) {
	    				String controlfilePath = controlFiles.get(filePath);
	    				logger.logException( GDBMessages.DSWRAPPER_LOGICAL_COLUMN_REF_ERROR, "Unable to set cell value from file's column " + (pColID+1)
	    						+ (currentRowOfStringsFromFile.length > pColID ? ", column value: [" + currentRowOfStringsFromFile[pColID] + "]" : "")
	    						+ " to the intended cell column type: " + vtiRow[pColID].getTypeName()
	    						+ ( null == controlfilePath ? " (using default formatting - no control file used)" :
	    							" (formatting control file: " + controlfilePath + ")" ) + ", cause: ", e );
	    				vtiRow[pColID].setToNull();
	    			}
	    		}
	    	}
    	} while (Arrays.equals(currentRowOfStringsFromFile,blankRowOfStrings)|| //The line is blank, get the next one
    			(null != qualifiers && false == RowsFilter.testQualifiers( vtiRow, qualifiers )) );//The line doesn't match qualifier conditions, get the next one
    	
    	return IFastPath.GOT_ROW;
	}
	
	public int getRowCount() throws Exception {
		
		// Note row count cannot be cached because file may change between invocations.
		
		// Use a row of String DVDs to hold the file's col values which will be tested
		// against qualifiers - note that String has lowest precedence, so the types of the constant 
		// values against which the strings are compared will always take precedence, meaning the strings
		// will be converted to whichever types the constants are when they are compared.
		
		fileImport.close();
		int i = 0;
		while ( fetchNextRow(dvdRowTemplateForFile) ) i++;
		
		// don't close() now - as we need any other calls to nextRow() to return SCAN_COMPLETED (e.g. in explain queries)
		
		return i;
	}
	
	public boolean reinitialise() throws SQLException {
		// Just need to cleanup the underlying FileImport object.
		fileImport.close();
		currentRowOfStringsFromFile = null;
//		logger.logInfo("REINITIALISED " + this);
		return true;
	}
	
	public void close() throws SQLException { reinitialise(); }
	
	public boolean isBeforeFirst() { return null == currentRowOfStringsFromFile; }
	public boolean isScrollable() { return false; }
	
	// This method is called by the udp driver when in LITE mode
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return !GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianNode.isLite() ? new LiteParameterMetaData() : null;
	}
	
	@Override public double getEstimatedCostPerInstantiation(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException { return 0; }
	@Override public boolean supportsMultipleInstantiations(VTIEnvironment arg0) throws SQLException { return false; }
}
