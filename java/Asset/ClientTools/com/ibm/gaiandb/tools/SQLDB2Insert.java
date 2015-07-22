/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Types;
import java.util.Enumeration;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianResultSetMetaData;


/**
 * @author DavidV
 */
public class SQLDB2Insert extends SQLDB2Runner {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
    
	private static String mProperties = "dbinsert";
	private static ResourceBundle rb = null;    
    
	private static void loadProperties() throws FileNotFoundException {
		
		rb = ResourceBundle.getBundle( mProperties );
	}
    
	private static ResourceBundle getRB() throws FileNotFoundException {
		
		if ( null == rb ) loadProperties();		
		return rb;
	}
    
    private static String getRBResource( String key ) {

    	try { return getRB().getString( key ); }
    	catch ( MissingResourceException e ) { return null; }
    	catch ( FileNotFoundException e ) { return null; }
    }
    
    private static String buildInsertSQL( String tableName, String colsList ) throws Exception {
    	
    	GaianResultSetMetaData rsmd = new GaianResultSetMetaData( GaianDBConfig.getLogicalTableDef(tableName) );
    	
    	StringBuffer sql = new StringBuffer( "insert into " + tableName + " values" );
    	
    	String[] cols = colsList.split(";");
    	
    	for (int c=0; c<cols.length; c++) {
    		
        	String[] colvals = cols[c].split(",");
        	
        	if ( colvals.length != rsmd.getColumnCount() ) 
        		throw new Exception("Number of columns does not match definition for insert line:\n" + cols);
        	
        	sql.append( '(' );
        	
        	for (int i=0; i<colvals.length; i++) {
        		
        		int colIndex = i+1;
        		String val = colvals[i].trim();
        		
        		switch ( rsmd.getColumnType( colIndex ) ) {
        			case Types.VARCHAR: case Types.CHAR: case Types.LONGVARCHAR: 
        				sql.append( "'" + val + "'" ); break;
        			default:
        				sql.append( val );
        		}
        		
        		if ( i+1 != colvals.length ) sql.append( ", " );
        	}
        	
        	sql.append( ')' );
        	if ( c+1 != cols.length && 0 != cols[c+1].trim().length() ) sql.append( ", " );
    	}
    	
    	return sql.toString();
    }
	
	public static void main( String[] args ) {
		
		SQLDB2Insert dbi = new SQLDB2Insert();
		
		dbi.setArgs( args );
		
		try {
			
			Connection c = dbi.sqlConnect();
//			Statement s = c.createStatement();
		
			Enumeration<String> keys = getRB().getKeys();
			while ( keys.hasMoreElements() ) {
			
				String key = (String) keys.nextElement();
				
				if ( key.startsWith("INSERT_") ) {
					
					String tableName = key.substring( key.indexOf('_')+1 );
					
					String cols = getRBResource( key );
					
					// create table if necessary
					try {
						String createSQL = "create table " + tableName + " (" + getRBResource( tableName + "_DEF" ) + ")";
						dbi.processSQLs( createSQL );
					} catch ( Exception e ) {
						// ignore - the table probably exists already
						System.out.println("INFO: Table already exists: " + tableName);
					}
					
					String insertSQL = buildInsertSQL( tableName, cols );						
					dbi.processSQLs( insertSQL );
				}
			}
			
//			s.close();
			c.close();
		
		} catch ( Exception e ) {
			System.out.println("Caught Exception: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}	
}
