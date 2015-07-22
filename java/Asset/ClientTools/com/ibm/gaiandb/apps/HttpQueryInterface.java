/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps;

/**
 * @author Dominic Harries
 */
public class HttpQueryInterface {
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	public static final String QUERIES_TABLE_NAME = "GDB_LOCAL_QUERIES";
	public static final String QUERY_FIELDS_TABLE_NAME = "GDB_LOCAL_QUERY_FIELDS";
	
	public static final int MAX_ID_LENGTH = 32;
	public static final int MAX_DESCRIPTION_LENGTH = 1024;
	public static final int MAX_ISSUER_LENGTH = 32;
	public static final int MAX_QUERY_LENGTH = 32672; //16384;
	public static final int MAX_RESPONSE_FORMAT_LENGTH = 16;
	
	/** When executed, creates the physical tables. */
	private static final String CREATE_QUERIES_TABLE_SQL =
		"CREATE TABLE " + QUERIES_TABLE_NAME + "(" +
		"  id VARCHAR(" + MAX_ID_LENGTH + ") NOT NULL PRIMARY KEY," +
		"  description VARCHAR(" + MAX_DESCRIPTION_LENGTH + ")," +
		"  last_extracted TIMESTAMP DEFAULT CURRENT TIMESTAMP," +
		"  issuer VARCHAR(" + MAX_ISSUER_LENGTH + ")," +
		"  query VARCHAR(" + MAX_QUERY_LENGTH + ")," +
		"  response_format VARCHAR(" + MAX_RESPONSE_FORMAT_LENGTH + ")" +
		")";
	
	private static final String CREATE_QUERY_FIELDS_TABLE_SQL =
		"CREATE TABLE " + QUERY_FIELDS_TABLE_NAME + "(" + 
		"  query_id VARCHAR(" + MAX_ID_LENGTH + ") NOT NULL " +
		"    REFERENCES " + QUERIES_TABLE_NAME + " (id) ON DELETE CASCADE," +
		"  seq SMALLINT," +
		"  offset SMALLINT," +
		"  name VARCHAR(" + MAX_ID_LENGTH + ")," +
		"  query VARCHAR(" + MAX_QUERY_LENGTH + ")," +
		"  PRIMARY KEY (query_id, name)" +
		")";
	
	public static String getCreateQueriesTableSQL() {
		return CREATE_QUERIES_TABLE_SQL;
	}
	
	public static String getCreateQueryFieldsTableSQL() {
		return CREATE_QUERY_FIELDS_TABLE_SQL;
	}
}
