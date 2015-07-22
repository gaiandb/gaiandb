/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.mongodb;

public class MongoMessages {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";

	/**
	 * <p>
	 * <b>Error:</b> The connection call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The connection call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * match the location of the mongoDB process and that the process is started
	 * and network-accessible.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_CONNECTION_ERROR = "DSWRAPPER_MONGODB_CONNECTION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * specify an existing, valid database in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_DATABASE_CONN_ERROR = "DSWRAPPER_MONGODB_DATABASE_CONN_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * match the user name and password in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_AUTHENTICATION_ERROR = "DSWRAPPER_MONGODB_AUTHENTICATION_ERROR";

	/**
	 * <p>
	 * <b>Error:</b> The call to the mongoDB process to get a collection failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The call to the mongoDB process to get a collection failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * identify a mongoDB collection that is valid.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_COLLECTION_ACCESS_ERROR = "DSWRAPPER_MONGODB_COLLECTION_ACCESS_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The authentication call to the mongoDB process failed.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * match the user name and password in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_META_DATA_ERROR = "DSWRAPPER_MONGODB_META_DATA_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The call to resolve metadata for the qualifier failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The call to resolve metadata for the qualifier failed
	 * <br/><br/>
	 * <b>Action:</b> Check the code in the area of resultset metadata
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_QUALIFIER_META_DATA_ERROR = "DSWRAPPER_MONGODB_QUALIFIER_META_DATA_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The mapping of data from the mongoDB to derby types is trying
	 * to map fields of incompatible types..
	 * <br/><br/>
	 * <b>Reason:</b>
	 * Possible mis-configured data source properties
	 * <br/><br/>
	 * <b>Action:</b> Check the configured mapping between Logical Table and DataSource fields.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR = "DSWRAPPER_MONGODB_INCOMPATIBLE_TYPE_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The mapping of data from the mongoDB to derby types is trying
	 * to map fields of incompatible types..
	 * <br/><br/>
	 * <b>Reason:</b>
	 * Possible mis-configured data source properties
	 * <br/><br/>
	 * <b>Action:</b> Check the configured mapping between Logical Table and DataSource fields.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_VALUE_CONVERSION_ERROR = "DSWRAPPER_MONGODB_VALUE_CONVERSION_ERROR";

	 /**
	 * <p>
	 * <b>Error:</b> The call to access a derby database qualifier failed.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * The call to access a derby database qualifier failed.
	 * <br/><br/>
	 * <b>Action:</b> Check the code in the area of resultset metadata
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_QUALIFIER_ACCESS_ERROR = "DSWRAPPER_MONGODB_QUALIFIER_ACCESS_ERROR";
	
	/**
	 * <p>
	 * <b>Error:</b> an attempt is made to execute a mongo query, but we have no connection to mongo
	 * <br/><br/>
	 * <b>Reason:</b>
	 * an attempt is made to execute a mongo query, but we have no connection to mongo
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * specify an existing, valid database in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_MONGODB_NOT_CONNECTED = "DSWRAPPER_MONGODB_NOT_CONNECTED";

	/**
	 * <p>
	 * <b>Error:</b> an attempt is made to execute a mongo query which fails.
	 * <br/><br/>
	 * <b>Reason:</b>
	 * an attempt is made to execute a mongo query which fails.
	 * <br/><br/>
	 * <b>Action:</b> Check that the VTI parameters in the configuration file
	 * specify an existing, valid database in mongoDB for the given database.
	 * <p>
	 * Related links:
	 * <ul>
	 * <li><a href="../../../../Readme.html">Readme</a></li>
	 * </ul>
	 */
	public static final String DSWRAPPER_RESULTSET_NOT_CONNECTED = "DSWRAPPER_RESULTSET_NOT_CONNECTED";
}
