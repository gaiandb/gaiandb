/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.apps.dashboard.Dashboard;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.security.common.SecurityToken;
import sun.misc.BASE64Encoder;
import com.ibm.gaiandb.Logger;

/**
 * Provides a framework for applications to connect to GaianDB easily.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class DBConnector {

	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final Logger logger = new Logger( "DBConnector", 30 );

	/** The default connection URL. */
	private static final String DEFAULT_URL =
		"jdbc:derby://localhost:" + GaianNode.DEFAULT_PORT + "/" + GaianDBConfig.GAIANDB_NAME;

	/** The default database user. */
	private static final String DEFAULT_USER = GaianDBConfig.GAIAN_NODE_DEFAULT_USR;

	/** The default database password. */
	private static final String DEFAULT_PASSWORD = GaianDBConfig.GAIAN_NODE_DEFAULT_PWD;

	/**
	 * True if the Derby connection driver is loaded, as we only need to load it
	 * once.
	 */
	private static boolean isDerbyDriverLoaded = false;

	/** The database connection. */
	protected Connection conn;

	/** The JDBC URL with which we last attempted to connect. */
	protected String url;

	/** True if we kept retrying last time we attempted to connect. */
	protected boolean retry;

	/** A copy of the properties used to connect. */
	protected Properties propsCache=null;

	/** The delay in seconds between connection retries. */
	protected int retryDelay = 5;

	/*
	 * Three methods of connection are supported:
	 * 	1. "simple", consisting of user's name and password;
	 *  2. "asserted", where the user's identity is asserted by a trusted authority;
	 *  3. "token", consisting of a token that represents the user's credentials.
	 * 
	 * Each of the modes above require a set of properties, which are passed in via the Properties object.
	 * These properties may contain two or more of the following:
	 * 	1. DB URL (e.g. jdbc:derby:/localhost:6414/gaiandb) -- mandatory
	 *  2. User name -- mandatory
	 *  3. Password
	 *  4. SecurityToken
	 *  See below for key names.
	 */
	/** Connection modes */
	public static final byte MODE_SIMPLE=1;
	public static final byte MODE_ASSERT=2;
	public static final byte MODE_TOKEN=3;

	public static final String MODEKEY="mode"; // indicates connection mode
	public static final String URLKEY="url";
	public static final String USERKEY="user";
	public static final String DOMAINKEY="domain";
	public static final String PWDKEY="password";
	public static final String PROXYUIDKEY = "proxy-user";
	public static final String PROXYPWDKEY = "proxy-pwd";
	public static final String TOKENKEY="token";
	private static final String ANONUID = "APP";;

	/**
	 * Retrieves the time we delay by between connections retries.
	 * 
	 * @return The delay time, in seconds.
	 */
	public int getRetryDelay() {
		return retryDelay;
	}

	/**
	 * Sets the delay time between connection retries.
	 * 
	 * @param retryDelay
	 *            The new delay time, in seconds.
	 */
	public void setRetryDelay(int retryDelay) {
		this.retryDelay = retryDelay;
	}

	/**
	 * Creates a new connector, but does not connect to any database until the
	 * <code>connect</code> method is called.
	 */
	public DBConnector() { this(DEFAULT_USER, DEFAULT_PASSWORD, false); }

	/**
	 * Initialises the connection URL, username and password from the
	 * <code>args</code> parameter - usually command-line arguments.
	 * 
	 * @param args
	 *            The application's command-line arguments.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 * @throws LoginException 
	 */
	public DBConnector(String[] args) {
		this(
				args.length > 0 ? args[0] : DEFAULT_URL,
				args.length > 1 ? args[1] : DEFAULT_USER,
				args.length > 2 ? args[2] : DEFAULT_PASSWORD);
	}

	/**
	 * Initialises the connection URL, username and password using the argument
	 * provided.
	 * 
	 * @param url
	 *            The JDBC URL of the database.
	 * @param user
	 *            The chosen username.
	 * @param password
	 *            The password associated with the given username.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 * @throws LoginException 
	 */
	private DBConnector(String url, String user, String password) {
		this(user, password, true);
		this.url=url;
	}
	
	public DBConnector(String user, String password, final boolean retry) {
		this.propsCache = new Properties();
		this.propsCache.setProperty(USERKEY, user);
		this.propsCache.setProperty(PWDKEY, password);
		this.propsCache.put(MODEKEY, MODE_SIMPLE);
		this.retry = retry;
	}

	
	public Connection connect(String url) throws LoginException, ClassNotFoundException {
		this.url=url;
		return this.connect();
	}
	
	/**
	 * Connects to the database at the URL provided, using the provided username
	 * and password. Will try again if it fails.
	 * 
	 * @param url
	 *            The JDBC URL of the database in the form
	 *            <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	 * @param user
	 *            The chosen username.
	 * @param password
	 *            The password associated with the given username.
	 * 
	 * @return The database connection.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 * @throws LoginException 
	 */
	public Connection connect(String url, Properties info) throws ClassNotFoundException, LoginException {
		return connect(url, info, true);
	}

	/**
	 * Connects to the database at the URL provided, using the provided properties.
	 * 
	 * @param url
	 *            The JDBC URL of the database in the form
	 *            <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	 * @param user
	 *            The chosen username.
	 * @param password
	 *            The password associated with the given username.
	 * @param retry
	 *            If true, this method will keep trying to connect indefinitely
	 *            until it succeeds. If false, it will return null on failure.
	 * 
	 * @return The database connection on success, or null on failure.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 */
	public Connection connect(String url, Properties info, final boolean retry) throws ClassNotFoundException, LoginException {
		this.url = url;
		this.retry = retry;

		if (null == info || null == url || null == info.getProperty(USERKEY) || null == info.get(MODEKEY)) return null;  // mandatory values are missing
		
		byte mode = Byte.parseByte(info.get(MODEKEY).toString());

		if (!isDerbyDriverLoaded) {
			Class.forName( GaianDBConfig.DERBY_CLIENT_DRIVER );

			isDerbyDriverLoaded = true;
		}

		logger.logInfo("Attempting to connect to " + url + " using mode: " + MODEKEY);

		conn = null;
		do {

			try {
				if (mode==MODE_SIMPLE || mode==MODE_ASSERT) {
					conn = DriverManager.getConnection(url, info);
				}
				if (mode==MODE_TOKEN) {
					SecurityToken secureToken = (SecurityToken)info.get(TOKENKEY);
					if (null == secureToken) return null;  // mandatory value is missing

					// initiate "anonymous" connection
					conn = DriverManager.getConnection(url, ANONUID, "anonymous");

					if (conn!=null && !conn.isClosed()) {
						String sid=sendAuthToken(secureToken);  // send token to server
						
						if (null != sid) {
							conn.close();

							// build a user name (with domain) -- must conform with SQL92 naming conventions
							StringBuffer sb = new StringBuffer();
							sb.append('\"');
							sb.append(info.getProperty(USERKEY));
							sb.append('@');
							sb.append(info.getProperty(DOMAINKEY));
							sb.append('\"');
							String uid=sb.toString();
							
							// initiate connection for "real" user
							conn = DriverManager.getConnection(url, uid, sid);
						}
					}
				}
				if (null != conn && !conn.isClosed()) logger.logInfo("Connected to the database at " + url);
				break;
			} catch (SQLException e) {
				logger.logWarning(GDBMessages.DBCONNECTOR_CANNOT_CONNECT, "Could not connect to the database with credentials: " + e);
				conn = null;
			}

			if (retry) {
				try {
					Thread.sleep(retryDelay * 1000);
				}
				catch (InterruptedException e) {
					return null;
				}
			}
		} while (retry);

		return conn;
	}


	private String sendAuthToken(SecurityToken st) {
		// sends a user ID (UID) and security token (st), returns a session ID (SID)
		String sid=null;
		
		if (null != st) {
			try {
				String query = "VALUES GAIANDB.AUTHTOKEN(?)";
				PreparedStatement ps = conn.prepareStatement(query);
				ps.setQueryTimeout(Dashboard.QUERY_TIMEOUT);
				String token="";
				
				if (st.isValid()) token = new BASE64Encoder().encodeBuffer(st.get());  // convert to a String with Base64Encoding
				ps.setString(1, token);
				ResultSet resultSet = ps.executeQuery();
				
				while (resultSet.next()) {
					sid = resultSet.getString(1);
				}
				
				// clean up
				resultSet.close();
				resultSet = null;
				ps.close();
				ps = null;
				
			} catch (SQLException e) {
				logger.logException(GDBMessages.DBCONNECTOR_CANNOT_FIND_SESSION_ID, "Could not find the session ID for this token", e);
			}
		}
		return sid;
	}
	/**
	 * Connects using the pre-defined properties.
	 * 
	 * @return The database connection.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 * @throws LoginException 
	 */
	public Connection connect() throws ClassNotFoundException, LoginException {
		return connect(this.url, this.propsCache, this.retry);
	}

	/**
	 * Returns the connection for use with other classes.
	 * 
	 * @return A connection to the database, or <code>null</code> if one has not
	 *         been established yet.
	 */
	public Connection getConnection() {
		return conn;
	}

	/**
	 * Displays the message you've given and a termination message, then quits
	 * the program. Useful for a quick bailout.
	 * 
	 * @param message
	 *            The message to display on <code>System.err</code>.
	 */
	public static void terminate(String message) {
		if (message != null && message.length() > 0) {
			logger.logWarning(GDBMessages.DBCONNECTOR_SHUTDOWN_MESSAGE, message);
		}

		logger.logInfo("This program will now terminate.");
		System.exit(1); // TODO replace this with a proper clean shutdown
	}
}
