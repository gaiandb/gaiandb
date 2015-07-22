/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;



import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import com.ibm.gaiandb.DataSourcesManager;
import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.mqtt.IMqttClient;
import com.ibm.mqtt.MqttClient;
import com.ibm.mqtt.MqttException;
import com.ibm.mqtt.MqttSimpleCallback;

/**
 * @author hbowyer / DavidVyvyan
 * 
 * This is sample WMQTT subscriber.
 */


public class MQTTMessageStorer implements MqttSimpleCallback {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final Logger logger = new Logger( "MQTTMessageStorer", 25 );

	public static final String MQTTMessageStorerBaseClassName = "MQTTMessageStorer";
	
	private static final long GARBAGE_COLLECTION_ROW_DELETION_PERIOD = 3600000; // 1hr = 60 * 60 * 1000 milliseconds
	public long lastDeletionTime = 0;
	
	private Connection connection = null;
	private Hashtable<String, PreparedStatement> preparedStatements = new Hashtable<String, PreparedStatement>();
	
	private static final String SSTRING = "VARCHAR(20)"; // Short String
	private static final String MSTRING = "VARCHAR(100)"; // Medium String
	private static final String LSTRING = "VARCHAR(500)"; // Long String
	
	private static Integer[] msgColumnsIndexes = null;
	private static HashMap<Integer, String> msgTimestampFormats = new HashMap<Integer, String>(); // col index -> format string (used to build a date to insert in the database)
	
	private static String configMsgCols = null;
	
	private String clientID = null ; // The client ID
															// must be unique
															// for every client
															// connecting to the
															// broker.
	
	private String brokerHost = null;
	private int brokerPort = -1;
	private String subscriptionTopic = null; // /battlefield2/#

	private boolean cleanStart = true; // Clean start is used to clear up any
										// subscription that the client may have
										// had from a previous connection.

	private short keepAliveInterval = 30; // The heartbeat interval for the
											// client in seconds.
	
	private int qualityOfService = 0; // Quality of Service (Qos) can be 0,1
										// or 2 depending on how reliably the
										// message is to be delivered.

	private IMqttClient wmqttClient = null; // ClientMQIsdp from the WMQTT
											// library is the underlying object
											// used for publish/subscribe
											// communication.

	private static final String BROKER_MSGS_TABLE = "BROKER_MSGS";
	
	private static final String[] baseCreateTablesSQLGeneric = {
			
		"CREATE TABLE " + BROKER_MSGS_TABLE + " (TOPIC " + MSTRING + 
		", T1 " + SSTRING + ", T2 " + SSTRING + ", T3 " + SSTRING + ", T4 " + SSTRING + ", T5 " + SSTRING + 
		", MSG " + LSTRING + ", MSG_RECEIVED TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
	};
	
	private static final String[] baseCreateTablesSQLBF2 = {

		"CREATE TABLE PLAYER_POSITIONS (PLAYER " + SSTRING + ", TIME DOUBLE, X INT, Y INT, Z INT, AZIMUTH INT, PITCH INT, ROLL INT, SESSIONID BIGINT)",
		"CREATE TABLE SESSIONS (PLAYER " + SSTRING + ", MAPNAME " + SSTRING + ", STARTTIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE CHATMSGS (PLAYER " + SSTRING + ", CHANNEL " + SSTRING + ", TEXT " + MSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_STATUS (PLAYER " + SSTRING + ", EVENT " + SSTRING + ", TEAM INT, TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_VEHICLE (PLAYER " + SSTRING + ", EVENT " + SSTRING + ", VEHICLE " + MSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_SCORE (PLAYER " + SSTRING + ", SCORE " + SSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_SCORE2 (PLAYER " + SSTRING + ", HEALTH DOUBLE" + ", SCORE " + SSTRING + 
			", KILLS INT, DEATHS INT, BULLETS " + LSTRING + ", BULLETSDAMAGE " + LSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_KIT (PLAYER " + SSTRING + ", EVENT " + SSTRING + ", KIT " + MSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_KILLED (PLAYER " + SSTRING + ", VICTIM " + SSTRING + ", ATTACKER " + SSTRING + ", WEAPON " + SSTRING + ", ASSISTS " + LSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE PLAYER_SQUAD (PLAYER " + SSTRING + ", OLDSQUAD INT, NEWSQUAD INT, TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE TEAM_COMMANDER (TEAMID INT, OLDCOMMANDER " + SSTRING + ", NEWCOMMANDER " + SSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE SQUAD_LEADER (SQUADID INT, OLDLEADER " + SSTRING + ", NEWLEADER " + SSTRING + ", TIME DOUBLE, SESSIONID BIGINT)",
		"CREATE TABLE CONTROL_POINTS (CONTROL_POINT " + MSTRING + ", ATTACKING_TEAM " + SSTRING + ", TIME DOUBLE, SESSIONID BIGINT)"
	};
	
//	/**
//	 * Create an instance of the subscriber and configure it. Stay subscribed
//	 * and connected till the user presses a key.
//	 */
//	public static void main(String[] args) {
//
//		try {
//			MQTTMessageStorer subscriber = new MQTTMessageStorer();
//			subscriber.connect();
//			subscriber.subscribe();
//			waitForUserInput();
//			subscriber.unsubscribe();
//			subscriber.disconnect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			System.exit(0);
//		}
//	}
		
	protected void finalize() throws Throwable {
		
		try {
			logInfo("finalize(): Closing derby connection, Unsubscribing to MQTT topics and Disconnecting from Microbroker...");
			connection.close();
			unsubscribe();
			terminate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			super.finalize();
		}
	}
	
	private final boolean cmdLineTopic;
	private final boolean usesGaianDBConfig;
	
	/**
	 * Constructor for the WMQTTSubscriber. Creates a WMQTT client object with
	 * the broker details. NOTE: Registers the object as a simple WMQTT callback
	 * handler to receive the connectionLost() and publishArrived() calls.
	 * @throws SQLException
	 * @throws SQLException
	 * 
	 */
	public MQTTMessageStorer( String mqttClientName, String brokerAddress, int brokerPortNumber, 
			String topic, boolean cmdLineTopic, Connection dbConnection, boolean usesGaianDBConfig ) throws MqttException, SQLException {
		
		this.clientID = mqttClientName;
		this.connection = dbConnection;
		
		this.cmdLineTopic = cmdLineTopic;
		this.usesGaianDBConfig = usesGaianDBConfig;
		
		// Setup the db, setup mqtt client and subscribe to the message topic
		if ( null != brokerAddress && 0 < brokerPortNumber && null != topic ) { // If one of these isn't set we have nothing to store msgs from yet
			logger.logInfo("Constructor starting MessageStorer with these values: brokerAddress: " + 
					brokerAddress + ", brokerPortNumber: " + brokerPortNumber + ", topic: " + topic);
			
			configMsgCols = GaianDBConfig.getMsgStorerMsgCols();
			setupDatabaseObjects();
			refreshBrokerClient( brokerAddress, brokerPortNumber, topic );
			
			if ( null == wmqttClient ) 
				logger.logWarning(GDBMessages.MQTT_MICROBROKER_CONN_ATTEMPT_FAILED, "Cannot connect to Microbroker (will re-try periodically in background)");
		}
		else if ( cmdLineTopic ) {
			if ( null == brokerAddress )
				logger.logWarning( GDBMessages.MQTT_MICROBROKER_HOST_DEF_UNSPECIFIED, "A valid MICROBROKER_HOST definition must be specified in the config file to start the Message Storer" );
			if ( 0 >= brokerPortNumber )
				logger.logWarning( GDBMessages.MQTT_MICROBROKER_PORT_DEF_UNSPECIFIED, "A valid MICROBROKER_PORT definition must be specified in the config file to start the Message Storer" );
		}
	}
	
	private void setupDatabaseObjects() throws MqttException, SQLException {
		
		Statement dbStatement = connection.createStatement();
		StringBuffer colDefs = new StringBuffer();
		
		if ( usesGaianDBConfig ) {
			// Work out message column defs from setting specified in the config file
			String configMsgColsDef = configMsgCols;
			
			if ( null != configMsgColsDef ) {
				
				logger.logInfo("Extracting db col defs from message columns def: " + configMsgColsDef);
			
				// The config string must be parsed to extract the column defs
				ArrayList<Integer> msgColIndexes = new ArrayList<Integer>();
				
				int argIdx = 0;
				
//				for ( int idx=0; idx<configMsgColsDef.length(); idx++ ) {
				while( 0 < configMsgColsDef.length() ) {
				
					// Skip skipped columns
					if ( ',' == configMsgColsDef.charAt(0) ) {
						argIdx++;
						configMsgColsDef = configMsgColsDef.substring(1).trim();
						continue;
					}
					
					int nxtCommaIdx = configMsgColsDef.indexOf(',');
					if ( -1 == nxtCommaIdx ) nxtCommaIdx = configMsgColsDef.length();
					int openBracketIdx = configMsgColsDef.lastIndexOf('(', nxtCommaIdx);
					int closeBracketIdx = configMsgColsDef.indexOf(')');
										
					if ( -1 != openBracketIdx && -1 == configMsgColsDef.lastIndexOf(')', nxtCommaIdx) ) {
						// There is an opening bracket before the next comma, but no closing bracket, 
						// ... so go to the next comma past the next closing bracket
						nxtCommaIdx = configMsgColsDef.indexOf(',', closeBracketIdx);
					}
					
					logger.logInfo("Getting next column info, nxtCommaIdx: " + nxtCommaIdx + 
							", openBracketIdx " + openBracketIdx + ", closeBracketIdx " + closeBracketIdx);
					
					String colDef = null;
					String timestampFormat = null;
					
					if ( -1 == openBracketIdx || -1 == configMsgColsDef.lastIndexOf("TIMESTAMP", openBracketIdx) ) {
						// No opening bracket or no TIMESTAMP def behind one, this is a simple col def string
						colDef = configMsgColsDef.substring(0, nxtCommaIdx).trim();
					} else {
						// There is an opening bracket AND it follows a TIMESTAMP def, pick off the format string to be used later
						colDef = configMsgColsDef.substring(0, openBracketIdx).trim();
						timestampFormat = configMsgColsDef.substring(openBracketIdx+1, closeBracketIdx);
						msgTimestampFormats.put(argIdx, timestampFormat );
					}
					
					logger.logInfo("Message column index " + argIdx + ", definition: " + colDef + 
							( null != timestampFormat ? ", timestamp format: " + timestampFormat : "" ) );
					
					colDefs.append(", " + colDef);
					msgColIndexes.add( new Integer(argIdx) ); // remember column index to insert later when messages arrive...
					
					argIdx++;
					if ( configMsgColsDef.length() < nxtCommaIdx+1 ) break;
					configMsgColsDef = configMsgColsDef.substring(nxtCommaIdx+1);
				}
				
				msgColumnsIndexes = (Integer[]) msgColIndexes.toArray( new Integer[0] );
				logger.logInfo("Message column indexes list: " + msgColIndexes );
			}
		}
		
		String[] baseCreateTablesSQL = usesGaianDBConfig ? baseCreateTablesSQLGeneric : baseCreateTablesSQLBF2;
		
		// Create known tables - (if they don't exist already)
		for (int i=0; i<baseCreateTablesSQL.length; i++) {
			try {
				String createTableSQL = baseCreateTablesSQL[i];
				if ( createTableSQL.startsWith("CREATE TABLE " + BROKER_MSGS_TABLE) ) {
					baseCreateTablesSQL[i] = createTableSQL.substring(0, createTableSQL.length()-1) + colDefs + ")";
					createTableSQL = baseCreateTablesSQL[i];
				}
				logInfo( createTableSQL );
				dbStatement.execute( createTableSQL );
			} catch (SQLException e) { logInfo( "Create table failed: " + e.getMessage() + ", SQL: " + baseCreateTablesSQL[i]); }
		}
		
		// Prepare Statements
		for (int i=0; i<baseCreateTablesSQL.length; i++) {
			try {
				prepareInsertStatement( baseCreateTablesSQL[i], connection );
			} catch (SQLException e) { logInfo( "Error preparing statement " + baseCreateTablesSQL[i] + ", msg: " + e.getMessage()); }
		}
		dbStatement.close();
	}
		
	public void refreshBrokerClient( String brokerAddress, int brokerPortNumber, String topic)
		throws SQLException, MqttException {
		
		boolean wmqttWasSet = null != wmqttClient;
		
		this.subscriptionTopic = topic;
		this.brokerHost = brokerAddress;
		this.brokerPort = brokerPortNumber;
		
		wmqttClient = MqttClient.createMqttClient(MqttClient.TCP_ID
				+ brokerHost + "@" + brokerPort, null);
		wmqttClient.registerSimpleHandler(this);
		
		String cause = "";
		try {
			
			wmqttClient.connect(clientID, cleanStart, keepAliveInterval);
			subscribe();
			return;
			
		} catch (Exception e) {
			terminate();
			cause = ", cause: " + e;
		}

		if ( wmqttWasSet ) 
			logger.logWarning(GDBMessages.MQTT_MICROBROKER_RECONN_ATTEMPT_FAILED, "Cannot re-connect to Microbroker (will re-try periodically in background): " + cause);

		// Set the client to null so we know that the client hasn't just been terminated (to avoid repeatedly 
		// logging the connection attempt msgs).
		wmqttClient = null;
	}
	
	private void prepareInsertStatement( String createTableSQL, Connection dbConnectionHandle ) throws SQLException {
		
		String table = createTableSQL.split(" ")[2];
//		int numCols = dbConnectionHandle.prepareStatement( "select * from " + table ).getMetaData().getColumnCount();
		String[] createSQLSplitByCommas = createTableSQL.toUpperCase().split(",");
		int numCols = createSQLSplitByCommas.length;
		
		StringBuffer pstmtSQL = new StringBuffer("insert into " + table + " values(");
		if ( 0 < numCols ) pstmtSQL.append( -1 == createSQLSplitByCommas[0].indexOf(" DEFAULT ") ? "?" : "default" );
		for (int i=1; i<numCols; i++)
			pstmtSQL.append( -1 == createSQLSplitByCommas[i].indexOf(" DEFAULT ") ? ", ?" : ", default" );
		pstmtSQL.append( ')' );
		
		logInfo( "Preparing Statement: " + pstmtSQL );
		PreparedStatement pstmt = connection.prepareStatement( pstmtSQL.toString() );
		preparedStatements.put( table, pstmt );
	}

//	/**
//	 * Try connecting to be broker. Retry every 5 seconds till you are
//	 * connected.
//	 */
//	private void connect() {		
//
//		logInfo("Attempting to connect to the microbroker (re-try every " + connectRetryInterval + " ms)");
//		
//		while (!wmqttClient.isConnected()) {
//			try {
//				wmqttClient.connect(clientID, cleanStart, keepAliveInterval);
//				logInfo("Connected as " + clientID );
//			} catch (Exception e) {
//				try {
//					Thread.sleep( connectRetryInterval );
//				} catch (InterruptedException ie) {
//					ie.printStackTrace();
//				}
//			}
//		}
//	}

	/**
	 * This method is implemented as part of the MQIsdpSimpleCallback interface.
	 * It is automatically called when the WMQTT client looses it connection to
	 * the broker. Simply call the connect method.
	 */
	public void connectionLost() throws Exception {
		logInfo("Lost the connection to the broker....");
//		connect();
//		subscribe();
	}
	
	private boolean isConnected() {
		return null != wmqttClient && wmqttClient.isConnected();
	}

	/**
	 * Subscribe to the subscription topic on the specified quality of service.
	 * Subscription information is specified in a String array allowing multiple
	 * subscriptions to be registered at one time. The quality of service for
	 * each subscription in specified as an int value (0-2) in an array.
	 */
	private void subscribe() throws Exception {
		
		if ( !isConnected() ) throw new Exception("subscribe() failed as Client is not connected");
		
		String[] subscriptionArray = Util.splitByCommas( subscriptionTopic );
		int[] qosArray = new int[ subscriptionArray.length ];
		for ( int i=0; i<qosArray.length; i++ ) qosArray[i] = qualityOfService;
		wmqttClient.subscribe(subscriptionArray, qosArray);
		logInfo("Subscribed to the " + subscriptionTopic + " topic(s) with qos = " + qualityOfService);
	}

	/**
	 * Unsubscribe from the subscription topic. One again a String array is used
	 * so that multiple subscription string can be specified together.
	 */
	private void unsubscribe() throws Exception {

		if ( !isConnected() ) throw new Exception("unsubscribe() failed as Client is not connected");
		
		String[] subscriptionArray = Util.splitByCommas( subscriptionTopic );
		wmqttClient.unsubscribe(subscriptionArray);
		logInfo("Unsubscribed from the " + subscriptionTopic + " topic(s).");
	}

	/**
	 * Disconnect the WMQTT client from the broker.
	 */
	public void terminate() {
		
		if ( null == wmqttClient ) return;
		
		try {
			wmqttClient.disconnect();
		} catch (MqttException e) {
			logger.logInfo("Unable to disconnect from broker (terminating anyway), cause: " + e);
		}
		wmqttClient.terminate(); // Note we DO NOT set wmqttClient to null - this allows us to know if we have just terminated.
		logInfo("Terminated client connection to broker...");
	}
	
	private String dbTableGeneric( String topic, String message, List<String> colsSQL ) {
		
		String tableName = BROKER_MSGS_TABLE;
		colsSQL.add( topic );
		
		String[] subscriptionTopics = Util.splitByCommas(subscriptionTopic);
		String relevantSubscriptionTopic = "";
		
		for ( String tp : subscriptionTopics ) {
			String tpTrimmed = tp;
			if ( tpTrimmed.endsWith("#") ) tpTrimmed = tpTrimmed.substring(0, tpTrimmed.length()-1);
			if ( tpTrimmed.endsWith("/") ) tpTrimmed = tpTrimmed.substring(0, tpTrimmed.length()-1);
			if ( topic.startsWith(tpTrimmed) && tp.length() > relevantSubscriptionTopic.length() )
				relevantSubscriptionTopic = tp;
		}
		
		int subscriptionTopicRootLength = relevantSubscriptionTopic.length();
		char lastChar = relevantSubscriptionTopic.charAt( subscriptionTopicRootLength-1 );
		if ( '#' == lastChar ) subscriptionTopicRootLength--;
		else if ( '/' != lastChar ) subscriptionTopicRootLength++;
		
		String[] subTopics = subscriptionTopicRootLength > topic.length() ? new String[0] :
			topic.substring( subscriptionTopicRootLength ).split("/");
		
		for(int i=0; i<5; i++) colsSQL.add( i<subTopics.length ? subTopics[i] : null );
		
		colsSQL.add( message );
		// add the msg_received timestamp column - note this is now commented out as generated by Derby as default current_timestamp
//		colsSQL.add( new Timestamp(System.currentTimeMillis()).toString() );	
		
		try {
			if ( null != msgColumnsIndexes ) {
				
				String[] msgTokens = Util.splitByCommas( message );
				
				int tokIdx = 0;
							
				for ( int i=0; i < msgColumnsIndexes.length; i++ ) {
					
					if ( tokIdx >= msgTokens.length ) {
						colsSQL.add( null );
						continue;
					}
					
					int colIdx = msgColumnsIndexes[i]; // 0-based
					
					// calculate the difference between the last 2 col indexes to know how may tokens to skip forward by.
					tokIdx += colIdx - ( 0<i ? msgColumnsIndexes[i-1] : 0 );
					
					logger.logDetail("Getting column info from message for csv 0-based index " + colIdx + " at tokIdx " + tokIdx);
											
					// tokIdx is the index of the column we want - its value will be at the token index
					// (and there may be more if it's a timestamp with commas in it)
					String colInfo = msgTokens[tokIdx];
					
					// Now if there is a format string for this column, it will need formating..
					String formatString = (String) msgTimestampFormats.get( new Integer(colIdx) );
					
					if ( null == formatString ) {
						logger.logDetail("Adding column data: " + colInfo);
						colsSQL.add( colInfo );
					} else {							
						int numCommas = formatString.split(",", -1).length - 1;	
						if ( tokIdx + numCommas >= msgTokens.length ) {
							// The message does not contain all the info for the remaining cols...
							// That's ok, just fill in the last cols as nulls.
							tokIdx = msgTokens.length;
							continue;
						}
						
						while ( 0 < numCommas-- )
							colInfo += "," + msgTokens[++tokIdx]; // add a token for each comma in the format field
						
						logger.logDetail("Formatting timestamp column data: " + colInfo + " using format: " + formatString);
						
						SimpleDateFormat sdf = new SimpleDateFormat( formatString );
						try {
							colsSQL.add( (new Timestamp( sdf.parse(colInfo).getTime() )).toString() );
						} catch (ParseException e) {
							logger.logWarning(GDBMessages.MQTT_TIMESTAMP_PARSE_ERROR, "Unable to parse timestamp: " + colInfo + " using format " + formatString);
							colsSQL.add( null );
						}
					}
				}
			}
		} catch ( Exception e ) {
			logger.logWarning(GDBMessages.MQTT_DB_COLUMNS_GET_ERROR, "Unable to get DB columns data from message (skipping it), cause: " + e);
		}
		
		return tableName;
	}
	
	/**
	 * Checks if the config parms (broker host, port, topic, ...) in the gaiandb_config.properties file have changed.
	 * If they have, attempts to update client appropriately.
	 * 
	 * @return true if valid client broker parms are set and client connection still alive and topic has not changed 
	 * since the parms were last loaded.
	 */
	public boolean checkRefreshConfig() {

		// Refresh registry if changed and check if BROKER HOST, PORT or TOPIC have changed
		try {
			String bh = null, bt = null;
			int bp = -1;
			String mcols = null;
			
			// synchronize to get vars in one go (in case reload occurs concurrently)
			synchronized ( DataSourcesManager.class ) {
				
				bh = GaianDBConfig.getBrokerHost();
				bp = GaianDBConfig.getBrokerPort();
				bt = cmdLineTopic ? subscriptionTopic : GaianDBConfig.getBrokerTopic();
				mcols = GaianDBConfig.getMsgStorerMsgCols();
			}
			
			// Check if we have broker parms
			if ( null == bh || null == bt || -1 == bp ) {
				if ( isConnected() ) terminate();
				return false;
			}
			
			// Check if parse string is specified (and has been changed), to be used to decompose the incoming messages.
			if ( null != mcols && !mcols.equals(configMsgCols) ) {
				configMsgCols = mcols;
				setupDatabaseObjects();
			}
			
			// Apply any broker config changes and establish connection to broker if we don't have one yet
			
			if ( !bh.equals(brokerHost) || bp != brokerPort || !bt.equals(subscriptionTopic) ) {
				
				// Config has changed
				
				if ( bh.equals(brokerHost) && bp == brokerPort ) {
					
					// Just update the topic
					
					logger.logInfo("Changing subscription topic from " + subscriptionTopic + " to " + bt);
					
					unsubscribe();
					this.subscriptionTopic = bt;
					subscribe();
					
					return false;
					
				} else
					// Terminate connection					
					if ( isConnected() ) terminate();
			}
			
			if ( !isConnected() ) {

				if ( null != wmqttClient )
					logger.logInfo("Attempting to re-start client connection to broker on host " + 
							bh + ", port " + bp + ", topic " + bt + " ...");
				
				refreshBrokerClient( bh, bp, bt );
				
				return false;
			}
			
		} catch ( Exception e ) {
			logger.logException( GDBMessages.MQTT_REFRESH_CONFIG_ERROR, "Exception checking or reloading registry and broker connection parms: ", e );
		}
		
		// Client still connected as before - topic still the same
		return true;
	}
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public void runRoutinePeriodicTasks() {
		
		long timeNow = System.currentTimeMillis();
		if ( timeNow - lastDeletionTime > GARBAGE_COLLECTION_ROW_DELETION_PERIOD ) {			
			long rowExpiryHours = GaianDBConfig.getMsgStorerRowExpiry();
			
			if ( 0 < rowExpiryHours ) {
				long expiryTime = System.currentTimeMillis() - ( 3600000 * rowExpiryHours );
				String sql = "delete from " + BROKER_MSGS_TABLE + " where msg_received < '" + sdf.format(new Date(expiryTime)) + "'";
				try {
					connection.createStatement().execute( sql );
				} catch (SQLException e) {
					logger.logWarning(GDBMessages.MQTT_ROWS_EXPIRED_DELETE_ERROR, "Routine garbage collect: Unable to delete expired rows, sql = '"
							+ sql + "', cause: " + e);
				}
			}
			
			// Reset last deletion time regardless of success
			lastDeletionTime = timeNow;
		}
	}
	
	/**
	 * publishArrived() is implemented as part of the MQIsdpSimpleCallback
	 * interface. It is a callback method used by subscribers to automatically
	 * receive publications that are forwarded to them by a broker.
	 */
	public void publishArrived(String topic, byte[] messageData, int qos,
			boolean retain) {
		
		if ( usesGaianDBConfig && false == checkRefreshConfig() ) {
			logger.logInfo("publishArrived, but found that MQTT host/port or topic config have changed... so ignoring message");
			return; // Don't process this message as the host/port/topic have changed...
		}
		
		String message = new String(messageData);
//		logInfo("Message: '" + message + "' received on the topic '" + topic
//				+ "'.  Quality of service is " + qos
//				+ ", retain flag is set to " + retain);

		List<String> colsSQL = new Vector<String>();
		
//		String tableName = dbTableBF2 ( topic, message, colsSQL );
		String tableName = usesGaianDBConfig ? // -1 == topic.indexOf("battlefield2") ? 
				dbTableGeneric ( topic, message, colsSQL ) : dbTableBF2 ( topic, message, colsSQL );
		
		if ( null == tableName ) return;
		
		try {			
			storeMessageInExistingDBTable( tableName, (String[]) colsSQL.toArray( new String[0] ) );
			
		} catch (Exception e) {
			logInfo("****************** Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private String dbTableBF2 ( String topic, String message, List<String> colsSQL ) {

		StringTokenizer st = new StringTokenizer(topic, "/");
		StringTokenizer sm = new StringTokenizer(message, ":");
		String tableName = null;
		
		String lastToken = null;
		String user = null;

		while (st.hasMoreTokens()) {
			user = lastToken;
			lastToken = st.nextToken();
		}
		
//		logInfo("Last topic token: " + lastToken + ", previous token: " + user);

		// User is a column in player tables only, received on player topics only...
		if ( !topic.endsWith( "General/" + user + "/" + lastToken ) ) colsSQL.add( morphIntoDBColName( user ) );
//		if ( !topic.endsWith( "battlefield2/" + user + "/" + lastToken ) ) colsSQL.add( morphIntoDBColName( user ) );

		if (lastToken.equalsIgnoreCase("pos")) {
			
			tableName = "PLAYER_POSITIONS";
			colsSQL.addAll( processPositionData(sm, user) );
		}
		else if (lastToken.equalsIgnoreCase("session")) tableName = "SESSIONS";
        else if (lastToken.equalsIgnoreCase("chat")) tableName = "CHATMSGS";
		else if (lastToken.equalsIgnoreCase("score")) tableName = "PLAYER_SCORE2";
		else if (lastToken.equalsIgnoreCase("info")) {
						
			String firstMessage = sm.nextToken();

			if ( firstMessage.equalsIgnoreCase("Spawned ") || firstMessage.equalsIgnoreCase("Died ") || 
				firstMessage.equalsIgnoreCase("Connected ") || firstMessage.equalsIgnoreCase("Disconnected ")) {
				
				tableName = "PLAYER_STATUS";
				colsSQL.add( morphIntoDBColName( firstMessage ) );
			}			
			else if (firstMessage.equalsIgnoreCase("Entered ") || firstMessage.equalsIgnoreCase("Exited ")) {
				
				tableName = "PLAYER_VEHICLE";
				colsSQL.add( morphIntoDBColName( firstMessage ) );
			}
			else if (firstMessage.equalsIgnoreCase("Scored ")) tableName = "PLAYER_SCORE";
			else if (firstMessage.equalsIgnoreCase("Picked up ") || firstMessage.equalsIgnoreCase("Dropped ")) {
				
				tableName = "PLAYER_KIT";
				colsSQL.add( morphIntoDBColName( firstMessage ) );
			}
	        else if (firstMessage.equalsIgnoreCase("Squad ")) tableName = "PLAYER_SQUAD";
	        else if (firstMessage.equalsIgnoreCase("Killed ")) tableName = "PLAYER_KILLED";	        
	        else if (firstMessage.equalsIgnoreCase("Commander ")) tableName = "TEAM_COMMANDER";
	        else if (firstMessage.equalsIgnoreCase("SquadLeader ")) tableName = "SQUAD_LEADER";
	        else if (firstMessage.equalsIgnoreCase("Captured ")) tableName = "CONTROL_POINTS";
	        else {
				logInfo("Unknown info token: '" + firstMessage + "'. Skipping this message...");
				return null;
			}
		}
		else {
			logInfo("Unknown token: '" + lastToken + "'. Skipping this message...");
			return null;
		}

		colsSQL.addAll( getTokensIntoVector(sm) );
		return tableName;
	}
	
	private static void logInfo( String s ) {
		
		logger.logInfo( s );
	}
	
//	private String buildStringsSchemaDef( String primaryKey, String[] stringCols ) {
//		
//		StringBuffer rc = new StringBuffer( primaryKey + " " + SSTRING );
//		for (int i=0; i<stringCols.length; i++) {
//			rc.append( ", " + stringCols[i] + " " + SSTRING );
//		}
//		
//		rc.append( ", PRIMARY KEY (" + primaryKey + ")" );
//		
//		return rc.toString();
//	}
	
//	private String buildStringsSchemaDef( String[] stringCols ) {
//		
//		StringBuffer rc = new StringBuffer();
//		for (int i=0; i<stringCols.length; i++) {
//			if ( 0<i ) rc.append( ", " );
//			rc.append( stringCols[i] + " " + SSTRING );
//		}
//		
//		return rc.toString();
//	}
	
	private String morphIntoDBColName(String rawString) {
		return rawString.trim();//.replaceAll(" ", "_").toUpperCase();
	}
	
//	private String getTokensAsDelimitedString( StringTokenizer st, String newDelimiter ) {
//		
//		StringBuffer rc = new StringBuffer();
//		int numTokens = st.countTokens();
//		
//		for (int i=0; i<numTokens-1; i++) {
//			
//			st.nextToken(); // skip the descriptor token
//			String tok = morphIntoDBColName( st.nextToken() );			
//			if ( 0 == tok.length() ) break;			
//			if ( 0 < i ) rc.append( newDelimiter );
//			rc.append( tok );
//		}
//		
//		logInfo("debug: Got tokens as string: half of numTokens: " + numTokens + ", result: " + rc);		
//		return rc.toString();
//	}
	
	private Vector<String> getTokensIntoVector( StringTokenizer st ) {
		
		Vector<String> rc = new Vector<String>();
		int numTokens = st.countTokens();
		
		for (int i=0; i<numTokens/2; i++) {
			
			st.nextToken(); // skip the descriptor token
			String tok = morphIntoDBColName( st.nextToken() );			
			if ( 0 == tok.length() ) break;
			rc.add( tok );
		}
		
		return rc;
	}
		
	
	private void storeMessageInExistingDBTable( String table, String[] valuesSQL ) {
				
		PreparedStatement pstmt = (PreparedStatement) preparedStatements.get( table );
		int numCols = valuesSQL.length;
		String debug = null;
		
		
		if ( null == pstmt ) {
			logInfo("****************** ERROR: PreparedStatement does not exist for table: " + table );
			return;			
		}
		
		StringBuffer logTxt = new StringBuffer("'insert into " + table + " values (");		
		for (int i=0; i<numCols; i++) {
			if ( 0<i ) logTxt.append( ", " );
			logTxt.append( valuesSQL[i] );
		}
		logTxt.append( ")'" );

		logInfo( "Executing Prepared Statement: " + logTxt );
		
		try {
						
			ParameterMetaData pmd = pstmt.getParameterMetaData();
			
			for (int arrayIndex=0; arrayIndex<numCols; arrayIndex++) {
				String s = valuesSQL[ arrayIndex ];
				int i = arrayIndex+1;
				
				debug = "Setting statement parameter for parm: " + i + ", value: '" + s + "', JDBC type: " + pmd.getParameterType(i);
				
				switch ( pmd.getParameterType(i) ) {
		            case Types.DECIMAL: case Types.NUMERIC: pstmt.setBigDecimal( i, BigDecimal.valueOf( Double.parseDouble(s) ) ); break;
		            case Types.CHAR: case Types.VARCHAR: case Types.LONGVARCHAR: pstmt.setString( i, s ); break;
		            case Types.BINARY: case Types.VARBINARY: case Types.LONGVARBINARY: pstmt.setBytes( i, s.getBytes() ); break;
		            case Types.BIT: case Types.BOOLEAN: pstmt.setBoolean( i, s.equals("true") ); break;
	//	            case Types.BLOB: pstmt.setBlob( i, (Blob) data ); break;
	//	            case Types.CLOB: pstmt.setClob( i, (Clob) data ); break;
		            case Types.DATE: pstmt.setDate( i, null == s ? null : Date.valueOf(s) ); break;
		            case Types.TIME: pstmt.setTime( i, null == s ? null : Time.valueOf(s) ); break;
		            case Types.TIMESTAMP: pstmt.setTimestamp( i, null == s ? null : Timestamp.valueOf(s) ); break;
		            case Types.INTEGER: pstmt.setInt( i, Integer.parseInt(s) ); break;
		            case Types.BIGINT: pstmt.setLong( i, Long.parseLong(s) ); break;
		            case Types.SMALLINT: pstmt.setShort( i, Short.parseShort(s) ); break;
		            case Types.TINYINT: pstmt.setByte( i, Byte.parseByte(s) ); break;
		            case Types.DOUBLE: case Types.FLOAT: pstmt.setDouble( i, Double.parseDouble(s) ); break;
		            case Types.REAL: pstmt.setFloat( i, Float.parseFloat(s) ); break;
	//	            case Types.ARRAY: pstmt.setArray( i, (Array) data ); break;
	//	            case Types.JAVA_OBJECT: case Types.STRUCT: pstmt.setObject( i, data ); break;
	//			    case Types.REF: case Types.BLOB: case Types.CLOB: case Types.ARRAY: pstmt.setObject( i, data ); break;
	//	            case Types.DATALINK: pstmt.setURL( i, (URL) data ); break;
	//	            case Types.REF: pstmt.setRef( i, (Ref) data ); break;
		            case Types.DISTINCT: case Types.NULL: case Types.OTHER: pstmt.setNull( i, Types.NULL ); break; // No distinct type supported
		            default: throw new SQLException("Unsupported JDBC type: " + pmd.getParameterType(i));
				}
			}
			
			debug = "execute()";
			pstmt.execute();
			
		} catch ( Exception e ) {
			logInfo("****************** Exception: " + e.getMessage() + ", context: " + debug);
			e.printStackTrace();
		}
	}

	private List<String> processPositionData(StringTokenizer sm, String user) {
		
        sm.nextToken();
        String position = sm.nextToken();
        sm.nextToken();
        String rotation = sm.nextToken();
        sm.nextToken();
        String time = sm.nextToken();
        position = position.substring(1, position.length()-1);
        rotation = rotation.substring(1, rotation.length()-1);
        logger.logInfo(position);
        logger.logInfo(rotation);

        StringTokenizer sp = new StringTokenizer(position, ",.");
        String xPos = sp.nextToken();
        sp.nextToken();
        String zPos = sp.nextToken();
        sp.nextToken();
        String yPos = sp.nextToken();
        yPos = yPos.substring(1);
        zPos = zPos.substring(1);
//        int x = Integer.valueOf(xPos).intValue();
//        int y = Integer.valueOf(yPos).intValue();
//        int z = Integer.valueOf(zPos).intValue();

        StringTokenizer sr = new StringTokenizer(rotation, ",.");
        String aRot = sr.nextToken();
        sr.nextToken();
        String pRot = sr.nextToken();
        sr.nextToken();
        String rRot = sr.nextToken();
        pRot = pRot.substring(1);
        rRot = rRot.substring(1);
//        int a = Integer.valueOf(aRot).intValue();
//        int p = Integer.valueOf(pRot).intValue();
//        int r = Integer.valueOf(rRot).intValue();
        
        return Arrays.asList( new String[] { time, xPos, yPos, zPos, aRot, pRot, rRot } );
	}

//	/**
//	 * Wait for the user to press a key.
//	 */
//	private static void waitForUserInput() {
//		logger.logInfo("Press any key to exit....");
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			br.readLine();
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		}
//	}
}
