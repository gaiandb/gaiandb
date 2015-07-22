/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import com.ibm.gaiandb.Logger;
import com.ibm.mqtt.MqttPersistenceException;

/**
 * @author DavidVyvyan
 */
public class DB2MessageStorer {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final String BROKER_HOST="broker.hursley.ibm.com";
	private static final int BROKER_PORT=1883;
	
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 50000;
	private static final String DEFAULT_DATABASE = "bf2";
	
	private static final String DBMS     = "db2";
	private static final String driver   = "com.ibm.db2.jcc.DB2Driver";
	
	private String mUsr = null;
	private String mPwd = null;
	private String mHost = DEFAULT_HOST;
	private String mDatabase = DEFAULT_DATABASE;
	private int mPort = DEFAULT_PORT;
	
	private int brokerPort = BROKER_PORT;
	private String brokerHost = BROKER_HOST;
	
	private String mqttTopic = null;
	
	private MQTTMessageStorer ms = null;
	
	private static String USAGE =
		"\nUSAGE: DB2MessageStorer -usr <usr> -pwd <pwd> -mt <mqtt topic> -log <log level> [-h <host>]  [-p <port>] [-d <database>] [-bh <broker host>] [-bp <broker port>] " +
		"\nDefault host: " + DEFAULT_HOST +
		"\nDefault port: " + DEFAULT_PORT +
		"\nDefault database: " + DEFAULT_DATABASE +
		"\nDefault broker host: " + BROKER_HOST +
		"\nDefault broker port: " + BROKER_PORT +
		"\nDefault log level: " + Logger.POSSIBLE_LEVELS[ Logger.LOG_DEFAULT ] + ", this must be one of: " + Arrays.asList( Logger.POSSIBLE_LEVELS );

	
	public static void main(String[] args) {
		
		DB2MessageStorer n = new DB2MessageStorer();
		n.setArgs( args );

		try {
			n.startListening();
			System.out.println("Press any key to exit....");
			waitForUserInput();
			n.terminate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void setArgs(String[] args) {
		
		for( int i=0; i<args.length; i+=2 ) {

			String arg = args[i];
			
			if ( i+1 == args.length ) syntaxError( "Missing value for argument: " + args );
			
			String val = args[i+1];
			
			if ( "-usr".equals( arg ) ) {
				mUsr = val;
			} else 	if ( "-pwd".equals( arg ) ) {
				mPwd = val;
			} else if ( "-p".equals( arg ) ) {
				mPort = Integer.parseInt( val );
			} else if ( "-h".equals( arg ) ) {
				mHost = val;
			} else if ( "-bp".equals( arg ) ) {
				brokerPort = Integer.parseInt( val );
			} else if ( "-bh".equals( arg ) ) {
				brokerHost = val;
			} else if ( "-d".equals( arg ) ) {
			    mDatabase = val;
			} else if ( "-mt".equals( arg ) ) {
				mqttTopic = val;
			} else 	if ( "-log".equals( arg ) ) {
				if ( !Logger.setLogLevel( val ) ) syntaxError("Cannot set log level to: " + val + 
						", possible levels are: " + Arrays.asList( Logger.POSSIBLE_LEVELS ));
			} else {
				syntaxError("Unrecognised argument: " + arg);
			}
		}
		
		if ( null == mqttTopic || null == mUsr || null == mPwd ) syntaxError( "Any argument may only be specified once" );
	}
	
	private static void syntaxError( String help ) {
		System.out.println( "\n" + help + "\n" + USAGE + "\n" ); System.exit( 1 );
	}
	
	private void terminate() throws MqttPersistenceException {
		ms.terminate();
	}
	
	private void startListening() {
		
		if ( null != mqttTopic ) {
			try {
				String ip = InetAddress.getLocalHost().getHostAddress();
				ms = new MQTTMessageStorer( 
						"DB2ST:" + ip,
						brokerHost,
						brokerPort,
						mqttTopic,
						true, // boolean to say whether the topic field was set on cmd line (and therefore cannot be changed by config file value)
						getDBConnection(),
						false); // boolean to say if any of the broker config is goverened by GAIANDB config file and is refreshable.
				
			} catch ( Exception e ) {
				System.out.println("************ Exception caught constructing " + 
						MQTTMessageStorer.MQTTMessageStorerBaseClassName + ": " + e.getMessage());
			}	
		}
	}
	
	private static void waitForUserInput() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try { br.readLine(); } catch(IOException ex) { ex.printStackTrace(); }
	}
	
	private void loadDriver( String driver ) {
        try {
            Class.forName( driver ).newInstance();                                    
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
	}
	
    public Connection getDBConnection() throws SQLException {
    	
    	loadDriver( driver );
    	
    	String url = "jdbc:" + DBMS + "://" + mHost + ":" + mPort + "/" + mDatabase;
    	
    	System.out.println( "DriverManager.getConnection( " + url + ", " + mUsr + ", pwd)" );
    	
    	Connection c = DriverManager.getConnection( url, mUsr, mPwd );
//    	c.setAutoCommit( false ); // when we re-enable this, we need to explicitely commit, esp. before closing ResultSets
    	
    	return c;
    }
	
}
