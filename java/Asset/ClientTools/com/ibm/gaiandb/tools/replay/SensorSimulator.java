/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools.replay;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.ibm.mqtt.MqttException;

public class SensorSimulator {
		
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	// Relevant time ranges where we have interesting data for "personnelTracking", "mortarFire" and "smallArms" events.
	// +1 hour added to values used in perl scripts (because Timestamp on left in logs is an hour ahead of sensor date and time logged in msg)
	// i.e. Timestamp on left is unix time which takes account of Daylight Saving Time (+1 hr in summer) - the sensor times are UTC (=GMT)
	private static final String[] startTimes = {
			"2007-09-24 21:15:00.000", // personnel start period
			"2007-09-25 15:15:00.000", // mortar start period
			"2007-09-25 15:20:00.000"  // small arms start period
	};
	
	private static final String[] endTimes = {
			"2007-09-24 22:00:00.000",
			"2007-09-25 16:00:00.000", //"2007-09-24 21:15:30.000"
			"2007-09-25 15:40:00.000"
	};
		
	// Topics for personelTracking, mortarFire and smallArms events.
	private static final String[] SENSOR_TOPICS = { "xyzzy/PITADA/STAT/010",
		"xyzzy/PITADA/STAT/011", "xyzzy/PITADA/STAT/012", "xyzzy/MITADS/LOBR/2185", "xyzzy/MITADS/LOBR/5324" };
	
	private static final String[] BROKER_TOPICS = { "xyzzy/PITADA/STAT/010",
		"xyzzy/PITADA/STAT/011", "xyzzy/PITADA/STAT/012", "xyzzy/MITADS/LOBR/2185", "xyzzy/MITADS/LOBR/5324" };
	
	// Set of forwarding links for each broker connector that will be created.
	// Each set of links identifies the brokers that the given broker connector will forward its messages to.
	// IMPORTANT NOTE!! CARE MUST BE TAKEN HERE NOT TO DEFINE LOOPING CONNECTIONS!
	private static final int[][] FORWARDING_LINKS = { null, null, null, { 0 }, { 0 } };
	
//	private static final String[] CLIENT_NAMES = { "PersonnelMovement1", "PersonnelMovement2", "PersonnelMovement3", 
//		"EventBearingSensor1", "EventBearingSensor2" };//, "fireEventSensorBroker" };

	public static void main(String[] args) throws MqttException, IOException, ParseException, InterruptedException {
		
		System.out.println("Starting SensorSimulator...");
		
		/*args.length == 1 || args.length > 2 ? args[args.length-1] :*/
		String filename =  "log.txt";
		String startTime = args.length > 0 ? args[0] : startTimes[1];
		String endTime = args.length > 1 ? args[1] : endTimes[1]; // the latest end time
		
		int argIdx = 2;
		while ( args.length > argIdx ) {
			if ( argIdx-2 >= BROKER_TOPICS.length ) {
				System.out.println("Ignoring overlay topic " + args[argIdx] + 
						", maximum number is " + BROKER_TOPICS.length);
			} else {
				String overlayTopicSuffix = args[argIdx];
				String topic = BROKER_TOPICS[argIdx-2];
				BROKER_TOPICS[argIdx-2] = 
					topic.substring(0, topic.length() - overlayTopicSuffix.length()) + overlayTopicSuffix;
			}
			
			argIdx++;
		}
		
		int numSensors = BROKER_TOPICS.length;

		System.out.println("Creating BrokerConnector objects...");
		
		BrokerConnector[] bcs = new BrokerConnector[ numSensors ];
		
		for (int i=0; i<numSensors; i++) {
			try {
				String topic = BROKER_TOPICS[i];
				bcs[i] = new BrokerConnector( "Sensor" + topic.substring(topic.lastIndexOf('/')+1), 
						"localhost", 1883 + i, topic + "/#" );
			} catch ( Exception e ) { 
				System.out.println("No broker to connect to on port " + (1883+i));
				bcs[i] = null; continue; 
			}
			int[] links = FORWARDING_LINKS[i];
			if ( null != links )
				for (int j=0; j<links.length; j++) {
					try { bcs[i].setForwardingLinkToBroker("localhost", 1883+links[j]); }
					catch ( Exception e ) {
						System.out.println("Connector to broker on " + (1883+i) +
							" could not setup forwarding link to broker on " + (1883+links[j]));
					}
				}
		}
		
		FileReader fr = null;
		
		try { fr = new FileReader( filename ); } catch ( FileNotFoundException e ) { fr = new FileReader( "C:\\temp\\log.txt" ); }
		BufferedReader br = new BufferedReader( fr );
		String line;
		
		long start = sdf.parse( startTime ).getTime()/1000;
		long end = sdf.parse( endTime ).getTime()/1000;
		
		System.out.println("Replaying sensor events from file " + filename + " between timestamps " + 
				startTime + " = " + start + " and " + endTime + " = " + end);
		
//		int lineNumber = 0;
//		int linesIncrement = 100;
		
//		String logMsg = null;
		
		int countPitada010 = 0, countPitada011 = 0, countPitada012 = 0, countPitada013 = 0;
		int countMitads2185 = 0, countMitads5324 = 0;
		
		int count = 0, msgCount=0;
		
		long previousTimeStamp = 0;
		
		while( null != ( line = br.readLine() ) ) {
			
//			lineNumber++;
//			if ( 0 == lineNumber%linesIncrement )
//				System.out.println("Processing next " + linesIncrement + " lines from line " + lineNumber );
			
			if ( ! line.startsWith("#!#") )
				continue;
			
			String lineMinusPrefix = line.substring(3);
			long stamp = Long.parseLong( lineMinusPrefix.substring(0, lineMinusPrefix.indexOf('#')) );
			
			if ( 0 == stamp%600 ) {// Print msg every 1 minutes of logs
//				System.out.println("Checking that timestamp is within targetted time period: " + start + " < " + stamp + " < " + end);
				System.out.print('.');
				count++;
			}
			
			if ( stamp < start ) continue;
			if ( stamp > end ) break;
			
			for ( int i=0; i<numSensors; i++ ) {
				
				if ( null == bcs[i] ) continue;
				
				String topic = SENSOR_TOPICS[i];
				int idx = line.indexOf( topic );
				if ( -1 != idx ) {
					// Apply potential overlay, i.e. replace topic with expected broker topic for a given sensor topic
					topic = BROKER_TOPICS[i] + line.substring( idx + topic.length(), line.indexOf("#", idx) );
					//topic = line.substring( idx, line.indexOf("#", idx) );
					String msg = line.substring( idx + topic.length() + 1 ); // remove the 1st char: "#"
					if ( ! msg .startsWith("$MITADS") && ! msg.startsWith("$PITADA") ) break; // Check for invalid msgs
					
					if ( 0 != previousTimeStamp ) {
						// wait the amount of time that passed in real life
//						if ( 0 == stamp%600 )
//						System.out.println( "Times: " + previousTimeStamp + ", " + stamp + ", diff " + ( stamp - previousTimeStamp ) );
						Thread.sleep(  100 * ( stamp - previousTimeStamp ) );
					}
					previousTimeStamp = stamp;
					
//					String log = stamp + " Topic " + topic + ", publishing message: " + msg;
//					if ( null == logMsg || msg.startsWith("$MITADS") )
//						System.out.println( log );
					char evt = topic.charAt(topic.length()-1);
					
//					if ( null != overlayTopic )
//						evt = overlayTopic.charAt( overlayTopic.length()-1 );
					
					switch ( evt ) {
					case '0': countPitada010++; break; case '1': countPitada011++; break; case '2': countPitada012++; break;
					case '3': countPitada013++; break; case '4': countMitads5324++; break; case '5': countMitads2185++; break;
					}
					
					System.out.print( evt );
					if ( 0 == ++count%150 ) System.out.println();
					
					msgCount++;
//					logMsg = log;
					bcs[i].publish( topic, msg );
				}
			}
		}
		
//		if ( null != logMsg )
////			System.out.println( logMsg );
//			System.out.println( "M" );
		
		System.out.println("\nPublished " + msgCount + " messages");
		System.out.println("Pitada counts: 010: " + countPitada010 + ", 011: " + countPitada011 + 
				", 012: " + countPitada012 + ", 013: " + countPitada013);
		System.out.println("Mitads counts: 2185: " + countMitads2185 + ", 5324: " + countMitads5324);
		
		br.close();
		fr.close();
		
		for ( int i=0; i<numSensors; i++ ) if ( null != bcs[i] ) bcs[i].terminate();
	}
}
