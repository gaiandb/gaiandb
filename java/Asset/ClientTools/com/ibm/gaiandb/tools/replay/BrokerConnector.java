/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.tools.replay;

import java.util.ArrayList;
import java.util.Iterator;

import com.ibm.mqtt.IMqttClient;
import com.ibm.mqtt.MqttClient;
import com.ibm.mqtt.MqttException;
import com.ibm.mqtt.MqttNotConnectedException;
import com.ibm.mqtt.MqttPersistenceException;
import com.ibm.mqtt.MqttSimpleCallback;

/**
 * @author hbowyer / DavidVyvyan
 * 
 * A BrokerConnector connects to a broker and subscribes to given topics.
 * It also allows messages to be published to the broker.
 * 
 * To process incoming messages differently (other than printing them out), override method publishArrived().
 * To publish messages, use method publishMessage().
 */


public class BrokerConnector implements MqttSimpleCallback {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
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

	private short connectRetryInterval = 5000; // ms
	
	private short keepAliveInterval = 30; // The heartbeat interval for the
											// client in seconds.
	
	private int qualityOfService = 0; // Quality of Service (Qos) can be 0,1
										// or 2 depending on how reliably the
										// message is to be delivered.

	private IMqttClient wmqttClient = null; // ClientMQIsdp from the WMQTT
											// library is the underlying object
											// used for publish/subscribe
											// communication.
	

	private ArrayList<IMqttClient> forwardingLinks = new ArrayList<IMqttClient>(); // List of brokers that
														 // messages should be forwarded to.
														 // see setForwardingLinkToBroker()
	
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
			unsubscribe();
			terminate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			super.finalize();
		}
	}
	
	/**
	 * Constructor for the WMQTTSubscriber. Creates a WMQTT client object with
	 * the broker details. NOTE: Registers the object as a simple WMQTT callback
	 * handler to receive the connectionLost() and publishArrived() calls.
	 * 
	 * 
	 * @throws SQLException
	 * @throws SQLException
	 * 
	 */
	public BrokerConnector( String mqttClientName, String brokerAddress, int brokerPortNumber, String topic ) throws MqttException {
		
		this.clientID = mqttClientName;
				
		// Now the db is ready, setup mqtt client and subscribe to the message topic
		if ( null != brokerAddress && 0 < brokerPortNumber && null != topic ) { // If one of these isn't set we have nothing to store msgs from yet
			logInfo("Constructor starting BrokerConnector with these values: brokerAddress: " + 
					brokerAddress + ", brokerPortNumber: " + brokerPortNumber + ", topic: " + topic);			
			refreshBrokerClient( brokerAddress, brokerPortNumber, topic );
		}
	}
	
	/**
	 * 
	 * @param brokerAddress
	 * @param brokerPortNumber
	 * @param topic
	 * @throws MqttException
	 */
	public void refreshBrokerClient( String brokerAddress, int brokerPortNumber, String topic ) throws MqttException {
		
		this.subscriptionTopic = topic;
		this.brokerHost = brokerAddress;
		this.brokerPort = brokerPortNumber;
		
		if ( null != wmqttClient ) terminate();
		
		wmqttClient = MqttClient.createMqttClient(MqttClient.TCP_ID + brokerHost + "@" + brokerPort, null);
		wmqttClient.registerSimpleHandler(this);
		wmqttClient.connect(clientID, cleanStart, keepAliveInterval);
		if ( !wmqttClient.isConnected() ) throw new MqttException( "MQTT client not connected" );
		
		if ( null != subscriptionTopic )
			subscribe();
	}

	public void setForwardingLinkToBroker( String brokerAddress, int brokerPortNumber ) throws MqttException {
				
		IMqttClient mc = MqttClient.createMqttClient(MqttClient.TCP_ID + brokerAddress + "@" + brokerPortNumber, null);
		
//		wmqttClient.registerSimpleHandler(this); // no subscriptions here.. we just publish on to this broker
		
		mc.connect(clientID, cleanStart, keepAliveInterval);
		if ( !mc.isConnected() ) throw new MqttException( "MQTT client not connected" );
		
		synchronized ( forwardingLinks ) {
			forwardingLinks.add( mc );
		}
		
//		if ( null != subscriptionTopic )
//			subscribe();
	}
	
	/**
	 * Try connecting to be broker. Retry every 5 seconds till you are
	 * connected.
	 */
	private void connect() {		

		logInfo("Attempting to connect to the microbroker (re-try every " + connectRetryInterval + " ms)");
		
		while (!wmqttClient.isConnected()) {
			try {
				wmqttClient.connect(clientID, cleanStart, keepAliveInterval);
				logInfo("Connected as " + clientID );
			} catch (Exception e) {
				System.out.print('.');
				try {
					Thread.sleep( connectRetryInterval );
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		}
	}

	/**
	 * This method is implemented as part of the MQIsdpSimpleCallback interface.
	 * It is automatically called when the WMQTT client looses it connection to
	 * the broker. Simply call the connect method.
	 */
	public void connectionLost() throws Exception {
		logInfo("Lost the connection to the broker....");
		connect();
		subscribe();
	}
	
	/**
	 * Subscribe to the subscription topic on the specified quality of service.
	 * Subscription information is specified in a String array allowing multiple
	 * subscriptions to be registered at one time. The quality of service for
	 * each subscription in specified as an int value (0-2) in an array.
	 */
	private void subscribe() throws MqttException, MqttNotConnectedException {
		
		String[] subscriptionArray = { subscriptionTopic };
		int[] qosArray = { qualityOfService };
		wmqttClient.subscribe(subscriptionArray, qosArray);
		logInfo("Subscribed to the " + subscriptionTopic + " topic.");
	}

	/**
	 * Unsubscribe from the subscription topic. One again a String array is used
	 * so that multiple subscription string can be specified together.
	 */
	private void unsubscribe() throws MqttException, MqttNotConnectedException {

		String[] subscriptionArray = { subscriptionTopic };
		wmqttClient.unsubscribe(subscriptionArray);
		logInfo("Unsubscribed from the " + subscriptionTopic + " topic.");
	}

	/**
	 * Disconnect the WMQTT client from the broker.
	 */
	public void terminate() {
		try {
			wmqttClient.disconnect();
		} catch (MqttException e) {
			System.out.println("BrokerConnector unable to disconnect from broker (terminating anyway), cause: " + e);
		}
		
		wmqttClient.terminate();
		
		logInfo(clientID + " terminated client connection to the broker...");
		synchronized( forwardingLinks ) {

			Iterator<IMqttClient> i = forwardingLinks.iterator();
			while ( i.hasNext() ) {
				IMqttClient mc = i.next();
				try {
					mc.disconnect();
				} catch (MqttException e) {
					System.out.println("BrokerConnector unable to disconnect from forwarding link to other broker, cause: " + e);
				}
				mc.terminate();
			}
		}

		logInfo(clientID + " disconnected forwarding links...");
	}
	
	public void publish( String topic, String msg ) throws MqttNotConnectedException, MqttPersistenceException, IllegalArgumentException, MqttException {
		
		int qos = 2; // Delivery qos: 0 = no guarantees, 1 = at least once, 2 = once and only once
		boolean retain = false;
		wmqttClient.publish( topic, msg.getBytes(), qos, retain );
	}
	
	/**
	 * publishArrived() is implemented as part of the MQIsdpSimpleCallback
	 * interface. It is a callback method used by subscribers to automatically
	 * receive publications that are forwarded to them by a broker.
	 * 
	 * Here it is used to forward messages on to other brokers that we have set up links to using method setForwardingLinkToBroker()
	 */
	public void publishArrived(String topic, byte[] messageData, int qos, boolean retain) {
		
		synchronized( forwardingLinks ) {

			Iterator<IMqttClient> i = forwardingLinks.iterator();
			while ( i.hasNext() ) {
				IMqttClient mc = i.next();
				try {
					mc.publish(topic, messageData, qos, retain);
				} catch (MqttException e) {
					System.out.println("BrokerConnector unable to forward message to broker, cause: " + e);
				}
			}
		}
		
//		String message = new String(messageData);
//		logInfo("Message: '" + message + "' received on topic '" + topic
//				+ "'. Quality of service is " + qos
//				+ ", retain flag is set to " + retain);
	}
	
	private static void logInfo( String s ) {
		System.out.println( s );
	}

//	/**
//	 * Wait for the user to press a key.
//	 */
//	private static void waitForUserInput() {
//		logInfo("Press any key to exit....");
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			br.readLine();
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		}
//	}
}
