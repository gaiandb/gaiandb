/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.jdbc.discoveryclient;



import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.GaianNodeSeeker;
import com.ibm.gaiandb.Logger;
import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;

/**
 * @author Paul Stone
 */

public class GaianConnectionSeeker {

	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";

	private static final Logger logger = new Logger( "GaianConnectionSeeker", 25 );
	
	static final String DEFAULT_CLIENT_CONFIG = "gaiandb_client_config";
	
	private static int discoveryPort;
	
	private static InetAddress defaultInterface;
	
	private static final String BROADCAST_ALL = "BROADCAST_ALL";
	
	// Multicast address must be between 224.0.0.0 and 239.255.255.255, inclusive.
	// The address 224.0.0.0 is reserved and should not be used.
	// Below is a hard coded multicast address used by previous versions of GaianDB
	public static final String DEFAULT_MULTICAST_GROUP_IP = "230.255.255.255";
	
	private static InetAddress DEFAULT_MULTICAST_GROUP_ADDRESS = null;
	
	private static final int DISCOVERY_TIMEOUT = 5000; // allow 5 seconds to discover connections (the underlying db may take longer to create though)
	
	//private static final int DERBY_CONNECTION_TIMEOUT_SECONDS = 10;
	
	static final String javaVersionS = System.getProperty("java.version");
	
	// java version is "0" on Android
	static final boolean isJavaVersion6OrMore = -1 == javaVersionS.indexOf('.', 3) ? true : //( "0".equals(javaVersionS) ? true : false ) 
		Float.parseFloat(javaVersionS.substring(0, javaVersionS.indexOf('.', 3))) >= 1.6;

		static {			
			// Set the logger print stream to a suitable file
			try {
				File f = new File(".", "GaianJDBCDiscovery.log");
				PrintStream mLogPrintStream = new PrintStream( new FileOutputStream(f) );
				if ( Logger.LOG_NONE < Logger.logLevel ) System.out.println("GAIAN CLIENT LOG FILE:\t" + f.getAbsolutePath() );
				Logger.setPrintStream( mLogPrintStream );
			}
			catch( Exception e ){
				System.out.println ("Could not set the Gaian Client log file "+ e.getMessage() );
			}

			// Set the config file to the client file.
			try {
				GaianDBConfig.setConfigFile( DEFAULT_CLIENT_CONFIG );
				File f = new File(DEFAULT_CLIENT_CONFIG + ".properties");
				if ( Logger.LOG_NONE < Logger.logLevel ) System.out.println("GAIAN CLIENT CONFIG FILE:\t" + f.getAbsolutePath() + " (exists:" + f.exists() + ")\n");
			}
			catch (Exception e) {
				logger.logException(GDBMessages.DISCOVERY_JDBC_SET_CONFIG_ERROR, "Could not set the JDBC client driver config file.", e);
			}
			
	    	//We need to have the Derby client driver loaded.
			try {
				Class.forName( GaianDBConfig.DERBY_CLIENT_DRIVER);
			}
			catch (ClassNotFoundException e) {
				logger.logException(GDBMessages.DISCOVERY_DERBY_JDBC_DRIVER_LOAD_ERROR, "Could not load the Derby JDBC driver.", e);
			}

			// Check that we can use the multicast address for discovery
			try {
				DEFAULT_MULTICAST_GROUP_ADDRESS = InetAddress.getByName( DEFAULT_MULTICAST_GROUP_IP );

			} catch (UnknownHostException e) {
				System.out.println("Unable to resolve default multicast address: " + e);
			}

		}
	
	// isLooping is used to determine if its worth logging processing for the next set of messages.
	// e.g. not the case if we received an irrelevant packet, or just timed out waiting for one - however if config just changed then it is false
	private static boolean isLooping = false;
	
	static { GaianDBConfig.setGaianNodeName("ClientSeeker_" + GaianDBConfig.getGaianNodeHostName().replaceAll("\\W", "")); }
	private static final String myNodeID = GaianDBConfig.getGaianNodeID();
	
	private static Set<InetAddress> broadcastIPsSet = new HashSet<InetAddress>();
	
	
	/**
	 * The following methods are provided to manage the use of  multicast sockets
	 */
	
	private static void joinMulticastGroupPerInterface( MulticastSocket skt, InetAddress multicastGroupAddress ) throws UnknownHostException {
		toggleMulticastGroupMembershipPerInterface(skt, multicastGroupAddress, true);
	}
	
	private static void leaveMulticastGroupPerInterface( MulticastSocket skt, InetAddress multicastGroupAddress ) throws UnknownHostException {	
		toggleMulticastGroupMembershipPerInterface(skt, multicastGroupAddress, false);
	}
	
	private static void toggleMulticastGroupMembershipPerInterface( MulticastSocket skt, InetAddress multicastGroupAddress, 
			boolean isJoining ) throws UnknownHostException {
		
		HashSet<InetAddress> multicastInterfaces = getMulticastInterfaces();
		
		logger.logDetail( (isJoining ? "Joining" : "Leaving") +
				" multicast group: " + multicastGroupAddress + " on interfaces: " + multicastInterfaces);
		for ( InetAddress localInterfaceAddress : multicastInterfaces )
			toggleMulticastGroupMembership(skt, multicastGroupAddress, localInterfaceAddress, isJoining);
	}
	
	private static void toggleMulticastGroupMembership( MulticastSocket skt, InetAddress multicastGroupAddress, 
			InetAddress localInterfaceAddress, boolean isJoining ) {
		
		try {
			skt.setInterface(localInterfaceAddress);
			
//			logger.logDetail( (isJoining ? "Joining" : "Leaving") + 
//					" multicast group: " + multicastGroupAddress + " on interface: " + skt.getInterface());

			if ( isJoining ) skt.joinGroup(multicastGroupAddress); else skt.leaveGroup(multicastGroupAddress);
			
		} catch (Exception e) {
			logger.logInfo("Unable to " + (isJoining?"join":"leave") + " multicast group " + multicastGroupAddress + 
					" on interface " + localInterfaceAddress + " (ignored), cause: " + e);
		}
	}
	
	private static HashSet<InetAddress> getMulticastInterfaces() {
		// Determine and apply MULTICAST_INTERFACES property
		String miProperty = GaianDBConfig.getMulticastInterfaces();
		HashSet<InetAddress> multicastInterfaces = new HashSet<InetAddress>();

		if ( null != miProperty ) try {

			logger.logDetail("Resolving MULTICAST_INTERFACES: " + miProperty);

			if ( "ALL".equals(miProperty) ) {
				multicastInterfaces.add(InetAddress.getByName("localhost"));
				multicastInterfaces.addAll(Arrays.asList(InetAddress.getAllByName( GaianDBConfig.getGaianNodeHostName())));
			}
			else {
				for ( String mi : Util.splitByCommas( miProperty == null ? "" : miProperty ) )
					multicastInterfaces.add( InetAddress.getByName(mi) );
			}

		} catch ( Exception e ) {
			logger.logWarning(GDBMessages.DISCOVERY_MULTICAST_INTERFACES_APPLY_ERROR, "Unable to apply specified MULTICAST_INTERFACES (using default " +
					defaultInterface + "), cause: " + e);
		}		
		
		if ( multicastInterfaces.isEmpty() )
			// Just use default interface
			multicastInterfaces.add(defaultInterface);

		return multicastInterfaces;
	}
	
	// Methods to determine the network configuration of this computer.

	private static Set<String> getLocalIPs() {
		
		Set<String> localIPs = new HashSet<String>();
		
		Enumeration<NetworkInterface> en = null;
		String msg = null;
		try { en = NetworkInterface.getNetworkInterfaces(); }
		catch (SocketException e) { msg = e.toString(); }
		
		if ( null == en ) {
			logger.logWarning(GDBMessages.DISCOVERY_NETWORK_INTERFACES_RESOLVE_ERROR,"getLocalIPs: Unable to resolve local network interfaces (using empty set): " + msg);
			return localIPs; 
		}
		
		while ( en.hasMoreElements() ) {			
			Enumeration<InetAddress> ias = en.nextElement().getInetAddresses();
			while( ias.hasMoreElements() ) {
				String localIP = ias.nextElement().toString();
				localIP = localIP.substring(localIP.indexOf('/')+1);
				if ( !localIP.matches("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+") ) {
					logger.logDetail("Ignoring non ipv4 address: " + localIP);
					continue;
				}
				localIPs.add( localIP );
			}
		}
		
		return localIPs;
	}
	
	public static String getDefaultLocalIP() throws Exception {
		
		String defaultIP = null;
		Set<String> localIPs = getLocalIPs();
		if ( localIPs.isEmpty() ) throw new Exception("No local address found"); // return InetAddress.getByName("localhost");
		
		for ( String ip : localIPs ) {
			defaultIP = ip;
			// Use the first ip that doesn't look like a localhost one - if none, then use the last ip found
			if ( !ip.startsWith("127.") ) break;
		}
		
		return defaultIP;
	}
	
	/**
	 * This method prepares the necessary socket for network discovery of gaian nodes
	 */
	private DatagramSocket openNetworkSocket () throws SQLException {

		// Use a MulticastSocket (rather than a DatagramSocket) to allow multiple nodes on the same machine to bind to the same port.
		// Configure the Multicast socket to send/receive using any local address, or bind the multicast socket to a specific address..
		// Note that binding to a specific address can prevent multicast from finding other nodes.
		MulticastSocket skt = null;
		try {
			discoveryPort = GaianDBConfig.getDiscoveryPort(); // set port which we will send messages to...
			skt = new MulticastSocket(); // ... but create a socket based on a dynamically allocated free port (this is where we will receive ACK responses)

			String defaultIP = getDefaultLocalIP();
			defaultInterface = InetAddress.getByName(defaultIP); // don't use skt.getInterface() as it picks "0.0.0.0" on linux
	
			// This parameter allows the packet to traverse multiple routers.
			skt.setTimeToLive(50);
	
			// Hard coded multicast address that all GaianDB nodes will join
			joinMulticastGroupPerInterface( skt, DEFAULT_MULTICAST_GROUP_ADDRESS );
			
			skt.setSoTimeout(DISCOVERY_TIMEOUT);
		
		} catch ( Exception e ) {
			String msg = "Failed to open Discovery Socket. POSSIBLE LACK OF NETWORK CONNECTIVITY: "+e;
			logger.logWarning(GDBMessages.DISCOVERY_NETWORK_SOCKET_OPEN_ERROR, msg);
			throw new SQLException(msg);
		}
		return skt;

//		try {
//			return new DatagramSocket();
//		} catch (SocketException e) {
//			String msg = "Failed to open Discovery Socket. POSSIBLE LACK OF NETWORK CONNECTIVITY: "+e;
//			logger.logWarning(GDBMessages.DISCOVERY_NETWORK_SOCKET_OPEN_ERROR, msg);
//			throw new SQLException(msg);
//		}
	}
	
	/**
	 * This method closes the network socket.
	 */
	private void closeNetworkSocket (DatagramSocket socket ) {

		// Use a MulticastSocket (rather than a DatagramSocket) to allow multiple nodes on the same machine to bind to the same port.
		// Configure the Multicast socket to send/receive using any local address, or bind the multicast socket to a specific address..
		// Note that binding to a specific address can prevent multicast from finding other nodes.
		try {
			if ( null != socket ) {
				logger.logDetail("Leaving multicast group and closing socket"); // ignore message
				leaveMulticastGroupPerInterface( (MulticastSocket)socket, DEFAULT_MULTICAST_GROUP_ADDRESS );
				socket.close();
			}
		}
		catch (Exception e){
			logger.logException(GDBMessages.DISCOVERY_NETWORK_SOCKET_CLOSE_ERROR, "Exception closing Network Socket", e);
		}
	}
	
	/** 
	 * This method discovers gaian databases in the visible network according to the parameters configured
	 * in the gaian configuration properties file, allowing broadcast and multi cast discovery and the 
	 * configuration of access clusters and permitted and denied hostnames. Gateway discovery is not supported
	 */
	public Connection discoverGaianConnection(Properties connProperties) throws SQLException {
		
		Connection thisConnection = null;
		
		// Not currently used - but would help for configuring different settings for different clients
		String configFileName = connProperties.getProperty("config");
		if ( null != configFileName )
			try { GaianDBConfig.setConfigFile(configFileName); }
			catch (Exception e1) {
				logger.logWarning(GDBMessages.DISCOVERY_CLIENT_CONFIG_FILE_ERROR, e1.getMessage());
				throw new SQLException(e1);
			}
		
		// Establish suitable network connectivity
		DatagramSocket socket = openNetworkSocket();
		
//		System.out.println("Using connection strategy: " + GaianDBConfig.getConnectionStrategy());
		
		// Send a discovery message to multicast sockets
		try {
			sendDiscoveryRequestMessage( socket, !isLooping, connProperties ); //TBD PDS check what these do!!
		}
		catch (IOException ioe)
		{
			String msg = "Error sending discovery requests";
			logger.logException(GDBMessages.DISCOVERY_REQUEST_SEND_ERROR, msg, ioe);
			throw new SQLException(msg);
		}

		// Receive responses to our discovery request
		
		byte[] buf = new byte[500];
		DatagramPacket packet = new DatagramPacket(buf, buf.length);

		// keep track of received acknowledgements
		String bestNodeID = null;
		String bestNodeIP = null;
		String bestUsr = null;
		String bestScrambledpwd = null;
		boolean ackReceived = false;
		
		// Work out when we should stop waiting for response messages.
		long timeoutTime = System.currentTimeMillis() + DISCOVERY_TIMEOUT;
		
		// Wait for incoming messages
		while (System.currentTimeMillis() < timeoutTime && !ackReceived) {

			try { 
				socket.setSoTimeout((int)(timeoutTime-System.currentTimeMillis()));
				socket.receive(packet);
			}
			catch ( SocketTimeoutException e ) {
				continue;
			}
			catch ( IOException e) {
				continue;
			}
			
			// Process incoming messages

			String msg = new String( packet.getData(), packet.getOffset(), packet.getLength() ).trim();

			String senderIP = packet.getAddress().getHostAddress();

			String permittedHostsProperty = GaianDBConfig.getAccessHostsPermitted();
			Set<String> permittedHosts = null == permittedHostsProperty ? null :
				new HashSet<String>( Arrays.asList( Util.splitByCommas(permittedHostsProperty.toUpperCase()) ) );
			String deniedHostsProperty = GaianDBConfig.getAccessHostsDenied();
			Set<String> deniedHosts = null == deniedHostsProperty ? null :
				new HashSet<String>( Arrays.asList( Util.splitByCommas(deniedHostsProperty.toUpperCase()) ) );
			Set<String> myAccessClusters = new HashSet<String>( Arrays.asList( Util.splitByCommas( GaianDBConfig.getAccessClusters() )));
			
			// Strip a rebroadcasted message back to original message and determine the originator.
			boolean isReBroadcastedMessage = msg.startsWith("X");
			if ( isReBroadcastedMessage ) {
				// Get the real originator of the request
				int idx = msg.indexOf(' ');
				if ( 0 < idx && msg.length() > idx+1 ) senderIP = msg.substring(1, idx);
				else {
					logger.logInfo("Erroneous message received (unable to resolve original sender ip): " + msg);
					continue;
				}
				msg = msg.substring(idx+1);
			}
			
			// IMPORTANT NOTE: For backwards compatibility, the message must start with the token 'REQ' or 'ACK'
			// followed by a space and then by the NodeID that the REQ is sent by (or ACK is destined to).
			String[] msgTokens = msg.split(" ");				
			
			if ( 2 > msgTokens.length ) continue;
			
			String prefix = msgTokens[0].trim();
			
			if ( prefix.equals("ACK") ) {
				// An acknowledgement has been received, which is a possible node for us to connect to.

				String senderNodeID = msgTokens[2].trim();
				
				// check the validity of this ack - can we confirm the access cluster behaviour, and is the 
				// responder in our configured set of permitted or denied hosts?
				
				// Check if some access clusters were specified and not respected by the other node...
				// This will be the case if there is no value in msgTokens[5] - meaning the version must be 1.03 or earlier
//				System.out.println("Node " + senderNodeID + ", msgTokens: " + Arrays.asList(msgTokens) + ", length: " + msgTokens.length);
				if ( !myAccessClusters.isEmpty() && ( msgTokens.length < 6 || msgTokens[5].compareTo("1.04") < 0 )) {
					logger.logDetail("ACK ignored because foreign node " + senderNodeID +
							" is running a version of GaianDB prior to 1.04 which cannot match our cluster membership IDs");
					continue;
				}

//				String intendedRecipient = msgTokens[1].trim();
//				
//				if ( myNodeID.equals(intendedRecipient) ) {
//					System.out.println("Message received for which ")
//					logger.logDetail(")
//				}

				int portDefIndex = senderNodeID.lastIndexOf(':');
				String host = -1 == portDefIndex ? senderNodeID : senderNodeID.substring(0, portDefIndex);
				host = host.toUpperCase();
				
				if ( null != deniedHosts && deniedHosts.contains(host) ||
						 null != permittedHosts && !permittedHosts.contains(host) ) continue;
				
				//Accept the first offered connection
				bestNodeID = senderNodeID;
				bestNodeIP = senderIP;
				bestUsr = msgTokens[3].trim();
				bestScrambledpwd = msgTokens[4].trim();
				ackReceived = true;
			}
		}
		
		// Connect to the offered node
		if (ackReceived) {
			int portDefIndex = bestNodeID.lastIndexOf(":");
			String port = -1 == portDefIndex ? ""+GaianNode.DEFAULT_PORT : bestNodeID.substring(portDefIndex+1);
			
			String physicalDB = GaianDBConfig.ATTACHMENT_TO_USER_DB_NODE.equals( GaianDBConfig.getConnectionStrategy() ) ?
					connProperties.getProperty("user").replaceAll("\\W", "_")
					: GaianDBConfig.GAIANDB_NAME + (-1 == portDefIndex ? "" : port);
			
			String nodeIPPortDB = bestNodeIP + ':' + port + "/" + physicalDB;
			String url = "jdbc:derby://" + nodeIPPortDB + ";create=true";

			String unscrambledPassword = GaianDBConfig.unscramble(bestScrambledpwd, bestNodeID);
			
			// load the driver class
			try {
				Class.forName( GaianDBConfig.DERBY_CLIENT_DRIVER);
			}
			catch (ClassNotFoundException e) {
				String msg = "Could not load the Derby JDBC client driver";
				logger.logException( GDBMessages.DISCOVERY_CONNECTION_ERROR, msg, e );
				throw new SQLException(msg);
			}

			try {
				logger.logInfo( "Discovered a Gaian Node, connecting to url: " + url );
//				DriverManager.setLoginTimeout(DERBY_CONNECTION_TIMEOUT_SECONDS);
				String connUser = (connProperties.containsKey("user")?connProperties.getProperty("user"):bestUsr);
				String connPassword = (connProperties.containsKey("password")?connProperties.getProperty("password"):unscrambledPassword);
				
				thisConnection =  DriverManager.getConnection( url, connUser, connPassword ); // TODO for SVA: Need to connect using user's kerberos token (not a pwd)
				
				if ( GaianDBConfig.ATTACHMENT_TO_USER_DB_NODE.equals( GaianDBConfig.getConnectionStrategy() ) ) {
					
					// Do some initialisation to GDB-enable the new user database
					// Create the utility procedure in the current schema as this is the one we want to bootstrap
					
					final String GDBINIT_USERDB = "GDBINIT_USERDB";
					Statement s = thisConnection.createStatement();
					if (false == s.getConnection().getMetaData().getProcedures(null, null, GDBINIT_USERDB).next())
						// register the bootstrap procedure on the new user db
						s.execute( "CREATE PROCEDURE "+GDBINIT_USERDB+"()"
								+ " PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA"
								+ " EXTERNAL NAME 'com.ibm.gaiandb.GaianDBUtilityProcedures.initialiseGdbUserSchema'"
								);
					s.execute("call "+GDBINIT_USERDB+"()"); // execute it
					s.close();
				}
				
			} catch ( SQLException sqle){
				String msg = "Could not get connection to url: " + url + ", cause: " + sqle;
				logger.logException( GDBMessages.DISCOVERY_CONNECTION_ERROR, msg, sqle );
				throw new SQLException(msg);
			}
		} else {
			// Failed to connect - no one responded
			String infoMsg = "";
			try {
				String bips = "";
				for ( InetAddress bip : getAllBroadcastIPs() )
					bips += ", " + bip.getHostAddress();
				File cf = new File(DEFAULT_CLIENT_CONFIG + ".properties");
				infoMsg = "BROADCAST IPs CURRENTLY AVAILABLE ON THIS CLIENT:\t" + (0==bips.length()?"None":bips.substring(2)) + "\n"
						+ "(You can set DISCOVERY_IP to 1 or more of the above in the client config file, or set DISCOVERY_IP=BROADCAST_ALL, "
						+ "or comment it out to target default multicast group IP " + GaianNodeSeeker.DEFAULT_MULTICAST_GROUP_IP + ")\n"
						+ "Config file location: " + cf.getAbsolutePath() + " (exists:"+cf.exists()+")" + "\n";
			}
			catch( Exception e ){
				infoMsg = "Unable to derive broadcast IPs or config file on this client, cause: "+ e;
			}
			
			throw new SQLException("Unable to discover any nodes.\n"+
					"Try adjusting parameters in the client config file: DISCOVERY_IP to a valid IP (or list of IPs), and ACCESS_CLUSTERS to a valid cluster name.\n\n" +
					infoMsg);
		}
		
		// Close the discovery socket.
		closeNetworkSocket(socket);
		
		// return the connection
		return thisConnection;
	}
	
	
	/**
	 * Sends message to gateway: REQ <local node id> <destination multicast or broadcast ip>
	 * 
	 * Send REQ message to either:
	 * 	- A single broadcast ip		-> if DISCOVERY_IP in config file is set to a broadcast ip, e.g. 192.168.0.255 
	 * 	- A single multicast group	-> if DISCOVERY_IP is not set, or set to a multicast group ip (i.e. in range 224.0.0.0 to 239.255.255.255, inclusive)
	 * 	- A set of broadcast ips	-> if DISCOVERY_IP is set to BROADCAST_ALL and the runtime is Java 6. (Otherwise for Java 5, 255.255.255.255 is used)
	 * 
	 * When the address is a multicast group, it will be sent on all interfaces.
	 */
	private void sendDiscoveryRequestMessage( DatagramSocket skt, boolean printLog, Properties connProps ) throws IOException {
		String acs = GaianDBConfig.getAccessClusters();
//		String reqArgs = GaianDBConfig.DISCOVERY_PORT + "='" + skt.getPort() + "'";
		String reqArgs = null==acs ? "" : " " + GaianDBConfig.ACCESS_CLUSTERS + "='" + acs + "'";
		String connStrat = GaianDBConfig.getConnectionStrategy();
		// Set the connStrat to the random strategy if none is specified.
		if (null==connStrat) connStrat=GaianDBConfig.ATTACHMENT_RANDOM;
		else if (connStrat.equals(GaianDBConfig.ATTACHMENT_TO_USER_DB_NODE))
			connStrat += ":" + connProps.getProperty("user"); // Try to connect to a user db having the same name as the user name.
		reqArgs = reqArgs + (null==connStrat ? "" : " " + GaianDBConfig.CONNECTION_STRATEGY + "='" + connStrat + "'");
		sendDiscoveryRequestMessage(skt, myNodeID, null, reqArgs, printLog);
	}

	// send the message to all destinations specified in the configuration
	private void sendDiscoveryRequestMessage(
			DatagramSocket skt, String originatorNodeID, String originatorIP, String reqArgs, boolean printLog ) throws IOException {
		
		String reBroadcastInfo = null==originatorIP ? "" : 'X' + originatorIP + ' ';

		//Determine where the discovery message(s) should be sent.
		Set <InetAddress> discoveryAddresses = new HashSet<InetAddress>();
		
		String discoveryIP = GaianDBConfig.getDiscoveryIP();

		broadcastIPsSet.clear();
		if ( null == discoveryIP ) {
			// No discovery IP is configured in the config
			discoveryAddresses.add(DEFAULT_MULTICAST_GROUP_ADDRESS);
		} else if (discoveryIP.equalsIgnoreCase( BROADCAST_ALL ) || 0 == discoveryIP.length()) {
			// Populate discoveryAddresses so we broadcast to all interfaces.
			if (isJavaVersion6OrMore) {
				// resolve broadcast ips - instead of using config variable DISCOVERY_IP.
				discoveryAddresses.addAll( getAllBroadcastIPs() );
			} else {
				discoveryAddresses.add(InetAddress.getByName("255.255.255.255"));
			}
		} else {
			for ( String ip : Util.splitByCommas(discoveryIP) )
				try { discoveryAddresses.add( InetAddress.getByName(ip) ); }
			catch ( Exception e ) { logger.logWarning( GDBMessages.DISCOVERY_CONN_IP_VALIDATE_ERROR, "Unable to validate discovery IP " + ip + " (ignored): " + e); }
		}

		// Send a discovery request to each address in the discoveryAddresses.
		String msg = reBroadcastInfo + "REQ " + originatorNodeID;
		if ( printLog )
			logger.logDetail("Sending msg: " + msg + ", destinations: " + discoveryAddresses);
		for ( InetAddress DiscoveryIP : discoveryAddresses ) {
			String ip = DiscoveryIP.toString();
			sendMessage( skt, msg + " " + ip.substring(ip.indexOf('/')+1) + reqArgs, DiscoveryIP, false );
		}
	}
	
	public static final Set<InetAddress> getAllBroadcastIPs() throws SocketException {
		Set<InetAddress> broadcastIPs = new HashSet<InetAddress>();
		Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
		while ( nics.hasMoreElements() )
			for ( InterfaceAddress ifa : nics.nextElement().getInterfaceAddresses() ) {
				if ( null == ifa ) continue;
				InetAddress ba = ifa.getBroadcast();
				if ( null == ba ) continue;
				broadcastIPs.add( ba );
			}
		return broadcastIPs;
	}
	
	private void sendMessage( DatagramSocket skt, String msg, InetAddress destinationAddress, boolean printLog ) throws IOException {
		if ( null == skt ) return; // GaianNodeSeeker is disabled		
//		System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " Discovery msg: " + msg);		

		DatagramPacket req = new DatagramPacket( msg.getBytes(), msg.length(), destinationAddress, discoveryPort );
				
		if ( destinationAddress.isMulticastAddress() &&
				(skt instanceof MulticastSocket)){ // isMulticastGroupIP( destinationAddress.toString().substring(1) ) ) { // remove leading '/'
						
			HashSet<InetAddress> multicastInterfaces = getMulticastInterfaces();
			if ( printLog )
				logger.logDetail("Sending msg: " + msg + 
						", multicast group: " + destinationAddress + ", interfaces: " + multicastInterfaces);
			
			for ( InetAddress localInterfaceAddress : multicastInterfaces ) {
				((MulticastSocket)skt).setInterface(localInterfaceAddress);
				skt.send(req);
			}
		}
		else {
			if ( printLog )
				logger.logDetail("Sending msg: " + msg + ", destination: " + destinationAddress);
			skt.send(req);
		}
	}
}
