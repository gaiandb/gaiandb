/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.udpdriver.client.UDPDriver;

/**
 * This class establishes and maintains network connectivity for the gaian node
 * it sends and receives discovery messages (REQuests and ACKnowledgements) to
 * find and negotiate connections with other nodes.
 * The detailed behaviour is determined by configuration settings from the 
 * GaianDBConfig class.
 * 
 * @author DavidVyvyan
 */
public class GaianNodeSeeker implements Runnable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";

	private static final Logger logger = new Logger( "GaianNodeSeeker", 25 );
	
	// Member attribute used in the discovery protocol. Created at class scope
	// so it is only created once.
	private Random random = new Random( System.currentTimeMillis() );

	private static final String BROADCAST_ALL = "BROADCAST_ALL";
	
	// Multicast address must be between 224.0.0.0 and 239.255.255.255, inclusive.
	// The address 224.0.0.0 is reserved and should not be used.
	// Below is a hard coded multicast address used by previous versions of GaianDB
	public static final String DEFAULT_MULTICAST_GROUP_IP = "230.255.255.255";
	
	private static InetAddress DEFAULT_MULTICAST_GROUP_ADDRESS = null;
	
	static {
		try {
			DEFAULT_MULTICAST_GROUP_ADDRESS = InetAddress.getByName( DEFAULT_MULTICAST_GROUP_IP );
		} catch (UnknownHostException e) {
			System.out.println("Unable to resolve default multicast address (discovery is disabled): " + e);
		}
	}
	
	// Instance attributes used to maintain the state of the discovery protocol for this node.
	private static InetAddress discoveryAddress = null;
	private Set<InetAddress> knownNetworkInterfaceAddresses;
	private String miProperty;
	private String defaultIP;
	private boolean isDiscoveryIPaList;
	
	private static final int PREFERENTIAL_ATTACHMENT_HOLD_TIME_UNIT_MS = 100; // ms
	private static int maxDiscoveredConnections = GaianDBConfig.getMaxDiscoveredConnections();
	private static int requiredOutboundConnections = -1; // Default is no required outbound connections
	
	private static final String myNodeID = GaianDBConfig.getGaianNodeID();
	private static final String myNodeIDWithoutPortSuffix = -1 == myNodeID.indexOf(':') ? myNodeID : myNodeID.substring(0, myNodeID.indexOf(':'));
	private static final String myUser = GaianDBConfig.getGaianNodeUser();
	private static final String myPasswordScrambled = GaianDBConfig.getGaianNodePasswordScrambled();


	// Attributes for network access
	private static int discoveryPort;
	private static InetAddress defaultInterface;
	
	private static String discoveryIP = null;
	private static Set<InetAddress> broadcastIPsSet = new HashSet<InetAddress>();
	
	private static final Set<InetAddress> localhostInterfaces = new HashSet<InetAddress>();
	private static final Set<InetAddress> multicastInterfaces = new HashSet<InetAddress>();
	
	// check for incoming messages at this rate - make this short so the checker loop
	// checks the sending sockets with only a short delay.
	private static final int SOCKET_RECEIVE_TIMEOUT = 100;

	// a socket used to receive discovery messages.
	private MulticastSocket receivingSkt;

	// packet for receiving messages
	private byte[] buf = new byte[500];
	private DatagramPacket packet = new DatagramPacket(buf, buf.length);;

	// define a sending socket for each network interface. It is expensive to switch a single
	// socket from one interface to the other so we create and use one for each interface.
	private static final HashMap<InetAddress, MulticastSocket> interfaceSockets = new HashMap<InetAddress, MulticastSocket>();
	private static MulticastSocket generalSendSocket = null;

	
	// state attribute used to terminiate the discovery process when we have finished.
	private static boolean isRunning = false;
	
	// isLooping is used to determine if its worth logging processing for the next set of messages.
	// e.g. not the case if we received an irrelevant packet, or just timed out waiting for one - however if config just changed then it is false
	private static boolean isLooping = false;
	
	private static AtomicBoolean refreshDataSources = new AtomicBoolean(false);
		
	private static final GaianNodeSeeker gnsInstance = new GaianNodeSeeker();

	private static boolean isNewConnectionsNeedLoading = false;

	
	// ATTRIBUTES TO TRACK WHAT WE HAVE DISCOVERED ABOUT OTHER NODES
	
	// This containe the Gaian node id strings of node to which we have a maintained outbound connection.
	private static Set<String> outgoingConnections = new HashSet<String>();
	
	// acknowledged nodeid (=host+port combination) -> "<ack time(millis)> <host-ip>"
	// Used for inbound connection establishment
	private static ConcurrentMap<String, String> acknowledgedConnections = new ConcurrentHashMap<String, String>();
	
	private static Set<String> discoveredIPsViaUnicast = new HashSet<String>();

	//	The problem with the idea below is that node1/node2 may both be members of clusters A and B, then later one may drop membership to A whilst the 
	//	other drops membership to B yet they wd stay connected because membership updates are not communicated...
	//	// Used to periodically match/validate cluster memberships. Only needs to be a Hashtable (i.e. synchronized) as infrequently accessed.
	//	private static Map<String, Set<String>> clusterIDsOfConnectedNodes = new Hashtable<String, Set<String>>();

	// Set of nodes to whom we have dropped our connections due to removing a cluster membership, or adding one when we had none.
	// This is required to halt queries coming from those nodes in future.
	private static Set<String> droppedNodesDueToClusterIdUpdate = new HashSet<String>();
	private static Set<String> previousClusterIDs = new HashSet<String>( Arrays.asList( Util.splitByCommas( GaianDBConfig.getAccessClusters() )));

	/**
	 * This method creates and runs a new thread executing the Gaian Node Seeker.
	 */
	public static void maintainSeeker() {

		// Query discovery ip (independantly from the global one or we cd get a NullPointerException)
		String discoveryIP = GaianDBConfig.getDiscoveryIP();
		if ( isRunning || null != discoveryIP && 0 == discoveryIP.length() ) return;
		
		// Just start multicasting on any local IP, ignore the network interfaces config..
		// Note, as found in testing on Ubuntu Linux, the network interfaces list may not always list all used IPs
		// e.g. an IP alias for the hostname as listed in /etc/hosts
		new Thread( gnsInstance, GaianNode.THREADNAME_NODE_SEEKER ).start();
		isRunning = true;
	}
	
	/****************************************************************
	 * A GROUP OF METHODS FOR MAINTAINING CONNECTIVITY TO THE NETWORK 
	 ****************************************************************/
	
	/**
	 * This method creates a socket for a non-specific  interface.
	 * @return the created socket for sending messages.
	 */
	private static MulticastSocket createSendingSocket() throws IOException {
		
		MulticastSocket newSkt = null;
		
//		try { // Better to throw the exception up to where we have the context of the request - to allow better logging and control flow.
		
		newSkt = new MulticastSocket( GaianDBConfig.getDiscoveryPort());
		// This parameter allows the packet to traverse multiple routers.
		newSkt.setTimeToLive(50); 
			
		newSkt.setSoTimeout( 1 ); //Set the timeout very low so we can quickly check if we have a message on the socket.
			
//		} catch (IOException e) {
//			logger.logException(GDBMessages.DISCOVERY_MULTICAST_INTERFACES_ERROR, "createSendingSocket, unable to create socket), cause: ", e);
//		}
			
		return newSkt;
	}
	// 
	/**
	 * We have separate sockets for each network interface - to avoid the overhead of switching a socket 
	 * between interfaces. This method creates a socket for the specified interfaces.
	 * @param interfaceAddress - An IP address associated with the required network interface 
	 * @return the created socket for sending messages
	 * @throws SocketException 
	 */
	private static MulticastSocket createSendingSocketforInterface (InetAddress interfaceAddress) throws IOException {
		
		MulticastSocket newSkt = createSendingSocket();
		newSkt.setInterface(interfaceAddress);
		return newSkt;
	}
	
	/**
	 * This method returns a socket which can be used to send a message over the named interfaces.
	 * The sockets are stored in interfaceSockets and reused. If one does not exist for that
	 * interface, it is created.
	 * @param interfaceAddress - An IP address associated with the required network interface
	 * @return the created socket for sending messages
	 * @throws SocketException
	 */
	private static MulticastSocket socketForInterface (InetAddress interfaceAddress) throws IOException {
		
		MulticastSocket interfaceSkt = interfaceSockets.get(interfaceAddress);
		
		if ( null == interfaceSkt ) {
			interfaceSkt = createSendingSocketforInterface(interfaceAddress);
			interfaceSockets.put(interfaceAddress, interfaceSkt);
		}
		
		return interfaceSkt;
	}

	/**
	 * This method returns a socket which can be used to send a message over a non specific interface.
	 * The socket is created and initialised if it doesn't yet exist and stored in the 
	 * generalSendSocket instance variable for reuse.
	 * @return the created socket for sending messages
	 */
	private static MulticastSocket socketForNonMulticast () throws IOException {
		
		if (generalSendSocket == null) {
		    generalSendSocket = createSendingSocket();
		}
		return generalSendSocket;
	}

	/**
	 * This method closes any multicast sockets - the passed receive socket and anyn.
	 * @param receivingSkt
	 * @return 
	 * @throws UnknownHostException
	 */
	private MulticastSocket closeMulticastSockets(MulticastSocket receivingSkt) {
		if ( null != receivingSkt ) {
			try {
				leaveMulticastGroupPerInterface( receivingSkt, DEFAULT_MULTICAST_GROUP_ADDRESS );
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				logger.logException( GDBMessages.DISCOVERY_LOOP_ERROR, "Failed to leave the multicast group: ", e );
			}
			receivingSkt.close();
			receivingSkt = null;
		}
		if ( null !=  generalSendSocket ) {
			generalSendSocket.close();
			generalSendSocket = null;
		}
		for (MulticastSocket sendingSocket : interfaceSockets.values()) {
			sendingSocket.close();
		}
		interfaceSockets.clear();
		return receivingSkt;
	}
	
	/**
	 * This method checks for incoming messages on the sockets that we send requests on.
	 * In some instances ACKs are received on these sockets.
	 * @param receivePacket, the packet into which we try to receive a message,
	 * @return a boolean flag which is true when a message is received, and needs to be processed.
	 */
	private boolean checkMulticastSocketsforIncomingMessages(DatagramPacket receivePacket) {
		for (InetAddress interf : interfaceSockets.keySet()){
			
			MulticastSocket sendingSocket = interfaceSockets.get(interf);
			try { sendingSocket.receive(receivePacket); 
		        String msg = new String( receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength() ).trim();
		        logger.logDetail("Received msg on sending datagramsocket for interface: " +interf+", msg: " + msg);
			    return true;
			}
			catch ( SocketTimeoutException e ) {
				// no message waiting.. a good thing!
			} catch (IOException e) {
				logger.logDetail("IO exception receiving from multicast socket : " + e);
			}
		}	
		
		try { 
			if (null != generalSendSocket) {
				generalSendSocket.receive(receivePacket); 
				String msg = new String( receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength() ).trim();
				logger.logDetail("Received msg on the general send socket : " + msg);
				return true;
			}
		}
		catch ( SocketTimeoutException e ) {
			// no message waiting.. a good thing!
		} catch (IOException e) {
			logger.logDetail("IO exception receiving from the general send socket : " + e);
		}
		return false;
	}
	
	private static void joinMulticastGroupPerInterface( MulticastSocket skt, InetAddress multicastGroupAddress ) throws UnknownHostException {
		toggleMulticastGroupMembershipPerInterface(skt, multicastGroupAddress, true);
	}
	
	private static void leaveMulticastGroupPerInterface( MulticastSocket skt, InetAddress multicastGroupAddress ) throws UnknownHostException {	
		toggleMulticastGroupMembershipPerInterface(skt, multicastGroupAddress, false);
	}
	
	private static void toggleMulticastGroupMembershipPerInterface( MulticastSocket skt, InetAddress multicastGroupAddress, 
			boolean isJoining ) throws UnknownHostException {
		
		logger.logInfo( (isJoining ? "Joining" : "Leaving") +
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
	


	/**
	 * @param dEFAULTMULTICASTGROUPADDRESS 
	 * @throws IOException
	 * @throws UnknownHostException
	 * @throws SocketException
	 */
	private void initialiseReceivingSocket( InetAddress defaultMulticastGroupAddress ) throws IOException, UnknownHostException, SocketException {
		
//		logger.logInfo("Setting up multicast socket.. default IP is now: " + InetAddress.getLocalHost().getHostAddress() );
		
		// Use a MulticastSocket (rather than a DatagramSocket) to allow multiple nodes on the same machine to bind to the same port.
		// Configure the Multicast socket to send/receive using any local address, or bind the multicast socket to a specific address..
		// Note that binding to a specific address can prevent multicast from finding other nodes.
		receivingSkt = new MulticastSocket( discoveryPort = GaianDBConfig.getDiscoveryPort() );
		
//				System.out.println( "skt receive buffere size: " + skt.getReceiveBufferSize() ); // usually quite high - ample for our needs
		
		
		// The socket times out when nothing has been received 1) to check/apply any newly updated config properties 
		// and 2) to send a new poll REQ request if this node is seeking connections to maintain.
		// The later should only happen when the ConnectionsChecker Heartbeat itself is exceeded.
		
		// Timeout after a long-ish amount of time if no one responds and we want to connect...
		// This should only happen when no nodes are in the same multicast group.
		receivingSkt.setSoTimeout( SOCKET_RECEIVE_TIMEOUT ); //isNeedsConnections ? POLL_TIMEOUT : 0 );
		
//				skt.setLoopbackMode(true); // Disable loop back of messages - default is enabled
//				System.out.println("Multicast loopback: " + skt.getLoopbackMode());

		// This parameter allows the packet to traverse multiple routers.
		receivingSkt.setTimeToLive(50);
		
		// Hard coded multicast address that all GaianDB nodes will join
		joinMulticastGroupPerInterface( receivingSkt, defaultMulticastGroupAddress );
	}

	/**
	 * @param receivingSkt
	 * @param previousDiscoveryIP
	 * @param isDiscoveryIPaList
	 * @param interfacesHaveChanged
	 * @return
	 * @throws UnknownHostException
	 */
	private String rejoinMulticastGroupIfDiscoveryIPChanged(
			MulticastSocket receivingSkt, String previousDiscoveryIP,
			boolean isDiscoveryIPaList, boolean interfacesHaveChanged)
			throws UnknownHostException {
		if ( !discoveryIP.equals( previousDiscoveryIP ) || interfacesHaveChanged ) {
			
			isLooping = false;
			
			// If there was a previous different DISCOVERY_IP config value which was not the default multicast value and was not a list...
			if ( null != previousDiscoveryIP && !previousDiscoveryIP.equals(DEFAULT_MULTICAST_GROUP_IP)
					&& -1 == previousDiscoveryIP.indexOf(',') ) {
				
				// Identify if this was a multicast group ip, and if so then leave the group now on all interfaces
				if ( InetAddress.getByName(previousDiscoveryIP).isMulticastAddress() ) //isMulticastGroupIP( previousDiscoveryIP ) )
					leaveMulticastGroupPerInterface( receivingSkt, discoveryAddress );
			}
			
			if ( !isDiscoveryIPaList ) {
				// Parse/Resolve the IP String as an IP address
				discoveryAddress = InetAddress.getByName( discoveryIP );

				// If there was a previous different DISCOVERY_IP config value which was not the default multicast value or a list...
				if ( !discoveryIP.equals(DEFAULT_MULTICAST_GROUP_IP) ) {
					
					// We will generally be broadcasting discovery messages within the local domain only. However, we may want 
					// to alternatively send messages to a Multicast group, as these can (unlike a broacast) extend to 
					// machines outside the local domain if the gateways allow it (assuming Multicast is enabled in the network).
					// Join a specific multicast group (hard coded for now) to make it possible to listen out for cross-domain msgs.
					if ( discoveryAddress.isMulticastAddress() ) //isMulticastGroupIP( discoveryIP ) )
						joinMulticastGroupPerInterface( receivingSkt, discoveryAddress );
				}
			}
			
			previousDiscoveryIP = discoveryIP;
		}
		return previousDiscoveryIP;
	}

	/**
	 * @param receivingSkt
	 * @return
	 * @throws Exception
	 * @throws UnknownHostException
	 */
	private boolean rejoinMulticastGroupIfInterfacesChange(
			MulticastSocket receivingSkt) throws Exception,
			UnknownHostException {
		boolean interfacesHaveChanged;

		// Check to see whether network interfaces have changed - rejoin the multicast groups if they have.
		Set<InetAddress> newNetworkInterfaceAddresses = Util.getAllMyHostIPV4Adresses(); //Arrays.asList( InetAddress.getAllByName( myHostName ) );
		interfacesHaveChanged = !Util.setDisjunction( knownNetworkInterfaceAddresses, newNetworkInterfaceAddresses ).isEmpty();

//						System.out.println("Checking if interfaces have changed.. previous: " + 
//								previousNetworkInterfaceAddresses + ", new: " + newNetworkInterfaceAddresses + ", result: " + interfacesHaveChanged);
		
		knownNetworkInterfaceAddresses.clear();
		knownNetworkInterfaceAddresses.addAll(newNetworkInterfaceAddresses);
		
		if ( interfacesHaveChanged ) {
			// refresh memberships for default multicast group
			leaveMulticastGroupPerInterface( receivingSkt, DEFAULT_MULTICAST_GROUP_ADDRESS );
			if ( "ALL".equals(miProperty) ) { multicastInterfaces.clear(); multicastInterfaces.addAll( newNetworkInterfaceAddresses ); }
			joinMulticastGroupPerInterface( receivingSkt, DEFAULT_MULTICAST_GROUP_ADDRESS );
		}
		return interfacesHaveChanged;
	}
	
	private static Set<String> getLocalIPs( boolean isIncludeVirtualIPs ) {
		
		Set<String> localIPs = new HashSet<String>();
		
		Enumeration<NetworkInterface> en = null;
		String msg = null;
		try { en = NetworkInterface.getNetworkInterfaces(); }
		catch (SocketException e) { msg = e.toString(); }
		
		if ( null == en ) {
			logger.logWarning(GDBMessages.DISCOVERY_IP_LOCAL_RESOLVE_ERROR, "getLocalIPs: Unable to resolve local network interfaces (using empty set): " + msg);
			return localIPs; 
		}
		
		while ( en.hasMoreElements() ) {
			NetworkInterface ni = en.nextElement();
			if ( false == isIncludeVirtualIPs && ni.isVirtual() ) continue;
			Enumeration<InetAddress> ias = ni.getInetAddresses();
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
	
	/**
	 * Get the best IP to be used as interface ID for multicasting by default on a single interface.
	 * This will preferentially be the first resolved IP on an external network interface.
	 * If none exist, then it will be a local interface IP.
	 * This will never be a virtual interface IP.
	 * 
	 * @return
	 * @throws Exception
	 */
	public static String getDefaultLocalIP() throws Exception {
		
		String defaultIP = null;
		Set<String> localIPs = getLocalIPs( false );
		if ( localIPs.isEmpty() ) throw new Exception("No local address found"); // return InetAddress.getByName("localhost");
		
		for ( String ip : localIPs ) {
			defaultIP = ip;
			// Use the first ip that doesn't look like a localhost one - if none, then use the last ip found
			if ( !ip.startsWith("127.") ) break;
		}
		
		return defaultIP;
	}
		
	/********************************************************************
	 * METHODS FOR MAINTAINING DISCOVERY BASED ON THE GAIAN CONFIGURATION 
	 *******************************************************************/
	
	/**
	 * @throws SocketException
	 */
	private void applyDiscoveryIPConfig() throws SocketException {
		discoveryIP = GaianDBConfig.getDiscoveryIP();
		if ( null == discoveryIP ) discoveryIP = DEFAULT_MULTICAST_GROUP_IP;
		
		isDiscoveryIPaList = false;
		
		broadcastIPsSet.clear();
		if (discoveryIP.equalsIgnoreCase( BROADCAST_ALL )) {
			if (GaianNode.isJavaVersion6OrMore) {
				isDiscoveryIPaList = true;
				// resolve broadcast ips - instead of using config variable DISCOVERY_IP.
				Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
				while ( nics.hasMoreElements() )
					for ( InterfaceAddress ifa : nics.nextElement().getInterfaceAddresses() ) {
						if ( null == ifa ) continue;
						InetAddress ba = ifa.getBroadcast();
						if ( null == ba ) continue;
						broadcastIPsSet.add( ba );
					}
			} else {
				discoveryIP = "255.255.255.255";
			}
		} else {
			isDiscoveryIPaList = -1 != discoveryIP.indexOf(',');
			// check if discoveryIP is a list - if so, fill up broadcastIPs with its elements
			if ( isDiscoveryIPaList )
				for ( String ip : Util.splitByCommas(discoveryIP) )
					try { broadcastIPsSet.add( InetAddress.getByName(ip) ); }
					catch ( Exception e ) { logger.logWarning(GDBMessages.DISCOVERY_IP_VALIDATE_ERROR, "Unable to validate discovery IP " + ip + " (ignored): " + e); }
		}
	}

	/**
	 * @return
	 */
	private String applyMulticastInterfacesConfig() {
		String miProperty = GaianDBConfig.getMulticastInterfaces();

		multicastInterfaces.addAll( localhostInterfaces ); // always add these? the problem is that external interfaces don't always allow multicast...
		
		if ( null == miProperty )
			// Just use default interface
			multicastInterfaces.add(defaultInterface);
		else try {

			logger.logInfo("Resolving MULTICAST_INTERFACES: " + miProperty);
			
			if ( "ALL".equals(miProperty) ) {
				
//						multicastInterfaces.add(InetAddress.getByName("localhost"));

//						multicastInterfaces.addAll(Arrays.asList(InetAddress.getAllByName(myHostName)));

//						for ( InetAddress iaddr : InetAddress.getAllByName(myHostName) )
//							if ( iaddr instanceof Inet4Address )
				multicastInterfaces.addAll( Util.getAllMyHostIPV4Adresses() );
				
//						System.out.println("Added all by name for my host to multicast interface addresses, new set: " + multicastInterfaces);
			}
			else {
				for ( String mi : Util.splitByCommas( miProperty == null ? "" : miProperty ) )
					multicastInterfaces.add( InetAddress.getByName(mi) );
			}
			
		} catch ( Exception e ) {
			logger.logWarning(GDBMessages.DISCOVERY_MULTICAST_INTERFACES_ERROR, "Unable to apply specified MULTICAST_INTERFACES (using default " +
					defaultInterface + "), cause: " + e);
		}
		return miProperty;
	}
	
	// Returns true if the flag was set, and sets the flag to false
	public static boolean testAndClearConfigReloadRequiredFlag() {
		isLooping = false;
		return refreshDataSources.compareAndSet(true, false);
	}
	
	/******************************************************
	 * DISCOVERY MESSAGE PROCESSING METHODS 
	 *****************************************************/

	/**
	 * processDiscoveryMessage performs whatever actions are necessary to process the message packet
	 * received and passed in the "packet" parameter.
	 * @param packet
	 * @param discoveredConnections
	 * @param permittedHosts
	 * @param deniedHosts
	 * @param myAccessClusters
	 * @param numOutgoingConnectionsSought
	 * @param isNeedsConnections
	 * @return The flag returned indicates whether the given message was processed.
	 * @throws IOException
	 * @throws UnknownHostException
	 * @throws InterruptedException
	 */
	private boolean processDiscoveryMessage(
			DatagramPacket packet, String[] discoveredConnections,
			Set<String> permittedHosts, Set<String> deniedHosts,
			Set<String> myAccessClusters, int numOutgoingConnectionsSought,
			boolean isNeedsConnections)
			throws IOException, UnknownHostException, InterruptedException {
			String msg = new String( packet.getData(), packet.getOffset(), packet.getLength() ).trim();
			
			String senderIP = packet.getAddress().getHostAddress();
			
			boolean isReBroadcastedMessage = msg.charAt(0)=='X';
			if ( isReBroadcastedMessage ) {
				// Get the real originator of the request
				int idx = msg.indexOf(' ');
				if ( 0 < idx && msg.length() > idx+1 ) senderIP = msg.substring(1, idx);
				else {
					logger.logInfo("Erroneous message received (unable to resolve original sender ip): " + msg);
					return true; //message has been processed
				}
				msg = msg.substring(idx+1);
			}
			
			// IMPORTANT NOTE: For backwards compatibility, the message must start with the token 'REQ' or 'ACK'
			// followed by a space and then by the NodeID that the REQ is sent by (or ACK is destined to).
			String[] msgTokens = msg.split(" ");				
			
			if ( 2 > msgTokens.length ) return true; //message has been processed
			
			String prefix = msgTokens[0].trim();
			String nodeID = msgTokens[1].trim();
							
			// Check if the message was potentially destined to a node running on a seperate port and was sent 
			// as a gateway message (therefore unicast)... in that case, re-broadcast the message on localhost...
			// Pseudo:
			//		If this msg was sent by a remote host (i.e. not re-broadcasted already) AND
			//			|		It is a REQ which is a gateway request (i.e. senderIP = <reply-to-ip>)
			//			|	OR	It is an ACK which was destined to a node running on different port
			if ( !isReBroadcastedMessage ) {
				
				// Re-broadcast REQ if we are being used as a Gateway to access other nodes in our network.
				// Also include this node in future list of nodes to forward REQs to (coming from other nodes)
				if (  prefix.equals("REQ") && 2 < msgTokens.length && senderIP.equals( msgTokens[2].trim() ) ) {
					logger.logInfo( "Acting as Gateway by forwarding REQ message: " + msg );
					
					// Forward as broadcast or multicast
					sendDiscoveryRequestMessage( nodeID, senderIP, getArgsStringFromRequestMessage(msg), true);
					
					// Forward as unicast to previous ips discovered via unicast
//							for ( String ip : GaianDBConfig.getIPsOfDiscoveredNodes() ) {
					for ( String ip : discoveredIPsViaUnicast ) {
						logger.logInfo("Forwarding discovery message from " + senderIP + " to: " + ip);
						sendMessage( 'X' + senderIP + ' ' + msg, InetAddress.getByName(ip), true);
					}
					
					// Add to list of ips discovered outside of local nets
					discoveredIPsViaUnicast.add(senderIP);
					return true; //message has been processed
				}
				
				// Re-broadcast ACK locally if the nodeid differs - (ACKs are point-to-point msgs except in old versions of GaianDB)
				// Don't validate based on hostname as a nodeID doesn't have to be based on the hostname anymore...
				if ( prefix.equals("ACK") && !nodeID.equals(myNodeID) ) { //&& (nodeID.equals(myHostName) || nodeID.startsWith(myHostName+':')) ) {
					logger.logInfo("Locally forwarding ACK message (from " + senderIP + ") destined to localhost node " + nodeID + ": " + msg);
					try { sendMessage( 'X' + senderIP + ' ' + msg, InetAddress.getByName(DEFAULT_MULTICAST_GROUP_IP), localhostInterfaces, true); }
					catch (IOException e1) {
						// try to re-broadcast on loopback interface if multicast didn't work - raise warning if both fail
						try { sendMessage( 'X' + senderIP + ' ' + msg, InetAddress.getByName("127.255.255.255"), true); }
						catch (IOException e2) {
							logger.logWarning(GDBMessages.DISCOVERY_LOCALHOST_REBROADCASTING_ERROR,
									"Unable to locally re-multicast or re-broadcast ACK discovery message (from " +
									senderIP + ") destined to localhost node " + nodeID + ": " + msg +
									"\nException 1: " + Util.getStackTraceDigest(e1) + "\nException 2: " + Util.getStackTraceDigest(e2));
						}
					}
					return true; //message has been processed
				}
			}

			// If this is a REQ from myself or an ACK to someone else, then ignore...
			// i.e. Ignore messages that aren't destined to us; or requests sent by us...
			if ( !nodeID.equals(myNodeID) ^ prefix.equals("REQ") ) {
				return true; //message has been processed
			}
			
			logger.logDetail("Received msg: " + msg);
			
			if ( prefix.equals("REQ") ) {
				if (processReceivedDiscoveryRequest(packet, discoveredConnections,
						permittedHosts, deniedHosts, myAccessClusters,
						isNeedsConnections, msg, senderIP, nodeID)) {
					return true;
				}
				
			} else if ( prefix.equals("ACK") ){
				if (processReceivedDiscoveryAcknowledgement(permittedHosts,
						deniedHosts, myAccessClusters,
						numOutgoingConnectionsSought, msg, senderIP, msgTokens,
						prefix)) {
					return true;
				}
			} else {
				logger.logInfo("Skipping unrecognized message: " + msg); // ignore message
				return true; //message has been processed
			}
			
		return false;//no action taken
	}

	/**
	 * @param permittedHosts
	 * @param deniedHosts
	 * @param myAccessClusters
	 * @param numOutgoingConnectionsSought
	 * @param msg
	 * @param senderIP
	 * @param msgTokens
	 * @param prefix
	 */
	private boolean processReceivedDiscoveryAcknowledgement(
			Set<String> permittedHosts, Set<String> deniedHosts,
			Set<String> myAccessClusters, int numOutgoingConnectionsSought,
			String msg, String senderIP, String[] msgTokens, String prefix) {
			
		String senderNodeID = msgTokens[2].trim();
		
		String accessClustersVersion = "1.04";
		
		// Check if some access clusters were specified and not respected by the other node...
		// This will be the case if there is no value in msgTokens[5] - meaning the version must be 1.03 or earlier
		if ( !myAccessClusters.isEmpty() && ( msgTokens.length < 6 || msgTokens[5].compareTo(accessClustersVersion) < 0 )) {
			logger.logDetail("ACK ignored because foreign node " + senderNodeID +
					" is running a version of GaianDB prior to "+accessClustersVersion+"which cannot match our cluster membership IDs");
			return true; //message has been processed
		}
		
		// Cluster membership is ok against this node (or it wouldn't have sent an ACK back) - so don't reject queries from it anymore
		droppedNodesDueToClusterIdUpdate.remove(senderNodeID);
		
		// Ignore this ACK if the host is not allowed - do this before showing a msg about msg receipt
		int portDefIndex = senderNodeID.lastIndexOf(":");
		String host = -1 == portDefIndex ? senderNodeID : senderNodeID.substring(0, portDefIndex);
		host = host.toUpperCase();
		
		if ( null != deniedHosts && deniedHosts.contains(host) ||
			 null != permittedHosts && !permittedHosts.contains(host) ) return true; //message has been processed

		// wave the restriction on maintaining more than 2 connections if this is the first ACK from a node reached through a gateway.
		// -> this doesn't work because now we forward REQs to nodes beyond the gateway for them to respond, and we have no way of knowing
		// that the ACk came from one of them rather than one of the nodes we can discover in our own local subnets
		// -> to solve this the ACK would have to hold an extra label defining it as a node reached through a gateway...

		// Ignore this ACK if no outgoing connections are sought
		if ( 0 >= numOutgoingConnectionsSought ) {
			logger.logDetail("Ignoring ACK as numOutgoingConnectionsSought <= 0: " + numOutgoingConnectionsSought );
			return true; //message has been processed
		}
		
		if ( outgoingConnections.contains(senderNodeID) ) {
			logger.logDetail("Node already an outgoing slave (ignoring ACK): " + senderNodeID);
			return true; //message has been processed
		}
		
		logger.logInfo("ACK from " + senderNodeID + ", My ACKs are: " + acknowledgedConnections);
		
		// Check for race condition where 2 nodes send an ACK to each other at the same time
		if ( acknowledgedConnections.containsKey(senderNodeID) ) {
			
			// Ignore an ACK from a node we have just sent an ACK to ourselves if their node id is
			// lexicographically less than ours, i.e. let them initiate the connection.
			if ( 0 > senderNodeID.compareTo(myNodeID) ) {
				logger.logInfo("Ignoring ACK from lexicographically lower node we sent an ACK to: " + senderNodeID);
				return true; //message has been processed
			}
			// OK - so we acknowledged the REQ from senderNodeID, but actually it is us that will maintain
			// this connection, so revoke the ACK -
			// Note the sender node will not attempt to setup an outbound connection anyway because
			// it will have sent its ACK before receiving/processing ours - so removal here should not strictly,
			// be necessary; but its good to do it asap in case the connections are dropped and need rebuilding.
			acknowledgedConnections.remove(senderNodeID);
		}
		
		logger.logDetail("Processing msg: " + msg);
		
		String usr = msgTokens[3].trim();
		String scrambledpwd = msgTokens[4].trim();
		
		// Get message arguments. They come after the 3rd space character.
		Map<String, String> msgArgs = getMsgArguments( getArgsStringFromAcknowledgeMessage(msg) );
		String sslMode = msgArgs.get("ssl");
		
		String connID = null;
		
		if ( null != ( connID = addNodeConnection(senderNodeID, sslMode, senderIP, usr, scrambledpwd) ) ) {

			refreshDataSources.set(true);
			
			outgoingConnections.add(senderNodeID);

			String pfx = Logger.sdf.format(new Date(System.currentTimeMillis())) + " -------> "; // "\t"
			String s = "OUTBOUND connection " + connID + " established:  -> " + senderNodeID;
			synchronized( gnsInstance ) {
				isNewConnectionsNeedLoading = true;
			}
			
			logger.logImportant( Logger.HORIZONTAL_RULE + "\n" + pfx + s + "\n" );
			
			DatabaseConnectionsChecker.maintainTwoWayConnection(connID);
			
		} else {
			logger.logDetail("Ignoring " + prefix + " message");
			return true; //message has been processed
		}
		return false; // message has not been processed
	}

	/**
	 * @param packet
	 * @param discoveredConnections
	 * @param permittedHosts
	 * @param deniedHosts
	 * @param myAccessClusters
	 * @param isNeedsConnections
	 * @param msg
	 * @param senderIP
	 * @param nodeID
	 * @return a flag specifying whether the message has been processed.
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	private boolean processReceivedDiscoveryRequest(DatagramPacket packet,
			String[] discoveredConnections, Set<String> permittedHosts,
			Set<String> deniedHosts, Set<String> myAccessClusters,
			boolean isNeedsConnections, String msg, String senderIP,
			String nodeID) throws InterruptedException, IOException,
			UnknownHostException {
		String senderNodeID = nodeID;
		
		// Get message arguments. They come after the 3rd space character.
		Map<String, String> msgArgs = getMsgArguments( getArgsStringFromRequestMessage(msg) );
		String connectionStrategy = msgArgs.get(GaianDBConfig.CONNECTION_STRATEGY);
		
		//If the connectionStrategy is blank, assume that it is preferential attachment
		if (null == connectionStrategy) {connectionStrategy = GaianDBConfig.ATTACHMENT_PREFERENTIAL_ON_HIGH_CONNECTIVITY;};

		if ( discoveredConnections.length - outgoingConnections.size() >= maxDiscoveredConnections - requiredOutboundConnections ) {
			logger.logDetail("Max number of discovered connections reached: " + 
					discoveredConnections.length + ", ignoring request message: " + msg);
			return true; //message has been processed
		}
		
		// Validate cluster membership
		Set<String> clusters = new HashSet<String>( Arrays.asList( Util.splitByCommas( msgArgs.get(GaianDBConfig.ACCESS_CLUSTERS) ) ) );
		if ( !myAccessClusters.isEmpty() || !clusters.isEmpty() ) {
			
//							NOTE ==> AVOID PRINTING THE ACTUAL CLUSTER IDs TO STDOUT OR IN THE LOGS!!
			
			if ( clusters.isEmpty() ) {
				logger.logDetail("Ignoring REQ message as it contains no cluster ID. Our cluster membership count is: " + myAccessClusters.size());
				return true; //message has been processed
			}
			int numClustersOfRequestor = clusters.size();
			clusters.retainAll( myAccessClusters );
			if ( clusters.isEmpty() ) {
				logger.logDetail("No local cluster IDs match the " + numClustersOfRequestor + " given in REQ msg (ignoring REQ)");
				return true; //message has been processed
			}
		}
		// Cluster membership is ok against this node - so don't reject queries from it anymore
		droppedNodesDueToClusterIdUpdate.remove(senderNodeID);
		
		int portDefIndex = senderNodeID.lastIndexOf(':');
		String host = -1 == portDefIndex ? senderNodeID : senderNodeID.substring(0, portDefIndex);
		host = host.toUpperCase();
		
		// Ignore this REQ if the host is not allowed
		if ( null != deniedHosts && deniedHosts.contains(host) ||
			 null != permittedHosts && !permittedHosts.contains(host) ) return true; //message has been processed
		
		// Acknowledge again for inbound connections but not for outbound - 
		// Inbound can remain after outbound has been lost on the other node
		// Check if we already have an outbound connection to this node
		if ( outgoingConnections.contains(senderNodeID) ) {
			logger.logDetail("Node already an outgoing slave (ignoring REQ): " + senderNodeID);
			return true; //message has been processed
		}
		
		boolean iAmAlreadyConnectedToThisNode = iAmAlreadyConnectedTo(senderNodeID);
	
		logger.logDetail("Processing msg: " + msg + ", connection Strategy: " + connectionStrategy);
		
		if ( GaianDBConfig.ATTACHMENT_RANDOM.equals(connectionStrategy) ) {
			// respond with a randomly generated delay - to spread client connections over the available nodes.
			logger.logDetail("Sleeping for client strategy");
			Thread.sleep( random.nextInt( (PREFERENTIAL_ATTACHMENT_HOLD_TIME_UNIT_MS) ) );
		
		} else if ( null!=connectionStrategy && connectionStrategy.startsWith( GaianDBConfig.ATTACHMENT_TO_USER_DB_NODE + ":" ) ) {
			// if this node is in charge of this db, then setup API initialisation and LT view updates and respond to the REQ
			String userdb = connectionStrategy.substring( (GaianDBConfig.ATTACHMENT_TO_USER_DB_NODE + ":" ).length() );
			if ( !userdb.toUpperCase().startsWith( myNodeIDWithoutPortSuffix.toUpperCase() )  ) {
				logger.logDetail("Ignoring REQ as this node (" + GaianDBConfig.getGaianNodeID() + ") is not responsible for this userdb: " + userdb);
				return true; //message has been processed
			}
			
			logger.logInfo("This node's NodeID matches and will therefore manage userdb: " + userdb);
			
			// The user database will be created and initialised with all spfs and views by the discovery client...
		
		} else {
			// Give other recipients more time to answer the requests depending on how many connections we already have.
			// Answer faster if we have many connections.
			// Pick a random time to respond between 0 and a factor of the number of connections we already have.
			if ( !isNeedsConnections ) {
				//Thread.sleep( random.nextInt( (MAX_GAIAN_CONNECTIONS-numDiscoveredConnections)*PREFERENTIAL_ATTACHMENT_HOLD_TIME_UNIT ) );
				logger.logDetail("Sleeping for Preferential Attachment strategy");
				Thread.sleep( random.nextInt( (PREFERENTIAL_ATTACHMENT_HOLD_TIME_UNIT_MS/(discoveredConnections.length+1) ) ) );
			}
		}
		
		final String sslMode = GaianDBConfig.getSSLMode();
		
		// Note an ACK is sent back even to hosts with which we have already connected... because their
		// connection to us might have been lost...
		
		// Send message: "ACK <to-nodeid> <local nodeid> <usr> <pwd scrambled>" to the <reply-to> ip address received in the REQ,
		// or to the default multicat ip if none was present
		sendMessage( "ACK " + senderNodeID + ' ' + myNodeID + ' ' +
				myUser + ' ' + myPasswordScrambled + ' ' + GaianNode.GDB_VERSION + (null==sslMode?"":" ssl='"+sslMode+"'"),
				InetAddress.getByName(senderIP),
				packet.getPort(), //null == destinationPort ? discoveryPort : destinationPort,
				false == iAmAlreadyConnectedToThisNode );
		
		// Add a "GW" at the end of this gateway request - this will mean we ignore the maximum number of
		// connections limit when validating the connection later.
		acknowledgedConnections.put( senderNodeID, System.currentTimeMillis() + " " + senderIP ); //+ (isGatewayReq?" GW":"") );
		
		logger.logDetail("Sent ACK to " + senderNodeID +", new list: " + acknowledgedConnections);
		
		// If we are already connected we may just be looping re-acknowledging the same connection - continue to avoid logging
		if ( iAmAlreadyConnectedToThisNode ) return true; //message has been processed 
		
		return false; // message has not been processed
	}

	// Get REQ message's extensible field's arguments. They come after the 3rd space character.
	private static final String getArgsStringFromRequestMessage( String msg ) { return getSubstringFromDelimiterInstanceIdx( msg, ' ', 3 ); }

	// Get ACK message's extensible field's arguments. They come after the 6th space character.
	private static final String getArgsStringFromAcknowledgeMessage( String msg ) { return getSubstringFromDelimiterInstanceIdx( msg, ' ', 6 ); }
	
	private static final String getSubstringFromDelimiterInstanceIdx( final String msg, final char delimiter, final int instanceIdx ) {
		int substringIndex = 0;
		for ( int delimiterCount = 0; delimiterCount<instanceIdx; delimiterCount++ )
			if ( 0 == ( substringIndex = msg.indexOf(delimiter, substringIndex) + 1 ) ) {
				logger.logDetail("No substring args found past instance #"+instanceIdx+" of delimiter '"+delimiter+"' in message: " + msg);
				return null; // No arguments found
			}
		
		return msg.substring( substringIndex );
	}
	
	/**
	 * Extracts arguments A1, A2, A3, ...  and their values V1, V2, V3, ... from an input String representing them as follows, where
	 * single quotes can be escaped within argument values: "A1='V1' A2='V2' A3='V3' ..."
	 * 
	 * @param msg
	 * @return
	 */
	private static Map<String, String> getMsgArguments( String spaceSeparatedArgs ) {
		
		Map<String, String> args = new HashMap<String, String>();
		if ( null == spaceSeparatedArgs ) return args;
				
		final int lastidx = spaceSeparatedArgs.length()-1;
		for ( int idx=0; lastidx > idx; ) {
			
			int eqidx = spaceSeparatedArgs.indexOf('=', idx);
			int idx2 = spaceSeparatedArgs.indexOf('\'', eqidx+2);
			if ( -1 == eqidx || -1 == idx2 ) break; // badly constructed argument, ignore anything remaining from here
			
			while ( 0 < idx2 && lastidx > idx2+1 && spaceSeparatedArgs.charAt(idx2+1) == '\'' )
				idx2 = spaceSeparatedArgs.indexOf('\'', idx2+2);
			
			if ( -1 == idx2 ) break;
			
			args.put(spaceSeparatedArgs.substring(idx,eqidx), spaceSeparatedArgs.substring(eqidx+2, idx2));
			
			idx = idx2+2;
		}
		return args;
	}

	/**
	 * @param timeNow
	 */
	private void invlidateExpiredAcknowledgements(long timeNow) {
		// Check validity of 'acknowledgedConnections'.
		// These designate records of acceptances to become a slave gaian node to a master who 
		// initiates then maintains the 2-way connection (for each maintenance function call issued
		// by the master, the slave checks its jdbc connection back to the master and reports status).
		
		// An 'acknowledgedConnections' record becomes stale if no connection is initiated by the master within a timeout.
		
		{
		Iterator<String> it = acknowledgedConnections.keySet().iterator();
		while ( it.hasNext() ) {
			
			String nodeid = it.next();
			String ackEntry = acknowledgedConnections.get( nodeid );
			String[] tokens = Util.splitByTrimmedDelimiter( ackEntry, ' ' );
			long ackTime = Long.parseLong( tokens[0] );
			
			// Give a Master node as much time to initiate a connection as we would give a Slave to report status to us.
			// Amended: Give a Master node enough time to create a JDBC connection to us and then initiate a maintenance call
			// GaianDBConfig.getConnectionAttemptTimeout() +
			if ( GaianDBConfig.getConnectionsCheckerHeartbeat() < timeNow - ackTime ) {
				acknowledgedConnections.remove( nodeid );
				logger.logDetail("Removed stale ACK for " + nodeid + ", list is now " + acknowledgedConnections);
			}
		}
		}
	}

	/*****************************
	 * CONNECTION HANDLING METHODS
	 ****************************/
	
	private static boolean iAmAlreadyConnectedTo( String nodeID ) {
		return Arrays.asList( GaianDBConfig.getGaianConnectedNodes( GaianDBConfig.getDiscoveredConnections() ) ).contains(nodeID);
	}
	
//	public static boolean isHostConnectionAllowed() {
//		
//	}
	
	public static void printConnections() {
				
		int neededCount = Math.max(0, Math.min( maxDiscoveredConnections, requiredOutboundConnections - outgoingConnections.size() ));

		Set<String> gcsDis = new HashSet<String>( Arrays.asList( GaianDBConfig.getGaianConnectedNodes( GaianDBConfig.getDiscoveredConnections() ) ) );
		gcsDis.removeAll(outgoingConnections);
		
		System.out.println(new Date(System.currentTimeMillis()) + 
				": Connections: Maintained to " + outgoingConnections + (neededCount != 0 ? " (seeking " + neededCount + ")" : "") + 
				", Accepted from " + gcsDis ); //+ (gcsDef.isEmpty() ? "" : ", Manually defined " + gcsDef) );
		
//		System.out.println(new Date(System.currentTimeMillis()) + ": Connections: " +
//				outgoingConnections + ", " + gcsDis + (neededCount != 0 ? " (seeking " + neededCount + ")" : ""));
	}
	
	private static String addNodeConnection(
			final String nodeID, final String sslMode, final String nodeIP, final String usr, final String scrambledpwd ) {
		
//		String senderIP = packet.getAddress().getHostAddress();
////	String senderIPPortDB = senderIP + senderDescription.substring( senderDescription.lastIndexOf(':') );
//		String senderPort = -1 == portDefIndex ? ""+GaianNode.DEFAULT_PORT : senderNodeID.substring(portDefIndex+1);
//		String senderDB = GaianDBConfig.GAIANDB_NAME + (-1 == portDefIndex ? "" : senderPort);
//		String url = "jdbc:derby://" + senderIP + ":" + senderPort + "/" + senderDB + ";create=true";
		
		int portDefIndex = nodeID.lastIndexOf(":");		
//		String senderIPPortDB = senderIP + senderDescription.substring( senderDescription.lastIndexOf(':') );
		String portDef = -1 == portDefIndex ? ":"+GaianNode.DEFAULT_PORT : nodeID.substring(portDefIndex);
		
		// Denis : Connect to the other nodes using the GaianDB udp driver
		if ( !GaianNode.IS_UDP_DRIVER_EXCLUDED_FROM_RELEASE && GaianNode.isNetworkDriverGDB() )
		{
			try
			{
				GaianDBConfig.loadDriver( GaianDBConfig.GDB_UDP_DRIVER );
			}
			catch( Exception e )
			{
				logger.logException( GDBMessages.DISCOVERY_NODE_ADD_ERROR, "Could not find the UDP Driver Client. ", e );
				return null;
			}
			
			
			String udpDP = UDPDriver.UDP_DRIVER_URL_PREFIX+"//";
			int port = Integer.decode( portDef.substring( 1 ) );// + 1000;
			String nodeIPPortDB = nodeIP + ":" +port + "/" + GaianDBConfig.GAIANDB_NAME + (-1 == portDefIndex ? "" : portDef.substring(1));
		
			// DRV - 22/09/2011
			// Don't set the timeout globally for the UDP driver as below anymore.
			// The timeout will be set individually for each statement.
			// This way, the default timeout used by other connecting client apps stays higher so they can still obtain partial results.
			String url = udpDP + nodeIPPortDB + ";datagramSize="+GaianDBConfig.getNetworkDriverGDBUDPDatagramSize();
//			+";timeout="+GaianDBConfig.getNetworkDriverGDBUDPTimeout();
			
			return GaianDBConfig.addDiscoveredGaianNode( GaianDBConfig.GDB_UDP_DRIVER, url, usr, scrambledpwd, nodeID );	
		}
		
		String nodeIPPortDB = nodeIP + portDef + "/" + GaianDBConfig.GAIANDB_NAME + (-1 == portDefIndex ? "" : portDef.substring(1));
		String url = "jdbc:derby://" + nodeIPPortDB + (null==sslMode?"":";ssl="+sslMode) + ";create=true";
				
		return GaianDBConfig.addDiscoveredGaianNode( GaianDBConfig.DERBY_CLIENT_DRIVER, url, usr, scrambledpwd, nodeID );		
	}
	

	private static boolean reciprocateConnection(
			final String senderNodeID, final String sslMode, final String senderIP, final String usr, final String scrambledpwd ) {
		
		String connID = null;		
		if ( null != ( connID = addNodeConnection(senderNodeID, sslMode, senderIP, usr, scrambledpwd) ) ) {
			
			String pfx = Logger.sdf.format(new Date(System.currentTimeMillis())) + " -------> "; // "\t"
			String s = "INBOUND  connection " + connID + " established:  <- " + senderNodeID;
			synchronized( gnsInstance ) {
				isNewConnectionsNeedLoading = true;
			}
			
			logger.logImportant( Logger.HORIZONTAL_RULE + "\n" + pfx + s + "\n" );
//			System.out.println(new Date(System.currentTimeMillis()) + " " + s);
//			printConnections();
			
//			updateLastCheckTime(connID, timeNow);

			refreshDataSources.set(true); // Let the seeker refresh the DataSources, as we are in a time critical thread
			
			return true;
		}
		
		// Defensive programming - should never happen
		logger.logWarning( GDBMessages.DISCOVERY_CONN_RECIPROCATE_ERROR, "FAILED to reciprocate connection to " + senderNodeID + ", " + senderIP );
		return false;
	}
	
	// Low value feature to skip maintenance for certain connections
//	public static boolean isMasterNodeAndReverseConnectionIsMissing( String nodeID ) {
//		// Check if we are a slave node to nodeID and if we are not connected back to it.
//		// This condition causes GaianTable to throw an InvocationTargetException so the master also drops his connection.
//		if ( !outgoingConnections.contains(nodeID) && !GaianDBConfig.isDiscoveredNode(nodeID) )
//			return true;
//		return false;
//	}
	
	
	public static String maintainConnection( final String senderNodeID, final String usr, final String scrambledpwd, final String extraInfo ) {
		
		if ( !isNodeMeetsAccessRestrictions( senderNodeID ) ) {
			String errmsg = "Node '" + senderNodeID + "' does not meet ACCESS restrictions for cluster membership or permitted/denied hosts";
			logger.logInfo("Rejecting maintenance call from " + senderNodeID + ": " + errmsg);
			// Return errmsg - dont also verify the connection isn't defined - the seeker will do that
			return errmsg;
		}
		
		// If this was an acknowledged host (who established an outgoing connection to us), then try to setup the link back to them
		
		final boolean isExtraInfoSet = null != extraInfo;
		String sslMode = null; // ssl default = off
		
		if ( isExtraInfoSet ) {
			// If this is not already a node that we maintain the connection with (= DRV fix for defect 62640 part 2)...
			// AND this is an initial maintenance request, then we need to remove any *existing* link we may already have as it will be stale
			if ( false == outgoingConnections.contains(senderNodeID) && extraInfo.startsWith("INIT") ) {
				String cid = GaianDBConfig.getDiscoveredConnectionID(senderNodeID);
				if ( null != cid ) {
					logger.logInfo("Removing stale connection to master node which has just been recycled (shown by INIT tag in maintenance request)");
					lostDiscoveredConnection(cid);
				}
			}
			
			int idx = extraInfo.indexOf(DatabaseConnectionsChecker.SSLMODE_TAG);
			if ( -1 < idx ) {
				idx += DatabaseConnectionsChecker.SSLMODE_TAG.length();
				sslMode = extraInfo.substring(idx, extraInfo.indexOf(',', idx));
			}
		}
		
		String rc = validateIncomingConnection(senderNodeID, sslMode, usr, scrambledpwd);
		if ( null != rc || false == isExtraInfoSet ) // When validation succeeds, just return null if the extraInfo is null (for backwards-compatibility)
			return rc;
		
		// Connection success and we are a node at version 1.04 or beyond and the caller has sent a version indicator saying so as well..
		// Advanced behaviour... process extraInfo and also send back info:
		//		- distance from the nearest non-lite server node having full Derby query capability (or 0 if we are one)
		//		- (future feature...) for each known LT, the distances from each node holding data for it
		
		DatabaseConnectionsChecker.resolveBestPathToNonLiteNodeFromMaintenanceMessage(extraInfo, senderNodeID);
		
		// Future feature:
		// If we are a node at version 1.04 or beyond, the caller will have sent a special indicator saying it is as well..
		// In that case, we send back stats info (rather than just null)
		//		- distance from the nearest server node (or 0 if we are one)
		// 		- for each known LT, the distances from each node holding data for it
		
		// Todo: also pass the version number in future...
		return DatabaseConnectionsChecker.SUCCESS_TAG +
			DatabaseConnectionsChecker.DISTANCE2SERVER_TAG + DatabaseConnectionsChecker.getDistanceToServerNode();
	}
	
	
	public static final String VALID_CONNECTION_BUT_INVALID_MAINTENANCE_DIRECTION = "Connection already established in opposite direction";

	/**
	 * Verify that we are expecting a incoming connection request - and if so then establish the connection.
	 * A connection is deemed 'incoming' if it was initiated by the peer node.
	 * In this case, our own node is passive whilst the other one maintains the 2-way connection.
	 * 
	 * @param senderNodeID
	 * @param sslMode - could be null if no ssl, or one of the derby ssl modes (off/basic/peerAuthentication)
	 * @param usr
	 * @param scrambledpwd
	 * @return null if succeeds; or an error message string otherwise.
	 */
	private static String validateIncomingConnection(
			final String senderNodeID, final String sslMode, final String usr, final String scrambledpwd ) {
		
		String errmsg, log = "Rejecting maintenance call from " + senderNodeID + ": ";
		
		// Check if connection exists as outbound already
		if ( outgoingConnections.contains(senderNodeID) ) {
			errmsg = VALID_CONNECTION_BUT_INVALID_MAINTENANCE_DIRECTION;
			logger.logInfo(log + errmsg);
			return errmsg;
		}
		
		// Check if connection exists (if so it must be inbound)
		if ( GaianDBConfig.isDiscoveredNode(senderNodeID) ) {
//			incomingConnections.add(senderNodeID);
			return null;
		}
		
		// Note Hashtable is synchronized	
		// DRV - Defect 62644 (part 2) - changed remove() with get(): DO NOT remove the 'sent ACK' entry, because we want to keep on 
		// rejecting potential erroneous ACKs from the other node until a "discovery period" is complete...
		// Remove the acknowledge asap so that if the connection is later lost, both nodes must go through the same protocol again.
//		if ( acknowledgedConnections.containsKey( senderNodeID ) ) logger.logInfo("Removing1 ACK to " + senderNodeID);
		String ackEntry = acknowledgedConnections.get( senderNodeID );
		
//		boolean isGatewayReq = ackEntry.endsWith(" GW");
		
		// If this is not a discovery gateway, then ensure the max number of connections is not exceeded
//		if ( !isGatewayReq ) {
			// Check if we have already reached the max number of allowed connections
			int numDiscoveredConnections = GaianDBConfig.getDiscoveredConnections().length;
			if ( numDiscoveredConnections - outgoingConnections.size() >= maxDiscoveredConnections - requiredOutboundConnections ) {
				errmsg = "Max number of discovered connections reached: " + numDiscoveredConnections;
				logger.logInfo(log + errmsg);
				return errmsg;
			}
//		}
		
		// Setup new inbound connection
		logger.logInfo("Connection maintenance call attempting to reciprocate inbound connection from " + senderNodeID);
		
		if ( null == ackEntry ) {
			// refuse to setup connection - acknowledgement was not granted or was revoked
			errmsg = "ACK revoked (connection aborted) for: " + senderNodeID + 
			": Either ACK validity timed out *OR* Cluster memberships were removed from " + GaianDBConfig.ACCESS_CLUSTERS + 
			". For high latency networks, increase " + GaianDBConfig.GAIAN_CONNECTIONS_CHECKER_HEARTBEAT_MS;
			logger.logInfo(log + errmsg);
			return errmsg;
		}
		
		String[] tokens = Util.splitByTrimmedDelimiter( ackEntry, ' ' );
		String senderIP = tokens[1];
		
		if ( getLocalIPs(true).contains(senderIP) ) {
			int portIndex = senderNodeID.lastIndexOf(':');
			// avoid possible number parsing exception by getting the port as a string
			String senderPort = -1 == portIndex ? GaianNode.DEFAULT_PORT+"" : senderNodeID.substring(portIndex+1);
			String myPort = GaianDBConfig.getDerbyServerListenerPort()+"";
			if ( senderPort.equals(myPort) ) {
				// refuse to setup connection - the request comes from myself!
				errmsg = "Connection request comes from node with identical ip and port(!) " +
					senderIP + ":" + senderPort;
				logger.logInfo(log + errmsg);
				return errmsg;
			}
		}
		
		if ( reciprocateConnection(senderNodeID, sslMode, senderIP, usr, scrambledpwd) ) return null;
		
		// Defensive programming - should never happen
		errmsg = "Connection to node already exists (but is neither inbound nor outbound!): " + senderNodeID;
		logger.logWarning(GDBMessages.DISCOVERY_CONN_EXISTS, errmsg );
		return errmsg;
	}
	
	/*******************************************************************************
	 * CONNECTION MAINTENANCE METHODS
	 ******************************************************************************/
	
	public static boolean reverseMaintenanceDirectionToIncoming( String connectionID ) {
		return outgoingConnections.remove( GaianDBConfig.getDiscoveredNodeID( connectionID ) );
	}
	
	/**
	 * Drops connections listed in connectionIDs.
	 * Returns true if any existing connections were actually removed and config was therefore consequently reloaded.
	 */
	private static boolean dropConnections( String[] connectionIDs ) {
		
		logger.logInfo("Removing connections: " + Arrays.asList(connectionIDs) + " - trace: " + Util.getStackTraceDigest());
		
		int removed = 0;
		String msg = null;
		synchronized( DataSourcesManager.class ) {
		
			for ( int i=0; i<connectionIDs.length; i++ ) {
				String connectionID = connectionIDs[i];
				String nodeID = GaianDBConfig.getDiscoveredNodeID( connectionID );
				
				// DRV - 24/01/2012 - Just remove the appropriate entries from the dsArrays and dataSources structures rather than having to do a full refresh...
				// Note this is done *before* unsetting the actual config properties, which is the final step in removeDiscoveredGaianNode().
				DataSourcesManager.unloadAllDataSourcesAndClearConnectionPoolForGaianConnection( connectionID );
				
				if ( true == GaianDBConfig.removeDiscoveredGaianNode( connectionID ) ) {
					
					DatabaseConnectionsChecker.invalidatePotentialPathToServerNode( GaianDBConfig.getDiscoveredNodeID(connectionID) );
					removed++;
					outgoingConnections.remove( nodeID ); // no effect if the connection was not outgoing.
					msg = "REMOVED  connection " + connectionID + " to " + nodeID;
					printConnections();
				} else
					msg = "FAILED TO REMOVE connection " + connectionID + " to " + nodeID + " (already removed)";
				
				String pfx = Logger.sdf.format(new Date(System.currentTimeMillis())) + " -------> ";
				logger.logImportant( Logger.HORIZONTAL_RULE + "\n" + pfx + msg + "\n" );
				if ( msg.startsWith("FAILED") )
					System.out.println( new Date(System.currentTimeMillis()) + ": " + msg );
			}
			
			if ( 0 < removed ) {
				// DRV - 24/01/2012 - commented out line below - don't do a full refresh anymore
//				DataSourcesManager.refresh();
				return true; // no need to check/refresh logical table views as the properties have not been reloaded and we are synchronized
			}
		}
		
		return false;
	}
	
	public static void lostDiscoveredConnection( String connectionID ) {		
		
		dropConnections( new String[] { connectionID } );
	}
	
	static void dropConnectionsToNodesNotMeetingAccessRestrictions() {
		
		String[] discoveredConnections = GaianDBConfig.getDiscoveredConnections();
		
		String permittedHostsProperty = GaianDBConfig.getAccessHostsPermitted();
		Set<String> permittedHosts = null == permittedHostsProperty ? null :
			new HashSet<String>( Arrays.asList( Util.splitByCommas(permittedHostsProperty.toUpperCase()) ) );
		String deniedHostsProperty = GaianDBConfig.getAccessHostsDenied();
		Set<String> deniedHosts = null == deniedHostsProperty ? null :
			new HashSet<String>( Arrays.asList( Util.splitByCommas(deniedHostsProperty.toUpperCase()) ) );

		Set<String> myAccessClusters = new HashSet<String>( Arrays.asList( Util.splitByCommas( GaianDBConfig.getAccessClusters() )));
		
		if ( false == previousClusterIDs.equals(myAccessClusters) )
			logger.logInfo("Detected update of ACCESS_CLUSTERS (Removing ALL connections if it was null/empty OR if IDs were removed): " + previousClusterIDs + " => " + myAccessClusters);
		
		// Remove all discovered connections if we went from having no cluster defs to having some, or if we removed one or more. 
		boolean removeAllDiscoveredConnections = previousClusterIDs.isEmpty() && !myAccessClusters.isEmpty();
		previousClusterIDs.removeAll( myAccessClusters );
		removeAllDiscoveredConnections = removeAllDiscoveredConnections || !previousClusterIDs.isEmpty();
		if ( removeAllDiscoveredConnections ) acknowledgedConnections.clear(); // Revoke all currently granted ACKs to stop new connections.
		
		previousClusterIDs = myAccessClusters;
		
		if ( null != deniedHosts || null != permittedHosts || removeAllDiscoveredConnections ) {
			
			logger.logInfo("Access restrictions changed. deniedHosts: " + deniedHosts + ", permittedHosts: " + permittedHosts + 
					", removeAllDiscoveredConnections: " + removeAllDiscoveredConnections);
			
			// Check validity of connections - If ACCESS_HOSTS_DENIED or ACCESS_HOSTS_PERMITTED are set, 
			// drop connections to hosts that are or aren't in the respective lists.
			// Only do this every 5s to avoid excessive processing
//			if ( GaianNode.WATCHDOG_POLL_TIMEOUT <= timeNow - lastPollTime ) {
//			if ( isSocketTimedOut ) {
				ArrayList<String> unwantedConnections = new ArrayList<String>();
				for ( int i=0; i<discoveredConnections.length; i++ ) {
					String connID = discoveredConnections[i];
					String node = GaianDBConfig.getGaianNodeID(connID);
					if ( removeAllDiscoveredConnections ) {
						unwantedConnections.add(connID);
						droppedNodesDueToClusterIdUpdate.add(node);
						continue;
					}
					if ( !isNodeMeetsAccessRestrictions(node, permittedHosts, deniedHosts) ) {
						unwantedConnections.add(connID);
						continue;
					}
//					if ( myAccessClusters.isEmpty() ) continue;
//					Set<String> matchingClusters = new HashSet<String>(myAccessClusters);
//					matchingClusters.retainAll( clusterIDsOfConnectedNodes.get(node) );
//					if ( matchingClusters.isEmpty() )
//						unwantedConnections.add(connID);
				}
				
				if ( 0 < unwantedConnections.size() )
					dropConnections( unwantedConnections.toArray( new String[0] ) );
//			}
		}
	}

	public static boolean isNodeMeetsAccessRestrictions( String nodeID ) {
		
		String permittedHostsProperty = GaianDBConfig.getAccessHostsPermitted();
		Set<String> permittedHosts = null == permittedHostsProperty ? null :
			new HashSet<String>( Arrays.asList( Util.splitByCommas(permittedHostsProperty.toUpperCase()) ) );
		String deniedHostsProperty = GaianDBConfig.getAccessHostsDenied();
		Set<String> deniedHosts = null == deniedHostsProperty ? null :
			new HashSet<String>( Arrays.asList( Util.splitByCommas(deniedHostsProperty.toUpperCase()) ) );
		
		return isNodeMeetsAccessRestrictions( nodeID, permittedHosts, deniedHosts );
	}
	
	private static boolean isNodeMeetsAccessRestrictions( String nodeID, Set<String> permittedHosts, Set<String> deniedHosts ) {
		
		int portDefIndex = nodeID.lastIndexOf(':');
		String host = -1 == portDefIndex ? nodeID : nodeID.substring(0, portDefIndex);
		host = host.toUpperCase();
		
		// todo: match nodeid, hostname or ip address
		
//		System.out.println("Checking access restrictions for node " + nodeID + ": dropped nodes were: " + droppedNodesDueToClusterIdUpdate);
		
		if ( null != deniedHosts && deniedHosts.contains(host) ||
			 null != permittedHosts && !permittedHosts.contains(host) ||
			 droppedNodesDueToClusterIdUpdate.contains(nodeID) )
			return false;
		
		return true;
	}
	
	/*************************
	 * MESSAGE SENDING METHODS 
	 ************************/	

	/**
	 * @param isNeedsConnections
	 * @throws IOException
	 */
	private void sendNecessaryDiscoveryRequests(boolean isNeedsConnections)
			throws IOException {
		// Send point-2-point REQs to all nodes that we aren't currently connected to.
		// This is for discovery in networks where broadcast and multicast doesn't work.
		for ( String gateway : Util.splitByCommas(GaianDBConfig.getDiscoveryGateways()) ) {
			int idx = gateway.indexOf(':');
			if ( 0 < idx ) gateway = gateway.substring(0, idx);
			if ( !GaianDBConfig.isGatewayConnectionDefined(gateway) || isNeedsConnections )
				// Send unicast REQ message, specify label GW to make the receiver send a unicast ACK back to us
				//sendDiscoveryRequestMessageViaGateway( gateway, !isLooping );
				sendDiscoveryRequestMessageViaGateway( gateway, true );
		}

		if ( isNeedsConnections ) {
			// This node needs to connect to other nodes
			// Send a connection request with one's own parms
//							sendDiscoveryRequestMessage( !isLooping );
			sendDiscoveryRequestMessage( true );
		} else {
//							System.out.println("isBound() " + skt.isBound() +
//									", isClosed() " + skt.isClosed() + ", isConnected() " + skt.isConnected());
//							multicastMessage( "PING", !isLooping ); // If this fails we will know that network connectivity was lost
		}
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
	private void sendDiscoveryRequestMessage(boolean printLog ) throws IOException {
		String acs = GaianDBConfig.getAccessClusters();
		String reqArgs = null==acs ? "" : GaianDBConfig.ACCESS_CLUSTERS + "='" + acs + "'";
		String connStrat = GaianDBConfig.getConnectionStrategy();
		reqArgs = reqArgs + (null==connStrat ? "" : (1>reqArgs.length()?"":" ") + GaianDBConfig.CONNECTION_STRATEGY + "='" + connStrat + "'");
		sendDiscoveryRequestMessage(myNodeID, null, reqArgs, printLog);
	}
	
	private void sendDiscoveryRequestMessage(String originatorNodeID, String originatorIP, String reqArgs, boolean printLog ) throws IOException {
		
		// Note - we dont want to add a space to reqArgs if it is not set..
		String reqArgsString = null == reqArgs || 0 == reqArgs.length() ? "" : " " + reqArgs; 
		
		String reBroadcastInfo = null==originatorIP ? "" : 'X' + originatorIP + ' ';

//		String acs = GaianDBConfig.getAccessClusters();
//		String reqArgs = null==acs ? "" : " " + GaianDBConfig.ACCESS_CLUSTERS + "='" + acs + "'";
		
		if ( null == broadcastIPsSet || 0 == broadcastIPsSet.size() ) {
			// Use multicast group address or singular DISCOVERY_IP (user specified multicast group or broadcast ip)
			String ip = discoveryAddress.toString();
			sendMessage( reBroadcastInfo + "REQ " + originatorNodeID + " " + ip.substring(ip.indexOf('/')+1) + reqArgsString, 
					discoveryAddress, printLog );
		} else {
			// A list of broadcast ips was resolved or set by the user in config - send REQs to each of these.
			String msg = reBroadcastInfo + "REQ " + originatorNodeID;
			if ( printLog )
				logger.logInfo("Sending msg: " + msg + ", broadcast ips: " + broadcastIPsSet);
			for ( InetAddress broadcastIP : broadcastIPsSet ) {
				String ip = broadcastIP.toString();
				sendMessage(msg + " " + ip.substring(ip.indexOf('/')+1) + reqArgsString, broadcastIP, true );
			}
		}
	}
	
	/**
	 * Sends message to gateway: REQ <local node id> <local ip visible by gateway>
	 * 
	 * @param skt
	 * @param gateway
	 * @param printlog
	 * @throws IOException
	 */	
	private void sendDiscoveryRequestMessageViaGateway( String gateway, boolean printlog ) throws IOException {

		InetAddress gwAddress = null;
		try { gwAddress = InetAddress.getByName(gateway); }
		catch (UnknownHostException e) {
			logger.logWarning(GDBMessages.DISCOVERY_GATEWAY_RESOLVE_ERROR, "Exception resolving gateway: " + gateway + " (ignored), cause: " + e);
			return;
		}
		
		// Find the ip local attributed in the same subnet domain as the gateway ip  
		// i.e. resolve the local ip attributed for the interface domain which this node and the recipient node will have in common
		String localIP = Util.getStringWithLongestMatchingPrefix(gateway, getLocalIPs(true));
		
		if ( null == localIP ) {
			if ( printlog ) logger.logWarning(GDBMessages.DISCOVERY_GATEWAY_IP_RESOLVE_ERROR, "Unable to resolve local IP address to gateway node: " + gateway);
			return;
		}
		
//		System.out.println("local ip: " + localIP + ", gateway: " + gateway + ", ips: " + getLocalIPs());
		String acs = GaianDBConfig.getAccessClusters();
		String reqArgsString = null==acs ? "" : " " + GaianDBConfig.ACCESS_CLUSTERS + "='" + acs + "'";
		String connStrat = GaianDBConfig.getConnectionStrategy();
		reqArgsString = reqArgsString + (null==connStrat ? "" : " " + GaianDBConfig.CONNECTION_STRATEGY + "='" + connStrat + "'");
		
		// Send the localIP in place of the destination ip to indicate that this is request for the destination node to act as gateway
		sendMessage( "REQ " + myNodeID + " " + localIP + reqArgsString, gwAddress, printlog );
	}
	
	// Point to point ACKs are a bad idea for 2 reasons:
	// First - because in the case where multiple nodes are running on one host, the first node to open a socket on port 7777 
	// will consume all p2p msgs sent on that port so others wont get a chance to connect.
	// Second - it is actually no more efficient to do this because each node still ends up sending an ACK which floods the network
	// just as much as a broadcast.
	// The only way to make things more efficent would be to prevent the slower nodes from sending an ACK if they have received 
	// at least 2 ACKs from the faster nodes while they were sleeping.
//	private void p2pMessage( DatagramSocket skt, String msg, InetAddress addr, boolean printLog ) throws IOException {
//		
//		if ( null == skt ) return; // GaianNodeSeeker is disabled		
////		System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " Discovery msg: " + msg);		
//		if ( printLog )
//			logger.logInfo("Sending point to point msg: " + msg + ", addr: " + addr);
//		sendMessage(skt, msg, addr);
//	}
	
	private void sendMessage( String msg, InetAddress destinationAddress,
			boolean printLog ) throws IOException {
		sendMessage( msg, destinationAddress, discoveryPort, null, printLog );
	}
	
	private void sendMessage( String msg, InetAddress destinationAddress,
			Set<InetAddress> targetInterfacesIfMulticast, boolean printLog ) throws IOException {
		sendMessage( msg, destinationAddress, discoveryPort, targetInterfacesIfMulticast, printLog );
	}
	
	private void sendMessage( String msg, InetAddress destinationAddress,
			int destinationPort, boolean printLog ) throws IOException {
		sendMessage( msg, destinationAddress, destinationPort, null, printLog );
	} 
	
	private void sendMessage( String msg, InetAddress destinationAddress, int destinationPort,
			Set<InetAddress> targetInterfacesIfMulticast, boolean printLog ) throws IOException {
		
//		System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " Discovery msg: " + msg);		

		DatagramPacket req = new DatagramPacket( msg.getBytes(), msg.length(), destinationAddress, destinationPort );
				
		if ( destinationAddress.isMulticastAddress() ) { // isMulticastGroupIP( destinationAddress.toString().substring(1) ) ) { // remove leading '/'

			Set<InetAddress> targetInterfaces = null == targetInterfacesIfMulticast ? multicastInterfaces : targetInterfacesIfMulticast;			
			if ( printLog )
				logger.logInfo("Sending msg: " + msg + ", multicast group: " + destinationAddress + ", interfaces: " + targetInterfaces);

				for ( InetAddress localInterfaceAddress : targetInterfaces ) {
					try {
						socketForInterface(localInterfaceAddress).send(req); // send on multicast socket provisioned for this interface
						
					} catch ( Exception e ) {
						// The Exception might be legitimate and repeating... log it as a warning to reduce logging
						logger.logWarning(GDBMessages.DISCOVERY_MULTICAST_INTERFACES_ERROR,
							"Unable to send discovery message on interface: '" + localInterfaceAddress
							+ "' (skipped), cause: " + Util.getStackTraceDigest(e, 4, -1, true) );
					}
				}
				if ( printLog )
					logger.logInfo("Sent msg: " + msg + ", multicast group: " + destinationAddress + ", interfaces: " + targetInterfaces);
		}
		else {
			if ( printLog )
				logger.logInfo("Sending msg: " + msg + ", destination: " + destinationAddress + ", port: " + destinationPort);
			
			try {
				socketForNonMulticast().send(req); // send on general-purpose send-socket (whose interface doesn't need setting)
				
				if ( printLog )
					logger.logInfo("Sent msg: " + msg + ", destination: " + destinationAddress + ", port: " + destinationPort+", on receiving socket.");
			} catch ( Exception e ) {
				// The Exception might be legitimate and repeating... log it as a warning to reduce logging
				logger.logWarning(GDBMessages.DISCOVERY_MULTICAST_INTERFACES_ERROR,
						"Unable to send non-multicast discovery message (skipped), cause: " + Util.getStackTraceDigest(e, 4, -1, true) );
			}
		}
	}

	/* This is the main processing method for the Node Seeker.
	 * It establishes and maintains network connectivity, sends and 
	 * receives discovery messages (REQuests and ACKnowledgements)
	 */
	public void run() {
				
		logger.logInfo("Entering discovery loop, my details are: " + myNodeID + " " + myUser + " <pwd>" );
		
		boolean isFreshConnectionAttempt = true;		
		boolean isDiscoveryStillRequired = true;
		
		while (isDiscoveryStillRequired) {
			
			receivingSkt = null;
			
			try {
				// INITIALISE DISCOVERY PROPERTIES FROM CONFIG
				
				for ( InetAddress iaddr : InetAddress.getAllByName("localhost") )
					if ( iaddr instanceof Inet4Address )
						localhostInterfaces.add(iaddr);
				
				defaultIP = getDefaultLocalIP();
				defaultInterface = InetAddress.getByName(defaultIP); // don't use skt.getInterface() as it picks "0.0.0.0" on linux
				
				miProperty = applyMulticastInterfacesConfig();
				discoveryAddress = DEFAULT_MULTICAST_GROUP_ADDRESS;
				knownNetworkInterfaceAddresses = Util.getAllMyHostIPV4Adresses();
				
				// INITIALISE THE MAIN NETWORK SOCKET
				initialiseReceivingSocket( discoveryAddress );

				// Parameters for the main discovery loop
				isFreshConnectionAttempt = true;
				long lastPollTime = 0;
				String previousDiscoveryIP = null;
				
				// THIS LOOP CHECKS AND APPLYS CONFIG UPDATES, SENDS REQ MESSAGES AT APPROPRIATE INTERVALS, AND PROCESSES RECEIVED REQs AND ACKs
				// Break the loop to re-initialise the socket if the discovery port or multicast interfaces change.
				while ( receivingSkt.getLocalPort() == (discoveryPort = GaianDBConfig.getDiscoveryPort()) ) {

					long timeNow = System.currentTimeMillis();

					// CHECK FOR CONFIG UPDATES AND APPLY IF THERE ARE ANY
					
					// Break if the multicast interfaces property has changed
					String mip = GaianDBConfig.getMulticastInterfaces();
					if ( null == miProperty ^ null == mip ) break;
					if ( null != miProperty && !miProperty.equals( mip ) ) break;
					
					// process the latest DiscoveryIP from the config
					applyDiscoveryIPConfig();
					
					// Completely exit discovery when required
					isDiscoveryStillRequired = 0 < discoveryIP.length() && GaianNode.isRunning();
					if ( false == isDiscoveryStillRequired ) return; // via finally() to clean up
					
					String[] discoveredConnections = GaianDBConfig.getDiscoveredConnections();
					
					// Determine which nodes and clusters we are allowed to connect to
					String permittedHostsProperty = GaianDBConfig.getAccessHostsPermitted();
					Set<String> permittedHosts = null == permittedHostsProperty ? null :
						new HashSet<String>( Arrays.asList( Util.splitByCommas(permittedHostsProperty.toUpperCase()) ) );
					String deniedHostsProperty = GaianDBConfig.getAccessHostsDenied();
					Set<String> deniedHosts = null == deniedHostsProperty ? null :
						new HashSet<String>( Arrays.asList( Util.splitByCommas(deniedHostsProperty.toUpperCase()) ) );
					Set<String> myAccessClusters = new HashSet<String>( Arrays.asList( Util.splitByCommas( GaianDBConfig.getAccessClusters() )));
			
//					logger.logInfo("About to load sources for new gaian connections, refreshDataSources = " + refreshDataSources.get());
					
					// Load discovery updates from config now (if this hasn't been done implicitly as part of removing unwanted connections above)
					// Get the data sources manager to load datasources for any new connections.
					synchronized( DataSourcesManager.class ) {
						if ( refreshDataSources.compareAndSet(true, false) ) {
							DataSourcesManager.loadAllDataSourcesForNewGaianConnections();
							isLooping = false;
							// no need to check/refresh logical table views as the properties have not been reloaded and we are synchronized
						}
					}
					synchronized( gnsInstance ) {
						if ( isNewConnectionsNeedLoading ) {
							printConnections();
							isNewConnectionsNeedLoading = false;
						}	
					}
					
					// Work out our requirements for requesting and accepting connections.
					requiredOutboundConnections = GaianDBConfig.getMinConnectionsToDiscover();
					if ( 1 > requiredOutboundConnections ) requiredOutboundConnections = 0;
					maxDiscoveredConnections = GaianDBConfig.getMaxDiscoveredConnections();
					int numOutgoingConnectionsSought = 
						Math.min( maxDiscoveredConnections, requiredOutboundConnections - outgoingConnections.size() ); //numDiscoveredConnections );
			
					// boolean indicating we are actively seeking new connections as we don't have 
					// the minimum specified.
					boolean isNeedsConnections = 0 < numOutgoingConnectionsSought;
										
					// Perform "Watchdog" actions at regular intervals.  
					if ( GaianNode.WATCHDOG_POLL_TIMEOUT <= timeNow - lastPollTime ) {
						lastPollTime = timeNow;
						
						boolean interfacesHaveChanged = rejoinMulticastGroupIfInterfacesChange(receivingSkt);
						
						// Refresh multicast group memberships if discoveryIP has changed to a different one - (but don't affect default multicast group)
						previousDiscoveryIP = rejoinMulticastGroupIfDiscoveryIPChanged(
								receivingSkt, previousDiscoveryIP,
								isDiscoveryIPaList, interfacesHaveChanged);
					
						invlidateExpiredAcknowledgements(timeNow);
						
						sendNecessaryDiscoveryRequests(isNeedsConnections);
					}
					
					// RECEIVE AND PROCESS INCOMING MESSAGES
					
					if ( !isLooping ) //&& !ignoredMessage ) //-1 < minimumConnectionsToDiscover )
						logger.logInfo("Awaiting msg from network [connections sought: " + numOutgoingConnectionsSought + "]...");
					
					isLooping = true;

					if (!checkMulticastSocketsforIncomingMessages(packet)){ //we can receive acks on these "outgoing" sockets. 
						// no message on the send sockets, check the main multicast receive socket
						try { receivingSkt.receive(packet); }
						catch ( SocketTimeoutException e ) {
							// This is OK, just a consequence of no message waiting to be received.
							continue; // no message found at all
						}
					}

					if ( 0 > requiredOutboundConnections ) {
						continue; // Ignore messages if MIN_DISCOVERED_CONNECTIONS is not set.
					}
					
					// Process the received message.
					 boolean messageProcessed = processDiscoveryMessage (packet,
							discoveredConnections, permittedHosts, deniedHosts,
							myAccessClusters, numOutgoingConnectionsSought,
							isNeedsConnections);
					 
					if (messageProcessed) {
						// Unset this flag so we log detailed actions
						isLooping = false; // we did something (either multicasted a msg or got a new connection or both)
					}
				}
				
				//This loop has probably created and used these sockets so we should close them.
				logger.logDetail("Closing Multicast Sockets");
				receivingSkt = closeMulticastSockets(receivingSkt);
				
			} catch ( IOException e ) {
				
				if ( isFreshConnectionAttempt ) {
					logger.logWarning(GDBMessages.DISCOVERY_LOOP_ATTEMPT_ERROR, "Discovery Loop Failure: POSSIBLE LACK OF NETWORK CONNECTIVITY (re-trying...): "
							+Util.getStackTraceDigest(e, 4, -1, true));
					// The system is given time to sort itself out in the finally block below
					// It is probably coming out of suspension or hibernation
				}
				
				String[] discoveredConnections = GaianDBConfig.getDiscoveredConnections();
				
				if ( null != discoveredConnections && 0 < discoveredConnections.length ) {
					dropConnections( discoveredConnections );
					continue; // continue after the finally block - don't reset the 'fresh connection attempts' flag yet
				}
				
				isFreshConnectionAttempt = false;
				
			} catch ( Exception e ) {
				if ( isFreshConnectionAttempt ) {
					logger.logException( GDBMessages.DISCOVERY_LOOP_ERROR, "Discovery Loop Failure: Unexpected Exception (re-trying...): ", e );
					isFreshConnectionAttempt = false;
				}
				
			}  catch ( Error er ) {
//				System.err.println("OUT OF MEMORY DETECTED IN GaianNodeSeeker - Running System.exit(2)");
//				System.exit(2);
				
				GaianNode.stop( "Error in GaianNodeSeeker", er );
				
			} finally {
				receivingSkt = closeMulticastSockets(receivingSkt);

				if ( isDiscoveryStillRequired ) {
					try { Thread.sleep( GaianNode.WATCHDOG_POLL_TIMEOUT * 2 ); } catch (InterruptedException e2) { }
					continue;
				}
				
				isRunning = false;
				logger.logImportant("Exiting as discovery is no longer required...");
			}
		} // while (isDiscoveryStillRequired)
	}

}
