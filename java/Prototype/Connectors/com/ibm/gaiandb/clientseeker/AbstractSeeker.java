/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.clientseeker;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Super-class for client/platform auto-discovery within the Fabric.
 * Use the appropriate subclass depending on whether attempting to discover a Fabric
 * node or a Fabric Registry.
 * 
 * @author DavidBarker
 *
 */
public class AbstractSeeker {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";	

	/** the multicast/broadcast address to use for discovery */
	protected String request_group = null;
	/** the multicast port to use */
	protected int request_port = 0;
	/** local ip address */
	protected String request_interface_address = null;
	/** local hostname */
	protected String local_host = null;
	
	/**
	 * Socket used to send the request packet.
	 */
	protected MulticastSocket udpSocket = null;
	
	/**
	 * InetAddress object representing the multicast/broadcast address.
	 */
	protected InetAddress groupAddr = null;
	
	/**
	 * InetAddress for the local interface used to publish the request.
	 */
	protected InetAddress interfaceAddr = null;
	
	/**
	 * List containing the payloads of each response.
	 */
	protected List<DatagramPacket> responses = new ArrayList<DatagramPacket>();
	
	/**
	 * Checks whether the repondent IP address is local by getting the complete
	 * list of local addresses for all interfaces and looking for a match.
	 * 
	 * @param ipAddress
	 * @return true if the address is assigned to a local interface.
	 */
	protected boolean responseIsLocal(String ipAddress) {
		String[] localAddresses;
		
		try {
			localAddresses = NetworkUtils.getLocalAddressStrings();
			
			if (localAddresses != null) {
				List<String> addressList = Arrays.asList(localAddresses);
				
				if (addressList.contains(ipAddress)) {
					return true;
				}
			}
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}

	/**
	 * Get details of the localhost.
	 * 
	 * @throws UnknownHostException
	 */
	protected void getLocalIPInformation() throws UnknownHostException {
		InetAddress localAddress = InetAddress.getLocalHost();
		request_interface_address = localAddress.getHostAddress();
		local_host = localAddress.getHostName();
	}
	
	/**
	 * Publish a request message to the specified multicast group. 
	 */
	protected void publishRequest(String message) throws IOException, SocketException {
			
			groupAddr = InetAddress.getByName(request_group);
			interfaceAddr = InetAddress.getByName(request_interface_address);
			
			/* is it a multicast address? */
			if (groupAddr.isMulticastAddress()) {
				
				/* open the socket and join the multicast group */
				udpSocket = new MulticastSocket();
				udpSocket.setNetworkInterface(NetworkInterface.getByInetAddress(interfaceAddr));
				udpSocket.setInterface(interfaceAddr);
				udpSocket.joinGroup(groupAddr);

				/* Send request packet */
				DatagramPacket p = new DatagramPacket(message.getBytes(),message.getBytes().length, groupAddr,7777);

				System.out.println("Sending request: " + new String(p.getData(),0,p.getLength()));
				udpSocket.send(p);
				
			} else {
				System.err.println("Invalid multicast address: " + groupAddr.toString());
			}

	}
	
	/**
	 * Wait the specified number of milliseconds for multicast packets to arrive.
	 * 
	 * @param waitMillis - the number of milliseconds to wait (sleep).
	 * @throws IOException if no packets arrive in the specified time.
	 */
	protected void waitForResponse(long waitMillis) throws ResourceNotFoundException {
		if (waitMillis <= 0) { /* wait for default time - 5 seconds */
			waitMillis = 5000;
		}
		
		try {
			Thread.sleep(waitMillis);
		} catch (InterruptedException e) {
			/* sleep interrupted */
		}
		
		System.out.println("Responses: " + responses.size());
		
		if (responses.size() == 0) { /* Nobody out there or taking too long to respond */
			throw new ResourceNotFoundException("No responses within specified timeout: " + waitMillis);
		}
	}
	
	/**
	 * Shutdown the listening and publishing resources used for the discovery, as appropriate.
	 * 
	 * @param grl
	 */
	protected void cleanupResources(DiscoveryListener grl) {
		if (grl != null) { // stop listener
			grl.stop();
		}
		
		if (udpSocket != null) {
			udpSocket.close();
		}
	}
	
	/*
	 * Classes
	 */
	
	/**
	 * Class used to listen for discovery responses.
	 * 
	 * @author DavidBarker
	 *
	 */
	class DiscoveryListener implements Runnable {
		
		/** Flag used to indicate when the main thread should terminate */
		private boolean isRunning = false;
		
		/** Multicast socket used to listen for discovery requests */
		private MulticastSocket udpListenRequestSocket = null;		
		private InetAddress groupListenRequestAddr = null;
		private InetAddress interfaceListenRequestAddr = null;
		
		private String listenPayload = null;
		
		/**
		 * Initialise the listener, including opening a multicast socket and joining
		 * the specified group.
		 * 
		 * @throws IOException if the socket cannot be opened.
		 * @throws SocketException if there is a problem setting the interface or joining the multicast group.
		 */
		public void init() throws IOException, SocketException {
			
			String request_interface_address = InetAddress.getLocalHost().getHostAddress();
			
			groupListenRequestAddr = InetAddress.getByName(request_group);
			interfaceListenRequestAddr = InetAddress.getByName(request_interface_address);

			/* is it a multicast address? */
			if (groupListenRequestAddr.isMulticastAddress()) {
				udpListenRequestSocket = new MulticastSocket(request_port);
				udpListenRequestSocket.setInterface(interfaceListenRequestAddr);
				udpListenRequestSocket.joinGroup(groupListenRequestAddr);
								
			} else {
				System.err.println("Not a valid multicast address!");
			}
		}
		
		/**
		 * Main processing method which handles received datagrams.
		 */
		public void run() {
			isRunning = true;

			while (isRunning) {
				try {
					/* TODO confirm packet size is appropriate */
					byte[] buf = new byte[256];
					DatagramPacket p = new DatagramPacket(buf, buf.length);

					try {
						if (! udpListenRequestSocket.isClosed()) { /* if socket not already closed */
							udpListenRequestSocket.receive(p);
							
							/* get the packet as a string */
							listenPayload = new String(p.getData(),0,p.getLength());
							
							/* add unique payloads - subclass responsibility to identify valid responses */
							if (! responses.contains(listenPayload)) {
								responses.add(p);		
							}	
						}
						
					} catch (SocketTimeoutException e) {
						// TODO what happens if we don't hear from anybody?						
					}
				} catch (IOException e) {
					if (!udpListenRequestSocket.isClosed()) {
						e.printStackTrace();	
					}
					/* stop looping */
					break;
				}
				
			}
				
		}
		
		/**
		 * Stop listening.
		 */
		public void stop() {
			isRunning = false;
			
			if (udpListenRequestSocket != null) {
				udpListenRequestSocket.close();
			}
		}
	}
}
