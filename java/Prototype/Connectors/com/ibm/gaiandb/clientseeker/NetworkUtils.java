/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.clientseeker;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Set of utility methods covering IP address related operations - anything from
 * getting a list of local IP addresses to comparing addresses to see if they are in the
 * same network. 
 * 
 * @author DavidBarker
 *
 */
public class NetworkUtils {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";	

	/** Reference to the local host address */
	private static InetAddress localAddress = null; 
	
	static {
		try {
			/* initialise the local address reference */
			localAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			System.err.println("Failed to get local host details.");
		}
	}
	
	/**
	 * Return all valid IP addresses for this local host. 
	 * Note: List can include both IPv4 and IPv6 addresses.
	 * 
	 * @return a list of addresses represented as string or null in the event of an error.
	 * @throws UnknownHostException If the localhost name cannot be resolved.
	 * @throws SocketException 
	 */
	public static String[] getLocalAddressStrings() throws UnknownHostException, SocketException {
		String[] localAddressStrings = null;
		
		if (localAddress != null) {
			InetAddress[] allMyAddresses = getLocalAddresses();
			if (allMyAddresses != null) {
				localAddressStrings = new String[allMyAddresses.length];
				for (int y=0; y < allMyAddresses.length; y++) {
					localAddressStrings[y] = allMyAddresses[y].getHostAddress();
				}		
			}
		}
		
		return localAddressStrings;
	}
	
	/**
	 * Return all valid IP addresses for this local host. 
	 * Note: List can include both IPv4 and IPv6 addresses.
	 * 
	 * @return a list of InetAddress objects or null in the event of an error.
	 * @throws UnknownHostException If the localhost name cannot be resolved.
	 * @throws SocketException if an error occurs getting the list of local interfaces
	 */
	public static InetAddress[] getLocalAddresses() throws UnknownHostException, SocketException {
		List<InetAddress> allMyAddresses = new ArrayList<InetAddress>();

		if (localAddress != null) {
			
			Enumeration<NetworkInterface> localInterfaces = getLocalInterfaces();
			Enumeration<InetAddress> interfaceAddresses = null;
			while (localInterfaces.hasMoreElements()) {
				/* get the set of addresses for each interface */
				interfaceAddresses = localInterfaces.nextElement().getInetAddresses();
				while (interfaceAddresses.hasMoreElements()) {
					allMyAddresses.add(interfaceAddresses.nextElement());
				}
			}
			
			// allMyAddresses = InetAddress.getAllByName(localAddress.getHostName());
		}
		
		InetAddress[] addresses = allMyAddresses.toArray(new InetAddress[allMyAddresses.size()]);
		return addresses;
	}
	
	/**
	 * Return the set of network interfaces for the local host.
	 * 
	 * @return an enumeration of the interfaces or null otherwise.
	 * @throws SocketException if an error occurs getting the interface list.
	 */
	public static Enumeration<NetworkInterface> getLocalInterfaces() throws SocketException {
		return NetworkInterface.getNetworkInterfaces();
	}
	
	
	/**
	 * Build a subnet mask using the specified network prefix length.
	 * 
	 * @see getNetworkPrefixLength()
	 * @param networkPrefixLength the prefix indicating the subnet mask (typically 8, 16, or 24 for IPv4).
	 * @return a byte array containing the full subnet mask.
	 */
	public static byte[] buildSubNetMaskFromNetworkPrefix(short networkPrefixLength) {
		
		int mask = 0xffffffff << (32 - networkPrefixLength);
		int value = mask;
		byte[] maskBytes = new byte[]{ 
		        (byte)(value >>> 24), (byte)(value >> 16 & 0xff), (byte)(value >> 8 & 0xff), (byte)(value & 0xff) };
		
		return maskBytes;
	}
	
	/**
	 * Get the network prefix length (indicating the subnet mask) for the specified
	 * IP address.  
	 * 
	 * @param ipAddress the ip address for which the prefix should be identified.
	 * @return the network prefix length or 0 if not found.
	 * @throws UnknownHostException if the ip address specified cannot be resolved to the local host.
	 * @throws SocketException if an error occurs while attempting to identify the network prefix length.
	 */
	public static short getNetworkPrefixLength(byte[] ipAddress) throws SocketException, UnknownHostException {
		short prefix = 0;
		
		if (ipAddress != null) {
			prefix = getNetworkPrefixLength(InetAddress.getByAddress(ipAddress));
		}
		
		return prefix;
	}
	
	/**
	 * Get the network prefix length (indicating the subnet mask) for the specified
	 * IP address.
	 * 
	 * @param ipAddress the ip address for which the prefix should be identified
	 * @return the network prefix length or 0 if not found.
	 * @throws SocketException if an error occurs while attempting to identify the network prefix length.
	 */	
	public static short getNetworkPrefixLength(InetAddress ipAddress) throws SocketException {
		short prefix = 0;
		
		if (ipAddress != null) {
			java.net.NetworkInterface myInterface = java.net.NetworkInterface.getByInetAddress(ipAddress);
			for (int x=0; x < myInterface.getInterfaceAddresses().size(); x++) {
				/* matching address will start /<ip of localhost> */
				if (myInterface.getInterfaceAddresses().get(x).getAddress().equals(ipAddress)) {
					prefix = myInterface.getInterfaceAddresses().get(x).getNetworkPrefixLength();
					break;
				}
			}	
		}
		
		return prefix;
	}
	
	/**
	 * Compute the network address given the specified IP address and subnet mask.
	 * Useful for comparing two IP addresses to determine if they are within the same subnet.
	 * 
	 * @param ipAddress
	 * @param subnetMask
	 * @return the computed network address or an empty byte array otherwise.
	 */
	public static byte[] computeNetworkAddressBytes(byte[] ipAddress, byte[] subnetMask) {
		byte[] networkAddressBytes = new byte[4];
		
		if (ipAddress != null && subnetMask != null) {
			/* Compute network address by ANDing the ip address with the subnet mask */
			for (int x=0; x < networkAddressBytes.length; x++) {
				networkAddressBytes[x] = (byte)(ipAddress[x] & subnetMask[x]);
			}
		} else {
			System.err.println("Cannot compute network address - one or more arguments are null.");
		}
		
		return networkAddressBytes;
	}
	
	/**
	 * Compute the network address given the specified IP address and subnet mask.
	 * Useful for comparing two IP addresses to determine if they are within the same subnet.
	 * 
	 * @param ipAddress the IP address
	 * @param subnetMask the subnet mask
	 * @return the computed network address or null otherwise.
	 * @throws UnknownHostException if the computed network address cannot be resolved to a valid InetAddress
	 */
	public static InetAddress computeNetworkAddress(InetAddress ipAddress, byte[] subnetMask) throws UnknownHostException {
		InetAddress networkAddress = null;
		
		byte[] networkAddressBytes = computeNetworkAddressBytes(ipAddress.getAddress(), subnetMask);
		networkAddress = InetAddress.getByAddress(networkAddressBytes);
		
		return networkAddress;
	}
}
