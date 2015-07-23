/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.clientseeker;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Uses multicast to discover the nearest Gaian node and return its details in the form of a descriptor.
 * This descriptor can then be used by other components in order to connect to a Fabric Registry.
 * 
 * Note: this implementation makes use of Gaian clusters - essentially private communities of Gaian
 * nodes. Each cluster will only respond to specific requests to join that cluster (the cluster id
 * is a private identifier). This should mean that only Gaian nodes in the "FabricRegistry" cluster
 * will respond - however, limitations in the current alphaWorks release of Gaian (v.1.03) could
 * mean that other non-Registry Gaian nodes respond.  
 * 
 * @author DavidBarker
 *
 */
public class RegistrySeeker extends AbstractSeeker {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";	

	/**
	 * Request message to send for Gaian.
	 * 
	 * Format:
	 * 
	 * REQ <my hostname> <my ip> <args - for our purposes, cluster id>
	 * 
	 */
	private String GAIAN_REQ_MESSAGE = "REQ @@hostname@@ @@ip@@ FabricRegistry";

	public RegistrySeeker() {
		/** TODO - make multicast group and port configurable */
		this.request_group = "230.255.255.255";
		this.request_port = 7777;
	}
	
	/**
	 * Discover a Fabric Registry using multicast and return its details in the form of a descriptor.
	 * 
	 * @throws ResourceNotFoundException if no (valid) responses are received from any Gaian nodes. 
	 */
	public RegistryDescriptor discoverFabricRegistry(long waitMillis) throws IOException, ResourceNotFoundException {
		
		DiscoveryListener grl = null;
		int discoveredHostPacketIndex = -1;
		
		try {
			/* get the local IP address information for the host where this code is running */
			getLocalIPInformation();
			
			/* start the multicast listener in a separate thread */
			grl = new DiscoveryListener();
			grl.init();
			new Thread(grl,"DiscoveryListener").start();
			
			/*
			 * Build the request payload
			 */
			String request = GAIAN_REQ_MESSAGE.replaceFirst("@@ip@@", request_interface_address);
			
			/*
			 * Send the request
			 */
			publishRequest(request);
			
			/* wait for a response from any Gaian nodes that are out there */
			waitForResponse(waitMillis);
			
			/* shutdown listener etc. */
			cleanupResources(grl);

			/* remove any invalid responses */
			pruneResponses();
			
			/* if we didn't get any valid responses, bail out */
			if (responses.size() == 0) {
				throw new ResourceNotFoundException("Failed to discover a Fabric Registry using interface with IP " + request_interface_address);
			} else {
				/* look for a local response (if one exists) */
				for (DatagramPacket acknowledgement: responses) {
					
					/* preferably want the local node (if such exists) */
					if (responseIsLocal(acknowledgement.getAddress().getHostAddress())) {
						discoveredHostPacketIndex = responses.indexOf(acknowledgement);
					}
				}
				
				if (discoveredHostPacketIndex == -1) { /* no local host - arbitrarily pick the first one instead */
					System.out.println("A local Fabric Registry was not found - using first respondent instead.");
					discoveredHostPacketIndex = 0;
				}	
			}
			
		} catch (UnknownHostException e1) {			
			throw new IOException("Unknown host encountered. See nested exception for details.", e1);
			
		} finally { // tidy up resources
			cleanupResources(grl);
		}
		
		/* return a descriptor containing the information required to connect */
		DatagramPacket p = responses.get(discoveredHostPacketIndex);
		return new GaianRegistryDescriptorImpl(p.getAddress().getHostAddress());
	}

	/**
	 * Process the received packets list, removing duplicates and any packets
	 * other than the ACKs that are of interest.
	 */
	private void pruneResponses() {
		
//		System.out.println("Responses before pruning: " + responses.size());
		
		List<String> seenHosts = new ArrayList<String>();
		
		synchronized (responses) {
			Iterator<DatagramPacket> response_it = responses.iterator();

			/* Check each node, if we've not seen a node for more than the node_timeout, delete it */
			DatagramPacket response = null;
			String payload = null;
			while(response_it.hasNext())
			{
				response = response_it.next();
				//System.out.println("response host: " + response.getAddress().getHostAddress());
					
				payload = new String(response.getData(),0,response.getLength());
				
				/* only interested in ACK responses */
				if (payload.startsWith("ACK")) {
					/* chop it up and check the version - needs to be 1.03 or greater */
					String[] ackParts = payload.split(" ");
					if (ackParts.length == 6 && (! ackParts[5].startsWith("1.0"))) {
						/* wrong version - remove */
						response_it.remove();
					} else if (ackParts.length < 6) { /* short ACK from an old version - remove */
						response_it.remove();
					} else { /* valid - ignore any other packets from same host */
						if (! seenHosts.contains(response.getAddress().getHostAddress())) {
							/* add to seen list */
							seenHosts.add(response.getAddress().getHostAddress());
						} else { /* duplicate packet - ignore */
							response_it.remove();
						}
					}
				} else { /* not an ACK - remove it from the list */
					response_it.remove();
				}	
			}	
		}
		
//		System.out.println("Responses after pruning: " + responses.size());
	}
}
