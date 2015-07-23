/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.clientseeker;

/**
 * Represents metadata for a Fabric Registry, such as the network address and other information
 * required to connect to it.
 * 
 * @author DavidBarker
 *
 */
public interface RegistryDescriptor {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";	

	/**
	 * Get the address of the Fabric Registry.
	 * 
	 * @return an implementation-specific string that can be used to connect to the Registry.
	 */
	public String address();
	
	/**
	 * Get the type of Fabric Registry.
	 * 
	 * @return "singleton" for a centralised Registry or "gaian" for a distributed Registry.
	 */
	public String type();
	
	/**
	 * Get the protocol used to communicate with the Registry.
	 * 
	 * @return "jdbc" - this is the only protocol supported currently.
	 */
	public String protocol();

	
}
