/*
 * (C) Copyright IBM Corp. 2011
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.clientseeker;

/**
 * Descriptor for a Gaian Fabric Registry.
 * 
 * @author DavidBarker
 *
 */
public class GaianRegistryDescriptorImpl implements RegistryDescriptor {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";	

	/** Template string for the registryUrl */
	private static String registryUrl = "jdbc:derby://@@ip@@:6414/FABRIC;user=fabric;password=fabric";
	
	/** JDBC address of the Registry */
	private String address;
	
	/** The Registry type - gaian or singleton */
	private String type;
	
	/** The protocol used to communicate with the Registry */
	private String protocol;
	
	public GaianRegistryDescriptorImpl(String registryHost) {
		/** construct the address using the host information */
		this.address = registryUrl.replaceFirst("@@ip@@", registryHost);
		type = "gaian";
		protocol = "jdbc";
	}
	
	/**
	 * @see fabric.discovery.RegistryDescriptor#address()
	 */
	public String address() {

		return address;
	}

	/**
	 * @see fabric.discovery.RegistryDescriptor#type()
	 */
	public String type() {

		return type;
	}

	/**
	 * @see fabric.discovery.RegistryDescriptor#protocol()
	 */
	public String protocol() {

		return protocol;
	}
	
	/**
	 * Simple string representation of the descriptor for debug purposes.
	 */
	public String toString() {
		StringBuffer buffy = new StringBuffer("GaianRegistryDescriptor::");
		buffy.append(" Address: ").append(address);
		buffy.append(", Type: ").append(type);
		buffy.append(", Protocol: ").append(protocol);
		return buffy.toString();
	}
	
	/**
	 * Equals method implemented to allow comparison of descriptors.
	 */
	public boolean equals(Object obj) {
		boolean equal = false;
		if (obj instanceof GaianRegistryDescriptorImpl) {
			GaianRegistryDescriptorImpl grd = (GaianRegistryDescriptorImpl)obj;
			if (grd.address() == null ? address == null : grd.address().equals(address) && 
					grd.type() == null ? type == null : grd.type().equals(type) &&
					grd.protocol() == null ? protocol == null : grd.protocol().equals(protocol)) {
				
				equal = true;
			}
		}
		return equal;
	}

}
