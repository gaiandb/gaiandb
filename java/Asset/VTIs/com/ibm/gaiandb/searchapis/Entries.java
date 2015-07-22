/*
 * (C) Copyright IBM Corp. 2008
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.searchapis;

/**
 * @author gabent
 */
public class Entries {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2008";
	
	private String id;
	
	private String updated;
	
	public Entries(){
		
	}
	
	public Entries(String id, String updated) {

		this.id  = id;
		this.updated= updated;	
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getId() {
		return id;
	}
	

	public void setUpdated(String updated) {
		this.updated = updated;
	}	
	
	public String getUpdated() {
		return updated;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Entity Details - ");
		sb.append("Soucedoc: " +getId().hashCode());
		sb.append(", ");
		sb.append("Id:" + getId());
		sb.append(", ");
		sb.append("Updated:" + getUpdated());
		sb.append('.');
		
		return sb.toString();
	}
}

