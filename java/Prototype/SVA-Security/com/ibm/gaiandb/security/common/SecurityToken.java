/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.security.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public abstract class SecurityToken {
	private String tName;  // name of the token in the authenticator implementation

	public abstract byte[] get();
	public abstract String getId();
	public abstract boolean isValid();	
	public abstract void set(byte[] bToken);
	
	/**
	 * get the token's name
	 * @return String the token name
	 */
	public String getName() {
		return this.tName;
	}
	
	/**
	 * set the token's name
	 * @param pName the token's name
	 */
	public void setName(String pName) {
		this.tName = pName;
	}

	/**
	 * transform an object to bytes
	 * @param obj
	 * @return
	 * @throws IOException
	 */
	public byte[] getBytes(Object obj) throws IOException {
		byte[] bytes = null;
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		if (null != bos) {
			ObjectOutputStream oos = new ObjectOutputStream(bos); 
			oos.writeObject(obj);
			// write it out
			oos.flush(); 
			oos.close(); 
			// write it out
			bos.flush();
			bos.close();
			// now get the bytes
			bytes = bos.toByteArray();
			//clean up
			oos = null;
			bos = null;
		}
		
		return bytes;
	}

	/**
	 * transforms bytes to an object
	 * @param bytes
	 * @return Object
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Object getObject(byte[] bytes) throws IOException, ClassNotFoundException {
		Object obj = null;
		
		if (null != bytes) {
			ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
			
			if (null != bis) {
				ObjectInputStream ois = new ObjectInputStream (bis);
				if (null != ois) obj = ois.readObject();
			}
		}
		
		return obj;
	}

}
