/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import com.ibm.gaiandb.webserver.WebServer;

public class CloudBootstrap {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";
	
	private static String vcap_app_port = System.getenv("VCAP_APP_PORT");

	public static void main(String args[]) {
		final String[] nodeArgs = args;

		try {			
			System.out.println("Starting the WebServer" );
			Thread WebServerThread = new Thread(new Runnable(){
				@Override
				public void run() {
					try {
						WebServer.main(new String[]{vcap_app_port});
					} catch (Exception e) {
						e.printStackTrace();
					}		
				}				
			},"WebServer");
			WebServerThread.start();

			Thread GaianNodeThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						GaianNode.main(nodeArgs);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}				
			},"GaianNode");
			GaianNodeThread.start();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
