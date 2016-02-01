/*
 * (C) Copyright IBM Corp. 2014
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.embedding;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianTask;

/**
 * This class is a basic sample wrapper for Gaian.
 * It shows examples of how to use the GaianTask class to easily embed Gaian federation technology into their application's JVM process.
 * 
 * @author DavidVyvyan
 */

public class SampleApplicationEmbeddingGaian {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2014";
	
	// Use the working directory (Java system property "user.dir") as Gaian workspace
	private static final String MY_GAIAN_WORKSPACE_FOLDER_FOR_CONFIG_AND_LOGS_AND_LOCAL_DERBY_INSTANCE = System.getProperty("user.dir");
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("Java class path: " + System.getProperty("java.class.path"));
		System.setProperty( "derby.system.home", MY_GAIAN_WORKSPACE_FOLDER_FOR_CONFIG_AND_LOGS_AND_LOCAL_DERBY_INSTANCE );
		
		Thread.currentThread().setName("Main thread for MyApplicationEmbeddingGaian");
		
		// Pass arguments to Gaian in the GaianTask constructor (these are the same as supported by the launchGaianServer.bat(/sh) scripts)
		// (Invoking the empty constructor GaianTask() uses port 6414 by default)
		final GaianTask gaianTask = new GaianTask( Arrays.asList("-p", "6414") );
		
		// Start Gaian as a "task" within this process
		gaianTask.startTask();
		
		System.out.println("\nGaian has now been started in this application...\n");
		
		runQueryAndPrintNumberOfRecordsRetrieved( "select * from LT0" );
		
		ThreadGroup parentThreadGroup = null, tg = Thread.currentThread().getThreadGroup();
		while ( null != tg ) { parentThreadGroup = tg; tg = tg.getParent(); }
		System.out.println("\nHere is a list of threads running in this application (should include all Gaian & Derby threads)...\n");
		
		parentThreadGroup.list();
		
		System.out.println("\nYou can also run the following SQL from another client (e.g. dashboard.bat): call listthreads()");
		System.out.println("Press [Enter] to shutdown the GaianTask, then list remaining threads, and finally end this application...");
		BufferedReader stdin = new BufferedReader( new InputStreamReader( System.in ) );
		stdin.readLine();
		
		// Stop the embedded Gaian threads (including Derby network server threads and Gaian watchdog, seeker, etc)
		gaianTask.shutDown();

		System.out.println("GaianTask has now been shutdown in this application...\n");
		System.out.println("Remaining application threads before completion:\n");
		
		parentThreadGroup.list();
	}
	
	private static void runQueryAndPrintNumberOfRecordsRetrieved( final String sql ) throws SQLException {

		System.out.println("Running query: " + sql);
		
		// You can use the embedded Derby JDBC driver/connection to run queries because Gaian is in this process
		// (Note - other applications outside this process can also access your instance of Gaian via TCP port 6414)
		ResultSet rs = GaianDBConfig.getEmbeddedDerbyConnection().createStatement().executeQuery( sql );
		
		int count = 0;
		while ( rs.next() ) count++;
		System.out.println("Number of records retrieved: " + count);
	}
} 
