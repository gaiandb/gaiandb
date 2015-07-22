/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensormonitor.sensors;

import java.io.File;

/**
 * Reads current system sensor metrics using JNI.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class SensorReader {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/**
	 * Loads SensorReader.dll in order to access system stats natively.
	 */
	static {
		final String[] libraryPaths = {
//			"SensorReader",
//		Note the following 2 cannot be loaded from GDB install dir - Install free Microsoft Visual C++ 2008 SP1 Redistributable Package (x86) - 9/16/2008 - vcredist_x86.exe
//			System.getenv("GDBL") + "/msvcr90.dll",
//			System.getenv("GDBL") + "/msvcp90.dll",
			System.getenv("GDBL") + "/SensorReader.dll",
//			"CppSource/Debug/SensorReader"
		};

		boolean loaded = false;
		try {
			for (String libraryPath : libraryPaths) {
				try {
//					System.out.println("p=" +new File(libraryPath+".dll").getPath() );
//					System.out.println("a=" +new File(libraryPath+".dll").getAbsolutePath() );
//					System.out.println("c=" +new File(libraryPath+".dll").getCanonicalPath() );
					libraryPath = new File(libraryPath).getCanonicalPath();
					System.out.println("Loading library " + libraryPath);
					System.load(libraryPath);
//					System.loadLibrary(libraryPath); // With Sun Java, this method does not allow paths.
//					System.out.println("Loaded " + libraryPath);
					loaded = true;
					break;
				}
				catch (UnsatisfiedLinkError e) { System.err.println(e.getMessage()); }
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}

		if (!loaded) {
			System.err.println("This utility is used for testing purposes and is not supported on your system.");
		}
	}

	/**
	 * Initialises the sensor reader.
	 */
	public SensorReader() {
		init();
	}

	/**
	 * Initialises the sensor reader.
	 */
	protected native void init();

	/**
	 * Gets the current CPU usage.
	 * 
	 * @return CPU usage as a percentage.
	 */
	public native int getCpuUsage();

	/**
	 * Gets the current used memory.
	 * 
	 * @return Used memory in MB.
	 */
	public native int getUsedMemory();

	/**
	 * Gets the current free memory.
	 * 
	 * @return Free memory in MB.
	 */
	public native int getFreeMemory();

	/**
	 * Gets the total memory.
	 * 
	 * @return Total memory in MB.
	 */
	public native int getTotalMemory();

	/**
	 * Gets the current disk I/O.
	 * 
	 * @return Disk I/O in KB/s.
	 */
	public native int getDiskIO();

	/**
	 * Gets the current network I/O.
	 * 
	 * @return Network I/O in KB/s.
	 */
	public native int getNetworkIO();

	/**
	 * Gets the current remaining battery power.
	 * 
	 * @return Battery power as a percentage.
	 */
	public native int getBatteryPowerRemaining();
}
