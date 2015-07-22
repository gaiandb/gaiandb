/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensormonitor.sensors;

/**
 * Reads current system sensor metrics using JNI, including ThinkPad-specific
 * stats.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class ThinkPadSensorReader extends SensorReader {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/**
	 * Initialises the ThinkPad sensor reader.
	 */
	protected native void init();

	/**
	 * Gets the current temperature.
	 * 
	 * @return The temperature in Celcius.
	 */
	public native int getTemperature();

	/**
	 * Gets the current acceleration.
	 * 
	 * @return The acceleration in two dimensions.
	 */
	public native Point getAcceleration();
}
