/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensormonitor;

import java.sql.SQLException;

import javax.security.auth.login.LoginException;

import com.ibm.gaiandb.apps.DBConnector;
import com.ibm.gaiandb.apps.MetricMonitor;
import com.ibm.gaiandb.apps.sensormonitor.sensors.Point;
import com.ibm.gaiandb.apps.sensormonitor.sensors.ThinkPadSensorReader;


/**
 * Records PC metrics and sensor data and stores the results in a table
 * to be queried later.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public final class SensorMonitor extends DBConnector {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private MetricMonitor metricMonitor;

	/**
	 * Creates a new <code>SensorMonitor</code> and begins reading and storing
	 * sensor data.
	 * 
	 * @param args
	 *            The program arguments.
	 * @throws InterruptedException
	 *             if the thread is interrupted while sleeping.
	 */
	public static void main(String[] args) {
		SensorMonitor app;
		try {
			app = new SensorMonitor(args);
		}
		catch (Exception e) {
			terminate(e.getMessage());
			return;
		}

		try {
			app.run();
		}
		catch (InterruptedException e) {
			System.err.println("The process was interrupted by the operating system.");
		}
	}

	/**
	 * Initializes the sensor monitor by connecting to the database using the
	 * arguments provided, then creates a <code>MetricMonitor</code> which
	 * inserts the sensor values into its table.
	 * 
	 * @param args
	 *            An array containing, in order, the JDBC URL, username, and
	 *            password.
	 * 
	 * @throws ClassNotFoundException
	 *             if the Derby client driver class cannot be found.
	 * @throws InterruptedException
	 *             if an interrupt occurs while sleeping between connection
	 *             attempts.
	 * @throws LoginException 
	 */
	public SensorMonitor(String[] args) throws ClassNotFoundException, InterruptedException, LoginException {
		super(args);
		connect();
	}

	/**
	 * Commences reading the sensors, and handles any disconnects by attempting
	 * to reconnect.
	 * 
	 * @throws InterruptedException
	 *             if the program is interrupted while sleeping.
	 */
	public void run() throws InterruptedException {
		while (true) {
			metricMonitor = MetricMonitor.getInstance(conn);
			addSensors();

			while (metricMonitor.isRunning()) {
				Thread.sleep(1000);
			}

			try {
				if (null == conn || conn.isClosed()) {
					System.err.println("The database connection was lost.");
					System.out.println("Attempting to reconnect to the server.");
					try {
						connect();
					}
					catch (Exception eC) {
						eC.printStackTrace();
						return;
					}
				}
			}
			catch (SQLException e) {
				terminate("The database connection was lost.");
				return;
			}
		}
	}

	/**
	 * Adds monitors which read values from the sensors and return them.
	 */
	protected void addSensors() throws InterruptedException {
		final ThinkPadSensorReader sensors;
		try {
			sensors = new ThinkPadSensorReader();
		}
		catch (UnsatisfiedLinkError e) {
			terminate("Could not load the required DLLs.");
			return;
		}

		System.out.println("Reading sensors...");

		metricMonitor.addMonitor("CPU Usage",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getCpuUsage());
               }
			}
		);

		metricMonitor.addMonitor("Used Memory",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getUsedMemory());
               }
			}
		);

		metricMonitor.addMonitor("Free Memory",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getFreeMemory());
               }
			}
		);

		metricMonitor.addMonitor("Total Memory",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getTotalMemory());
               }
			}
		);

		metricMonitor.addMonitor("Disk I/O",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getDiskIO());
               }
			}
		);

		metricMonitor.addMonitor("Network I/O",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getNetworkIO());
               }
			}
		);

		metricMonitor.addMonitor("Battery Power",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getBatteryPowerRemaining());
               }
			}
		);

		metricMonitor.addMonitor("Temperature",
			new MetricMonitor.Monitor<Integer>() {
				public Integer getValue() {
					return checkForError(sensors.getTemperature(), -100);
               }
			}
		);

		metricMonitor.addMonitor("Acceleration",
			new MetricMonitor.Monitor<Point>() {
				public Point getValue() {
					return sensors.getAcceleration();
               }
			}
		);
	}

	private Integer checkForError(int value) {
		return checkForError(value, 0);
	}

	private Integer checkForError(int value, int minimum) {
		if (value >= minimum) {
			return value;
		}
		else {
			return null;
		}
	}
}
