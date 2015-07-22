/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.sensormonitor.sensors;

/**
 * Represents a point in two-dimensional space.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class Point {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	/** The ordinate in the <em>x</em> dimension. */
	private int x;
	/** The ordinate in the <em>y</em> dimension. */
	private int y;

	/**
	 * Retrieves the <em>x</em> ordinate.
	 * 
	 * @return <em>x</em>.
	 */
	public int getX() {
		return x;
	}

	/**
	 * Sets a new <em>x</em> ordinate.
	 * 
	 * @param x
	 *            <em>x</em>.
	 */
	public void setX(int x) {
		this.x = x;
	}

	/**
	 * Retrieves the <em>y</em> ordinate.
	 * 
	 * @return <em>y</em>.
	 */
	public int getY() {
		return y;
	}

	/**
	 * Sets a new <em>y</em> ordinate.
	 * 
	 * @param y
	 *            <em>y</em>.
	 */
	public void setY(int y) {
		this.y = y;
	}

	/**
	 * Initializes a new <code>Point</code> at the origin.
	 */
	public Point() {
		this(0, 0);
	}

	/**
	 * Initializes a new <code>Point</code> at the specified location.
	 * 
	 * @param x
	 *            The <em>x</em> ordinate.
	 * @param y
	 *            The <em>y</em> ordinate.
	 */
	public Point(int x, int y) {
		setX(x);
		setY(y);
	}

	/**
	 * Creates a string representation of the point.
	 * 
	 * @return A human-readable representation of the point.
	 */
	public String toString() {
		return "(" + x + ", " + y + ")";
	}
}
