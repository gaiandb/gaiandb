/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

//import java.util.logging.Logger;

abstract class Updater implements Runnable {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	//private static final Logger LOGGER = Logger.getLogger(Updater.class.getName());

	protected final String name;
	protected Thread thread = null;
	protected boolean done = false;
	protected boolean awake = true;
	protected int interval = 0;

	public Updater() {
		this(null, 0);
	}

	public Updater(String name) {
		this(name, 0);
	}

	public Updater(int interval) {
		this(null, interval);
	}

	public Updater(String name, int interval) {
		this.name = name;
		this.interval = interval;
	}

	public String getName() {
		return name;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		if (interval > 0) {
			this.interval = interval;
		}
		else {
			this.interval = 0;
		}
	}

	public final void run() {
		thread = Thread.currentThread();

		while (!done) {

			boolean justAwoke = false;
			if (!awake) {
				try {
					synchronized (this) {
						wait();
					}
					justAwoke = true;
				}
				catch (InterruptedException e) {
					interrupted();
					return;
				}
			}
			
			if (!update()) {
				break;
			}

			if (interval > 0) {
				try {
					// update/recenter graph asap after its had a chance to render
					Thread.sleep( justAwoke ? 1000 : interval );
//					Thread.sleep(interval );
				}
				catch (InterruptedException e) {
					interrupted();
					return;
				}
			}
		}
	}

	protected abstract boolean update();

	public final void suspend() {
		awake = false;
	}

	public void wake() {
		awake = true;
		synchronized (this) {
			notifyAll();
		}
	}

	public final void stop() {
		if (!done) {
			done = true;
			if (null != thread) {
				thread.interrupt();
			}
		}
	}

	protected void interrupted() {
		//LOGGER.info("The " + Thread.currentThread().getName() + " thread was interrupted.");
	}
}
