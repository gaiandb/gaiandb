/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import java.awt.LayoutManager;

import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.SwingConstants;

public abstract class UpdatingTab extends Tab {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = 8300590269022746777L;

	protected Connection conn = null;

	protected List<Updater> updaters;

	protected boolean awake = false;

	private final JLabel info = new JLabel("", SwingConstants.CENTER);

	public UpdatingTab(Dashboard container) {
	    super(container);
	    disconnected();
    }

	public UpdatingTab(Dashboard container, LayoutManager layout) {
	    super(container, layout);
	    disconnected();
    }

	public void addUpdater(Updater updater) {
		if (null == updater) {
			throw new NullPointerException("Null updater.");
		}

		updaters.add(updater);

		if (!awake) {
			updater.suspend();
		}

		String name = updater.getName();
		if (null == name) {
			new Thread(updater,"Updater").start();
		}
		else {
			new Thread(updater, name).start();
		}
	}

	protected abstract void create();

	protected abstract void destroy();

	public synchronized void connected(Connection newConn) {
		disconnected();
		showMessage("Loading...", LOADING_ICON);
		conn = newConn;

		updaters = new ArrayList<Updater>();
		create();
	}

	public synchronized void disconnected() {
		conn = null;

		if (null != updaters) {
			for (Updater updater : updaters) {
				updater.stop();
			}
			updaters = null;
		}

		destroy();

		showMessage("You must connect to a Gaian node.");
	}

	public synchronized void activated() {
		awake = true;
		if (null != updaters) {
			for (Updater updater : updaters) {
				updater.wake();
			}
		}
	}

	public synchronized void deactivated() {
		awake = false;
		if (null != updaters) {
			for (Updater updater : updaters) {
				updater.suspend();
			}
		}
	}

	protected void showMessage(String message) {
		showMessage(message, null);
	}

	protected void showMessage(String message, Icon icon) {
		info.setText(message);
		info.setIcon(icon);
		add(info);
	}

	protected void showError(String message) {
		showMessage(message, ERROR_ICON);
	}

	protected void hideMessage() {
		remove(info);
		info.setText("");
		info.setIcon(null);
	}
}
