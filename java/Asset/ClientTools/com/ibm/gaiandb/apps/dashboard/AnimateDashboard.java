/*
 * (C) Copyright IBM Corp. 2012
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.util.Timer;
import java.util.TimerTask;

/**
 * This class is an alternative wrapper to Dashboard.java
 * It uses a Timer to schedule path visualisation updates on it.
 * 
 */
public class AnimateDashboard {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2012";

	public static void main(String[] args) {

//		System.out.println("Main Class");	
		final Dashboard dashb = new Dashboard();
		dashb.setVisible(true);
//		dashb.show();

		new Timer().schedule(
			new TimerTask() {
				int tickCount = 0;

				@Override
				public void run() {
					
					try { dashb.updatePathVisualisation(tickCount); //System.out.println("Tick" + dashb.getHeight());
					} catch (Exception e) { e.printStackTrace(); }

					tickCount++;
				}
			}, 0, 50
		);
//		t1.schedule(new AnimDashboardTick(db),0,500);
	}
}
