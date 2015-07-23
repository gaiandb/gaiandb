/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.sql.Connection;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.LayoutManager;

import javax.swing.BorderFactory;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;
import javax.swing.border.Border;

import com.ibm.gaiandb.Util;

public abstract class Tab extends JPanel {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = -2799508810551303647L;
	
	private static String getInstallPathOrDot() {
		try { return Util.getInstallPath(); }
		catch ( Exception e ) { System.err.println("Unable to resolve GaianDB install path (Using '.' instead), cause: " + e); return "."; }
	}

	private static final String ICON_DIR = getInstallPathOrDot() + "/" + "resources/";
	public static final Icon BACK_ICON = new ImageIcon(ICON_DIR + "back.png");
	public static final Icon BACK_DISABLED_ICON = new ImageIcon(ICON_DIR + "back_disabled.png");
	public static final Icon FORWARD_ICON = new ImageIcon(ICON_DIR + "forward.png");
	public static final Icon FORWARD_DISABLED_ICON = new ImageIcon(ICON_DIR + "forward_disabled.png");
	public static final Icon LOADING_ICON = new ImageIcon(ICON_DIR + "loading.gif");
	public static final Icon WARNING_ICON = new ImageIcon(ICON_DIR + "warning.png");
	public static final Icon ERROR_ICON = new ImageIcon(ICON_DIR + "error.png");

	protected final Dashboard container;

	public Tab(Dashboard container) {
		this(container, new BorderLayout(Dashboard.BORDER_SIZE, Dashboard.BORDER_SIZE));
	}

	public Tab(Dashboard container, LayoutManager layout) {
		super(layout);
		this.container = container;
		setBorder(Dashboard.DEFAULT_BORDER);
	}

	public abstract void connected(Connection newConn);

	public abstract void disconnected();

	public abstract void activated();

	public abstract void deactivated();

	protected static JScrollPane createScroller(Component view, int width, int height) {
		return createScroller(view, BorderFactory.createEtchedBorder(), width, height);
	}

	protected static JScrollPane createScroller(Component view, Border border, int width, int height) {
		JScrollPane scroller = new JScrollPane(view);
		if (null != border) {
			scroller.setBorder(border);
		}
		if ( -1 < width || -1 < height )
			scroller.setPreferredSize(new Dimension(width, height));
		scroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
		return scroller;
	}

	protected static String unravelMessages(Throwable t) {
		StringBuilder message = new StringBuilder();
		String lastMessage = null;
		while (null != t) {
			String currentMessage = t.getMessage();
			if (!currentMessage.equals(lastMessage)) {
				message.append(currentMessage + "\n");
			}
			lastMessage = currentMessage;
			t = t.getCause();
		}
		return message.toString();
	}
}
