/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import java.awt.Color;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Insets;

import javax.swing.JPanel;

public class StatsChart extends JPanel {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	private static final long serialVersionUID = -5062918008888904844L;

	private static final int AXIS_MARGIN = 5;

	private static final Color BACKGROUND_COLOR = new Color(0xFFFFFF);
	private static final Color AXIS_COLOR = new Color(0xAAAAAA);
	private static final Color LINE_COLOR = new Color(0x000000);

	public final class Stat implements Comparable<Stat> {
		public final long timestamp;
		public final int value;

		public Stat(int timestamp_s, int value) {
			this((long)timestamp_s * 1000, value);
		}

		public Stat(long timestamp_ms, int value) {
			this.timestamp = timestamp_ms;
			this.value = value;
		}

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null) {
				return false;
			}

			if (!(o instanceof Stat)) {
				return false;
			}

			Stat other = (Stat)o;
			if (timestamp != other.timestamp) {
				return false;
			}

			return true;
		}

		public int hashCode() {
			return (int)(timestamp ^ (timestamp >>> 32));
		}

		public int compareTo(Stat o) {
			if (null == o) {
				throw new NullPointerException();
			}

			if (timestamp < o.timestamp) {
				return -1;
			}
			else if (timestamp > o.timestamp) {
				return 1;
			}

			return 0;
		}

		public String toString() {
			return "( " + timestamp + ", " + value + " )";
		}
	}

	public class ValueRange {
		public final int min;
		public final int max;

		public ValueRange(int min, int max) {
			if (min < max) {
				this.min = min;
				this.max = max;
			}
			else if (min > max) {
				throw new IllegalArgumentException("Min cannot be greater than max.");
			}
			else {
				throw new IllegalArgumentException("Min cannot be equal to max.");
			}
		}
	}

	private String title;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		if (null == title) {
			this.title = "";
		}
		else {
			this.title = title;
		}
	}

	private long duration;

	public long getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		setDuration((long)duration * 1000);
	}

	public void setDuration(long duration) {
		if (duration <= 0) {
			throw new IllegalArgumentException(String.valueOf(duration));
		}

		this.duration = duration;
	}

	private ValueRange valueRange;

	public ValueRange getValueRange() {
		return valueRange;
	}

	public void setValueRange(ValueRange valueRange) {
		this.valueRange = valueRange;
	}

	public void setValueRange(int min, int max) {
		if (min != max) {
			setValueRange(new ValueRange(min, max));
		}
		else {
			setValueRange(null);
		}
	}

	private Insets insets = new Insets(0, 0, 0, 0);

	public Insets getInsets() {
		return insets;
	}

	public void setInsets(Insets insets) {
		this.insets = insets;
	}

	public void setInsets(int size) {
		this.insets = new Insets(size, size, size, size);
	}

	public void setInsets(int top, int left, int bottom, int right) {
		this.insets = new Insets(top, left, bottom, right);
	}

	private Map<String, List<Stat>> stats = new HashMap<String, List<Stat>>();

	private Long lastUpdateTime = 0L;

	private Map<String, Color> nodeColors = null;

	public StatsChart(int duration) {
		super();
		setDuration(duration);
	}

	public StatsChart(long duration) {
		super();
		setDuration(duration);
	}

	public StatsChart(int duration, Map<String, Color> nodeColors) {
		this(duration);
		this.nodeColors = nodeColors;
	}

	public StatsChart(long duration, Map<String, Color> nodeColors) {
		this(duration);
		this.nodeColors = nodeColors;
	}

	public boolean addStat(String node, int timestamp_s, int value) {
		return addStat(node, new Stat(timestamp_s, value));
	}

	public boolean addStat(String node, long timestamp_ms, int value) {
		return addStat(node, new Stat(timestamp_ms, value));
	}

	public boolean addStat(String node, Stat s) {
		synchronized (lastUpdateTime) {
			List<Stat> nodeStats = stats.get(node);
			if (null == nodeStats) {
				nodeStats = new LinkedList<Stat>();
				stats.put(node, nodeStats);
			}

			// Insert in order.
			ListIterator<Stat> iterator = nodeStats.listIterator(nodeStats.size());
			while (iterator.hasPrevious()) {
				int compare = iterator.previous().compareTo(s);
				if (compare == 0) {
					return false;
				}
				else if (compare >= 0) {
					// Whoa, too far.
					iterator.next();
					break;
				}
			}

			iterator.add(s);

			lastUpdateTime = System.currentTimeMillis();
			lastUpdateTime -= lastUpdateTime % 1000;
		}

		return true;
	}

	public void removeOldStats() {
		synchronized (lastUpdateTime) {
			for (List<Stat> nodeStats : stats.values()) {
				ListIterator<Stat> iterator = nodeStats.listIterator();
				while (iterator.hasNext()) {
					if (lastUpdateTime - iterator.next().timestamp > duration) {
						iterator.remove();
					}
				}
			}
		}
	}

	public int statCount() {
		int total = 0;
		for (List<Stat> nodeStats : stats.values()) {
			total += nodeStats.size();
		}
		return total;
	}

	public void paintComponent(Graphics g) {
		removeOldStats();

		super.paintComponent(g);

		synchronized (lastUpdateTime) {
			Color originalColor = g.getColor();

			// Wipe it out.
			g.setColor(BACKGROUND_COLOR);
			g.fillRect(0, 0, getWidth(), getHeight());

			// Calculate the bounds.
			int left = insets.left;
			int top = insets.top;
			int width = getWidth() - insets.left - insets.right;
			int height = getHeight() - insets.top - insets.bottom;

			// Get the minimum and maximum values.
			int min;
			int max;
			if (null != valueRange) {
				min = valueRange.min;
				max = valueRange.max;
			}
			else {
				min = Integer.MAX_VALUE;
				max = Integer.MIN_VALUE;
				for (List<Stat> nodeStats : stats.values()) {
					for (Stat stat : nodeStats) {
						if (stat.value < min) {
							min = stat.value;
						}

						if (stat.value > max) {
							max = stat.value;
						}
					}
				}
			}

			// Draw the axes.
			FontMetrics fontMetrics = g.getFontMetrics();
			int fontHeight = (int)fontMetrics.getLineMetrics("0123456789", g).getAscent();

			String minValue = String.valueOf(min);
			String maxValue = String.valueOf(max);

			// Figure out the widths of each value so we can right-align them.
			int minValueWidth = (int)fontMetrics.getStringBounds(minValue, 0, minValue.length(), g).getWidth();
			int maxValueWidth = (int)fontMetrics.getStringBounds(maxValue, 0, maxValue.length(), g).getWidth();
			int axisWidth = (int)fontMetrics.getStringBounds("00000", g).getWidth() + AXIS_MARGIN;

			g.setColor(AXIS_COLOR);

			if (stats.size() > 0) {
				g.drawString(maxValue,
					left + axisWidth - AXIS_MARGIN - maxValueWidth,
					top + fontHeight);
				g.drawString(minValue,
					left + axisWidth - AXIS_MARGIN - minValueWidth,
					top + height);
			}

			g.drawLine(
				left + axisWidth, top,
				left + axisWidth, top + height);
			g.drawLine(
				left + axisWidth, top + height - fontHeight / 2,
				left + width, top + height - fontHeight / 2);

			left += axisWidth;
			width -= axisWidth;

			top += fontHeight / 2;
			height -= fontHeight;

			// Draw the lines.
			g.setColor(LINE_COLOR);
			for (Entry<String, List<Stat>> entry : stats.entrySet()) {
				String node = entry.getKey();
				if (null == nodeColors || nodeColors.containsKey(node)) {
					if (null != nodeColors) {
						g.setColor(nodeColors.get(node));
					}

					List<Stat> nodeStats = entry.getValue();
	
					boolean first = true;
					int lastX = 0;
					int lastY = 0;
					for (Stat stat : nodeStats) {
						int x = (int)((stat.timestamp - lastUpdateTime + duration) * width / duration) + left;
						int y;
						if (min == max) {
							y = height / 2 + top;
						}
						else {
							y = height - ((stat.value - min) * height / (max - min)) + top;
						}
	
						if (first) {
							g.drawLine(x, y, x, y);
							first = false;
						}
						else {
							g.drawLine(lastX, lastY, x, y);
						}
	
						lastX = x;
						lastY = y;
					}
				}
			}

			g.setColor(originalColor);
		}
	}
}
