/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.util.Map;

import java.awt.Color;

public class MonitorInfo {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	public final String name;
	public final String shortName;
	public final String unit;
	public final String[] requiredMetrics;
	public final boolean hardBounds;
	public int minBound;
	public int maxBound;
	public final int defaultMinBound, defaultMaxBound;
	public final Color[] colorPalette;
	public final ValueRetriever valueRetriever;
	
	public boolean isInternalMetric() { return MetricValueProcessor.internalMetricsSet.contains(name); }

	public static interface ValueRetriever {
		public Integer get(Map<String, Integer> currentValues);
	}

	public static class DefaultValueRetriever implements ValueRetriever {
		private final String fieldName;

		public DefaultValueRetriever(String fieldName) {
			this.fieldName = fieldName;
		}

		public Integer get(Map<String, Integer> currentValues) {
			return currentValues.get(fieldName);
		}
	}

	public MonitorInfo(String name, String shortName, String unit, String[] requiredMetrics, boolean hardBounds, int minBound, int maxBound, Color[] colorPalette, ValueRetriever valueRetriever) {
		this.name = name;
		this.shortName = shortName;
		this.unit = unit;
		this.requiredMetrics = requiredMetrics;
		this.hardBounds = hardBounds;
		this.minBound = defaultMinBound = minBound;
		this.maxBound = defaultMaxBound = maxBound;
		this.colorPalette = colorPalette;
		this.valueRetriever = valueRetriever;
	}

	public Color getColor(Integer value) {
		return getColor(value, minBound, maxBound);
	}
	
	private Color getColor(Integer value, int minBound, int maxBound) {
		if (null == value) {
			return null;
		}

		int colorCount = colorPalette.length - 1;
		
		if ( minBound == maxBound )	return colorPalette[colorCount];
		
		double index = (double)(value - minBound) / (double)(maxBound - minBound) * colorCount;
		
		if (index < 0) return colorPalette[0];		
		if (index > colorCount) return colorPalette[colorCount];
		
		Color a = colorPalette[(int)Math.floor(index)];
		Color b = colorPalette[(int)Math.ceil(index)];
		double weighting = index - Math.floor(index);

		return new Color(
			weight(a.getRed(), b.getRed(), weighting),
			weight(a.getGreen(), b.getGreen(), weighting),
			weight(a.getBlue(), b.getBlue(), weighting),
			weight(a.getAlpha(), b.getAlpha(), weighting)
		);
	}
	
	public Color[] getColorRange(int numColorsInRange) {
		
//		final double valueIncrementPerColor = ((double)defaultMaxBound-defaultMinBound)/numColorsInRange;
//		final Color[] colorRange = new Color[ numColorsInRange ];
//		for ( int i=0; i<colorRange.length; i++ )
//			colorRange[i] = getColor(defaultMinBound + (int)Math.floor(i*valueIncrementPerColor + 0.5), defaultMinBound, defaultMaxBound);
		
		final double valueIncrementPerColor = ((double)maxBound-minBound)/numColorsInRange;
		final Color[] colorRange = new Color[ numColorsInRange ];
		for ( int i=0; i<colorRange.length; i++ )
			colorRange[i] = getColor(minBound + (int)Math.floor(i*valueIncrementPerColor + 0.5), minBound, maxBound);
		
		return colorRange;
	}

	private int weight(int a, int b, double weighting) {
		return (int)(a + (b - a) * weighting);
	}

	public static String getRequiredMetricsAsSql(MonitorInfo[] monitors) {
		StringBuilder sql = new StringBuilder();
		boolean first = true;
		for (MonitorInfo monitor : monitors) {
			for (String metric : monitor.requiredMetrics) {
				if (first) {
					first = false;
				}
				else {
					sql.append(", ");
				}

				sql.append("'" + metric + "'");
			}
		}
		return sql.toString();
	}

	public String toString() {
		return name;
	}
}
