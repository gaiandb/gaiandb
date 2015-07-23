/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.util.HashMap;
import java.util.Map;

import java.awt.Color;

public class ColorAllocator<T> {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	// TODO: Add more colors.
	public static final Color[] COLORS = {
		new Color(0x0000CC),
		new Color(0xCC0000),
		new Color(0x00CC00)
	};

	private Map<T, Color> allocatedColors = new HashMap<T, Color>();
	private Map<Color, Integer> count = new HashMap<Color, Integer>(COLORS.length);

	public ColorAllocator() {
		for (Color color : COLORS) {
			count.put(color, 0);
		}
	}

	public Color get(T obj) {
		Color color = allocatedColors.get(obj);
		if (null == color) {
			int minCount = Integer.MAX_VALUE;
			for (Color currentColor : COLORS) {
				int currentCount = count.get(currentColor);
				if (currentCount < minCount) {
					color = currentColor;
					minCount = currentCount;
				}
			}

			allocatedColors.put(obj, color);
			count.put(color, count.get(color) + 1);
		}
		return color;
	}

	public void deallocate(T obj) {
		Color color = allocatedColors.remove(obj);
		if (null != color) {
			count.put(color, count.get(color) - 1);
		}
	}
}
