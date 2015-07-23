/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.util.logging.Logger;

import prefuse.Constants;
import prefuse.action.assignment.ColorAction;
import prefuse.util.ColorLib;
import prefuse.visual.VisualItem;

/**
 * <p>Using the color palette provided as waypoints, returns a color along a
 * gradient for each {@link prefuse.visual.VisualItem VisualItem} passed to
 * {@link #getColor}. Only works for numerical (quantitative) data types.</p>
 * 
 * <p>Based on prefuse's
 * {@link prefuse.action.assignment.DataColorAction DataColorAction}, but
 * allows us to have values between the ones provided in the color palette.</p>
 */
public class GradientColorAction extends ColorAction {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	/**
	 * The name of the data field to be evaluated when determining the color
	 * of the visual item.
	 */
	private String dataFieldName;

	/** The type of the data field to be evaluated. */
	private int dataFieldType;

	/**
	 * The color palette to use in creating the gradient. Colors are assumed to
	 * be evenly spaced across the gradient. Defaults to a gradient from white
	 * to black.
	 */
	private int[] colorPalette = { 0xFFFFFFFF, 0xFF000000 };

	/**
	 * Retrieves the color palette currently being used in calculating the
	 * gradient.
	 * 
	 * @return The gradient color palette, as an array of colors represented as
	 *         integers.
	 */
	public int[] getColorPalette() {
		return colorPalette;
	}

	/**
	 * Sets the color palette to be used to calculate the new gradient.
	 * 
	 * @param colorPalette
	 *            The new gradient color palette, as an array of colors
	 *            represented as integers.
	 */
	public void setColorPalette(int[] colorPalette) {
		this.colorPalette = colorPalette;
	}

	/** The minimum value of the data field. */
	private double min = 0;

	/** The maximum value of the data field. */
	private double max = 1;

	/**
	 * Retrieves the value currently believed to be the minimum possible value
	 * stored in the data field. Used to determine the scale of the gradient.
	 * 
	 * @return The minimum value of the data field.
	 */
	public double getMinValue() { return min; }

	/**
	 * Sets a new minimum possible value for the data field.
	 * 
	 * @param minValue
	 *            The new minimum value of the data field.
	 */
	public void setMinValue(double minValue) { min = minValue; }

	/**
	 * Retrieves the value currently believed to be the maximum possible value
	 * stored in the data field. Used to determine the scale of the gradient.
	 * 
	 * @return The maximum value of the data field.
	 */
	public double getMaxValue() { return max; }

	/**
	 * Sets a new maximum possible value for the data field.
	 * 
	 * @param maxValue
	 *            The new maximum value of the data field.
	 */
	public void setMaxValue(double maxValue) { max = maxValue; }

	/**
	 * Creates a new prefuse color action which returns colors according to a
	 * gradient.
	 * 
	 * @param group The prefuse data group.
	 * @param dataField The data field from which to retrieve the values.
	 * @param dataType The type of the data field.
	 * @param colorField The prefuse color field.
	 */
	public GradientColorAction(String group,
	                           String dataFieldName,
	                           int dataFieldType,
	                           String colorField) {
		super(group, colorField);
		this.dataFieldName = dataFieldName;
		this.dataFieldType = dataFieldType;
	}

	/**
	 * Creates a new prefuse color action which returns colors according to a
	 * gradient.
	 * 
	 * @param group The prefuse data group.
	 * @param dataField The data field from which to retrieve the values.
	 * @param dataType The type of the data field.
	 * @param colorField The prefuse color field.
	 * @param colorPalette The color palette to base the gradient on.
	 */
	public GradientColorAction(String group,
	                           String dataField,
	                           int dataType,
	                           String colorField,
	                           int[] colorPalette) {
		this(group, dataField, dataType, colorField);
		setColorPalette(colorPalette);
	}

	public int getColor(VisualItem item) {
		Object o = lookup(item);
		if (o != null) {
			if (o instanceof ColorAction) {
				return ((ColorAction)o).getColor(item);
			}
			else if (o instanceof Integer) {
				return ((Integer)o).intValue();
			}
			else {
				Logger.getLogger(this.getClass().getName())
					.warning("Unrecognized Object from predicate chain.");
			}
		}

		switch (dataFieldType) {
			case Constants.NUMERICAL:
				double value = item.getDouble(dataFieldName);
				int colors = colorPalette.length;
				double color = (value - min) / (max - min) * (colors - 1);
				return ColorLib.interp(
					colorPalette[(int)Math.floor(color)],
					colorPalette[(int)Math.ceil(color)],
					color - Math.floor(color));
			default:
				return colorPalette[0];
		}
	}
}
