/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.draw;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prefuse.action.assignment.ColorAction;
import prefuse.visual.VisualItem;

/**
 * A color action that remembers which color was allocated to a particular item
 * beforehand and allocates the same color to the item on every subsequent
 * request.
 * 
 * @author Samir Talwar - stalwar@uk.ibm.com
 */
public class ReservedColorAction<T> extends ColorAction {
	
//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	/**
	 * The name of the data field to be evaluated when determining the color
	 * of the visual item.
	 */
	private String field;

	/**
	 * The color palette to be used in deciding the color of the item passed to
	 * {@link #getColor}.
	 */
	private int[] colorPalette = { 0xFF000000 };

	/**
	 * Retrieves the color palette used in deciding the colors of items. Colors
	 * are allocated on a round-robin system. The color palette defauls to one
	 * color, black.
	 * 
	 * @return The color palette, as an array of colors represented as
	 *         integers.
	 */
	public int[] getColorPalette() {
		return colorPalette;
	}

	/**
	 * Sets the color palette to be used in determining item colors.
	 * 
	 * @param colorPalette
	 *            The new color palette, as an array of colors represented as
	 *            integers.
	 */
	public void setColorPalette(int[] colorPalette) {
		this.colorPalette = colorPalette;
	}

	/** A set of named lists of values with reserved colors. */
	private static Map<String, List<?>> reservedValues = new Hashtable<String, List<?>>();

	/**
	 * Corresponds to the current list of values being used in this instance.
	 */
	private List<T> values;

	/**
	 * Creates a new color action that remembers the colors of previous
	 * requests.
	 * 
	 * @param colorGroup
	 *            The name of the list of reserved values.
	 * @param group
	 *            The prefuse data group name.
	 * @param dataField
	 *            The data field from which to retrieve the values.
	 * @param colorField
	 *            The prefuse color field name.
	 */
	public ReservedColorAction(String colorGroup,
	                           String group,
	                           String dataField,
	                           String colorField) {
		super(group, colorField);
		this.field = dataField;

		values = (List<T>)reservedValues.get(colorGroup);
		if (null == values) {
			values = new ArrayList<T>();
			reservedValues.put(colorGroup, values);
		}
	}

	/**
	 * Creates a new color action that remembers the colors of previous
	 * requests.
	 * 
	 * @param colorGroup
	 *            The name of the list of reserved values.
	 * @param group
	 *            The prefuse data group name.
	 * @param dataField
	 *            The data field from which to retrieve the values.
	 * @param colorField
	 *            The prefuse color field name.
	 * @param colorPalette
	 *            The color palette to use when choosing colors for items.
	 */
	public ReservedColorAction(String colorGroup,
	                           String group,
	                           String dataField,
	                           String colorField,
	                           int[] colorPalette) {
		this(colorGroup, group, dataField, colorField);
		setColorPalette(colorPalette);
	}

	/**
	 * Adds a value to the set of reserved values.
	 * 
	 * @param value
	 *            The value to remember.
	 * 
	 * @return True if the value was added; false if the set already contained
	 *         the value.
	 */
	private boolean add(T value) {
		if (!values.contains(value)) {
			return values.add(value);
		} else {
			return false;
		}
	}

	//VisualItem is prefuse code we don't control.
	@SuppressWarnings("unchecked")  
    public int getColor(VisualItem item) {
		T value = (T) item.get(field);
		add(value);
		return colorPalette[values.indexOf(value) % colorPalette.length];
	}
}
