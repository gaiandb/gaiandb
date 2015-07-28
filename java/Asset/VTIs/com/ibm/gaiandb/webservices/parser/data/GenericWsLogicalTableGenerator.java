/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.data;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ibm.db2j.GenericWS;
import com.ibm.gaiandb.webservices.parser.properties.GenericWsPropertiesPrinter;
import com.ibm.gaiandb.webservices.patternmatcher.TagMatcher;
//import com.ibm.gaiandb.webservices.patternmatcher.TagPattern;
import com.ibm.gaiandb.webservices.scanner.WsDataFormat;

/**
 * The purpose of this class is to provide a tool generating the properties for 
 * GenericWS once these ones have been generated when calling the super constructor.
 * 
 * @author remi - IBM Hursley
 *
 */
public class GenericWsLogicalTableGenerator extends LogicalTableGenerator {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";

	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	private Map<String, String> properties;
	private String patternForObjectToReturn = null;

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * <p>
	 * Will scan a stream in order to:											<br/>
	 * - define what the column names of the logical table designing the 
	 * received data are														<br/>
	 * - define the TagMatchers allowing GenericWS to display the data into a VTI.
	 * <p>
	 */
	public GenericWsLogicalTableGenerator(InputStream is, WsDataFormat format) {
		super(is,format);
		this.properties = new LinkedHashMap<String, String>();
		GenericWsPropertiesPrinter printer = new GenericWsPropertiesPrinter();
		this.getGeneratedMatcherManager().setCommonRoot();
		this.patternForObjectToReturn = printer.printTagMatcher(this.getGeneratedMatcherManager());
		
	}

	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Adds the property: GenerciWS.prefix.propertyName=propertyValue to the 
	 * properties defined by the object. 
	 */
	public void addProperty(String prefix, String propertyName, String propertyValue) {
		String propertiesPrefix = GenericWS.class.getSimpleName() + "." + prefix + ".";
		
		this.properties.put(propertiesPrefix + propertyName, propertyValue);
	}
	
	/**
	 * <p>
	 * Defines the following properties of the VTI. :
	 * <ul>
	 * 	<li>[ltName]_DEF</li>
	 * 	<li>[ltName]_DS0_ARGS</li>
	 * 	<li>[ltName]_DS0_VTI</li>
	 * 	<li>GenericWS.[prefix].schema</li>
	 * 	<li>GenericWS.[prefix].C#.XML_LOCATE_EXPRESSION</li>
	 * 	<li>GenericWS.[prefix].sendWhenClosing</li>
	 * </ul> 
	 */
	public void addGenericWsProperties(String ltName, String prefix) {
		
		String propertiesPrefix = GenericWS.class.getSimpleName() + "." + prefix + ".";
		
		// ----------------------------------------------------------------------------
		// Sets the property *** LTNAME_DEF=[logical table structure] ***
		String [] columnNames = this.getLTColumnNames();
		StringBuilder property = new StringBuilder(ltName).append("_DEF");
		StringBuilder value = new StringBuilder("");
		
		for (String columnName : columnNames) {
			// all the columns are defined as VARCHAR(255)
			value.append(columnName).append(" VARCHAR(255),");
		}
		// removes the last ","
		if (value.length() > 0) {
			value.deleteCharAt(value.length()-1);
		}
		
		final String ltStructure = value.toString();
		
		this.properties.put(property.toString(), ltStructure);

		// ----------------------------------------------------------------------------
		// Sets the property *** LTNAME_DS0_ARGS=prefix,100 ***
		property = new StringBuilder(ltName).append("_DS0_ARGS");
		value = new StringBuilder(prefix);

		this.properties.put(property.toString(), value.toString());

		// ----------------------------------------------------------------------------
		// Sets the property *** GenericWS.LTNAME.schema=[physical table structure] ***
		property = new StringBuilder(propertiesPrefix).append(GenericWS.PROP_SCHEMA);
		// value stays the same

		this.properties.put(property.toString(), ltStructure);

		// ----------------------------------------------------------------------------
		// Sets the property *** LTNAME_DS0_VTI=com.ibm.db2j.GenericWS ***
		property = new StringBuilder(ltName).append("_DS0_VTI");
		value = new StringBuilder(GenericWS.class.getName());

		this.properties.put(property.toString(), value.toString());

		// ----------------------------------------------------------------------------
		// Sets the property *** GenericWS.LTNAME.C#.XML_LOCATE_EXPRESSION=[autoFound] ***
		ArrayList<TagMatcher> matchers = this.getGeneratedMatcherManager().getMatchers();
		int indexCurrentPattern = 1; // column numbers are 1 based
		
		for (TagMatcher matcher : matchers) {
			GenericWsPropertiesPrinter printer = new GenericWsPropertiesPrinter();
			String currentLinePattern = printer.printTagMatcher(matcher);
			property = new StringBuilder(propertiesPrefix)
										.append(GenericWS.PROP_COLUMN_TAG_PREFIX)
										.append(indexCurrentPattern)
										.append(GenericWS.PROP_COLUMN_TAG_SUFFIX);
			
			this.properties.put(property.toString(), currentLinePattern);
			
			indexCurrentPattern++;
		}

		// ----------------------------------------------------------------------------
		// Sets the property *** GenericWS.LTNAME.sendWhenClosing=[autoFound] ***
		property = new StringBuilder(propertiesPrefix).append(GenericWS.PROP_SEND_WHEN_CLOSING);
		
		this.properties.put(property.toString(), this.patternForObjectToReturn);
		
	}
	
	/**
	 * Returns the properties set within the class.
	 * @return the properties set within the class.
	 */
	public Map<String, String> getProperties() {
		return properties;
	}
	

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
