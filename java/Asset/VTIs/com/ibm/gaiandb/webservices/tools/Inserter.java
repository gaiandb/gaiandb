/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.tools;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.Orderable;

import com.ibm.gaiandb.GaianResultSetMetaData;

/**
 * The purpose of this class is to insert qualifiers into the url.
 * 
 * @author remi - IBM Hursley
 *
 */
public class Inserter {

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
	
	/**
	 * <p>
	 * Delimits a variable in an url. The variable is between two of these delimiters
	 * and the whole String (delimiter + variable name + delimiter) represents the marker.
	 * <p>
	 * 
	 */
//	private static final String URL_VARIABLE_DELIMITER = "$";
	private static final String URL_VARIABLE_DELIMITER_REGEX = "\\$";
	
	
	// -------------------------------------------------------------------------- Dynamic

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * insert the qualifiers into the url thanks to the values saved into the grsmd.
	 * @param url
	 * @param qualifiers to insert
	 * @param grsmd
	 */
	public String qualifiersIntoUrl(String url, Qualifier[][] qualifiers, GaianResultSetMetaData grsmd) {
		if (url != null && qualifiers != null && qualifiers.length > 0 &&  grsmd != null) {
			for (Qualifier qualifier : qualifiers[0]) {
				int columnIdQual = qualifier.getColumnId(); // 0-based
				String qualName = grsmd.getColumnName(columnIdQual + 1); // +1 because 1-based in grsmd
				try {
					// TODO - COMMENT THIS LINE!!!
	//				url = url.replaceAll(URL_VARIABLE_DELIMITER_REGEX + qualName + "(\\D|$)", qualifier.getOrderable().getString() + "$1");
					if (qualifier.getOperator() == Orderable.ORDER_OP_EQUALS 
							&& !qualifier.negateCompareResult()) {
						url = url.replaceAll(URL_VARIABLE_DELIMITER_REGEX + qualName, qualifier.getOrderable().getString());
					}
				} catch (StandardException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return url;
	}

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
