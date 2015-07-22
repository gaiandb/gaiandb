/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.properties;

/**
 * 
 * This abstract class provides a pattern for a comparator of xml tags attribute values.
 * <p>
 * It also provides a list of comparator already initialised, and used for comparing 
 * attribute in the language parsed by the GenericWSPropertyParser class.
 * 
 * @see {@link com.ibm.gaiandb.webservices.parser.properties.GenericWsPropertiesParser}
 * 
 * @author remi - IBM Hursley
 *
 */
public abstract class AttributeComparator {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	
	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2011";
	
	/** List of default comparators. */
	public static final AttributeComparator[] comparators;
	
	
	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static

	private static final String SYMBOL_EQUAL = "=";
	private static final String SYMBOL_DIFFERENT = "!=";
	private static final String SYMBOL_GREATER_THAN = "+=";
	private static final String SYMBOL_SMALLER_THAN = "-=";
	
	private static final String REGEX_SYMBOL_EQUAL = "=";
	private static final String REGEX_SYMBOL_DIFFERENT = "!=";
	private static final String REGEX_SYMBOL_GREATER_THAN = "\\+=";
	private static final String REGEX_SYMBOL_SMALLER_THAN = "-=";

	// Initiation of the variable 'comparators':
	static {
		
		comparators = new AttributeComparator[4];
		
		/*
		 * Implementation of the 'equal' comparator
		 */
		comparators[0] = new AttributeComparator() {
			
			@Override
			public boolean validates(String value, String reference) {
				// If the Strings are equals
				if (value.equals(reference)) {
					return true;
				}
				
				// Otherwise:
				try {
					// Check if their value can be parsed into doubles and
					// if their double value are equal
					double dblValue = Double.parseDouble(value);
					double dblReference = Double.parseDouble(reference);
					if (dblValue == dblReference) {
						return true;
					}
				}
				catch (Exception e) { }
				
				return false;
			}
			
			@Override
			public String getSymbol() {
				return SYMBOL_EQUAL;
			}
			
			@Override
			public String getRegexSymbol() {
				return REGEX_SYMBOL_EQUAL;
			}
		};
		
		/*
		 * Implementation of the 'different' comparator
		 */
		comparators[1] = new AttributeComparator() {
			
			@Override
			public boolean validates(String value, String reference) {
				// Checks  if the strings are equals...
				if (value.equals(reference)) {
					return false;
				}
				
				try {
					// If they are not equal, compare their double values (if possible) 
					double dblValue = Double.parseDouble(value);
					double dblReference = Double.parseDouble(reference);
					
					// If both vales can be parsed into double values: 
					if (dblValue == dblReference) {
						// If they have the same value
						return false;
					}
				}
				catch (Exception e) { 
					// The Strings cannot be parsed into double values, so the values 
					// are definitely different
				}
				
				return true;
			}
			
			@Override
			public String getSymbol() {
				return SYMBOL_DIFFERENT;
			}
			
			@Override
			public String getRegexSymbol() {
				return REGEX_SYMBOL_DIFFERENT;
			}
		};
		
		/*
		 * Implementation of the 'greater than' comparator
		 */
		comparators[2] = new AttributeComparator() {
			
			@Override
			public boolean validates(String value, String reference) {
				try {
					// This comparator can only work with Strings 
					// which can be parsed into double values.
					double dblValue = Double.parseDouble(value);
					double dblReference = Double.parseDouble(reference);
					if (dblValue > dblReference) {
						return true;
					}
				}
				catch (Exception e) { }
				
				return false;
			}
			
			@Override
			public String getSymbol() {
				return SYMBOL_GREATER_THAN;
			}
			
			@Override
			public String getRegexSymbol() {
				return REGEX_SYMBOL_GREATER_THAN;
			}
		};
		
		/*
		 * Implementation of the 'smaller than' comparator
		 */
		comparators[3] = new AttributeComparator() {
			
			@Override
			public boolean validates(String value, String reference) {
				try {
					// This comparator can only work with Strings 
					// which can be parsed into double values.
					double dblValue = Double.parseDouble(value);
					double dblReference = Double.parseDouble(reference);
					if (dblValue < dblReference) {
						return true;
					}
				}
				catch (Exception e) { }
				
				return false;
			}
			
			@Override
			public String getSymbol() {
				return SYMBOL_SMALLER_THAN;
			}
			
			@Override
			public String getRegexSymbol() {
				return REGEX_SYMBOL_SMALLER_THAN;
			}
		};
	}
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	
	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * For a given value, the current comparator will compare it (with its own
	 * operation) with the reference given.
	 * <p>
	 * ex: 																<br/>
	 * - current comparator implement greater than						<br/>
	 * - reference = "2"												<br/>
	 * - value = "3"													<br/>
	 * The method should return true.
	 * 
	 * @param value
	 * 			The value to be tested.
	 * @param reference
	 * 			The reference value, the 'value' parameter has to be tested with.
	 * 
	 * @return for the implementation of a comparator #, returns the boolean
	 * expression [value # reference].
	 */
	public abstract boolean validates(String value, String reference);
	
	/** 
	 * Returns the symbol representing the comparator.
	 * @return the symbol representing the comparator.
	 */
	public abstract String getSymbol();
	
	/**
	 * Returns the regular expression representing the symbol of the comparator.
	 * @return the regular expression representing the symbol of the comparator.
	 */
	public abstract String getRegexSymbol();

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
}
