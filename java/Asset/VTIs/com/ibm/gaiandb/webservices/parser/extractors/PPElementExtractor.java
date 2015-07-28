/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.extractors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
//import org.junit.Test;

import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.webservices.parser.AttributeDefinition;
import com.ibm.gaiandb.webservices.parser.NonParsableStringException;
import com.ibm.gaiandb.webservices.parser.properties.AttributeComparator;

/**
 * <p>
 * Properties Parser Element Extractor.
 * <p>
 * Extract the different elements composing a tag pattern.
 * 
 * @author remi - IBM Hursley
 *
 */
public abstract class PPElementExtractor extends Extractor {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static

	private static final String REGEX_NAME_EXTRACTOR = "^\"?([a-zA-Z][\\w- ]*)\"?$|(^\\*$)";
	private static final String REGEX_ATTRIBUTE_EXTRACTOR = "^\"?([a-zA-Z][\\w- ]*)\"?.[^\"]?(\"([^<\">]+)\")$";//"^([\\w-]+).[^\"]?((\"([^<\">]+)\")|\\?)$";
	private static final String REGEX_POSITION_EXTRACTOR = "^\\[([0-9]+)\\]$";
	private static final String REGEX_ATTRIBUTE_TO_FIND_EXTACTOR = "^\"?([a-zA-Z][\\w- ]*)\"?=\\?$";

	// -------------------------------------------------------------------------- Dynamic

	
	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";	
	
	public static final String ATTRIBUTE_TO_FIND_MARKER = "?";
	
	/** Index of the name extractor in the extractor list. */ 
	public static final int INDEX_NAME_EXTRACTOR = 0;

	/** Index of the name position in the extractor list. */
	public static final int INDEX_POSITION_EXTRACTOR = 1;

	/** Index of the attribute extractor in the extractor list. */
	public static final int INDEX_ATTRIBUTE_EXTRACTOR = 2;

	/** Index of the attribute-to-find extractor in the extractor list. */
	public static final int INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR = 3;
	
	/** List of extractors for parsing the elements of the GenericWS tree routing language. */
	public static final PPElementExtractor[] extractors;
	
	static {
		extractors = new PPElementExtractor[4];
		
		// Extractor for a name
		// ex <tag-name>
		// the name is tag-name
		extractors[INDEX_NAME_EXTRACTOR] = new PPElementExtractor() {
			
			/**
			 * Return the parameter element if it can be extracted.
			 * @param element
			 * 			element to extract values from.
			 * @return element if it can be extracted.
			 * @throws NonParsableStringException if the value of element
			 * cannot be extracted.
			 */
			@Override
			public Object extract(String element) throws NonParsableStringException {
				if (!canExtract(element)) {
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
							"The name element given :" + element + ": cannot be extracted as the " +
									"name of a tag.");
				}
				return element.replaceAll("\"", "");
			}
			
			@Override
			public boolean canExtract(String element) {
				return element.matches(REGEX_NAME_EXTRACTOR);
			}
			
			@Override
			public PPElementType getExtractedType() {
				return PPElementType.NAME;
			}
		};
		
		// Extractor for the position in [ ] 
		// ex <tagName [2]>
		// the position is 1
		// ex <tagName [21]>		// 1-based
		// the position is 20		// 0-based
		extractors[INDEX_POSITION_EXTRACTOR] = new PPElementExtractor() {
			
			/**
			 * Return the parameter element if it can be extracted.
			 * Can be extracted if it has the format [X] with X an 
			 * integer value.
			 * @param element
			 * 			element to extract values from.
			 * @return The integer value between brackets.
			 * @throws NonParsableStringException if the value of element
			 * cannot be extracted.
			 */
			@Override
			public Object extract(String element) throws NonParsableStringException {
				
				if (!canExtract(element)) {
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
							"The name element given :" + element + ": cannot be extracted as the " +
									"position of a tag.");
				}
				
				Pattern p = Pattern.compile(REGEX_POSITION_EXTRACTOR);
				Matcher m = p.matcher(element);
		
				if (m.find()) {
					String position = m.group(1);
					try {
						Integer ret = new Integer(position);
//						ret = new Integer(ret.intValue() + 1); // read in 1-based, stored in 0-based
						return ret;
					}
					catch (Exception e) {
						// do nothing, will return null;
					}
				}
				return null;
			}
			
			/**
			 * Returns true if element matches [X]. With X an integer. False otherwise.
			 * @param element The String to check.
			 * @return true if element matches [X]. With X an integer. False otherwise.
			 */
			@Override
			public boolean canExtract(String element) {
				return element.matches(REGEX_POSITION_EXTRACTOR);
			}
			
			@Override
			public PPElementType getExtractedType() {
				return PPElementType.POSITION;
			}
		};
		
		// Extractor for the attributes and comparator and value 
		// ex <tagName attr="value"> or <tagName2 attr2!="value2"> 
		// 			or <tagName3 attr3+="value3">... 
		// the attribute name is 'attr', the comparator '=' and the value is 'value'
		// the attribute name is 'attr2', the comparator '!=' and the value is 'value2'
		// the attribute name is 'attr3', the comparator '>' and the value is 'value3'
		extractors[INDEX_ATTRIBUTE_EXTRACTOR] = new PPElementExtractor() {
			
			/**
			 * <p>
			 * Returns a {@link AtributeDefinition} object containing the
			 * attribute name, the comparator, and the value affected to 
			 * the attribute.
			 * <p>
			 * Can be extracted if it has the format attr="value"
			 * or attr!="value" or attr+="value"...
			 * @param element
			 * 			element to extract values from.
			 * @return A AtributeDefinition object defined by the element.
			 * @throws NonParsableStringException if the value of element
			 * cannot be extracted.
			 */
			@Override
			public Object extract(String element) throws NonParsableStringException {
				
				if (!canExtract(element)) {
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
							"The name element given :" + element + ": cannot be extracted as the " +
									"attribute definition of a tag.");
				}
				
				return extractAttributeDefinition(element);
			}
			
			@Override
			public boolean canExtract(String element) {
				for (AttributeComparator comp : AttributeComparator.comparators) {
					if (element.matches(getRegexAttributeDeclaration(comp))) {
						return true;
					}
				}
				return false;
//				return element.matches(REGEX_ATTRIBUTE_EXTRACTOR);
			}
			
			@Override
			public PPElementType getExtractedType() {
				return PPElementType.ATTRIBUTE;
			}
		};
		
		// Extractor for the attribute to return 
		// ex <tagName attr="value" attr2=?>
		// the attribute to find is "attr2"
		extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR] = new PPElementExtractor() {
			
			@Override
			public Object extract(String element) throws NonParsableStringException {
				
				if (!canExtract(element)) {
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_ELEMENT_FORMAT, 
							"The name element given :" + element + ": cannot be extracted as an " +
									"attribute to find of a tag.");
				}
				
				Pattern p = Pattern.compile(REGEX_ATTRIBUTE_TO_FIND_EXTACTOR);
				Matcher m = p.matcher(element);
		
				if (m.find()) {
					String attributeName = m.group(1);
					try {
						return attributeName;
					}
					catch (Exception e) {
						// do nothing, will return null;
					}
				}
				return null;
			}
			
			@Override
			public boolean canExtract(String element) {
				return element.matches(REGEX_ATTRIBUTE_TO_FIND_EXTACTOR);
			}
			
			@Override
			public PPElementType getExtractedType() {
				return PPElementType.ATTRIBUTE_TO_FIND;
			}
		};
		
	}
			
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
	
	public abstract PPElementType getExtractedType();

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	/**
	 * Parses the parameter token into an attribute definition containing the 
	 * name of the attribute, it reference value, and the comparator used for 
	 * the current comparison. 
	 * 
	 * @param token
	 * 			The element to parse.
	 * 
	 * @return AttributeDefinition containing the name of the attribute, the 
	 * value affected to this one, and the comparator defining the correspondence 
	 * between the attribute and its value.
	 *  
	 * @throws NonParsableStringException if the string given cannot be 
	 * parsed into an object. Pair<String, Pair<AttributeComparator, String>>
	 */
	private static AttributeDefinition extractAttributeDefinition(String token) 
				throws NonParsableStringException {
		
		// Gets through all the comparators to know which one is used
		for (AttributeComparator comparator : AttributeComparator.comparators) {
			
			String attribute;
			String value;
			
			String REGEX = PPElementExtractor.getRegexAttributeDeclaration(comparator);
			
			Pattern p = Pattern.compile(REGEX);
			Matcher m = p.matcher(token);
	
			if (m.find()) {
				attribute = m.group(2);
				value = m.group(3);
				
				// if attribute and value are null, that mans that the regex has matched 
				// the pattern attribute to find
				if (attribute == null && value == null) {
					attribute = m.group(5);
					value = m.group(6);
	
					if (attribute == null && value == null) {
						throw new NonParsableStringException(
								GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_FORMAT_ATTRIBUTE_DEFINITON,
								"The format of the attribute declaration " + 
								token + " is not valid.");
					}
				}
				
			    return new AttributeDefinition(attribute, comparator,value);
//			    Pair<String, Pair<AttributeComparator, String>>(
//			    			attribute, 
//			    			new Pair<AttributeComparator, String>(comparator, value)); 
			}
			// else { -- It is not the right comparator -- }
		}
		
		// If none of the known comparators is used
		throw new NonParsableStringException(
						GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_WRONG_FORMAT_ATTRIBUTE_DEFINITON,
						"The format of the attribute declaration " + 
						token + " is not valid.");
	}
	
	/**
	 * Generates a regular expression for matching an attribute definition with 
	 * a given comparator.
	 * @param comp 
	 * 			The comparator to use in the regular expression generation.
	 */
	private static String getRegexAttributeDeclaration(AttributeComparator comp) {
		return "(^([\\w]+)" + comp.getRegexSymbol() + "\"(.*)\"$)";// +
			//"|(^([\\w]+)" + comp.getRegexSymbol() + "(\\?)$)";
	}


	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TESTS
	
	public static void testFormatSpecifierInputStream () {
		
		System.out.println("Test for PPElementExtractors");
		
		// --------------------------------------------------------
		// -- position extractor
		Assert.assertTrue("'[23]'" + " should match for the position", "[23]".matches(REGEX_POSITION_EXTRACTOR));
		Assert.assertFalse("'[a]'" + " should not match for the position", "[a]".matches(REGEX_POSITION_EXTRACTOR));
		Assert.assertFalse("'23'" + " should not match for the position", "23".matches(REGEX_POSITION_EXTRACTOR));

		// -- name extractor
		Assert.assertTrue("'name_is-here'" + " should match for the name", "name_is-here".matches(REGEX_NAME_EXTRACTOR));
		Assert.assertTrue("'*'" + " should match for the name", "*".matches(REGEX_NAME_EXTRACTOR));
		Assert.assertFalse("'**'" + " should not match for the name", "**".matches(REGEX_NAME_EXTRACTOR));
		Assert.assertFalse("'*test'" + " should not match for the name", "*test".matches(REGEX_NAME_EXTRACTOR));
		Assert.assertFalse("'2tr'" + " should not match for the name", "2tr".matches(REGEX_NAME_EXTRACTOR));
		
		// -- attribute extractor
		Assert.assertTrue("'name=\"is-here\"'" + " should match for the attribute", "name=\"is-here\"".matches(REGEX_ATTRIBUTE_EXTRACTOR));
		Assert.assertTrue("'naMe!=\"is-he_re\"'" + " should match for the attribute", "naMe!=\"is-he_re\"".matches(REGEX_ATTRIBUTE_EXTRACTOR));
		Assert.assertTrue("'a!=\"is-he_re\"'" + " should match for the attribute", "a!=\"is-he_re\"".matches(REGEX_ATTRIBUTE_EXTRACTOR));
		Assert.assertFalse("'name=\"is-here'" + " should not match for the attribute", "name=\"is-here".matches(REGEX_ATTRIBUTE_EXTRACTOR));
		Assert.assertFalse("'name=\"\"is-here\"'" + " should not match for the attribute", "name=\"\"is-here\"".matches(REGEX_ATTRIBUTE_EXTRACTOR));
		Assert.assertFalse("'name=\"is-h\"ere\"'" + " should not match for the attribute", "name=\"is-h\"ere\"".matches(REGEX_ATTRIBUTE_EXTRACTOR));
		Assert.assertFalse("'name=?'" + " should not match for the attribute", "name=?".matches(REGEX_ATTRIBUTE_EXTRACTOR));

		// -- attribute-to-find extractor
		Assert.assertTrue("'name=?'" + " should match for the attribute to find", "name=?".matches(REGEX_ATTRIBUTE_TO_FIND_EXTACTOR));
		Assert.assertTrue("'a=?'" + " should match for the attribute to find", "a=?".matches(REGEX_ATTRIBUTE_TO_FIND_EXTACTOR));
		Assert.assertFalse("'name=\"is-here\"'" + " should not match for the attribute to find", "name=\"is-here\"".matches(REGEX_ATTRIBUTE_TO_FIND_EXTACTOR));

		// --------------------------------------------------------
		// -- name extractor
		try {
			String toExtract = "myAttribute=\"value\"";
			extractors[INDEX_NAME_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the name extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "*&^%$";
			extractors[INDEX_NAME_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the name extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "24356";
			extractors[INDEX_NAME_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the name extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		
		// -- attribute extractor
		try {
			String toExtract = "name=\"is-here";
			extractors[INDEX_ATTRIBUTE_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "a=\"is-here\"";
			extractors[INDEX_ATTRIBUTE_EXTRACTOR].extract(toExtract);
		} catch (NonParsableStringException e) {
			Assert.fail("'a=\"is-here\"' shouldn't match the attribute extractor.");
		}
		try {
			String toExtract = "*&^%$";
			extractors[INDEX_ATTRIBUTE_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "name";
			extractors[INDEX_ATTRIBUTE_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "*";
			extractors[INDEX_ATTRIBUTE_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "[1]";
			extractors[INDEX_ATTRIBUTE_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		
		// -- position extractor
		try {
			String toExtract = "[1]";
			extractors[INDEX_POSITION_EXTRACTOR].extract(toExtract);
		} catch (NonParsableStringException e) {
			Assert.fail("[1] should match the position extractor.");
		}
		try {
			String toExtract = "*&^%$";
			extractors[INDEX_POSITION_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the position extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "[a]";
			extractors[INDEX_POSITION_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the position extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "name";
			extractors[INDEX_POSITION_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the position extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "attr=\"val\"";
			extractors[INDEX_POSITION_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the position extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "att=?";
			extractors[INDEX_POSITION_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the position extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		
		// -- attribute-to-find extractor
		try {
			String toExtract = "att=?";
			extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR].extract(toExtract);
		} catch (NonParsableStringException e) {
			Assert.fail("'att=?' should match the attribute-to-find extractor.");
		}
		try {
			String toExtract = "a=?";
			extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR].extract(toExtract);
		} catch (NonParsableStringException e) {
			Assert.fail("'a=?' should match the attribute-to-find extractor.");
		}
		try {
			String toExtract = "*&=%$";
			extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute-to-find extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "name";
			extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute-to-find extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "attr=\"val\"";
			extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute-to-find extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
		try {
			String toExtract = "[4]";
			extractors[INDEX_ATTRIBUTE_TO_FIND_EXTRACTOR].extract(toExtract);
			Assert.fail(toExtract + " shouldn't match the attribute-to-find extractor.");
		} catch (NonParsableStringException e) {
			// Yay! Gatcha!
		}
	}
}
