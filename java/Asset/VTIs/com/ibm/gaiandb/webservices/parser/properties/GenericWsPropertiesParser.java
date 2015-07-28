/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.parser.properties;
import java.util.ArrayList;

import com.ibm.gaiandb.Util;
import com.ibm.gaiandb.diags.GDBMessages;
import com.ibm.gaiandb.utils.Pair;
import com.ibm.gaiandb.webservices.XmlElement;
import com.ibm.gaiandb.webservices.parser.AttributeDefinition;
import com.ibm.gaiandb.webservices.parser.NonParsableStringException;
import com.ibm.gaiandb.webservices.parser.extractors.PPElementExtractor;
import com.ibm.gaiandb.webservices.parser.extractors.PPElementType;
import com.ibm.gaiandb.webservices.patternmatcher.TagPattern;
import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * <p>
 * The purpose of this class is to parse a configuration file, written in 
 * a language defined by IBM, in order to generate a list of 
 * {@link com.ibm.gaiandb.webservices.patternmatcher.TagPattern} objects.
 * <p>
 * The language is defined as a list of tags, which will be used as a pattern
 * for filling a GAIANDB VTI. 
 * <p>
 * If the user wants a pattern finding the third tag 'td' in whatever tag in
 * a tag 'table' which id is greater than "123" and which class is "display", in 
 * the body of a html document, the language line would be:				<br/>  
 * &#60;html&#62;&#60;body&#62;&#60;table id+="123" class="display"&#62;&#60;*&#62;&#60;tr [3]&#62;
 * <br/>
 * If the user wants to find the id of the tags div, in the body of a html file, 
 * the line would be:													<br/>
 * &#60;html&#62;&#60;body&#62;&#60;div id=?&#62;
 * 
 * @author remi - IBM Hursley
 *
 */
public class GenericWsPropertiesParser {

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
	
//	private static final Logger logger = new Logger( "GenericWS", 20 );

	/** 
	 * Regex defining a general idea of a tag (just checks that there is not 
	 * '<' or '>' in the current scanned tag. 
	 */
	private static final String REGEX_LINE = "^(\\s*<[^<>]*>)*\\s*$";

	/** Regex defining the end of a tag. */
	private static final String REGEX_SPLIT_TAGS = "(>\\s*)";
	
	/** Regex defining the beginning of a tag. */
	private static final String REGEX_REMOVE_START_TAG = "\\s*<";
	
//	/** 
//	 * Defines the value of an attribute, found in the config file, when the 
//	 * purpose of the pattern is to find an attribute value.
//	 * <p>
//	 * If the user wants to find the id of the tags div, in the body of a html file, 
//	 * the line would be:													<br/>
//	 * &#60;html&#62;&#60;body&#62;&#60;div id=?&#62;
//	 */
//	private static final String ATTRIBUTE_TO_FIND = "?";
	
	
	// -------------------------------------------------------------------------- Dynamic
	
	/** The String which will be parsed into a list of 
	 * {@link com.ibm.gaiandb.webservices.patternmatcher.TagPattern}
	 * objects. 
	 */
	private String lineToCompile = "";
	
	/** 
	 * The object which is looked for by a {@link com.ibm.gaiandb.webservices.patternmatcher.TagMatcher}
	 * object.
	 * <p>
	 * When the {@link com.ibm.gaiandb.webservices.parser.properties.GenericWsPropertiesParser#kindOfRequest}
	 * is of type {@link com.ibm.gaiandb.webservices.XmlElement#VALUE}, this 
	 * attribute is null, when it is of type 
	 * {@link com.ibm.gaiandb.webservices.XmlElement#TAG_ATTIBUTE}, this attribute 
	 * is a Pair<String, Integer> object, with the name of an attribute as a first 
	 * value, and the index of the tag the user wants the attribute of as the 
	 * second element.
	 */
	private Object returnObject = null;
	
	/** 
	 * Defines if the request of the line that the object has to parse is 
	 * looking for a value, tag attribute... 
	 */
	private XmlElement kindOfRequest;
	
	
	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * Generates a GenericWsPropertiesParser object.
	 * @param line
	 * 			The line to parse for generating the object.
	 */
	public GenericWsPropertiesParser(String line) {
		if (line != null) { 
			this.lineToCompile = line; 
			}
		this.kindOfRequest = XmlElement.UNDEFINED;
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Parses the line defining the object into a list of TagPattern objects.
	 * @return The TagPattern list specified by the String defining the current
	 * object.
	 */
	public ArrayList<TagPattern> parseTags() throws NonParsableStringException {
		
		this.returnObject = null;
		
		ArrayList<TagPattern> ret = new ArrayList<TagPattern>();
		// Checks that the line has the right format
		if (this.lineToCompile.matches(REGEX_LINE)) {
			
			// Splits the line with the character of end of tag '>'
			String[] tags = this.lineToCompile.split(REGEX_SPLIT_TAGS);
			int index = 0;
			
			// for each content of tag:
			for (String tag : tags) {
				
				TagPattern tagFound;
				
				// remove the beginning of the tag: '<'
				// and parses the tag
				String tagContent = tag.replaceFirst(REGEX_REMOVE_START_TAG, "");
				tagFound = this.lineToTag(tagContent, index++);
				
				// Saves the new parsed TagPattern
				ret.add(tagFound);
			}
			
			// Once all the parsing is done, if the kindOfRequest is still UNDEFINED, it
			// is considered as looking for the value of a tag by default
			if (this.kindOfRequest == XmlElement.UNDEFINED) {
				this.kindOfRequest = XmlElement.VALUE;
			}
		}
		
		return ret;
	}
	
	/**
	 * Returns the XmlElement defining the type of the sent request. 
	 * @return the XmlElement defining the type of the sent request.
	 */
	public XmlElement getRequestType() {
		return this.kindOfRequest;
	}
	
	/**
	 * Returns the Object defining the qualifiers of the sent request. 
	 * <p>
	 * Depending on the value of the method getRequestType(), the 
	 * returned type will be:
	 * 	<table border="1">
	 * 		<tr>
	 * 			<th>value from getRequestType()</th>
	 * 			<th>type returned by getRequestQualifiers()</th>
	 * 			<th>Meaning</th>
	 * 		</tr>
	 * 		<tr>
	 * 			<td>VALUE</td>
	 * 			<td>null</td>
	 * 			<td></td>
	 * 		</tr>
	 * 		<tr>
	 * 			<td>TAG_ATTIBUTE</td>
	 * 			<td>Pair&#60;String, Integer&#62;</td>
	 * 			<td>String: the name of the attribute to return<br/>
	 * 				Integer: the depth of the tag which attribute needs to be found</td>
	 * 		</tr>
	 * 		<tr>
	 * 			<td>ERROR_TAG</td>
	 * 			<td>null</td>
	 * 			<td></td>
	 * 		</tr>
	 * 		<tr>
	 * 			<td>UNDEFINED</td>
	 * 			<td>null</td>
	 * 			<td></td>
	 * 		</tr>
	 * </table>
	 * 
	 * @return the Object defining the qualifiers of the sent request.
	 */
	public Object getRequestQualifiers() {
		return this.returnObject;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * Parses the content of a tag (in a String) into a 
	 * TagPattern object.
	 * 
	 * @param tagContent
	 * 			The String to parse into a tag.
	 * A NullPointerException will be thrown if 
	 * this parameter is null.
	 * @param tagId
	 * 			eeerh?
	 * 
	 * @return a TagPattern defined in the tagContent. 
	 * 
	 * @throws NonParsableStringException
	 * 			If the String cannot be parsed into a tag.
	 */
	private TagPattern lineToTag(String tagContent, int tagId) 
										throws NonParsableStringException {
		
		// The name of the tag currently read
		String name = null;
		
		// List of attribute definitions
		// - element.getFirst() is the attribute's name
		// - element.getSecond().getFirst() is the operation defining 
		// the attribute declaration 
		// - element.getSecond().getSecond() is the attribute's value
		ArrayList<AttributeDefinition> attributes = 
					new ArrayList<AttributeDefinition>();
		
		// index position of the tag in its parent's list of children 
		int position = Tag.NO_INDEX_POSITION;
		
		// The TagPattern to return
		TagPattern retTag = null;
		
		// Gets the different declarations in the tagContent String
		String[] tagObjects = Util.splitByTrimmedDelimiterNonNestedInCurvedBracketsOrDoubleQuotes(tagContent, ' ');
		
		// For each declaration
		for (String contentPart : tagObjects) {
			
			// --- Defines which Properties Parser element type it is 
			// (name of the tag, attribute declaration, position...)
			PPElementType type = PPElementType.NONE;
			PPElementExtractor extractor = null;
			
			for (PPElementExtractor extr : PPElementExtractor.extractors) {
				if (extr.canExtract(contentPart)) {
					type = extr.getExtractedType();
					extractor = extr;
					break;
				}
			}
			
			// --- Extracts the data considering the received type of 
			// Properties Parser element
			switch (type) {
			case NAME:
				
				if (name != null) {
					this.kindOfRequest = XmlElement.ERROR_TAG;
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_NAME_DEFINITION,
							"The tag which content is \"" + tagContent +
							"\" has at least two name definitions.");
				}
				//else: name not defined twice
				name = (String)extractor.extract(contentPart); 
				break;
				
			case ATTRIBUTE:
				// Extract the attribute and its properties
				// - element.getFirst() is the attribute's name
				// - element.getSecond().getFirst() is the operation defining 
				// the attribute declaration 
				// - element.getSecond().getSecond() is the attribute's value
				AttributeDefinition att_val = 
									(AttributeDefinition)extractor.extract(contentPart);
				attributes.add(att_val);
				break; 
			
			case ATTRIBUTE_TO_FIND:
				// Checks if the kindOfRequest has already been set up at TAG_ATTRIBUTE
				if (this.kindOfRequest != XmlElement.TAG_ATTIBUTE) {
					this.kindOfRequest = XmlElement.TAG_ATTIBUTE;
					
					// Gets the name of the attribute to be found
					String AttributeName = (String)extractor.extract(contentPart);
					
					this.returnObject = new Pair<String, Integer>(AttributeName, new Integer(tagId));
				}
				// If the kindOfRequest has already been set up at TAG_ATTRIBUTE, 
				// that means there is already an attribute to be found
				else {
					// an att=? already exists
					this.kindOfRequest = XmlElement.ERROR_TAG;
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_ATTRIBUTE_TO_LOOK_FOR_DEFINITION,
							"There are at least two attributes to " +
							"find ( attr1=? attr2=? ) in this GenericWS property.");
				}
				break;
				
			case POSITION:
				
				if (position != Tag.NO_INDEX_POSITION) {
					this.kindOfRequest = XmlElement.ERROR_TAG;
					throw new NonParsableStringException(
							GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_DOUBLE_POSITION_DEFINITION,
							"The tag which content is \"" + tagContent +
							"\" has at least two position definitions ( [pos1] [pos2] ).");
				}
				
				Integer pos = (Integer)extractor.extract(contentPart);
				if (pos != null) {
					position = pos.intValue() - 1; // -1 because the internal 
									// representation for the position is 
									// 0-based and the one for the user 
									// interface is 1-based
				} 
				// else stays null

			case NONE:
			default:
				// The application shouldn't be able to arrive here
				break;
			}
		}
		
		if (name == null) {
			this.kindOfRequest = XmlElement.ERROR_TAG;
			throw new NonParsableStringException(
					GDBMessages.DSWRAPPER_GENERICWS_PROPERTY_PARSING_NAME_NOT_DEFINED,
					"The tag does not have any names.");
		}
		
		// --- Generates the new tagPattern...
		retTag = new TagPattern(name, position);
		
		// ... and complete it
		for (AttributeDefinition attribute : attributes) {
			retTag.addAttribute(attribute);
		}
		
		return retTag;
	}
	
	
}
