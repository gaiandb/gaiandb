/*
 * (C) Copyright IBM Corp. 2013
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */
package com.ibm.gaiandb.webservices.patternmatcher;

import java.util.ArrayList;

import com.ibm.gaiandb.webservices.scanner.Tag;

/**
 * <p>
 * This class provides methods to manage TagMatcher objects. In this object, 
 * two kind of methods can be distinguished: 									<br/>
 * - the configuration methods (getResult(), addMatcher(...), reinitializeResults(...))													<br/>
 * - The TagMatcher-objects-managing methods (getIn(...), getOut(...), getValue(...))
 * <p>
 * Once the object has been set, the managing methods are going through all the
 * TagMatcher objects and call a specific method on all of them. Each TagMatcher will 
 * then have their own behaviour.
 * 
 * @author remi - IBM Hursley
 * 
 * @see {@link TagMatcher}
 *
 */
public class MatcherManager extends ValueMatcher {

	// ----------------------------------------------------------------------------------
	// ----------------------------------------------------------------------- ATTRIBUTES

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static

	// Use PROPRIETARY notice if class contains a main() method, otherwise use
	// COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2013";
	
	/** Value returned when the matcher has a result to return. */
	public static final String GOT_RESULT = "GOT RESULT";
	

	// -------------------------------------------------------------------------- Dynamic

	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/** All the TagMatchers that the object manages. */
	private ArrayList<TagMatcher> matchers;
	
	/** 
	 * The number of TagMatcher objects the object will manage. This value is defined 
	 * when the object is created since the attribute result is a static array and will 
	 * be created in the object constructor. This avoids having to redefine the array 
	 * each time a TagMatcher is added to the object. 
	 */
	protected int nbMatchers = 0;
	
	/**
	 * The record the TagMatcher objects are filling. each TagMatcher will fill its own 
	 * cell in the array. The first TagMatcher fills the cell 0, the second TagMtcher 
	 * the cell 1... 
	 */
	protected String[] results;
	

	// ----------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------- TOOLS

	// ----------------------------------------------------------------------------------
	// -------------------------------------------------------------------------- METHODS

	// ===================================================================== Constructors
	// --------------------------------------------------------------------------- Public
	
	/**
	 * Creates a MatcherManager object.
	 * @param nbMatchersManagedToInsert
	 * 			The number of matchers which will be inserted into the object.
	 */
	public MatcherManager(int nbMatchersManagedToInsert) {
		this(nbMatchersManagedToInsert, null);
	}
	
	public MatcherManager(int nbMatchersManagedToInsert, ArrayList<TagPattern> patterns) {
		super(patterns);
		this.matchers = new ArrayList<TagMatcher>(); // Once filled, this one won't change
//		this.visitedTags = new ArrayList<Tag>(); // This array will be filled and unfilled 
					// as the scanner will go through a document
		this.nbMatchers = nbMatchersManagedToInsert;
		this.results = new String[this.nbMatchers];
	}
	
	
	// -------------------------------------------------------------------------- Private

	// =========================================================================== Public
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	/**
	 * TODO
	 */
	public ArrayList<TagMatcher> getMatchers() {
		return this.matchers;
	}
	
	/**
	 * Adds a TagMatcher object to the object's list of TagMatcher.
	 * @param matcher
	 * 			The TagMatcher to add.
	 */
	public void addMatcher(TagMatcher matcher) {
		this.matchers.add(matcher);
	}
	
	/**
	 * Sets the matchers.
	 * @param matchers
	 * 			The matchers to define as the 
	 */
	public void setMatchers(ArrayList<TagMatcher> matchers) {
		this.matchers = matchers;
	}
	
	/**
	 * Returns the set of results which have been read while going though the tags.
	 * @return the set of results which have been read while going though the tags.
	 */
	public String[] getResult() {
		return this.results;
	}
	
	/**
	 * Resets the records. Sets all the values at null.
	 */
	public void reinitializeResults() {
		this.results = new String[this.nbMatchers];
	}
	
	/**
	 * Go through all the TagMatcher added in the object and defines the common 
	 * root of them all. 
	 */
	public void setCommonRoot() {
		for (TagMatcher matcher : this.matchers) {
			this.patternToMatch = this.defineCommonRoot(
												this.patternToMatch, 
												matcher.patternToMatch);
		}
	}


	/**
	 * Goes through all the TagMatcher objects of the current object and call 
	 * their method getIn(...)
	 *   
	 * @param tag
	 * 			The start element that the object using the MatcherManager 
	 * is scanning.
	 * 
	 * @return null
	 */
	@Override
	public String getIn(Tag tag) {
		
		// warns all the matchers that it accesses to a new start element
		int matcherIndex = 0;
		for (TagMatcher matcher : this.matchers) {
			// And sets the new value returned by the the matcher
			if (matcher != null ) {
				String value = matcher.getIn(tag);
				if (value != null) { 
					this.results[matcherIndex] = value;
				}
			}
			matcherIndex++;
		}
		
		super.getIn(tag);
		
		return null;
	}
	
	/**
	 * Goes through all the TagMatcher objects of the current object and call 
	 * their method getOut(...)
	 *   
	 * @param tagName
	 * 			The name of the end element that the object using the 
	 * MatcherManager is scanning.
	 * 
	 * @return null
	 */
	@Override
	public String getOut(String tagName) {
		
		int matcherIndex = 0;
		for (TagMatcher matcher : this.matchers) {
			if (matcher != null) {
				String value = matcher.getOut(tagName);
				if (value != null) {
					this.results[matcherIndex] = value;
				}
			}
			matcherIndex++;
		}
		
		String ret = super.getOut(tagName);
		if (ret != null) {
			return MatcherManager.GOT_RESULT;
		}
		return null;
	}
	
	/**
	 * Goes through all the TagMatcher objects of the current object and call 
	 * their method getValue(...)
	 *   
	 * @param value
	 * 			The value that the object using the MatcherManager is scanning.
	 * 
	 * @return null
	 */
	@Override
	public String getValue(String value) {
		
		int matcherIndex = 0;
		for (TagMatcher matcher : this.matchers) {
			if (matcher != null) {
				String resultsValue = matcher.getValue(value);
				if (resultsValue != null && !resultsValue.matches("\\s*")) {
					this.results[matcherIndex] = resultsValue;
				}
			}
			matcherIndex++;
		}
		
		if (value != null && !value.matches("\\s*")) {
			String ret = super.getValue("");
			return ret;
		}
		return null;
	}
	
	
	// ======================================================================== Protected
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic

	// ========================================================================== Private
	// --------------------------------------------------------------------------- Static
	// -------------------------------------------------------------------------- Dynamic
	
	public void defineCommonRoot() {
		
		for (TagMatcher matcher : this.matchers) {
			this.patternToMatch = defineCommonRoot(
											this.patternToMatch, 
											matcher.getPatternSequence());
		}
	}
	
	/**
	 * Defines the common root between two lists of TagPattern. Returns
	 * a list of TagPattern objects containing this common root. The 
	 * TagPattern object only have names and no attribute or position.
	 * If the parameters have a different root, returns an empty List.  
	 * 
	 * @param a1
	 * 			The first List.
	 * @param a2
	 * 			YThe second List.
	 * 
	 * @return a list of TagPattern objects containing this common root. 
	 * If the parameters have a different root, returns an empty List. 
	 * If one of the parameter is null, returns the other one (which can
	 * also be null).
	 */
	private ArrayList<TagPattern> defineCommonRoot(
										ArrayList<TagPattern> a1, 
										ArrayList<TagPattern> a2) {
		
		// ArrayList tto fill and return
		ArrayList<TagPattern> ret = new ArrayList<TagPattern>();
		
		// If one is null, returns the other
		if (a1 == null || a1.isEmpty()) {
			return a2;
		}
		if (a2 == null || a2.isEmpty()) {
			return a1;
		}
		
		// Checks common root
		int minSize = a1.size();
		int a2Size = a2.size();
		
		if (minSize > a2Size) {
			minSize = a2Size;
		}
		
		for (int i = 0; i < minSize; i++) {
			String name1 = a1.get(i).getName();
			String name2 = a2.get(i).getName();

			// if a Jocker is found, store the name value of the other Tag.
			if (name1.equals(TagPattern.JOCKER_NAME)) { 
				if (name2.equals(TagPattern.JOCKER_NAME)) {
					ret.add(TagPattern.JOCKER);
				}
				else {
					ret.add(new TagPattern(name2));
				}
			}
			else if (name2.equals(TagPattern.JOCKER_NAME)) { 
				ret.add(new TagPattern(name1)); 
			}
			else if (name1.equals(name2)) {
				ret.add(new TagPattern(name1));
			}
			else {
				return ret;
			}
		}
		return ret;
	}
}
